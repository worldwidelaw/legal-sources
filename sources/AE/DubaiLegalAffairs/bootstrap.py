#!/usr/bin/env python3
"""
AE/DubaiLegalAffairs -- Dubai Official Gazette via Legal Affairs Department

Downloads gazette PDFs from legal.dubai.gov.ae and extracts text with pdfplumber.
Each gazette issue becomes one record with the full text of all legislation it contains.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import json
import os
import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Tuple
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AE.DubaiLegalAffairs")

BASE_URL = "https://legal.dubai.gov.ae"
GAZETTE_PAGE = f"{BASE_URL}/en/Services/Pages/Official-Gazette.aspx"
DDL_NAME = "ctl00$ctl82$g_2cb19bc1_b4d0_42e3_8484_d28431570be2$ctl00$ddlyear"
DELAY = 2.0
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

SOURCE_DIR = Path(__file__).resolve().parent
DATA_DIR = SOURCE_DIR / "data"
SAMPLE_DIR = SOURCE_DIR / "sample"
SOURCE_ID = "AE/DubaiLegalAffairs"


def _parse_date(date_str: str) -> Optional[str]:
    """Parse date from DD/MM/YYYY or M/D/YYYY format to ISO 8601."""
    for fmt in ("%d/%m/%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _extract_hidden_fields(html: str) -> Dict[str, str]:
    """Extract all ASP.NET hidden form fields from HTML."""
    fields = {}
    for m in re.finditer(
        r'<input type="hidden" name="([^"]*)"[^>]*value="([^"]*)"', html
    ):
        fields[m.group(1)] = m.group(2)
    return fields


# The gazette filename, e.g. /OfficialGazette/2026/OGD-2026-779.pdf. The number
# the publisher puts in the file it serves is the authoritative issue number; the
# listing cell next to it is hand-typed and sometimes wrong (see below).
PDF_ISSUE_RE = re.compile(r"/OGD-\d{4}-0*(\d+)\.pdf$", re.IGNORECASE)

# Zero-width and bidi-control characters, literal or as numeric entities. This
# is a hand-edited table on an RTL site, so they land anywhere: three 2026 rows
# carry a U+200B *inside the date* ("15​/01/2026"), which is invisible in a
# browser and fatal to `(\d+/\d+/\d+)`.
_INVISIBLES_RE = re.compile(
    r"[​-‏‪-‮⁠﻿]"
    r"|&#(?:8203|8204|8205|8206|8207|65279);"
    r"|&#x(?:200[b-f]|202[a-e]|feff);",
    re.IGNORECASE,
)


def _parse_gazette_items(html: str) -> List[Dict[str, str]]:
    """Parse gazette items from page HTML. Returns list of {issue, date, url}."""
    # Strip the invisibles before matching, not per-field: they are noise from
    # the CMS editor and a row that carries one is otherwise dropped whole, with
    # no error — the crawl just comes back a few issues short.
    html = _INVISIBLES_RE.sub("", html)
    items = []
    # Match rows: <td>Issue No. 769</td><td>5/5/2026</td>...<a href="/OfficialGazette/...pdf"
    #
    # The issue cell tolerates trailing inline markup: at least one row ships as
    # `<td>Issue No. 775<br></td>`, and against a `</td>`-anchored pattern that
    # row simply did not match — the scan then resumed inside the *next* row, so
    # issue 775 was never downloaded and nothing said so. A listing parser that
    # drops a row on a stray <br> loses documents silently, which is the same
    # failure shape whatever the markup happens to be.
    pattern = re.compile(
        r'<td>Issue No\.\s*(\d+)\s*(?:<[^>]*>\s*)*</td>'
        r'<td>(\d+/\d+/\d+)</td>'
        r'.*?'
        r'href="(/OfficialGazette/[^"]+\.pdf)"',
        re.DOTALL,
    )
    for m in pattern.finditer(html):
        listed_num = int(m.group(1))
        date_str = m.group(2)
        pdf_path = unquote(m.group(3))

        # Prefer the number in the PDF filename. The listing mislabels at least
        # one row — OGD-2026-779.pdf sits under a cell reading "Issue No. 780",
        # directly below the real 780 — and since `_id` is built from the issue
        # number, trusting the cell collapsed two different gazettes onto one id:
        # 779 vanished and 780 was overwritten by it. Neither shows up as an
        # error, only as a corpus one document short.
        issue_num = listed_num
        fm = PDF_ISSUE_RE.search(pdf_path)
        if fm:
            issue_num = int(fm.group(1))
            if issue_num != listed_num:
                logger.warning(
                    "Listing says 'Issue No. %d' but the PDF is %s — using %d "
                    "from the filename",
                    listed_num,
                    pdf_path.rsplit("/", 1)[-1],
                    issue_num,
                )

        items.append({
            "issue": issue_num,
            "date": date_str,
            "pdf_url": f"{BASE_URL}{pdf_path}",
        })

    # The page links one gazette PDF per row, so parsed rows and gazette links
    # must come out equal. Every defect this parser has hit — a <br> in the
    # issue cell, a zero-width space in the date — showed up only as a quietly
    # shorter list, so compare the two and say which links were dropped.
    linked = {
        unquote(h)
        for h in re.findall(r'href="(/OfficialGazette/[^"]+\.pdf)"', html)
    }
    parsed = {i["pdf_url"][len(BASE_URL):] for i in items}
    if linked - parsed:
        logger.warning(
            "%d gazette PDFs on this page matched no table row (row markup "
            "changed?): %s",
            len(linked - parsed),
            ", ".join(sorted(p.rsplit("/", 1)[-1] for p in linked - parsed)[:10]),
        )

    return items


def _doc_id(issue_num: str) -> str:
    """The `_id` normalize() emits for a gazette issue.

    Shared with the extraction path so `extract_pdf_markdown` receives the same
    string that reaches Neon, which is what keeps its skip-if-already-stored
    guard live (issue #1480).
    """
    return f"AE-DubaiGazette-{issue_num}"


def _extract_pdf_text(pdf_bytes: bytes, doc_id: str) -> str:
    """Extract text from a gazette PDF via the shared helper.

    This used to call pdfplumber directly. pdfplumber emits glyphs in the order
    the content stream lists them, which for these Arabic gazettes is *visual*
    order, so every line was stored character-reversed — ``قانون`` as ``نوناق``
    (issue #1560) — on top of Arabic presentation-form glyphs that no keyword
    query spells. Search over such text matches nothing, and silently: the
    semantic half of a hybrid index still returns something.

    ``common.pdf_extract`` normalizes the shaped glyphs back to base letters and
    routes RTL-heavy output through ``common.arabic_pdf``, which orders glyph
    clusters by x-geometry rather than trusting the emitted sequence. The Latin
    lines in these same PDFs are not RTL and pass through untouched.

    ``force=True`` so a refresh re-extracts the issues already stored reversed
    instead of skipping them as present.
    """
    try:
        return (
            extract_pdf_markdown(
                SOURCE_ID,
                doc_id,
                pdf_bytes=pdf_bytes,
                table="legislation",
                force=True,
            )
            or ""
        )
    except Exception as e:
        logger.warning(f"PDF extraction error: {e}")
        return ""


class DubaiLegalAffairsScraper(BaseScraper):
    """Scraper for Dubai Official Gazette PDFs."""

    def __init__(self):
        # Initialize BaseScraper (loads config, sets source_dir/storage/status).
        # Without this the generic VPS runner crashes accessing self.config
        # (see issue #863).
        super().__init__()
        self.http = HttpClient(
            headers={"User-Agent": UA},
            timeout=60,
        )
        self._session = None

    def _get_session(self):
        """Get or create a requests session."""
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.headers.update({"User-Agent": UA})
        return self._session

    def _get_gazette_items_for_year(
        self, year: int, hidden_fields: Dict[str, str]
    ) -> Tuple[List[Dict[str, str]], Dict[str, str]]:
        """Fetch gazette items for a specific year via ASP.NET PostBack."""
        session = self._get_session()

        data = dict(hidden_fields)
        data["__EVENTTARGET"] = DDL_NAME
        data["__EVENTARGUMENT"] = ""
        data[DDL_NAME] = str(year)

        resp = session.post(GAZETTE_PAGE, data=data, timeout=30)
        resp.raise_for_status()
        html = resp.text

        items = _parse_gazette_items(html)
        new_fields = _extract_hidden_fields(html)

        return items, new_fields

    def _get_all_gazette_items(
        self, years: Optional[List[int]] = None
    ) -> Generator[Dict[str, str], None, None]:
        """Iterate gazette items across all years."""
        session = self._get_session()

        # Get initial page (loads 2026 by default)
        resp = session.get(GAZETTE_PAGE, timeout=30)
        resp.raise_for_status()
        html = resp.text

        hidden_fields = _extract_hidden_fields(html)
        default_items = _parse_gazette_items(html)

        if years is None:
            # Extract available years from dropdown
            years = sorted(
                [int(m) for m in re.findall(r'value="(\d{4})"', html)],
                reverse=True,
            )

        # Yield default year items (2026)
        if years and years[0] == 2026:
            for item in default_items:
                yield item
            years = years[1:]
            time.sleep(DELAY)

        # PostBack for each remaining year
        for year in years:
            try:
                items, hidden_fields = self._get_gazette_items_for_year(
                    year, hidden_fields
                )
                logger.info(f"Year {year}: {len(items)} gazette issues")
                for item in items:
                    yield item
                time.sleep(DELAY)
            except Exception as e:
                logger.warning(f"Failed to fetch year {year}: {e}")
                continue

    def _download_and_extract(self, pdf_url: str, doc_id: str) -> str:
        """Download PDF and extract text."""
        session = self._get_session()
        try:
            resp = session.get(pdf_url, timeout=120)
            resp.raise_for_status()
            return _extract_pdf_text(resp.content, doc_id)
        except Exception as e:
            logger.warning(f"Failed to download {pdf_url}: {e}")
            return ""

    def normalize(self, item: Dict, text: str) -> Dict:
        """Normalize a gazette item into a standard record."""
        iso_date = _parse_date(item["date"])
        issue_num = item["issue"]

        return {
            "_id": _doc_id(issue_num),
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"Dubai Official Gazette - Issue No. {issue_num}",
            "text": text,
            "date": iso_date,
            "url": item["pdf_url"],
            "issue_number": issue_num,
            "year": int(item["pdf_url"].split("/")[-2])
            if "/" in item["pdf_url"]
            else None,
            "language": "ar",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict, None, None]:
        """Fetch all gazette issues."""
        if sample:
            # For sample, just get recent issues from 2026
            years = [2026]
        else:
            years = None  # All years

        count = 0
        for item in self._get_all_gazette_items(years=years):
            logger.info(
                f"Downloading gazette issue #{item['issue']} ({item['date']})"
            )
            text = self._download_and_extract(item["pdf_url"], _doc_id(item["issue"]))
            if not text:
                logger.warning(
                    f"No text extracted from issue #{item['issue']}, skipping"
                )
                continue

            record = self.normalize(item, text)
            yield record
            count += 1

            if sample and count >= 15:
                break

            time.sleep(DELAY)

        logger.info(f"Total records fetched: {count}")

    def fetch_updates(self, since: str) -> Generator[Dict, None, None]:
        """Fetch gazette issues published since a date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        since_dt = datetime.fromisoformat(since)
        current_year = datetime.now().year

        # Check current year and previous year
        years = [current_year, current_year - 1]

        for item in self._get_all_gazette_items(years=years):
            iso_date = _parse_date(item["date"])
            if iso_date and iso_date >= since:
                text = self._download_and_extract(
                    item["pdf_url"], _doc_id(item["issue"])
                )
                if text:
                    yield self.normalize(item, text)
                time.sleep(DELAY)

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            session = self._get_session()
            resp = session.get(GAZETTE_PAGE, timeout=15)
            resp.raise_for_status()
            items = _parse_gazette_items(resp.text)
            logger.info(f"Test OK: found {len(items)} gazette issues on default page")
            return len(items) > 0
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="AE/DubaiLegalAffairs scraper")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Full fetch")
    args = parser.parse_args()

    scraper = DubaiLegalAffairsScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    elif args.command == "bootstrap":
        sample = args.sample and not args.full
        out_dir = SAMPLE_DIR if sample else DATA_DIR
        out_dir.mkdir(parents=True, exist_ok=True)

        records_file = out_dir / "records.jsonl"
        count = 0

        with open(records_file, "w", encoding="utf-8") as f:
            for record in scraper.fetch_all(sample=sample):
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
                logger.info(
                    f"[{count}] Issue #{record.get('issue_number')} "
                    f"({len(record.get('text', ''))} chars)"
                )

                # Also write individual sample files
                if sample:
                    sample_file = out_dir / f"{record['_id']}.json"
                    with open(sample_file, "w", encoding="utf-8") as sf:
                        json.dump(record, sf, indent=2, ensure_ascii=False)

        logger.info(f"Done. {count} records written to {records_file}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
