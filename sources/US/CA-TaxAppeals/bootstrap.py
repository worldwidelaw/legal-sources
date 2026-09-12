#!/usr/bin/env python3
"""
US/CA-TaxAppeals -- California Office of Tax Appeals (Precedential Opinions)

Fetches the full text of California's precedential tax-appeal opinions
published by the Office of Tax Appeals (OTA) at ota.ca.gov/opinions/.
The corpus is dominated by the legacy **State Board of Equalization
(SBE)** precedential opinions (which remain binding precedent before the
OTA) plus OTA's own precedential opinions. Each opinion resolves a tax
controversy between a taxpayer/appellant and the Franchise Tax Board or
the CDTFA, so the corpus is case_law.

The opinions are published openly as born-digital, text-layer PDFs. No
JS needed, no CAPTCHA, no auth. They live in TWO places:

  * The legacy SBE archive — one server-rendered TablePress listing on
    /opinions/ holding ~4,087 `{YY}-SBE-{NNN}` opinion PDFs adopted
    1930-2015. The DataTables widget only paginates client-side, so
    every row is already in the HTML.
  * OTA's own opinions, 2018-present — NOT on /opinions/. They are split
    across per-year, per-tax-programme pages linked from that index
    (/{YYYY}-franchise-income-tax-opinions/, /{YYYY}-business-tax-opinions/
    and their `precedential` variants; 2018-2022 use older slugs such as
    /2018-opinions/ and /2021-fit-opinions/). Crawling only /opinions/ is
    what left this source frozen at 2015 (issue #1504).

Strategy:
  1. GET the /opinions/ HTML page (one request). It yields both the
     legacy PDF rows and the links to every OTA year page.
  2. GET each year page and collect its opinion PDFs.
  3. Collect every opinion PDF link (filter out admin docs / errata
     notices), dedup by URL.
  4. Parse the opinion number ({YY}-SBE-{NNN}) from legacy filenames; OTA
     opinions carry their `{YYYY}-OTA-{NNN}` citation in the PDF text.
  5. Download each PDF and extract its text layer via common.pdf_extract.
  6. Derive the appellant name and decision date from the document text.
  7. Normalize into the standard case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-TaxAppeals")

BASE_URL = "https://ota.ca.gov"
INDEX_URL = BASE_URL + "/opinions/"

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
MONTH_ALT = "|".join(MONTHS)
# "Done at Sacramento ... this 24th day of January, 1990"
DAY_OF_DATE_RE = re.compile(
    rf"\b(\d{{1,2}})(?:st|nd|rd|th)?\s+day\s+of\s+({MONTH_ALT}),?\s+(\d{{4}})",
    re.I,
)
# Plain "Month DD, YYYY"
TEXT_DATE_RE = re.compile(rf"\b({MONTH_ALT})\s+(\d{{1,2}}),\s+(\d{{4}})", re.I)
PDF_HREF_RE = re.compile(r'href="([^"]+/wp-content/uploads[^"]+\.pdf)"', re.I)
# {YY}-SBE-{NNN}[-a]  /  {YY}-OTA-{NNN}[-a]  (any of -, _, or none separators).
# The optional trailing letter is ONLY a real amendment marker (a/b) when it
# is a delimited single letter — not the first letter of a party name that
# follows in the filename (e.g. "06-SBE-003-Deluxe_FO").
OPNUM_RE = re.compile(
    r"(\d{2})[-_]?(sbe|ota)[-_]?(\d+)(?:[-_]([ab])(?=[._-]|$))?", re.I)
# Admin / non-opinion documents to skip.
SKIP_RE = re.compile(r"(org-chart|high-level|errata|agenda|minutes|"
                     r"meeting|notice-of|fact-sheet|brochure)", re.I)

# Links from /opinions/ to OTA's per-year opinion listings, e.g.
# /2024-franchise-income-tax-opinions/, /2021-fit-opinions/, /2018-opinions/,
# /all-precedential-business-tax-opinions/.
YEAR_PAGE_RE = re.compile(
    r'href="(https?://ota\.ca\.gov/[^"]*opinion[^"]*?)"', re.I)
# WordPress upload path carries the publication year/month of every OTA PDF.
UPLOAD_PATH_RE = re.compile(r"/uploads/(?:sites/\d+/)?(\d{4})/(\d{2})/")
# Header citation printed on every OTA opinion: "2024 – OTA – 010SCP".
OTA_CITE_RE = re.compile(
    r"\b(20\d{2})\s*[-‐-―]\s*OTA\s*[-‐-―]\s*(\d+)([A-Z]{0,4})\b")
# "OTA Case No. 22039832" / "OTA Case No.: 240415805" (sometimes several).
CASE_NO_RE = re.compile(
    r"OTA\s+Case\s+(?:Nos?|ID)[.:]*\s*([\d]{5,}(?:\s*(?:,|and)\s*[\d]{5,})*)", re.I)
# The signature block stamps "Date Issued: 6/4/2026". The stamp is a form field
# flattened into the page, so extraction often splits a digit run with a stray
# space ("6 /4/2026", "8/1/202 5") and sometimes emits it just ABOVE the label —
# both shapes have to be tolerated or the date silently falls through to an
# unrelated date from the facts.
_D = r"([\d\s]{1,6}?)"
DATE_ISSUED_AFTER_RE = re.compile(
    rf"Date\s+Issued\s*:?\s*{_D}/{_D}/{_D}(?![\d/])", re.I)
DATE_ISSUED_RE = re.compile(
    rf"{_D}/{_D}/{_D}\s*\n?\s*Date\s+Issued", re.I)
# Early (2018-2019) opinions spell it out — and the day is a form field that
# usually extracts blank: "Date Issued: March , 2018".
DATE_ISSUED_TEXT_RE = re.compile(
    rf"Date\s+Issued\s*:?\s*({MONTH_ALT})\s*(\d{{1,2}})?\s*,?\s*(\d{{4}})", re.I)
# Docket / account references that sit in the caption column beside the party.
CAPTION_NOISE_RE = re.compile(
    r"(?:\b(?:OTA|CDTFA|FTB|BOE)\s+(?:Case|Acct\.?|Account)\s+(?:Nos?|IDs?)?[.:]*"
    r"[\s\d.:-]*)|(?:Date\s+Issued\s*:?[^,]{0,20},?\s*\d{4})|_{3,}", re.I)
PRECEDENTIAL_RE = re.compile(r"\bnon-?precedential\b", re.I)


class CATaxAppealsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_text(self, url: str) -> str | None:
        out = self._curl_bytes(url)
        return out.decode("utf-8", "replace") if out else None

    def _curl_bytes(self, url: str) -> bytes | None:
        """Fetch raw bytes via the curl CLI with a browser UA."""
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @classmethod
    def _opinion_number(cls, pdf_url: str) -> tuple[str | None, int | None]:
        """Return (normalized opinion number, 4-digit year) from the filename,
        e.g. '90_sbe_001_a.pdf' -> ('90-SBE-001-A', 1990)."""
        name = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        m = OPNUM_RE.search(name)
        if not m:
            return None, None
        yy, body, num, suffix = m.group(1), m.group(2).upper(), m.group(3), m.group(4)
        number = f"{yy}-{body}-{int(num):03d}"
        if suffix:
            number += f"-{suffix.upper()}"
        yy_i = int(yy)
        year = 2000 + yy_i if yy_i <= 26 else 1900 + yy_i
        return number, year

    @staticmethod
    def _slug(pdf_url: str, era: str = "sbe") -> str:
        """Stable per-document id fragment.

        The legacy SBE archive keys on the bare filename (its opinion numbers
        are already unique) — that scheme is frozen so previously ingested
        rows keep their `_id`. OTA-era filenames are NOT unique: 17 of them
        repeat across upload folders (e.g. two different `J.-Parker.pdf`),
        which would collapse distinct opinions onto one key, so those are
        prefixed with the `/uploads/YYYY/MM/` folder from the URL.
        """
        stem = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")[:180]
        if era == "sbe":
            return stem
        m = UPLOAD_PATH_RE.search(pdf_url)
        return f"{m.group(1)}-{m.group(2)}-{stem}"[:200] if m else stem

    @staticmethod
    def _parse_appellant(text: str) -> str | None:
        """Pull the appellant out of the 'In the Matter of the Appeal[s] of
        [No. ...] <NAME>' caption near the top of the opinion. Newer opinions
        carry interleaved pleading line-numbers (lines that are just digits),
        which are dropped before matching."""
        head_lines = text[:1500].splitlines()
        kept = [ln for ln in head_lines if not re.fullmatch(r"\s*\d{1,3}\s*", ln)]
        flat = re.sub(r"\s+", " ", " ".join(kept))
        m = re.search(
            r"Appeals?\s+of\s*:?\s*(?:No\.?\s*\S+\s+)?(.+?)\s+"
            r"(?:Appearances|OPINION|For\s+Appellant|Representing|This\s+appeal)",
            flat, re.I,
        )
        if not m:
            return None
        name = m.group(1)
        # OTA captions interleave the docket column with the party column:
        # "Appeal of: ) OTA Case No. 22039832 ) COMPNOVA LLC ) ) )". Drop the
        # docket reference so only the party name survives.
        name = CASE_NO_RE.sub(" ", name)
        name = CAPTION_NOISE_RE.sub(" ", name)
        # Strip inline pleading line-numbers (1-3 digit standalone tokens) and
        # column-separator parens that pdfplumber leaves in the caption.
        name = re.sub(r"\b\d{1,3}\b", " ", name)
        name = re.sub(r"[)(]+", " ", name)
        name = re.sub(r"\s+", " ", name).strip(" ,.")
        # Reject right-column boilerplate that leaked in (FORMAL OPINION, etc).
        if re.search(r"\b(FORMAL|OPINION|MEMORANDUM|DECISION)\b", name, re.I):
            return None
        if not name or len(name) < 3 or len(name) > 200:
            return None
        return name

    @classmethod
    def _parse_date(cls, text: str, fallback_year: int | None,
                    ota: bool = False, upload_ym: str | None = None) -> str | None:
        """Derive the decision date. Prefer OTA's explicit 'Date Issued'
        stamp, then the SBE-era 'this Nth day of Month, YYYY' clause near the
        end, then any 'Month DD, YYYY', then the filename year.

        The 'Date Issued' pass matters for the OTA era: those opinions cite
        many other dates in the facts, so falling through to the last
        'Month DD, YYYY' in the body picks an unrelated one.
        """
        tail = text[-2500:]
        for rx in (DATE_ISSUED_AFTER_RE, DATE_ISSUED_RE):
            for dm in list(rx.finditer(tail)) + list(rx.finditer(text)):
                try:
                    mo, day, yr = (int(re.sub(r"\s+", "", g))
                                   for g in dm.groups())
                except ValueError:
                    continue
                if 1930 <= yr <= 2035 and 1 <= mo <= 12 and 1 <= day <= 31:
                    return f"{yr:04d}-{mo:02d}-{day:02d}"
        tm = DATE_ISSUED_TEXT_RE.search(tail) or DATE_ISSUED_TEXT_RE.search(text)
        if tm:
            mo, day, yr = MONTHS[tm.group(1).lower()], tm.group(2), int(tm.group(3))
            day = int(day) if day else 1
            if 1930 <= yr <= 2035 and 1 <= day <= 31:
                return f"{yr:04d}-{mo:02d}-{day:02d}"
        if ota:
            # An OTA opinion cites many dates in its facts, so guessing from the
            # body is worse than using the month it was published under.
            return f"{upload_ym}-01" if upload_ym else None
        m = DAY_OF_DATE_RE.search(tail) or DAY_OF_DATE_RE.search(text)
        if m:
            day, mo_name, yr = int(m.group(1)), m.group(2).lower(), int(m.group(3))
            mo = MONTHS[mo_name]
            if 1930 <= yr <= 2035 and 1 <= day <= 31:
                return f"{yr:04d}-{mo:02d}-{day:02d}"
        dates = TEXT_DATE_RE.findall(text)
        if dates:
            mo_name, day, yr = dates[-1]
            mo, day, yr = MONTHS[mo_name.lower()], int(day), int(yr)
            if 1930 <= yr <= 2035 and 1 <= day <= 31:
                return f"{yr:04d}-{mo:02d}-{day:02d}"
        if fallback_year:
            return f"{fallback_year:04d}-01-01"
        return None

    @staticmethod
    def _year_pages(html: str) -> list[str]:
        """OTA year-listing URLs linked from the /opinions/ index."""
        pages = set()
        for href in YEAR_PAGE_RE.findall(html):
            href = href.split("#", 1)[0].replace("http://", "https://")
            if href.lower().endswith(".pdf") or "wp-json" in href:
                continue
            href = href.rstrip("/") + "/"
            if href == INDEX_URL:
                continue
            pages.add(href)
        # Newest year first; the undated /all-precedential-*/ pages come last.
        def key(u):
            m = re.search(r"/(\d{4})-", u)
            return (int(m.group(1)) if m else 0, u)
        return sorted(pages, key=key, reverse=True)

    def _page_pdfs(self, page_url: str, html: str) -> list[str]:
        out, seen = [], set()
        for href in PDF_HREF_RE.findall(html):
            full = urllib.parse.urljoin(page_url, href).split("#", 1)[0]
            if full in seen:
                continue
            seen.add(full)
            if SKIP_RE.search(full.rsplit("/", 1)[-1]):
                continue
            out.append(full)
        return out

    def discover_opinions(self, sample: bool = False,
                          min_year: int | None = None) -> list:
        """Return ordered (slug, number, year, pdf_url) tuples for every
        opinion PDF — the legacy SBE archive on /opinions/ plus OTA's own
        per-year listings — newest first.

        `min_year` skips OTA year pages older than that year (used by the
        incremental refresh lane so it does not re-crawl the whole corpus).
        """
        html = self._curl_text(INDEX_URL)
        if not html:
            raise RuntimeError(f"Failed to fetch opinions index {INDEX_URL} "
                               f"— cannot enumerate the corpus")
        seen: set[str] = set()
        out = []

        # 1. Legacy State Board of Equalization archive (1930-2015).
        legacy = 0
        for full in self._page_pdfs(INDEX_URL, html):
            number, year = self._opinion_number(full)
            if number is None:
                # Not an opinion-numbered PDF — skip admin docs.
                continue
            if full in seen:
                continue
            seen.add(full)
            if min_year and year and year < min_year:
                continue
            out.append((self._slug(full, "sbe"), number, year, full))
            legacy += 1

        # 2. OTA's own opinions, 2018-present, one page per year+programme.
        pages = self._year_pages(html)
        if not pages:
            raise RuntimeError(
                f"No OTA year-listing pages found on {INDEX_URL} — the index "
                f"layout changed; refusing to report a 2015-capped corpus")
        ota = 0
        for page in pages:
            py = re.search(r"/(\d{4})-", page)
            if min_year and py and int(py.group(1)) < min_year:
                continue
            page_html = self._curl_text(page)
            if not page_html:
                logger.warning(f"Failed to fetch year page {page}")
                continue
            found = 0
            for full in self._page_pdfs(page, page_html):
                if full in seen:
                    continue
                seen.add(full)
                pm = UPLOAD_PATH_RE.search(full)
                year = int(pm.group(1)) if pm else (
                    int(py.group(1)) if py else None)
                if min_year and year and year < min_year:
                    continue
                out.append((self._slug(full, "ota"), None, year, full))
                found += 1
            ota += found
            logger.info(f"  {found:5d} new opinion PDFs on {page}")

        out.sort(key=lambda t: (t[2] or 0, t[0], t[3]), reverse=True)
        # A handful of legacy opinions were re-uploaded to a second folder under
        # the same opinion number; keep one so we don't fetch the same PDF twice.
        deduped, by_slug = [], set()
        for row in out:
            if row[0] in by_slug:
                continue
            by_slug.add(row[0])
            deduped.append(row)
        logger.info(f"Discovered {len(deduped)} opinion PDFs "
                    f"({legacy} legacy SBE, {ota} OTA over {len(pages)} year "
                    f"pages, {len(out) - len(deduped)} re-uploads dropped)")
        return deduped

    def _build_raw(self, slug: str, number: str | None, year: int | None,
                   pdf_url: str) -> dict | None:
        pdf_bytes = self._curl_bytes(pdf_url)
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {pdf_url}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            pdf_url, "US/CA-TaxAppeals", pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {pdf_url} "
                           f"({len(text) if text else 0} chars)")
            return None
        head = text[:2000]
        cite = OTA_CITE_RE.search(head)
        if cite:
            # "2024 – OTA – 010SCP" -> "2024-OTA-010SCP" (the official citation).
            number = f"{cite.group(1)}-OTA-{cite.group(2)}{cite.group(3).upper()}"
        case_no = CASE_NO_RE.search(head)
        is_ota = bool(cite or case_no)
        pm = UPLOAD_PATH_RE.search(pdf_url)
        return {
            "slug": slug,
            "opinion_number": number,
            "case_number": case_no.group(1).strip() if case_no else None,
            "precedential": (None if not cite
                             else not PRECEDENTIAL_RE.search(head)),
            "appellant": self._parse_appellant(text),
            "text": text.strip(),
            "url": pdf_url,
            "date": self._parse_date(
                text, year, ota=is_ota,
                upload_ym=f"{pm.group(1)}-{pm.group(2)}" if pm else None),
        }

    def test_api(self) -> bool:
        logger.info("Testing CA Office of Tax Appeals index + PDF extraction...")
        try:
            ops = self.discover_opinions(sample=True)
            if not ops:
                logger.error("  No opinions discovered")
                return False
            logger.info(f"  Discovered {len(ops)} opinions")
            raw = self._build_raw(*ops[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars)")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        number = raw.get("opinion_number")
        appellant = (raw.get("appellant") or "").strip()
        is_ota = bool(raw.get("case_number")) or bool(number and "OTA" in number)
        body = "Office of Tax Appeals" if is_ota else "State Board of Equalization"
        if appellant:
            title = f"Appeal of {appellant}"
        elif number:
            title = f"California Tax Appeal Opinion {number}"
        else:
            title = "California Tax Appeal Opinion"
        if number:
            title += f" ({number})"
        title = title[:300]
        return {
            "_id": f"US/CA-TaxAppeals/{raw['slug']}",
            "_source": "US/CA-TaxAppeals",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "opinion_number": number,
            "case_number": raw.get("case_number"),
            "precedential": raw.get("precedential"),
            "court": f"California {body}",
            "appellant": appellant or None,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False,
                  min_year: int | None = None) -> Generator[dict, None, None]:
        ops = self.discover_opinions(sample=sample, min_year=min_year)
        if sample:
            # Cover both eras: the newest OTA opinions and some legacy SBE ones.
            legacy = [o for o in ops if o[1] and "SBE" in o[1]]
            ops = ops[:10] + legacy[:6]
        emitted = 0
        for slug, number, year, pdf_url in ops:
            raw = self._build_raw(slug, number, year, pdf_url)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 16:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Incremental refresh.

        OTA files each opinion under a year page, so a `since` date lets us
        skip every earlier year page instead of re-downloading the whole
        ~8,200-PDF corpus to discard it at the date filter.
        """
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        min_year = None
        m = re.match(r"(\d{4})", since or "")
        if m:
            # -1 for opinions issued in December but published the next year.
            min_year = int(m.group(1)) - 1
        for raw in self._iter_raw(min_year=min_year):
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CA-TaxAppeals bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--since", help="ISO date for `update`")
    args = parser.parse_args()

    scraper = CATaxAppealsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    if args.command == "update":
        count = 0
        for raw in scraper.fetch_updates(args.since or ""):
            record = scraper.normalize(raw)
            scraper.storage.write(scraper._dedup_key(record), record)
            count += 1
        logger.info(f"Update complete: {count} records")
        sys.exit(0)

    if args.sample and not args.full:
        # Sample mode writes reviewable JSON files next to the scraper.
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for raw in scraper.fetch_sample():
            record = scraper.normalize(raw)
            safe_id = record["_id"].replace("/", "_")
            with open(sample_dir / f"{safe_id}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")
        logger.info(f"Sample complete: {count} records saved to {sample_dir}")
        sys.exit(0 if count else 1)

    # Full runs go through BaseScraper so records stream to data/records.jsonl
    # with dedup + validation instead of being dumped into sample/ (#798 class).
    stats = scraper.bootstrap()
    logger.info(f"Bootstrap complete: {stats}")
    if not stats.get("records_fetched"):
        logger.error("No records fetched — see the log above for the cause")
        sys.exit(1)


if __name__ == "__main__":
    main()
