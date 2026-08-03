#!/usr/bin/env python3
"""
US/TX-AGOpinions -- Texas Attorney General Opinions

Fetches the full text of written opinions issued by the Texas Attorney
General under Tex. Gov't Code ch. 402. Each opinion is the AG's
authoritative interpretation of Texas law issued at the request of an
authorized public official (doctrine). The corpus spans the lettered
series by AG -- current KP- (Paxton, 2015-present), GA- (Abbott
2002-15), JC- (Cornyn 1999-2002), DM- (Morales 1991-99) and the older
letter series back to O-0001 (Gerald Mann, 1939).

Strategy (official site texasattorneygeneral.gov, Drupal):
  1. The landing page /opinions links one page per Attorney General:
     /opinions/{ag-slug} (e.g. /opinions/ken-paxton).
  2. Each AG page embeds every one of that AG's opinions as options of
     per-year <select name="opinion_selectYYYY"> dropdowns. Each option
     carries the year (from the select name), the Drupal node path
     (/node/{id}) and the opinion number text (e.g. "KP-0520").
  3. Fetch /node/{id} -> the detail page carries the born-digital PDF
     href (/sites/default/files/opinion-files/opinion/{YEAR}/{file}.pdf;
     the file-name format varies per series so it is read from the page,
     not guessed) plus a "Summary" abstract.
  4. Download the PDF and extract the full opinion text (born-digital
     text layer; older scans were pre-OCR'd by the AG office). Parse the
     issue date and the "Re:" subject line from the body.

Iterated newest AG / newest year / newest number first, so samples come
from the clean modern PDFs.

Usage:
  python bootstrap.py bootstrap            # Full pull (all AGs, all years)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import fitz  # PyMuPDF
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-AGOpinions")

BASE_URL = "https://www.texasattorneygeneral.gov"

# Attorney General index slugs, newest term first. Discovered dynamically
# from /opinions, but ordered here so the sample pulls modern clean PDFs.
AG_ORDER = [
    "ken-paxton",        # 2015-present  (KP-)
    "angela-colmenero",  # 2023 interim
    "john-scott",        # 2021-2022 interim
    "greg-abbott",       # 2002-2015     (GA-)
    "john-cornyn",       # 1999-2002     (JC-)
    "dan-morales",       # 1991-1999     (DM-)
    "jim-mattox",        # 1983-1991
    "mark-white",        # 1979-1983
    "john-hill",         # 1973-1979
    "crawford-martin",   # 1967-1973
    "waggoner-carr",     # 1963-1967
    "will-wilson",       # 1957-1963
    "john-ben-shepperd", # 1953-1957
    "price-daniel",      # 1947-1953
    "grover-sellers",    # 1944-1947
    "gerald-mann",       # 1939-1944     (O-)
]

PDF_HREF_RE = re.compile(
    r'href="(/sites/default/files/opinion-files/[^"]+?\.pdf)"', re.IGNORECASE
)
SELECT_RE = re.compile(
    r'<select[^>]*name="opinion_select(\d{4})"[^>]*>(.*?)</select>', re.S | re.I
)
OPTION_RE = re.compile(
    r'<option[^>]*value="(/node/\d+)"[^>]*>(.*?)</option>', re.S | re.I
)
MONTHS = ("January|February|March|April|May|June|July|August|September|"
          "October|November|December")
DATE_RE = re.compile(rf"\b({MONTHS})\s+(\d{{1,2}}),\s+(\d{{4}})\b")
MONTH_NUM = {m: i for i, m in enumerate(
    "January February March April May June July August September October "
    "November December".split(), start=1)}


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_body_date(text: str) -> str | None:
    """Return the first 'Month DD, YYYY' in the opinion body as ISO date."""
    m = DATE_RE.search(text[:2000])
    if not m:
        return None
    month, day, year = m.group(1), int(m.group(2)), int(m.group(3))
    try:
        return f"{int(year):04d}-{MONTH_NUM[month]:02d}-{day:02d}"
    except Exception:
        return None


def parse_subject(text: str) -> str:
    """Extract the 'Re: ...' subject line from the opinion body."""
    m = re.search(r"\bRe:\s*(.+?)(?:\n\s*\n|\bOpinion No\b|\bDear\b)",
                  text, re.S | re.I)
    if not m:
        return ""
    subj = re.sub(r"\s+", " ", m.group(1)).strip(" .")
    return subj[:300]


class TXAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=90,
        )
        self.delay = 1.0

    def _get(self, url: str, retries: int = 4) -> bytes | None:
        """Fetch a URL (bytes) with rate limiting and retry/backoff."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code == 404:
                    logger.debug(f"404: {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_text(self, url: str, retries: int = 4) -> str:
        data = self._get(url, retries=retries)
        return data.decode("utf-8", "replace") if data else ""

    def discover_ags(self) -> list:
        """Return AG index slugs, ordered newest-term first."""
        html = self._get_text(f"{BASE_URL}/opinions")
        found = set(re.findall(r'/opinions/([a-z][a-z\-]+)', html or ""))
        found -= {"categories", "opinions-overruled-modified-affirmed-withdrawn"}
        ordered = [s for s in AG_ORDER if s in found]
        # append any AG slug not in our static order (defensive)
        for s in sorted(found):
            if s not in ordered and s not in ("how-request-attorney-general-opinion",):
                # only keep slugs that look like a person name (has a dash)
                if "-" in s:
                    ordered.append(s)
        return ordered or list(AG_ORDER)

    def parse_ag_page(self, html: str) -> list:
        """Parse an AG page into [(year, node_path, number), ...] newest first."""
        items = []
        seen = set()
        for year, body in SELECT_RE.findall(html or ""):
            for node, label in OPTION_RE.findall(body):
                num = re.sub(r"\s+", " ",
                             re.sub(r"<[^>]+>", " ", label)).strip()
                if not num or num.lower().startswith("- select"):
                    continue
                if node in seen:
                    continue
                seen.add(node)
                items.append((int(year), node, num))
        # newest year first, then by descending node id (proxy for recency)
        items.sort(key=lambda t: (t[0], int(t[1].rsplit("/", 1)[-1])),
                   reverse=True)
        return items

    def extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract full text from a born-digital opinion PDF."""
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as e:
            logger.warning(f"PDF open failed: {e}")
            return ""
        parts = []
        try:
            for page in doc:
                parts.append(page.get_text())
        finally:
            doc.close()
        return clean_text("\n".join(parts))

    def _build_raw(self, ag: str, year: int, node: str, number: str) -> dict | None:
        """Fetch a single opinion's detail page + PDF into a raw record."""
        detail = self._get_text(f"{BASE_URL}{node}")
        if not detail:
            return None
        m = PDF_HREF_RE.search(detail)
        if not m:
            logger.debug(f"No PDF on {node} ({number})")
            return None
        pdf_url = BASE_URL + m.group(1)

        summary = ""
        soup = BeautifulSoup(detail, "html.parser")
        idx = detail.find("Summary")
        if idx != -1:
            seg = re.sub(r"<[^>]+>", " ", detail[idx + len("Summary"):idx + 1600])
            seg = re.sub(r"\s+", " ", seg).strip()
            # cut off trailing page chrome ("Opinion File ...", "RQ-...", etc.)
            seg = re.split(r"\b(?:Opinion File|Request File|RQ-\d)", seg)[0]
            summary = seg.strip()[:600].rstrip(" .")

        pdf_bytes = self._get(pdf_url)
        if not pdf_bytes:
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(f"Short/empty text for {number} ({len(text) if text else 0})")
            return None

        return {
            "opinion_number": number,
            "ag_slug": ag,
            "year": year,
            "node": node,
            "summary": summary,
            "subject": parse_subject(text),
            "date": parse_body_date(text),
            "text": text,
            "pdf_url": pdf_url,
            "url": f"{BASE_URL}{node}",
        }

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        number = raw["opinion_number"]
        subject = (raw.get("subject") or "").strip()
        title = f"Texas AG Opinion {number}"
        if subject:
            short = subject if len(subject) <= 200 else subject[:197] + "..."
            title = f"{title} — {short}"
        node_id = raw["node"].rsplit("/", 1)[-1]
        date = raw.get("date")
        if not date and raw.get("year"):
            date = f"{int(raw['year']):04d}-01-01"
        return {
            "_id": f"US/TX-AGOpinions/{number}",
            "_source": "US/TX-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": title,
            "summary": raw.get("summary") or None,
            "text": raw["text"],
            "url": raw["url"],
            "pdf_url": raw.get("pdf_url"),
            "date": date,
            "node_id": node_id,
        }

    def test_api(self) -> bool:
        """Test connectivity: enumerate one AG page and pull one opinion."""
        logger.info("Testing TX AG opinions index...")
        try:
            html = self._get_text(f"{BASE_URL}/opinions/ken-paxton")
            items = self.parse_ag_page(html)
            if not items:
                logger.error("  AG page parse returned no opinions")
                return False
            logger.info(f"  AG page parse OK ({len(items)} Paxton opinions)")

            year, node, number = items[0]
            raw = self._build_raw("ken-paxton", year, node, number)
            if raw and len(raw["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(raw['text'])} chars, "
                    f"{number}, date={raw.get('date')})"
                )
            else:
                logger.error("  Full-text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Iterate opinion records, newest AG / year / number first."""
        emitted = 0
        seen = set()
        for ag in self.discover_ags():
            html = self._get_text(f"{BASE_URL}/opinions/{ag}")
            items = self.parse_ag_page(html)
            if items:
                logger.info(f"AG {ag}: {len(items)} opinions")
            for year, node, number in items:
                if number in seen:
                    continue
                seen.add(number)
                try:
                    raw = self._build_raw(ag, year, node, number)
                except Exception as e:
                    logger.warning(f"Error building {number}: {e}")
                    raw = None
                if not raw:
                    continue
                yield self.normalize(raw)
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch opinions issued on/after `since` (ISO date)."""
        for record in self.fetch_all():
            if not since or (record.get("date") and record["date"] >= since):
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/TX-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TXAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for record in gen:
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
