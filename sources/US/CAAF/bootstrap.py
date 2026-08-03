#!/usr/bin/env python3
"""
US/CAAF -- United States Court of Appeals for the Armed Forces

Fetches the full text of the precedential opinions of the U.S. Court of Appeals
for the Armed Forces (CAAF), the Article I civilian appellate court that sits
atop the military-justice system and reviews the decisions of the four service
Courts of Criminal Appeals (Army, Navy-Marine Corps, Air Force, Coast Guard).
Each opinion decides a specific court-martial appeal, so the corpus is
`case_law`. CAAF opinions are federal-government works in the public domain
(17 U.S.C. § 105).

Access (no JavaScript, no CAPTCHA, no auth):
  The court publishes every opinion as a born-digital PDF on its website:

      https://www.armfor.uscourts.gov/newcaaf/opinions.htm

  Flow:
    1. GET /opinions.htm -> lists every "term of court" index page
       /opinions/{term}.htm (e.g. 2023OctTerm, 2015SepTerm).
    2. GET /opinions/{term}.htm -> an HTML table with columns
       CASE NAME | DOCKET # | OPINION DATE | MJ CITATION; the docket cell
       links the opinion PDF at /opinions/{term}/{docket}.pdf (filename is
       the docket digits, e.g. 240093.pdf == docket 24-0093).
    3. GET /opinions/{term}/{docket}.pdf -> the opinion PDF (full text).

  Metadata (case name, docket, decision date, Military Justice Reporter
  citation) is parsed from the term-table row; the full text is read from the
  PDF. Digitised coverage runs ~2001 term to present; pre-2001 term pages
  carry no PDF links and yield nothing.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CAAF")

BASE_URL = "https://www.armfor.uscourts.gov/newcaaf"
INDEX_URL = f"{BASE_URL}/opinions.htm"

MIN_TEXT_CHARS = 400

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

TERM_LINK_RE = re.compile(r'href="(opinions/[^"]+\.htm)"', re.I)
DOCKET_RE = re.compile(r'\b(\d{2}-\d{3,4}/[A-Z]{2})\b')
CITATION_RE = re.compile(r'\b(\d{1,3}\s+M\.?\s?J\.?\s+\d{1,4})\b')
DATE_RE = re.compile(
    r'\b('
    r'jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|'
    r'jul(?:y)?|aug(?:ust)?|sep(?:t)?(?:ember)?|oct(?:ober)?|nov(?:ember)?|'
    r'dec(?:ember)?)\.?\s+(\d{1,2}),?\s+(\d{4})\b',
    re.I,
)
_MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05",
    "jun": "06", "jul": "07", "aug": "08", "sep": "09", "oct": "10",
    "nov": "11", "dec": "12",
}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_date(s: str) -> str | None:
    if not s:
        return None
    m = DATE_RE.search(s)
    if not m:
        return None
    mon = _MONTHS.get(m.group(1)[:3].lower())
    if not mon:
        return None
    try:
        d, y = int(m.group(2)), int(m.group(3))
    except ValueError:
        return None
    if 1950 <= y <= 2100 and 1 <= d <= 31:
        return f"{y}-{mon}-{d:02d}"
    return None


def _norm_citation(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("M. J.", "MJ").replace("M.J.", "MJ").replace("M J", "MJ")
    return s or None


class CAAFScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": BROWSER_UA,
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get_text(self, url: str, retries: int = 3) -> str | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    ctype = (resp.headers.get("Content-Type") or "").lower()
                    if "pdf" in ctype or resp.content[:5] == b"%PDF-":
                        return resp.content
                    logger.warning(f"Non-PDF content ({ctype}) for {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def _term_pages(self) -> list[str]:
        html = self._get_text(INDEX_URL)
        if not html:
            return []
        seen, out = set(), []
        for rel in TERM_LINK_RE.findall(html):
            rel = rel.strip()
            # skip the index page itself
            if rel.lower().endswith("opinions.htm"):
                continue
            url = f"{BASE_URL}/{rel}"
            if url not in seen:
                seen.add(url)
                out.append(url)
        return out

    def _parse_term(self, term_url: str) -> list[dict]:
        """Parse a term index page into per-opinion descriptors."""
        html = self._get_text(term_url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        term_slug = term_url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        out = []
        seen_urls = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href.lower().endswith(".pdf"):
                continue
            pdf_url = urllib.parse.urljoin(term_url, href)
            if "/opinions/" not in pdf_url.lower():
                continue
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)
            # The row holding this link carries the metadata cells.
            row = a.find_parent("tr")
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])] if row else []
            row_text = " | ".join(cells)
            dm = DOCKET_RE.search(row_text)
            cm = CITATION_RE.search(row_text)
            dt = _parse_date(row_text)
            # Case name = first cell that isn't docket/date/citation noise.
            case_name = ""
            for c in cells:
                cl = c.strip()
                if not cl or cl.upper() in ("PDF",):
                    continue
                if DOCKET_RE.search(cl) or CITATION_RE.fullmatch(cl.replace(".", "").strip()):
                    continue
                if _parse_date(cl) and len(cl) < 25:
                    continue
                case_name = cl
                break
            case_name = re.sub(r"\s+", " ", case_name).strip(" *").strip()[:250]
            docket = dm.group(1) if dm else None
            filename = pdf_url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            slug = re.sub(r"[^A-Za-z0-9._-]+", "-",
                          f"{term_slug}-{docket or filename}").strip("-")[:90]
            out.append({
                "pdf_url": pdf_url,
                "docket": docket,
                "citation": _norm_citation(cm.group(1)) if cm else None,
                "case_name": case_name or None,
                "date": dt,
                "term": term_slug,
                "slug": slug,
            })
        return out

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        term_pages = self._term_pages()
        if sample:
            # newest few terms are richest and quickest
            term_pages = term_pages[:2]
        logger.info(f"Found {len(term_pages)} term pages")
        for term_url in term_pages:
            docs = self._parse_term(term_url)
            if docs:
                logger.info(f"{term_url.rsplit('/',1)[-1]}: {len(docs)} opinions")
            for doc in docs:
                if doc["slug"] in seen:
                    continue
                seen.add(doc["slug"])
                yield doc
        logger.info(f"Discovered {len(seen)} unique CAAF opinions")

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/CAAF",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars): {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        if not doc.get("date"):
            m = re.search(r'Decided\s+(.{0,25}\d{4})', text[:1500])
            doc["date"] = _parse_date(m.group(1)) if m else _parse_date(text[:1500])
        if not doc.get("citation"):
            cm = CITATION_RE.search(text[:2000])
            doc["citation"] = _norm_citation(cm.group(1)) if cm else None
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing U.S. Court of Appeals for the Armed Forces...")
        try:
            docs = []
            for d in self.discover_documents(sample=True):
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No opinions discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ opinions (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_name')} [{raw.get('docket')}]")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        case_name = raw.get("case_name")
        docket = raw.get("docket")
        citation = raw.get("citation")
        if case_name and citation:
            title = f"{case_name} ({citation})"
        elif case_name:
            title = case_name
        elif citation:
            title = f"CAAF — {citation}"
        elif docket:
            title = f"CAAF — {docket}"
        else:
            title = "U.S. Court of Appeals for the Armed Forces opinion"
        title = title[:300]
        return {
            "_id": f"US/CAAF/{raw['slug']}",
            "_source": "US/CAAF",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": docket,
            "citation": citation,
            "term": raw.get("term"),
            "court": "U.S. Court of Appeals for the Armed Forces",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 40:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CAAF bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CAAFScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
