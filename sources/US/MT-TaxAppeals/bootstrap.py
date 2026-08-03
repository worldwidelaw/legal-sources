#!/usr/bin/env python3
"""
US/MT-TaxAppeals -- Montana Tax Appeal Board (MTAB)
Final Decisions & Orders (case_law)

Fetches the full text of the Montana Tax Appeal Board's published decisions.
MTAB is the independent quasi-judicial state agency that hears and decides all
Montana state tax appeals -- income, corporation, natural-resource, centrally
assessed, motor-fuels, cigarette, and every class of property tax (residential,
commercial, agricultural, timber, etc.). Every published document (Final
Decision, Findings of Fact / Conclusions of Law, Order on Summary Judgment,
Opportunity for Judicial Review) adjudicates a specific taxpayer appeal ->
case_law.

Access (no JavaScript, no CAPTCHA, no auth):
  The decisions index is one server-rendered page:
    https://mtab.mt.gov/decisions/
  It links out to ~24 subject-matter category pages (relative slugs such as
  `incometax`, `residential-property`, `corptax`, `New-Decisions`, ...). Each
  category page is a server-rendered <ul> whose <li> items ship:
    <a href="{FILE}.pdf">Case Name v. MDOR</a>, DOCKET[, DOCKET...]
  The PDF href is a plain filename (recent decisions, resolved against
  /decisions/) or a `../_docs/decisions/{FILE}.pdf` path (older decisions).
  The decision date is encoded in the filename (M.D.YY, M.D.YYYY, MM-DD-YYYY,
  or a year-only token); the docket number(s) follow the anchor.

  Text layer: the decision PDFs are born-digital -- common.pdf_extract reads the
  full text directly (pdfplumber); a <200-char guard skips the rare scanned
  image-only PDF (OCR fallback via tesseract if present).

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
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MT-TaxAppeals")

BASE_URL = "https://mtab.mt.gov/"
INDEX_URL = "https://mtab.mt.gov/decisions/"

MIN_TEXT_CHARS = 200

# One decision = a <li> whose <span> holds an <a href="...pdf">case name</a>
# followed by the docket number(s).
ITEM_RE = re.compile(
    r'<a\s+[^>]*href="(?P<href>[^"]+?\.pdf)"[^>]*>(?P<name>.*?)</a>'
    r'(?P<tail>[^<]*)',
    re.I | re.S,
)
# Category subpage links on the index: pure relative slugs (no slash, no dot,
# no scheme) such as `incometax`, `residential-property`, `New-Decisions`.
CAT_RE = re.compile(r'href="(?P<slug>[A-Za-z][A-Za-z0-9-]+)"', re.I)
TAG_RE = re.compile(r"<[^>]+>")

# Docket tokens like IT-2024-18, PT-2025-4, CT-2019-2 ...
DOCKET_RE = re.compile(r"\b[A-Z]{2,4}-\d{4}-\d+[A-Za-z]?\b")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_html(s: str) -> str:
    s = TAG_RE.sub(" ", s or "")
    s = html_module.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def _slug_from_href(href: str) -> str:
    """Filename stem, sanitised into a stable slug."""
    name = href.rsplit("/", 1)[-1]
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    slug = re.sub(r"[^A-Za-z0-9]+", "-", name).strip("-")
    return slug.lower() or "decision"


def _parse_date_from_href(href: str) -> str | None:
    """Derive the decision date from the PDF filename.

    Handles M.D.YYYY / MM-DD-YYYY (full year), M.D.YY (optionally with a stray
    trailing version digit, e.g. '12.3.251' -> 12/3/25), and year-only tokens.
    """
    name = href.rsplit("/", 1)[-1]
    name = re.sub(r"\.pdf$", "", name, flags=re.I)

    # 1) M/D/YYYY  (separators '.' or '-')
    m = re.search(r"(?<!\d)(\d{1,2})[.\-](\d{1,2})[.\-]((?:19|20)\d{2})(?!\d)", name)
    if m:
        mo, da, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
        iso = _mk_date(yr, mo, da)
        if iso:
            return iso

    # 2) M/D/YY  (optionally with a single stray trailing version digit)
    for m in re.finditer(r"(?<!\d)(\d{1,2})[.\-](\d{1,2})[.\-](\d{2})\d?(?!\d)", name):
        mo, da, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        iso = _mk_date(2000 + yy, mo, da)
        if iso:
            return iso

    # 3) year-only token (older Final Decisions named '...-2011.pdf')
    m = re.search(r"(?<!\d)((?:19|20)\d{2})(?!\d)", name)
    if m:
        return f"{m.group(1)}-01-01"

    return None


def _mk_date(yr: int, mo: int, da: int) -> str | None:
    try:
        if 1 <= mo <= 12 and 1 <= da <= 31 and 1990 <= yr <= 2100:
            return datetime(yr, mo, da).date().isoformat()
    except ValueError:
        pass
    return None


class MTTaxAppealsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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

    def _category_urls(self) -> list[str]:
        page = self._get_text(INDEX_URL)
        if not page:
            logger.error("Failed to fetch the MTAB decisions index")
            return []
        slugs: list[str] = []
        seen: set[str] = set()
        for m in CAT_RE.finditer(page):
            slug = m.group("slug")
            low = slug.lower()
            if low in ("index", "decisions") or low in seen:
                continue
            seen.add(low)
            slugs.append(slug)
        urls = [urljoin(INDEX_URL, s) for s in slugs]
        logger.info(f"Discovered {len(urls)} MTAB decision category pages")
        return urls

    def _parse_category(self, cat_url: str) -> list[dict]:
        page = self._get_text(cat_url)
        if not page:
            return []
        out: list[dict] = []
        for m in ITEM_RE.finditer(page):
            href = html_module.unescape(m.group("href").strip())
            pdf_url = urljoin(cat_url, href)
            name = _strip_html(m.group("name"))
            tail = _strip_html(m.group("tail"))
            dockets = DOCKET_RE.findall((name + " " + tail).upper())
            docket = ", ".join(dict.fromkeys(dockets)) or None
            title = name or "Montana Tax Appeal Board decision"
            if docket and name:
                title = f"{name} ({docket})"
            out.append({
                "slug": _slug_from_href(href),
                "docket_number": docket,
                "title": title,
                "case_name": name or None,
                "date": _parse_date_from_href(href),
                "pdf_url": pdf_url,
            })
        return out

    def discover_documents(self, sample: bool = False) -> list[dict]:
        docs: list[dict] = []
        seen: set[str] = set()
        for cat_url in self._category_urls():
            for doc in self._parse_category(cat_url):
                if doc["pdf_url"] in seen:
                    continue
                seen.add(doc["pdf_url"])
                docs.append(doc)
            if sample and len(docs) >= 40:
                break
        logger.info(f"Discovered {len(docs)} MTAB decisions")
        return docs

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/MT-TaxAppeals",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars) — scanned PDF, "
                           f"OCR (tesseract) required: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Montana Tax Appeal Board decisions...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents")
            for doc in docs[:5]:
                raw = self._build_raw(doc)
                if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                    logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                                f"{raw.get('docket_number')}")
                    logger.info("API test PASSED")
                    return True
            logger.error("  Text extraction failed on the first 5 documents")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard schema."""
        title = raw.get("title") or "Montana Tax Appeal Board decision"
        return {
            "_id": f"US/MT-TaxAppeals/{raw['slug']}",
            "_source": "US/MT-TaxAppeals",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": raw.get("docket_number"),
            "case_name": raw.get("case_name"),
            "court": "Montana Tax Appeal Board",
            "title": title[:300],
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-MT",
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
            if sample and examined >= 60:
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

    parser = argparse.ArgumentParser(description="US/MT-TaxAppeals bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MTTaxAppealsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
