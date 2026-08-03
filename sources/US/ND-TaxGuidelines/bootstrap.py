#!/usr/bin/env python3
"""
US/ND-TaxGuidelines -- North Dakota Office of State Tax Commissioner
Tax Guidelines (doctrine)

Fetches the full text of the North Dakota Office of State Tax Commissioner's
published tax Guidelines. Each guideline is the agency's official interpretive
explanation of how a North Dakota tax applies to a particular activity or
taxpayer class (alcohol, sales & use, income tax withholding, property-tax
assessment, motor fuel, etc.) -> doctrine (official government tax guidance),
jurisdiction US-ND.

Access (no JavaScript, no CAPTCHA, no auth):
  The Guidelines index
    https://www.tax.nd.gov/guidelines
  is a server-rendered Drupal page. Every guideline is an <a href> to a
  born-digital PDF under
    https://www.tax.nd.gov/sites/www/files/documents/guidelines/{category}/...
  and the anchor's text is a clean human title (e.g. "Alcohol Carriers",
  "Income Taxation of Native Americans"). ~94 guidelines across the categories
  business / individual / military / property-tax / homestead-veterans-renters.

  The PDFs are born-digital (real text layer), so full text is extracted
  directly via common.pdf_extract -- no OCR needed.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py bootstrap --full     # Fetch all guidelines
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
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.ND-TaxGuidelines")

BASE_URL = "https://www.tax.nd.gov/"
INDEX_URL = "https://www.tax.nd.gov/guidelines"

MIN_TEXT_CHARS = 200
# Anchor to a guideline PDF: capture href + inner text
ANCHOR_RE = re.compile(
    r'<a\b[^>]*href="([^"]*documents/guidelines/[^"]+\.pdf)"[^>]*>(.*?)</a>',
    re.S | re.I,
)
TAG_RE = re.compile(r"<[^>]+>")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _anchor_title(inner_html: str) -> str:
    txt = TAG_RE.sub(" ", inner_html)
    txt = html_module.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


def _slug(pdf_url: str) -> str:
    base = unquote(pdf_url).rsplit("/", 1)[-1].rsplit(".", 1)[0]
    base = re.sub(r"[^A-Za-z0-9._,-]+", "-", base).strip("-")[:100]
    return base or "guideline"


def _category(pdf_url: str) -> str | None:
    m = re.search(r"documents/guidelines/([^/]+)/", unquote(pdf_url), re.I)
    if m:
        return m.group(1).replace("-", " ")
    return None


class NDTaxGuidelinesScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=120,
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

    def discover_documents(self, sample: bool = False) -> list[dict]:
        docs: list[dict] = []
        seen: set[str] = set()
        page = self._get_text(INDEX_URL)
        if not page:
            logger.error(f"Failed to fetch index page {INDEX_URL}")
            return docs
        for href, inner in ANCHOR_RE.findall(page):
            pdf_url = urljoin(BASE_URL, html_module.unescape(href.strip()))
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            title = _anchor_title(inner)
            category = _category(pdf_url)
            if not title:
                title = _slug(pdf_url).replace("-", " ").title()
            full_title = f"North Dakota Tax Guideline — {title}"
            if category:
                full_title = f"North Dakota Tax Guideline ({category.title()}) — {title}"
            docs.append({
                "slug": _slug(pdf_url),
                "guideline_title": title,
                "category": category,
                "title": full_title[:300],
                "pdf_url": pdf_url,
            })
        logger.info(f"Discovered {len(docs)} ND tax guideline PDFs")
        return docs

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/ND-TaxGuidelines",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars): {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing North Dakota tax guidelines...")
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
                                f"{raw.get('guideline_title')}")
                    logger.info("API test PASSED")
                    return True
            logger.error("  Text extraction failed on the first 5 documents")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard schema."""
        title = raw.get("title") or "North Dakota tax guideline"
        return {
            "_id": f"US/ND-TaxGuidelines/{raw['slug']}",
            "_source": "US/ND-TaxGuidelines",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "guideline_title": raw.get("guideline_title"),
            "category": raw.get("category"),
            "issuer": "North Dakota Office of State Tax Commissioner",
            "title": title[:300],
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": None,
            "jurisdiction": "US-ND",
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
            if sample and examined >= 30:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/ND-TaxGuidelines bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NDTaxGuidelinesScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
