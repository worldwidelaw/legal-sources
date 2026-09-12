#!/usr/bin/env python3
"""
TZ/BOT-Regulations — Bank of Tanzania Regulations, Circulars & Guidelines

Fetches regulatory documents from bot.go.tz/BankSupervision/Regulations.
The page lists ~115 PDFs across Acts, Regulations, Circulars, Guidelines,
Procedures, and Code of Conduct categories. Most PDFs have text layers.

Strategy:
  1. Scrape the regulations listing page for all PDF links
  2. Categorize by URL path (Acts, Regulations, Circulars, etc.)
  3. Download PDFs and extract text with pdfplumber
  4. Skip non-regulatory PDFs (public holidays, transfer operator lists, etc.)

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TZ.BOT-Regulations")

BASE_URL = "https://www.bot.go.tz"
SOURCE_ID = "TZ/BOT-Regulations"
LISTING_URL = f"{BASE_URL}/BankSupervision/Regulations"

# Paths to skip — not regulatory documents
SKIP_PATTERNS = re.compile(
    r"(public.?holidays|ListofMtos|PressRelease|Client.?Service.?Charter|"
    r"Banking.?Supervision.?Annual.?Reports)",
    re.IGNORECASE,
)

# Category detection from URL path
CATEGORY_MAP = {
    "/Acts/": "act",
    "/Regulations/": "regulation",
    "/Circulars/": "circular",
    "/Guidelines/": "guideline",
    "/Procedures/": "procedure",
    "/Conducts/": "conduct",
}


def _extract_text_pdfplumber(pdf_bytes: bytes) -> str:
    import pdfplumber
    parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                parts.append(t)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    return "\n\n".join(parts)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    text = ""
    try:
        text = _extract_text_pdfplumber(pdf_bytes)
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")
    if not text.strip():
        try:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            parts = []
            for page in doc:
                t = page.get_text()
                if t:
                    parts.append(t)
            doc.close()
            text = "\n\n".join(parts)
        except Exception as e:
            logger.warning(f"Both PDF extractors failed: {e}")
    return text.strip()


def _make_id(pdf_url: str) -> str:
    return hashlib.sha256(pdf_url.encode()).hexdigest()[:16]


def _detect_category(url: str) -> str:
    for pattern, cat in CATEGORY_MAP.items():
        if pattern in url:
            return cat
    return "other"


def _classify_type(title: str, category: str) -> str:
    if category in ("act", "regulation"):
        return "legislation"
    if re.search(r"\bact\b", title, re.IGNORECASE):
        return "legislation"
    return "doctrine"


class BOTRegulationsScraper(BaseScraper):
    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            max_retries=3,
            timeout=60,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

    def test_api(self) -> bool:
        try:
            resp = self.http.get(LISTING_URL)
            if resp.status_code == 200 and "Bank of Tanzania" in resp.text:
                logger.info("API test passed — regulations page accessible")
                return True
            logger.error(f"API test failed — status {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False

    def _collect_pdfs(self) -> list[dict]:
        """Scrape the regulations page and collect all regulatory PDF links."""
        from bs4 import BeautifulSoup

        resp = self.http.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        seen_urls = set()
        docs = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower():
                continue
            if href in seen_urls:
                continue
            seen_urls.add(href)

            if SKIP_PATTERNS.search(href):
                continue

            title = a.get_text(strip=True)
            if not title:
                parent = a.find_parent(["li", "p", "div", "td"])
                title = parent.get_text(strip=True)[:200] if parent else ""
            if not title:
                continue

            # Clean HTML entities
            title = title.replace("&#8211;", "—").replace("&#8212;", "—")
            title = title.replace("&amp;", "&")

            full_url = urljoin(BASE_URL, href.replace(" ", "%20"))
            category = _detect_category(href)

            docs.append({
                "title": title,
                "pdf_url": full_url,
                "category": category,
            })

        logger.info(f"Found {len(docs)} regulatory PDFs on listing page")
        return docs

    def _download_and_extract(self, pdf_url: str) -> Optional[str]:
        try:
            resp = self.http.get(pdf_url, timeout=90)
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "")
            if "html" in ct or resp.content[:5] == b"<!DOC":
                logger.warning(f"Got HTML instead of PDF: {pdf_url}")
                return None
            if len(resp.content) < 500:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return None
            text = extract_text_from_pdf(resp.content)
            if not text or len(text) < 50:
                logger.warning(f"Insufficient text from {pdf_url}")
                return None
            return text
        except Exception as e:
            logger.warning(f"Failed to download/extract {pdf_url}: {e}")
            return None

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": _make_id(raw["pdf_url"]),
            "_source": SOURCE_ID,
            "_type": _classify_type(raw["title"], raw.get("category", "")),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "category": raw.get("category", ""),
            "language": "en",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        # Yield RAW doc dicts; BaseScraper.bootstrap_fast()/update() call
        # self.normalize() on each. Previously this yielded normalized records,
        # so the framework double-normalized them — normalize() reads raw["pdf_url"]
        # which the normalized record lacks (it has "url"), raising KeyError for
        # every record → "0 fetched, N errors" on the VPS (issue #960).
        pdf_docs = self._collect_pdfs()
        for i, doc in enumerate(pdf_docs):
            logger.info(f"[{i+1}/{len(pdf_docs)}] Downloading: {doc['title'][:80]}")
            text = self._download_and_extract(doc["pdf_url"])
            if not text:
                continue
            doc["text"] = text
            yield doc
            time.sleep(1)

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="TZ/BOT-Regulations bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = BOTRegulationsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if args.sample else None

        # fetch_all() yields RAW docs (per BaseScraper contract); normalize here
        # to match the VPS bootstrap_fast() path and write proper sample records.
        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                f"  #{count} | {record['title'][:60]} | "
                f"text={text_len} chars | type={record['_type']} | cat={record.get('category', '')}"
            )
            if args.sample or count <= 15:
                fname = f"{record['_id']}.json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            if limit and count >= limit:
                logger.info(f"Sample limit reached ({limit})")
                break

        logger.info(f"Done. {count} records fetched.")
        print(json.dumps({"_source": SOURCE_ID, "records": count}))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
