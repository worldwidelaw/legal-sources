#!/usr/bin/env python3
"""
SR/CBvS-Regulations — Centrale Bank van Suriname Regulatory Documents

Fetches regulatory documents (laws, guidelines/richtlijnen, AML/CFT directives,
circulars) from cbvs.sr. Static Joomla site with direct PDF links.

Strategy:
  1. Crawl key regulatory pages on cbvs.sr
  2. Collect all PDF URLs matching regulatory content paths
  3. Download PDFs and extract text with pdfplumber (fallback PyMuPDF)
  4. Normalize into standard schema

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
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SR.CBvS-Regulations")

BASE_URL = "https://www.cbvs.sr"
SOURCE_ID = "SR/CBvS-Regulations"

# Pages to crawl for regulatory PDF links
CRAWL_PAGES = [
    "/",  # Homepage has many regulatory links
    "/en/82-acts/426-laws-and-regulations-on-the-supervision-of-the-financial-system",
    "/en/acts/regulations/banking-supervision",
    "/en/acts/regulations/pension-funds",
    "/en/acts/regulations/banking-supervision/82-acts",
    "/en/supervision-policy/regulations/417-fatf-cfatf",
]

# Path patterns that indicate regulatory content (vs stats/reports/press)
REGULATORY_PATH_PATTERNS = [
    r"/Wetten/",
    r"/Richtlijnen/",
    r"/Richtlijnen2024",
    r"/DTK\d{4}/T",  # DTK2024/TBG, DTK2024/TSP, DTK2024/TVZ
    r"/DTZ\d{4}/",   # DTZ2025/...
    r"/AML-CFT",
    r"/SB\d{4}no",
    r"Wet.*\.pdf$",
    r"Richtlijn.*\.pdf$",
    r"Directive.*\.pdf$",
    r"Guideline.*\.pdf$",
    r"Act.*\.pdf$",
    r"CORPORATE_GOVERNANCE",
    r"SOLVABILITEIT",
    r"KREDIET",
    r"LIQUIDITEIT",
    r"INTEGRITEIT",
    r"DEUGDELIJK",
    r"GESCHIKTHEID",
    r"Memorandum",
    r"Circulaire",
    r"Bankwet",
    r"Pensioenfondsen",
    r"Kapitaalmarkt",
    r"GTK",
]


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


def _extract_text_pymupdf(pdf_bytes: bytes) -> str:
    import fitz
    parts = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    for page in doc:
        t = page.get_text()
        if t:
            parts.append(t)
    doc.close()
    return "\n\n".join(parts)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    text = ""
    try:
        text = _extract_text_pdfplumber(pdf_bytes)
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")
    if not text.strip():
        try:
            text = _extract_text_pymupdf(pdf_bytes)
        except Exception as e:
            logger.warning(f"Both PDF extractors failed: {e}")
    return text.strip()


def _title_from_url(url: str) -> str:
    """Generate a human-readable title from a PDF URL."""
    fname = unquote(url.split("/")[-1])
    if fname.lower().endswith(".pdf"):
        fname = fname[:-4]
    # Clean up common patterns
    fname = fname.replace("_", " ").replace("-", " ").replace("%20", " ")
    fname = re.sub(r"\s+", " ", fname).strip()
    return fname


def _make_id(pdf_url: str) -> str:
    return hashlib.sha256(pdf_url.encode()).hexdigest()[:16]


def _is_regulatory_pdf(url: str) -> bool:
    """Check if a PDF URL matches regulatory content patterns."""
    for pattern in REGULATORY_PATH_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    return False


class CBvSRegulationsScraper(BaseScraper):
    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(max_retries=3, timeout=60)

    def test_api(self) -> bool:
        try:
            resp = self.http.get(BASE_URL)
            if resp.status_code == 200 and "cbvs" in resp.text.lower():
                logger.info("API test passed — cbvs.sr accessible")
                return True
            logger.error(f"API test failed — status {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False

    def _collect_pdf_urls(self) -> list[dict]:
        """Crawl regulatory pages and collect all PDF URLs."""
        seen_urls = set()
        docs = []

        for page_path in CRAWL_PAGES:
            url = BASE_URL + page_path
            logger.info(f"Crawling {url}")
            try:
                resp = self.http.get(url, timeout=30)
                if resp.status_code != 200:
                    logger.warning(f"Got {resp.status_code} for {url}")
                    continue
            except Exception as e:
                logger.warning(f"Failed to fetch {url}: {e}")
                continue

            # Extract all PDF links
            pdf_links = re.findall(r'href="([^"]*\.pdf)"', resp.text, re.IGNORECASE)
            for link in pdf_links:
                full_url = urljoin(url, link)
                if full_url in seen_urls:
                    continue
                if not _is_regulatory_pdf(full_url):
                    continue
                seen_urls.add(full_url)
                docs.append({
                    "pdf_url": full_url,
                    "title": _title_from_url(full_url),
                    "found_on": page_path,
                })
            logger.info(f"  Found {len(pdf_links)} PDF links, {len(docs)} regulatory so far")
            time.sleep(1)

        logger.info(f"Total unique regulatory PDFs: {len(docs)}")
        return docs

    def _download_and_extract(self, pdf_url: str) -> Optional[str]:
        try:
            resp = self.http.get(pdf_url, timeout=90)
            resp.raise_for_status()
            if len(resp.content) < 100:
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
        lang = "nl"
        url = raw.get("pdf_url", "")
        title = raw.get("title", "")
        if any(x in url.lower() for x in ["/en/", "_eng", "_en_", "english"]) or \
           any(x in title.lower() for x in ["act", "guideline", "directive", "decree"]):
            lang = "en"
        return {
            "_id": _make_id(raw["pdf_url"]),
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": None,
            "url": raw["pdf_url"],
            "language": lang,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        pdf_docs = self._collect_pdf_urls()
        for i, doc in enumerate(pdf_docs):
            logger.info(f"[{i+1}/{len(pdf_docs)}] Downloading: {doc['title'][:80]}")
            text = self._download_and_extract(doc["pdf_url"])
            if not text:
                logger.warning(f"Skipping (no text): {doc['title'][:80]}")
                continue
            doc["text"] = text
            yield doc
            time.sleep(1)

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="SR/CBvS-Regulations bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    scraper = CBvSRegulationsScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if args.sample else None

        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                f"  #{count} | {record['title'][:60]} | "
                f"text={text_len} chars | lang={record.get('language', 'N/A')}"
            )
            if args.sample or count <= 15:
                fname = f"{record['_id']}.json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            if limit and count >= limit:
                logger.info(f"Sample limit reached ({limit} records)")
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
