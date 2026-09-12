#!/usr/bin/env python3
"""
DZ/ARPCE -- Algeria Telecom & Postal Regulatory Authority

Fetches laws, presidential decrees, executive decrees, arrêtés, and decisions
from ARPCE (Autorité de Régulation de la Poste et des Communications
Électroniques) website.

Strategy:
  - Scrape 6 category listing pages (/en/reg/loi, /dp, /de, /arr, /dec, /res)
  - Each page lists document cards with title, summary, date, and link to
    detail page (/en/pub/{id})
  - Each detail page has a PDF download link (/en/file/{id})
  - Download PDFs and extract full text using pdfplumber
  - ~37 regulatory documents total

Endpoints:
  - Listings: https://www.arpce.dz/en/reg/{category}
  - Document: https://www.arpce.dz/en/pub/{id}
  - PDF file: https://www.arpce.dz/en/file/{id}

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import hashlib
import time
import ssl
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DZ.ARPCE")

BASE_URL = "https://www.arpce.dz"
SOURCE_ID = "DZ/ARPCE"

# Categories to scrape: (url_suffix, category_label, data_type)
CATEGORIES = [
    ("loi", "Laws and Regulations", "legislation"),
    ("dp", "Presidential Decrees", "legislation"),
    ("de", "Executive Decrees", "legislation"),
    ("arr", "Arrêtés / Judgements", "legislation"),
    ("dec", "Decisions", "doctrine"),
    ("res", "Resolutions", "doctrine"),
]


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_date(date_str: str) -> Optional[str]:
    """Parse date string like '25 February 2026 10:17' to ISO format."""
    date_str = date_str.strip()
    for fmt in [
        "%d %B %Y %H:%M",
        "%d %B %Y",
        "%d/%m/%Y",
    ]:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class ARPCEScraper(BaseScraper):
    SOURCE_ID = SOURCE_ID

    def __init__(self):
        super().__init__()
        self.http = HttpClient(
            verify=False,
        )

    def _get(self, url: str) -> Optional[str]:
        """Fetch a URL, return text or None."""
        try:
            resp = self.http.get(url)
            if resp and resp.status_code == 200:
                return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
        return None

    def _get_bytes(self, url: str) -> Optional[bytes]:
        """Fetch a URL, return bytes or None."""
        try:
            resp = self.http.get(url)
            if resp and resp.status_code == 200:
                return resp.content
        except Exception as e:
            logger.warning(f"Failed to fetch bytes {url}: {e}")
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        try:
            import pdfplumber
            pages_text = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
            return "\n\n".join(pages_text)
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    def _parse_listing_page(self, html: str, category: str, data_type: str) -> List[Dict[str, Any]]:
        """Parse a category listing page, return list of document metadata."""
        docs = []

        # Extract document cards: each has title, summary, date, and pub link
        # Pattern: find all card blocks between card-title and btn-read
        title_pattern = re.compile(
            r'<h5\s+class="card-title">\s*(.*?)\s*<span\s+class="badge',
            re.DOTALL
        )
        summary_pattern = re.compile(
            r'<p\s+class="card-text card-text-custom card-text-custom-list">\s*(.*?)\s*</p>',
            re.DOTALL
        )
        date_pattern = re.compile(
            r'<span\s+class="card-date">(.*?)</span>'
        )
        link_pattern = re.compile(
            r'<a\s+class="btn-read see"\s+role="button"\s+href="(/en/pub/[a-z0-9]+)"'
        )

        titles = title_pattern.findall(html)
        summaries = summary_pattern.findall(html)
        dates = date_pattern.findall(html)
        links = link_pattern.findall(html)

        count = min(len(titles), len(links))
        for i in range(count):
            title = _strip_html(titles[i]) if i < len(titles) else ""
            summary = _strip_html(summaries[i]) if i < len(summaries) else ""
            date_str = dates[i].strip() if i < len(dates) else ""
            pub_path = links[i]
            pub_id = pub_path.split("/")[-1]

            docs.append({
                "pub_id": pub_id,
                "pub_path": pub_path,
                "title": title,
                "summary": summary,
                "date_str": date_str,
                "category": category,
                "data_type": data_type,
            })

        return docs

    def _fetch_document(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch a single document's detail page and PDF, return normalized record."""
        pub_url = f"{BASE_URL}{doc['pub_path']}"
        logger.info(f"Fetching document: {doc['title'][:80]}...")

        # Fetch detail page to get PDF link
        html = self._get(pub_url)
        if not html:
            logger.warning(f"Could not fetch detail page: {pub_url}")
            return None

        # Extract PDF download link(s)
        file_links = re.findall(r'href="(/en/file/[a-z0-9]+)"', html)
        if not file_links:
            logger.warning(f"No PDF links found on {pub_url}")
            return None

        # Download and extract text from all PDFs (usually just one)
        all_text = []
        for file_path in file_links:
            file_url = f"{BASE_URL}{file_path}"
            pdf_bytes = self._get_bytes(file_url)
            if pdf_bytes:
                text = self._extract_pdf_text(pdf_bytes)
                if text:
                    all_text.append(text)

        full_text = "\n\n".join(all_text)
        if not full_text or len(full_text) < 50:
            logger.warning(f"Insufficient text extracted for {doc['pub_id']}: {len(full_text)} chars")
            return None

        date_iso = _parse_date(doc["date_str"])

        doc_id = hashlib.sha256(
            f"ARPCE-{doc['pub_id']}".encode()
        ).hexdigest()[:16]

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": doc["data_type"],
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": doc["title"],
            "text": full_text,
            "summary": doc["summary"],
            "date": date_iso,
            "url": pub_url,
            "category": doc["category"],
            "pub_id": doc["pub_id"],
            "language": "fr",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all ARPCE regulatory documents."""
        all_docs = []

        for cat_suffix, cat_label, data_type in CATEGORIES:
            url = f"{BASE_URL}/en/reg/{cat_suffix}"
            logger.info(f"Listing category: {cat_label} ({url})")
            html = self._get(url)
            if not html:
                logger.warning(f"Could not fetch listing: {url}")
                continue
            docs = self._parse_listing_page(html, cat_label, data_type)
            logger.info(f"  Found {len(docs)} documents in {cat_label}")
            all_docs.extend(docs)

        logger.info(f"Total documents found: {len(all_docs)}")

        if sample:
            all_docs = all_docs[:15]
            logger.info(f"Sample mode: processing {len(all_docs)} documents")

        count = 0
        for doc in all_docs:
            record = self._fetch_document(doc)
            if record:
                count += 1
                yield record

        logger.info(f"Fetched {count}/{len(all_docs)} documents with full text")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents updated since a given date."""
        yield from self.fetch_all(sample=False)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Already normalized during fetch."""
        return raw


def bootstrap(sample: bool = False):
    """Run bootstrap: fetch all documents and save to sample/."""
    scraper = ARPCEScraper()
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    for record in scraper.fetch_all(sample=sample):
        count += 1
        fname = f"{record['_id']}.json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(
            f"[{count}] Saved {fname} — {record['title'][:60]}... "
            f"({len(record['text'])} chars)"
        )

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
    return count


def test():
    """Quick connectivity test."""
    scraper = ARPCEScraper()
    html = scraper._get(f"{BASE_URL}/en/reg/loi")
    if html and "card-title" in html:
        logger.info("PASS: ARPCE site accessible, document listings found")
        return True
    else:
        logger.error("FAIL: Could not access ARPCE regulation pages")
        return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse
    parser = argparse.ArgumentParser(description="DZ/ARPCE bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        success = test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        sys.exit(0 if count > 0 else 1)
    elif args.command == "update":
        count = bootstrap(sample=False)
        sys.exit(0 if count > 0 else 1)
