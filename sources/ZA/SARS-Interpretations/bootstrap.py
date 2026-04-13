#!/usr/bin/env python3
"""
ZA/SARS-Interpretations -- South Africa Revenue Service Tax Interpretations

Fetches tax doctrine from SARS: Interpretation Notes, Binding General Rulings (BGR),
Binding Class Rulings (BCR), Binding Private Rulings (BPR), VAT Rulings (VR), and
VAT Class Rulings (VCR). ~700+ PDF documents.

Data access:
  - Index pages list documents with links to individual PDF pages
  - Each document URL serves a PDF directly
  - Text extracted via pdfplumber

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZA.SARS-Interpretations")

BASE_URL = "https://www.sars.gov.za"
DELAY = 2.0

# Index pages for each document category
INDEX_PAGES = [
    # Interpretation Notes
    ("IN", "Interpretation Note", "/legal-counsel/interpretation-rulings/interpretation-notes/in-1-20/"),
    ("IN", "Interpretation Note", "/legal-counsel/interpretation-rulings/interpretation-notes/in-21-40/"),
    ("IN", "Interpretation Note", "/legal-counsel/interpretation-rulings/interpretation-notes/in-41-60/"),
    ("IN", "Interpretation Note", "/legal-counsel/interpretation-rulings/interpretation-notes/in-61-80/"),
    ("IN", "Interpretation Note", "/legal-counsel/interpretation-rulings/interpretation-notes/in-81-100/"),
    ("IN", "Interpretation Note", "/legal-counsel/interpretation-rulings/interpretation-notes/in-101-120/"),
    ("IN", "Interpretation Note", "/legal-counsel/interpretation-rulings/interpretation-notes/in-121-140/"),
    ("IN", "Interpretation Note", "/legal-counsel/interpretation-rulings/interpretation-notes/in-141-160/"),
    # Binding General Rulings
    ("BGR", "Binding General Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-general-rulings/bgr-1-20/"),
    ("BGR", "Binding General Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-general-rulings/bgr-21-40/"),
    ("BGR", "Binding General Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-general-rulings/bgr-41-60/"),
    ("BGR", "Binding General Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-general-rulings/bgr-61-80/"),
    # Binding Class Rulings
    ("BCR", "Binding Class Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-class-rulings/bcr-1-20/"),
    ("BCR", "Binding Class Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-class-rulings/bcr-21-40/"),
    ("BCR", "Binding Class Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-class-rulings/bcr-41-60/"),
    ("BCR", "Binding Class Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-class-rulings/bcr-61-80/"),
    ("BCR", "Binding Class Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-class-rulings/bcr-81-100/"),
    # Binding Private Rulings (many pages)
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-1-20/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-21-40/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-41-60/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-61-80/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-81-100/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-101-120/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-121-140/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-141-160/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-161-180/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-181-200/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-201-220/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-221-240/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-241-260/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-261-280/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-281-300/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-301-320/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-321-340/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-341-360/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-361-380/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-381-400/"),
    ("BPR", "Binding Private Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/binding-private-rulings/bpr-401-420/"),
    # VAT Rulings
    ("VR", "VAT Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/value-added-tax-rulings/"),
    # VAT Class Rulings
    ("VCR", "VAT Class Ruling", "/legal-counsel/interpretation-rulings/published-binding-rulings/value-added-tax-class-rulings/"),
]


def extract_doc_links(html: str, category: str) -> List[Dict[str, str]]:
    """Extract document links from an index page."""
    docs = []
    cat_lower = category.lower()

    # Find all links containing the category code in the URL
    # Links are absolute URLs like https://www.sars.gov.za/legal-intr-in-20-...
    # or https://www.sars.gov.za/lapd-intr-in-2012-07-...
    link_pattern = re.compile(
        r'href="(https://www\.sars\.gov\.za/[^"]*?[-/]' + re.escape(cat_lower) + r'[-_][^"]*?)"',
        re.IGNORECASE,
    )

    seen = set()
    for match in link_pattern.finditer(html):
        url = match.group(1)
        # Skip index/range pages (e.g., /in-1-20/, /bgr-21-40/)
        if re.search(r'/' + re.escape(cat_lower) + r'-\d+-\d+/?$', url, re.IGNORECASE):
            continue
        # Skip register pages
        if 'register' in url.lower():
            continue
        if url in seen:
            continue
        seen.add(url)

        # Convert to relative path
        path = url.replace("https://www.sars.gov.za", "")

        # Extract doc number from URL
        num_match = re.search(re.escape(cat_lower) + r'[-_](?:20\d{2}[-_])?(\d+)', path, re.IGNORECASE)
        doc_num = f"{category} {num_match.group(1)}" if num_match else ""

        # Extract title from link text
        title_match = re.search(
            re.escape(url) + r'[^>]*>([^<]+)',
            html,
        )
        title = title_match.group(1).strip() if title_match else doc_num

        docs.append({
            "path": path,
            "doc_number": doc_num,
            "title": title,
            "category": category,
        })

    return docs


class SARSInterpretationsScraper(BaseScraper):
    SOURCE_ID = "ZA/SARS-Interpretations"

    def __init__(self):
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "text/html,application/xhtml+xml,application/pdf",
            },
        )

    def discover_documents(self, categories: List[Tuple[str, str, str]] = None) -> List[Dict[str, str]]:
        """Crawl index pages to discover all document URLs."""
        if categories is None:
            categories = INDEX_PAGES

        all_docs = []
        seen_paths = set()

        for category_code, category_name, index_path in categories:
            url = f"{BASE_URL}{index_path}"
            logger.info("Crawling index: %s (%s)...", category_code, index_path)
            resp = self.http.get(url)
            time.sleep(DELAY)
            if resp is None or resp.status_code != 200:
                logger.warning("Failed to fetch index %s: %s", index_path,
                               resp.status_code if resp else "None")
                continue

            docs = extract_doc_links(resp.text, category_code)
            for doc in docs:
                if doc["path"] not in seen_paths:
                    seen_paths.add(doc["path"])
                    doc["category_name"] = category_name
                    all_docs.append(doc)

            logger.info("  Found %d documents on this page (total: %d)",
                        len(docs), len(all_docs))

        return all_docs

    def fetch_pdf_text(self, path: str) -> str:
        """Download a PDF from SARS and extract text."""
        url = f"{BASE_URL}{path}" if path.startswith("/") else path
        resp = self.http.get(url)
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            return ""

        content_type = resp.headers.get("content-type", "")
        if "pdf" not in content_type and not resp.content[:5] == b'%PDF-':
            # Not a PDF - might be an HTML page wrapping a PDF link
            # Try to find a PDF link in the HTML
            if b'<html' in resp.content[:500].lower():
                pdf_match = re.search(r'href="([^"]+\.pdf)"', resp.text, re.IGNORECASE)
                if pdf_match:
                    pdf_url = pdf_match.group(1)
                    if not pdf_url.startswith("http"):
                        pdf_url = f"{BASE_URL}{pdf_url}"
                    resp = self.http.get(pdf_url)
                    time.sleep(DELAY)
                    if resp is None or resp.status_code != 200:
                        return ""
                else:
                    return ""

        try:
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                return "\n\n".join(pages)
        except Exception as e:
            logger.warning("Failed to extract PDF from %s: %s", path, e)
            return ""

    def normalize(self, doc_meta: Dict[str, str], text: str) -> Dict[str, Any]:
        """Normalize a document into the standard schema."""
        doc_num = doc_meta.get("doc_number", "")
        title = doc_meta.get("title", doc_num)
        category = doc_meta.get("category", "")
        category_name = doc_meta.get("category_name", category)
        path = doc_meta.get("path", "")

        if doc_num and doc_num not in title:
            title = f"{doc_num}: {title}"

        # Generate a stable ID
        doc_id = re.sub(r'[^\w]', '_', doc_num) if doc_num else re.sub(r'[^\w]', '_', path.split("/")[-2] if path.endswith("/") else path.split("/")[-1])

        return {
            "_id": doc_id,
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": None,  # PDFs don't have a consistent date in metadata
            "url": f"{BASE_URL}{path}",
            "language": "en",
            "doc_number": doc_num,
            "doc_category": category_name,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all SARS interpretation documents."""
        if sample:
            # For sample, only crawl first 2 index pages (IN 1-20, IN 21-40)
            categories = INDEX_PAGES[:2]
        else:
            categories = INDEX_PAGES

        docs = self.discover_documents(categories)
        logger.info("Total documents discovered: %d", len(docs))

        total_yielded = 0
        sample_limit = 15 if sample else None

        for doc_meta in docs:
            if sample_limit and total_yielded >= sample_limit:
                break

            path = doc_meta.get("path", "")
            text = self.fetch_pdf_text(path)
            if not text:
                logger.warning("Empty text for %s (%s)", doc_meta.get("doc_number", ""), path)
                continue

            record = self.normalize(doc_meta, text)
            yield record
            total_yielded += 1

            if total_yielded % 20 == 0:
                logger.info("  Progress: %d documents fetched", total_yielded)

        logger.info("Fetch complete. Total documents: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (crawl first few index pages)."""
        categories = INDEX_PAGES[:4]  # Recent IN and BGR pages
        docs = self.discover_documents(categories)
        for doc_meta in docs:
            text = self.fetch_pdf_text(doc_meta.get("path", ""))
            if text:
                yield self.normalize(doc_meta, text)

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            docs = self.discover_documents(INDEX_PAGES[:1])
            logger.info("Test passed: %d documents in IN 1-20", len(docs))
            return len(docs) > 0
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="ZA/SARS-Interpretations bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    args = parser.parse_args()

    scraper = SARSInterpretationsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', str(record['_id']))
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                "  [%d] %s | %s | text=%d chars",
                count, record["doc_number"], record["title"][:60], text_len
            )

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["doc_number"], record["title"][:60])
        logger.info("Update complete: %d new records since %s", count, since)


if __name__ == "__main__":
    main()
