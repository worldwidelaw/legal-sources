#!/usr/bin/env python3
"""
MR/BCM-Regulations -- Banque Centrale de Mauritanie — Banking Regulations

Fetches banking laws, prudential regulations, monetary policy instructions,
and circulars from bcm.mr (Drupal 10 backend with JSON:API).

Strategy:
  1. Query bo.bcm.mr JSON:API for page nodes containing legal texts
  2. Extract PDF links from HTML content fields
  3. Download and extract full text from PDFs via pdfplumber
  4. Also capture inline HTML legal text from pages without PDFs

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import unquote, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MR.BCM-Regulations")

API_BASE = "https://bo.bcm.mr/fr/jsonapi/node/page"
DELAY = 2.0

# Page nodes containing legal texts (nid -> category)
LEGAL_PAGES = {
    810: "Lois et textes fondateurs",
    811: "Statuts de la BCM",
    825: "Réglementation prudentielle",
    828: "Lutte contre le blanchiment (LCB/FT)",
    898: "Normes nationales",
    899: "Conformité internationale",
    904: "Lois et règlements applicables",
    948: "Lois et normes",
}


def _api_fetch(nid: int) -> Optional[Dict]:
    """Fetch a page node from Drupal JSON:API."""
    import requests
    url = f"{API_BASE}?filter%5Bdrupal_internal__nid%5D={nid}"
    headers = {"Accept": "application/vnd.api+json"}
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
            if r.status_code == 200:
                data = r.json()
                if data.get("data") and len(data["data"]) > 0:
                    return data["data"][0]
            logger.warning("API attempt %d for nid=%d: HTTP %d", attempt + 1, nid, r.status_code)
        except Exception as e:
            logger.warning("API attempt %d for nid=%d: %s", attempt + 1, nid, e)
        if attempt < 2:
            time.sleep(3)
    return None


def _extract_pdf_links(html: str) -> List[Tuple[str, str]]:
    """Extract (title, url) pairs for PDF links from HTML content."""
    results = []
    # Match <a href="...pdf">Title</a> patterns
    pattern = re.compile(
        r'<a\s[^>]*href="(https://bo\.bcm\.mr/sites/default/files/[^"]+)"[^>]*>(.*?)</a>',
        re.DOTALL | re.IGNORECASE,
    )
    for m in pattern.finditer(html):
        url = m.group(1)
        title = re.sub(r"<[^>]+>", "", m.group(2)).strip()
        if not title or len(title) < 3:
            title = _title_from_url(url)
        # Clean up title
        title = re.sub(r"&nbsp;", " ", title)
        title = re.sub(r"&amp;", "&", title)
        title = re.sub(r"\.pdf\s*$", "", title, flags=re.IGNORECASE).strip()
        title = re.sub(r"\s+", " ", title)
        results.append((title, url))
    return results


def _clean_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r"<br\s*/?>", "\n", html)
    text = re.sub(r"</?p[^>]*>", "\n", text)
    text = re.sub(r"</?li[^>]*>", "\n• ", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&#\d+;", "", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def _title_from_url(url: str) -> str:
    """Extract readable title from PDF URL."""
    m = re.search(r"/([^/]+)$", url)
    if m:
        name = unquote(m.group(1))
        name = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[_-]+", " ", name)
        return name.strip()
    return ""


def _extract_year(text: str) -> Optional[str]:
    """Extract a year from text or filename."""
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if m:
        year = int(m.group(1))
        if 1990 <= year <= 2030:
            return f"{year}-01-01"
    return None


def _make_id(prefix: str, text: str) -> str:
    """Create a stable document ID."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_").lower()
    if len(slug) > 60:
        slug = slug[:60]
    return f"MR_BCM_{prefix}_{abs(hash(slug)) % 10**10}"


def _download_pdf(url: str) -> Optional[bytes]:
    """Download a PDF file."""
    import requests
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            r = requests.get(url, timeout=60)
            if r.status_code == 200 and len(r.content) > 200:
                return r.content
            logger.warning("PDF download attempt %d: HTTP %d, %d bytes", attempt + 1, r.status_code, len(r.content))
        except Exception as e:
            logger.warning("PDF download attempt %d: %s", attempt + 1, e)
        if attempt < 2:
            time.sleep(3)
    return None


class BCMRegulationsScraper(BaseScraper):
    """Scraper for MR/BCM-Regulations."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _collect_documents(self) -> List[Dict[str, Any]]:
        """Collect all document references from legal page nodes."""
        docs = []
        seen_urls = set()

        for nid, category in LEGAL_PAGES.items():
            logger.info("Fetching page node %d: %s", nid, category)
            node = _api_fetch(nid)
            if node is None:
                logger.warning("Could not fetch node %d", nid)
                continue

            title = node.get("title", "")
            content = node.get("field_content", {}) or {}
            html = content.get("value", "") or ""

            # Extract PDF links
            pdf_links = _extract_pdf_links(html)
            for pdf_title, pdf_url in pdf_links:
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)
                docs.append({
                    "type": "pdf",
                    "title": pdf_title,
                    "url": pdf_url,
                    "category": category,
                    "page_nid": nid,
                })

            # If page has substantial inline text (not just PDF links),
            # capture it as a standalone record
            clean = _clean_html(html)
            # Remove lines that are just PDF link titles
            for _, pdf_url in pdf_links:
                fname = _title_from_url(pdf_url)
                if fname:
                    clean = clean.replace(fname, "")
            clean = re.sub(r"\n\s*\n+", "\n\n", clean).strip()

            if len(clean) > 200:
                docs.append({
                    "type": "inline",
                    "title": title,
                    "text": clean,
                    "url": f"https://www.bcm.mr/page/{nid}",
                    "category": category,
                    "page_nid": nid,
                })

        logger.info("Collected %d documents (%d PDFs, %d inline)",
                     len(docs),
                     sum(1 for d in docs if d["type"] == "pdf"),
                     sum(1 for d in docs if d["type"] == "inline"))
        return docs

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "MR/BCM-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        doc_list = self._collect_documents()
        if not doc_list:
            logger.error("No documents found")
            return

        count = 0
        for doc in doc_list:
            if max_records and count >= max_records:
                return

            if doc["type"] == "pdf":
                title = doc["title"]
                url = doc["url"]
                doc_id = _make_id("pdf", url)
                logger.info("Downloading PDF [%d/%d]: %s", count + 1, len(doc_list), title[:60])

                pdf_bytes = _download_pdf(url)
                if pdf_bytes is None:
                    logger.warning("Failed to download: %s", url)
                    continue
                if not pdf_bytes[:5].startswith(b"%PDF"):
                    logger.warning("Not a PDF: %s", url)
                    continue

                try:
                    text = extract_pdf_markdown(
                        source="MR/BCM-Regulations",
                        source_id=doc_id,
                        pdf_bytes=pdf_bytes,
                    )
                except Exception as e:
                    logger.warning("PDF extraction failed for %s: %s", url, e)
                    continue

                if not text or len(text) < 50:
                    logger.warning("Insufficient text (%d chars): %s",
                                   len(text or ""), title[:50])
                    continue

                date = _extract_year(title) or _extract_year(url)
                raw = {
                    "doc_id": doc_id,
                    "title": title,
                    "text": text,
                    "date": date,
                    "url": url,
                    "category": doc["category"],
                }
                count += 1
                yield raw

            elif doc["type"] == "inline":
                title = doc["title"]
                text = doc["text"]
                doc_id = _make_id("page", str(doc["page_nid"]))
                logger.info("Inline text [%d/%d]: %s (%d chars)",
                           count + 1, len(doc_list), title[:60], len(text))

                raw = {
                    "doc_id": doc_id,
                    "title": title,
                    "text": text,
                    "date": None,
                    "url": doc["url"],
                    "category": doc["category"],
                }
                count += 1
                yield raw

        logger.info("Completed: %d documents fetched", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        logger.info("Testing API access to bo.bcm.mr...")
        node = _api_fetch(810)
        if node is None:
            logger.error("Cannot reach Drupal JSON:API")
            return False

        title = node.get("title", "")
        content = node.get("field_content", {}) or {}
        html = content.get("value", "") or ""
        pdfs = _extract_pdf_links(html)
        logger.info("Node 810 (%s): %d PDF links", title, len(pdfs))

        if pdfs:
            test_title, test_url = pdfs[0]
            logger.info("Test download: %s", test_title[:60])
            pdf_bytes = _download_pdf(test_url)
            if pdf_bytes and len(pdf_bytes) > 200:
                logger.info("PDF download OK: %d bytes", len(pdf_bytes))
            else:
                logger.warning("PDF download failed")

        return True


def main():
    parser = argparse.ArgumentParser(description="MR/BCM-Regulations data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BCMRegulationsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
