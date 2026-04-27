#!/usr/bin/env python3
"""
EU/CPCS -- EU Consumer Protection Cooperation Network

Fetches CPC Network coordinated actions, common positions, and biennial reports
from the European Commission website. Sources include:
- 9 coordinated action category pages (with linked PDF documents)
- Biennial overview reports
- CPC Network governance documents

Each category page contains both narrative text (describing enforcement actions)
and linked PDF documents (common positions, factsheets, formal decisions).

Usage:
  python bootstrap.py bootstrap            # Full fetch
  python bootstrap.py bootstrap --sample   # Sample records for validation
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EU.CPCS")

BASE_URL = "https://commission.europa.eu"

# Coordinated action category pages
CATEGORIES = {
    "accommodation-booking": "/topics/consumers/consumer-rights-and-complaints/enforcement-consumer-protection/coordinated-actions/accommodation-booking_en",
    "air-travel": "/topics/consumers/consumer-rights-and-complaints/enforcement-consumer-protection/coordinated-actions/air-travel_en",
    "consumer-traps-scams": "/topics/consumers/consumer-rights-and-complaints/enforcement-consumer-protection/coordinated-actions/consumer-frequent-traps-and-scams_en",
    "dieselgate": "/topics/consumers/consumer-rights-and-complaints/enforcement-consumer-protection/coordinated-actions/dieselgate_en",
    "market-places-digital-services": "/topics/consumers/consumer-rights-and-complaints/enforcement-consumer-protection/coordinated-actions/market-places-and-digital-services_en",
    "other-travel-services": "/topics/consumers/consumer-rights-and-complaints/enforcement-consumer-protection/coordinated-actions/other-travel-services_en",
    "quality-differences": "/topics/consumers/consumer-rights-and-complaints/enforcement-consumer-protection/coordinated-actions/quality-differences_en",
    "social-media-games-search": "/topics/consumers/consumer-rights-and-complaints/enforcement-consumer-protection/coordinated-actions/social-media-online-games-and-search-engines_en",
    "sustainable-consumption": "/topics/consumers/consumer-rights-and-complaints/enforcement-consumer-protection/coordinated-actions/sustainable-consumption-actions_en",
}

# Additional pages with reports
EXTRA_PAGES = {
    "biennial-overview": "/live-work-travel-eu/consumer-rights-and-complaints/enforcement-consumer-protection/consumer-protection-cooperation-network/biennial-overview-activities-cpc-network_en",
    "cpc-network-main": "/live-work-travel-eu/consumer-rights-and-complaints/enforcement-consumer-protection/consumer-protection-cooperation-network_en",
}


class CPCScraper:
    def __init__(self):
        self.source_dir = Path(__file__).parent
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _get(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """GET with retry."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                time.sleep(3)
        return None

    def _clean_html_text(self, soup_element) -> str:
        """Extract clean text from a BeautifulSoup element."""
        if not soup_element:
            return ""
        # Remove script and style elements
        for el in soup_element.find_all(["script", "style", "nav", "header", "footer"]):
            el.decompose()
        text = soup_element.get_text(separator="\n")
        # Clean up whitespace
        lines = [line.strip() for line in text.split("\n")]
        lines = [line for line in lines if line]
        return "\n".join(lines)

    def _extract_page_content(self, url: str, category: str) -> Optional[dict]:
        """Extract the main text content from a category page as a document."""
        resp = self._get(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        main = soup.find("main") or soup.find("article") or soup

        # Get page title
        title_el = soup.find("h1")
        title = title_el.get_text(strip=True) if title_el else category.replace("-", " ").title()

        # Extract main content text
        text = self._clean_html_text(main)
        if len(text) < 200:
            return None

        # Try to find a date
        date = self._extract_date_from_page(soup)

        return {
            "title": f"CPC Coordinated Action: {title}",
            "text": text,
            "date": date,
            "url": url,
            "category": category,
            "doc_type": "page_content",
        }

    def _extract_date_from_page(self, soup) -> Optional[str]:
        """Try to extract publication/update date from page metadata."""
        # Check meta tags
        for meta in soup.find_all("meta"):
            name = meta.get("name", "") or meta.get("property", "")
            if "date" in name.lower() or "modified" in name.lower():
                content = meta.get("content", "")
                if re.match(r"\d{4}-\d{2}-\d{2}", content):
                    return content[:10]
        return None

    def _extract_pdfs_from_page(self, url: str, category: str) -> list:
        """Find all commission PDF links on a page."""
        resp = self._get(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        main = soup.find("main") or soup

        pdfs = []
        seen_urls = set()
        for a in main.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href:
                continue
            # Only commission-hosted PDFs
            if href.startswith("/document/download/") or href.startswith("/system/files/"):
                full_url = BASE_URL + href
            elif "commission.europa.eu" in href and ".pdf" in href:
                full_url = href
            else:
                continue

            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Get context: nearby text or link text
            link_text = a.get_text(strip=True)
            # Try to get the parent section heading for context
            parent = a.find_parent(["li", "p", "div", "section"])
            context = ""
            if parent:
                context = parent.get_text(strip=True)[:200]

            pdfs.append({
                "url": full_url,
                "link_text": link_text,
                "context": context,
                "category": category,
            })

        return pdfs

    def _fetch_pdf_document(self, pdf_info: dict) -> Optional[dict]:
        """Download a PDF and extract text."""
        url = pdf_info["url"]
        resp = self._get(url, timeout=120)
        if not resp:
            return None

        if len(resp.content) < 500:
            return None

        # Extract filename for title
        filename = ""
        cd = resp.headers.get("content-disposition", "")
        match = re.search(r'filename[*]?="?([^";]+)', cd)
        if match:
            filename = match.group(1).strip('"')
        if not filename:
            # Extract from URL
            filename = url.split("/")[-1].split("?")[0]
            if "filename=" in url:
                filename = url.split("filename=")[-1].split("&")[0]

        # Clean filename for title
        title = filename.replace(".pdf", "").replace("%20", " ").replace("_", " ").replace("-", " ")
        title = re.sub(r"\s+", " ", title).strip()
        if not title or title == "Download":
            title = pdf_info.get("context", "")[:100] or pdf_info.get("link_text", "CPC Document")

        # Generate stable ID from URL
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]

        text = extract_pdf_markdown(
            source="EU/CPCS",
            source_id=url_hash,
            pdf_bytes=resp.content,
            table="doctrine",
        ) or ""

        if len(text) < 100:
            logger.warning(f"Insufficient text ({len(text)} chars) for {filename}")
            return None

        return {
            "title": title,
            "text": text,
            "date": None,
            "url": url,
            "category": pdf_info["category"],
            "doc_type": "pdf",
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        # Generate stable ID
        url_hash = hashlib.md5(raw["url"].encode()).hexdigest()[:10]
        doc_type = raw.get("doc_type", "unknown")

        return {
            "_id": f"EU-CPCS-{doc_type}-{url_hash}",
            "_source": "EU/CPCS",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def _fetch_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Core fetcher: crawl categories and extract documents."""
        total_count = 0

        all_pages = {**CATEGORIES}
        if not sample:
            all_pages.update(EXTRA_PAGES)
        elif sample:
            # For sample, just use first 3 categories
            all_pages = dict(list(CATEGORIES.items())[:3])

        for category, path in all_pages.items():
            url = BASE_URL + path
            logger.info(f"Processing category: {category}")

            # 1. Extract page content as a document
            page_doc = self._extract_page_content(url, category)
            if page_doc and len(page_doc.get("text", "")) >= 500:
                record = self.normalize(page_doc)
                total_count += 1
                logger.info(f"[{total_count}] Page: {category} ({len(record['text'])} chars)")
                yield record

            if sample and total_count >= 15:
                break

            # 2. Find and download PDFs
            pdfs = self._extract_pdfs_from_page(url, category)
            logger.info(f"  Found {len(pdfs)} PDFs in {category}")

            for pdf_info in pdfs:
                pdf_doc = self._fetch_pdf_document(pdf_info)
                if pdf_doc:
                    record = self.normalize(pdf_doc)
                    total_count += 1
                    logger.info(f"[{total_count}] PDF: {record['title'][:50]} ({len(record['text'])} chars)")
                    yield record

                if sample and total_count >= 15:
                    break

            if sample and total_count >= 15:
                break

        logger.info(f"TOTAL: {total_count} records")

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._fetch_documents(sample=False)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self._fetch_documents(sample=False)

    def test_api(self):
        """Quick connectivity test."""
        logger.info("Testing commission.europa.eu connectivity...")
        url = BASE_URL + list(CATEGORIES.values())[0]
        resp = self._get(url)
        if resp:
            logger.info(f"OK: {resp.status_code}, {len(resp.text)} bytes")
            soup = BeautifulSoup(resp.text, "html.parser")
            main = soup.find("main") or soup
            pdfs = [a["href"] for a in main.find_all("a", href=True) if ".pdf" in a["href"]]
            logger.info(f"Found {len(pdfs)} PDF links on first category page")
        else:
            logger.error("Failed to connect")


def main():
    scraper = CPCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()
    elif command in ("bootstrap", "update"):
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper._fetch_documents(sample=sample):
            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Saved {count} records to {sample_dir}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
