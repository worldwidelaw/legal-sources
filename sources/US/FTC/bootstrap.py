#!/usr/bin/env python3
"""
US/FTC -- Federal Trade Commission Decisions and Orders

Fetches FTC enforcement actions from the Legal Library at ftc.gov.
~6,000+ cases including complaints, consent orders, decision and orders,
analysis to aid public comment, and commissioner statements.

Data access:
  - HTML case listing at /legal-library/browse/cases-proceedings
  - 20 results per page, paginated with ?items_per_page=20&page=N
  - Individual case pages contain metadata + PDF links
  - Full text extracted from PDFs via pdfplumber

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FTC")

BASE_URL = "https://www.ftc.gov"
LISTING_URL = BASE_URL + "/legal-library/browse/cases-proceedings"
DELAY = 2.0
MAX_PAGES = 303  # ~6,055 cases / 20 per page


def get_session() -> requests.Session:
    """Create a requests session with browser-like headers (FTC blocks bare UA)."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def extract_text_from_pdf(content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/FTC",
        source_id="",
        pdf_bytes=content,
        table="case_law",
    ) or ""

def parse_listing_page(html: str) -> List[str]:
    """Extract case page URLs from a listing page."""
    case_urls = []

    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=re.compile(r"/legal-library/browse/cases-proceedings/")):
            href = a.get("href", "")
            title = a.get_text(strip=True)
            # Skip category pages and public statements
            if not title or "/public-statements/" in href or "/petitions-quash/" in href:
                continue
            if title in ("Adjudicative Proceedings", "Commissioner Statements",
                         "Petitions to Quash", "Find banned debt collectors"):
                continue
            if href not in case_urls:
                case_urls.append(href)
    else:
        # Regex fallback
        for match in re.finditer(
            r'href="(/legal-library/browse/cases-proceedings/[^"]+)"[^>]*>([^<]+)',
            html
        ):
            href, title = match.group(1), match.group(2).strip()
            if "/public-statements/" in href or "/petitions-quash/" in href:
                continue
            if title in ("Adjudicative Proceedings", "Commissioner Statements",
                         "Petitions to Quash", "Find banned debt collectors"):
                continue
            if href not in case_urls:
                case_urls.append(href)

    return case_urls


def parse_case_page(html: str) -> Dict[str, Any]:
    """Parse an individual case page for metadata and PDF links."""
    meta: Dict[str, Any] = {
        "title": "",
        "matter_number": "",
        "enforcement_type": "",
        "last_updated": "",
        "pdf_urls": [],
        "pdf_titles": [],
    }

    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")

        # Title
        h1 = soup.find("h1")
        if h1:
            meta["title"] = h1.get_text(strip=True)

        # Field values
        for div in soup.find_all("div", class_=re.compile(r"field--name")):
            label_el = div.find(class_="field__label")
            item_el = div.find(class_="field__item")
            if not label_el or not item_el:
                continue
            label = label_el.get_text(strip=True)
            value = item_el.get_text(strip=True)

            if label == "FTC Matter/File Number":
                meta["matter_number"] = value
            elif label == "Enforcement Type":
                meta["enforcement_type"] = value
            elif label == "Last Updated":
                meta["last_updated"] = value

        # PDF links
        for a in soup.find_all("a", href=re.compile(r"\.pdf", re.IGNORECASE)):
            href = a.get("href", "")
            title = a.get_text(strip=True)
            # Clean up title (remove file size info)
            title = re.sub(r"\(\d+[\d.]*\s*[KMGT]?B\)", "", title).strip()
            if href and href not in meta["pdf_urls"]:
                meta["pdf_urls"].append(href)
                meta["pdf_titles"].append(title)
    else:
        # Regex fallback for title
        title_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
        if title_match:
            meta["title"] = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()

        # Matter number
        matter_match = re.search(
            r"FTC Matter/File Number.*?field__item[^>]*>(.*?)<", html, re.DOTALL
        )
        if matter_match:
            meta["matter_number"] = matter_match.group(1).strip()

        # Enforcement type
        enforce_match = re.search(
            r"Enforcement Type.*?field__item[^>]*>(.*?)<", html, re.DOTALL
        )
        if enforce_match:
            meta["enforcement_type"] = enforce_match.group(1).strip()

        # Last Updated
        updated_match = re.search(
            r"Last Updated.*?field__item[^>]*>(.*?)<", html, re.DOTALL
        )
        if updated_match:
            meta["last_updated"] = updated_match.group(1).strip()

        # PDF links
        for pdf_match in re.finditer(
            r'href="([^"]*\.pdf[^"]*)"[^>]*>([^<]*)', html, re.IGNORECASE
        ):
            href = pdf_match.group(1)
            title = re.sub(r"\(\d+[\d.]*\s*[KMGT]?B\)", "", pdf_match.group(2)).strip()
            if href not in meta["pdf_urls"]:
                meta["pdf_urls"].append(href)
                meta["pdf_titles"].append(title)

    return meta


def parse_date(date_str: str) -> Optional[str]:
    """Parse various date formats to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ["%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d", "%b %d, %Y"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# Priority order for picking the primary PDF (most substantive first)
DOC_PRIORITY = [
    "decision and order",
    "opinion of the commission",
    "opinion",
    "final order",
    "consent order",
    "initial decision",
    "complaint",
    "agreement containing consent order",
    "analysis",
    "order",
]


def pick_primary_pdf(pdf_urls: List[str], pdf_titles: List[str]) -> Tuple[str, str]:
    """Pick the most substantive PDF from a case's documents."""
    if not pdf_urls:
        return "", ""

    # Try priority order
    for keyword in DOC_PRIORITY:
        for i, title in enumerate(pdf_titles):
            if keyword in title.lower():
                return pdf_urls[i], pdf_titles[i]

    # Default to first PDF
    return pdf_urls[0], pdf_titles[0]


class FTCScraper:
    SOURCE_ID = "US/FTC"

    def __init__(self):
        self.session = get_session()

    def _get(self, url: str) -> Optional[requests.Response]:
        """HTTP GET with retry."""
        full_url = urljoin(BASE_URL, url) if url.startswith("/") else url
        for attempt in range(3):
            try:
                resp = self.session.get(full_url, timeout=30)
                if resp.status_code == 200:
                    return resp
                if resp.status_code == 429:
                    wait = 10 * (attempt + 1)
                    logger.warning("Rate limited, waiting %ds...", wait)
                    time.sleep(wait)
                    continue
                logger.warning("HTTP %d for %s", resp.status_code, full_url)
                return None
            except requests.RequestException as e:
                logger.warning("Request error (attempt %d): %s", attempt + 1, e)
                time.sleep(5)
        return None

    def fetch_listing_page(self, page: int) -> List[str]:
        """Fetch one page of the case listing and return case URLs."""
        url = f"{LISTING_URL}?items_per_page=20&page={page}"
        resp = self._get(url)
        time.sleep(DELAY)
        if resp is None:
            return []
        return parse_listing_page(resp.text)

    def fetch_case_metadata(self, case_url: str) -> Dict[str, Any]:
        """Fetch and parse an individual case page."""
        resp = self._get(case_url)
        time.sleep(DELAY)
        if resp is None:
            return {}
        meta = parse_case_page(resp.text)
        meta["case_url"] = case_url
        return meta

    def fetch_pdf_text(self, pdf_url: str) -> str:
        """Download a PDF and extract its full text."""
        resp = self._get(pdf_url)
        time.sleep(DELAY)
        if resp is None:
            return ""
        # Skip very large PDFs (>50MB)
        if len(resp.content) > 50_000_000:
            logger.warning("Skipping oversized PDF (%d bytes): %s",
                           len(resp.content), pdf_url)
            return ""
        return extract_text_from_pdf(resp.content)

    def normalize(self, meta: Dict[str, Any], text: str, doc_title: str) -> Dict[str, Any]:
        """Normalize a case into the standard schema."""
        title = meta.get("title", "Unknown")
        matter = meta.get("matter_number", "")
        case_url = meta.get("case_url", "")

        # Build a stable ID from the URL slug
        slug = case_url.rstrip("/").split("/")[-1] if case_url else ""
        _id = slug or re.sub(r"[^\w-]", "_", title.lower())[:80]

        return {
            "_id": _id,
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": parse_date(meta.get("last_updated", "")),
            "url": urljoin(BASE_URL, case_url) if case_url.startswith("/") else case_url,
            "language": "en",
            "matter_number": matter,
            "enforcement_type": meta.get("enforcement_type", ""),
            "doc_title": doc_title,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all FTC cases with full text from PDFs."""
        total_yielded = 0
        sample_limit = 15 if sample else None
        consecutive_empty = 0

        for page in range(MAX_PAGES):
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("Fetching listing page %d/%d...", page, MAX_PAGES - 1)
            case_urls = self.fetch_listing_page(page)

            if not case_urls:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("3 consecutive empty pages, stopping.")
                    break
                continue
            consecutive_empty = 0

            for case_url in case_urls:
                if sample_limit and total_yielded >= sample_limit:
                    break

                logger.info("  Processing case: %s", case_url.split("/")[-1])
                meta = self.fetch_case_metadata(case_url)
                if not meta:
                    continue

                pdf_urls = meta.get("pdf_urls", [])
                pdf_titles = meta.get("pdf_titles", [])
                if not pdf_urls:
                    logger.warning("  No PDFs found for %s", meta.get("title", ""))
                    continue

                primary_url, primary_title = pick_primary_pdf(pdf_urls, pdf_titles)
                if not primary_url:
                    continue

                text = self.fetch_pdf_text(primary_url)
                if not text:
                    logger.warning("  Empty text from PDF for %s", meta.get("title", ""))
                    continue

                record = self.normalize(meta, text, primary_title)
                yield record
                total_yielded += 1

                if total_yielded % 10 == 0:
                    logger.info("  Progress: %d documents fetched", total_yielded)

        logger.info("Fetch complete. Total documents: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch cases updated since a given date (listing is newest-first)."""
        for page in range(100):
            case_urls = self.fetch_listing_page(page)
            if not case_urls:
                break

            found_older = False
            for case_url in case_urls:
                meta = self.fetch_case_metadata(case_url)
                if not meta:
                    continue
                doc_date = parse_date(meta.get("last_updated", ""))
                if doc_date and doc_date < since:
                    found_older = True
                    break

                pdf_urls = meta.get("pdf_urls", [])
                pdf_titles = meta.get("pdf_titles", [])
                primary_url, primary_title = pick_primary_pdf(pdf_urls, pdf_titles)
                if not primary_url:
                    continue
                text = self.fetch_pdf_text(primary_url)
                if text:
                    yield self.normalize(meta, text, primary_title)

            if found_older:
                break

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            urls = self.fetch_listing_page(0)
            logger.info("Test passed: %d case links on first page", len(urls))
            return len(urls) > 0
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/FTC bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FTCScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r"[^\w\-.]", "_", str(record["_id"]))[:100]
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(
                json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                "  [%d] %s | %s | text=%d chars",
                count, record["date"], record["title"][:60], text_len,
            )

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["date"], record["title"][:60])
        logger.info("Update complete: %d new records since %s", count, since)


if __name__ == "__main__":
    main()
