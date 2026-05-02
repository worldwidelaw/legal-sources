#!/usr/bin/env python3
"""
TH/RD-TaxGuidance -- Thailand Revenue Department Tax Guidance (English)

Fetches tax treaties, royal decrees, ministerial regulations, board of
taxation rulings, revenue departmental orders, and amendment acts from
the English-language section of the Thailand Revenue Department website.

All documents are PDFs hosted at rd.go.th.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any
from urllib.parse import unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TH.RD-TaxGuidance")

BASE_URL = "https://www.rd.go.th"
DELAY = 2.0

# Category pages and their document types
CATEGORY_PAGES = [
    ("royal_decree", "Royal Decrees", f"{BASE_URL}/english/27736.html"),
    ("ministerial_regulation", "Ministerial Regulations", f"{BASE_URL}/english/27738.html"),
    ("board_ruling", "Board of Taxation Rulings", f"{BASE_URL}/english/27741.html"),
    ("departmental_order", "Revenue Departmental Orders", f"{BASE_URL}/english/27737.html"),
    ("amendment_act", "Amendment Acts", f"{BASE_URL}/english/59159.html"),
    ("notification", "Ministry of Finance Notifications", f"{BASE_URL}/english/27742.html"),
    ("tax_treaty", "Double Tax Agreements", f"{BASE_URL}/english/766.html"),
]


def _be_to_ce(be_year: str) -> Optional[str]:
    """Convert Buddhist Era year to Common Era ISO date."""
    try:
        be = int(be_year)
        ce = be - 543
        if 1900 <= ce <= 2030:
            return f"{ce}-01-01"
    except (ValueError, TypeError):
        pass
    return None


def _extract_date(title: str) -> Optional[str]:
    """Extract date from title, handling both CE and BE years."""
    # Try CE year first
    m = re.search(r"\b(20\d{2}|19\d{2})\b", title)
    if m:
        return f"{m.group(1)}-01-01"
    # Try BE year (25xx or 24xx)
    m = re.search(r"B\.?E\.?\s*(2[45]\d{2})", title)
    if m:
        return _be_to_ce(m.group(1))
    return None


def _make_id(doc_type: str, title: str, pdf_url: str) -> str:
    """Generate a stable document ID."""
    # Try to extract a number from the title (e.g., "NO 602")
    m = re.search(r"NO\.?\s*(\d+)", title, re.I)
    if m:
        num = m.group(1)
        prefix_map = {
            "royal_decree": "RD",
            "ministerial_regulation": "MR",
            "board_ruling": "BOTR",
            "departmental_order": "RDO",
            "amendment_act": "RCA",
            "notification": "MFN",
            "tax_treaty": "DTA",
        }
        prefix = prefix_map.get(doc_type, "DOC")
        return f"TH_RD_{prefix}_{num}"

    # For tax treaties, use country name
    if doc_type == "tax_treaty":
        # Extract country from filename or title
        name = unquote(pdf_url).split("/")[-1]
        name = re.sub(r"\.pdf$", "", name, flags=re.I)
        name = re.sub(r"[^a-zA-Z]+", "_", name).strip("_")
        if len(name) > 60:
            name = name[:60]
        return f"TH_RD_DTA_{name}"

    # Fallback: use filename
    name = unquote(pdf_url).split("/")[-1]
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")
    if len(name) > 80:
        name = name[:80]
    return f"TH_RD_{name}"


class THRDScraper(BaseScraper):
    """Scraper for Thailand Revenue Department tax guidance documents."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

    def _discover_documents(self) -> List[Dict[str, Any]]:
        """Discover all PDF documents from RD category pages."""
        from bs4 import BeautifulSoup

        all_docs = []
        seen_urls = set()

        for doc_type, label, page_url in CATEGORY_PAGES:
            try:
                resp = self.http.get(page_url, timeout=30)
                if resp.status_code != 200:
                    logger.warning("HTTP %d for %s", resp.status_code, page_url)
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")

                for a in soup.find_all("a"):
                    href = a.get("href", "")
                    text = a.get_text(strip=True)
                    if not href:
                        continue

                    # Normalize URL
                    href_lower = href.lower()
                    if ".pdf" not in href_lower:
                        continue

                    # For treaty pages: link wraps <img>, get title from
                    # parent <td>'s <strong> or from <img> alt text
                    if not text or len(text) < 2:
                        parent_td = a.find_parent("td")
                        if parent_td:
                            strong = parent_td.find("strong")
                            if strong:
                                text = strong.get_text(strip=True)
                        if not text or len(text) < 2:
                            img = a.find("img")
                            if img and img.get("alt"):
                                text = re.sub(r"^Download\s*PDF\s*", "", img["alt"], flags=re.I).strip()
                        if not text or len(text) < 2:
                            continue

                    # Build absolute URL
                    if href.startswith("/"):
                        href = BASE_URL + href
                    elif href.startswith("http://"):
                        href = href.replace("http://", "https://", 1)
                    elif not href.startswith("https://"):
                        href = urljoin(page_url, href)

                    # Deduplicate
                    url_key = href.split("?")[0].lower()
                    if url_key in seen_urls:
                        continue
                    seen_urls.add(url_key)

                    doc_id = _make_id(doc_type, text, href)
                    all_docs.append({
                        "doc_id": doc_id,
                        "pdf_url": href,
                        "title": text.strip(),
                        "category": label,
                        "doc_type": doc_type,
                        "date": _extract_date(text),
                    })

                logger.info("Discovered %d PDFs from %s (%d total)", len([d for d in all_docs if d["category"] == label]), label, len(all_docs))
                time.sleep(1.0)

            except Exception as e:
                logger.warning("Error scraping %s: %s", page_url, e)

        logger.info("Total unique documents discovered: %d", len(all_docs))
        return all_docs

    def _download_and_extract(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 200:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None
            text = extract_pdf_markdown("TH/RD-TaxGuidance", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all RD tax guidance documents with full text."""
        all_docs = self._discover_documents()
        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            doc_id = doc["doc_id"]
            logger.info("Processing: %s", doc["title"][:80])

            text = self._download_and_extract(doc["pdf_url"], doc_id)
            if not text or len(text.strip()) < 50:
                logger.warning("Insufficient text for %s, skipping", doc_id)
                continue

            yield {
                "_id": doc_id,
                "title": doc["title"],
                "date": doc["date"],
                "doc_type": doc["doc_type"],
                "category": doc["category"],
                "pdf_url": doc["pdf_url"],
                "text": text,
            }
            time.sleep(DELAY)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["_id"],
            "_source": "TH/RD-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "doc_type": raw.get("doc_type", ""),
            "category": raw.get("category", ""),
            "url": raw.get("pdf_url", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="TH/RD-TaxGuidance bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = THRDScraper()

    if args.command == "test":
        docs = scraper._discover_documents()
        print(f"OK -- found {len(docs)} documents")
        types = {}
        for d in docs:
            t = d["doc_type"]
            types[t] = types.get(t, 0) + 1
        for t, c in sorted(types.items()):
            print(f"  {t}: {c}")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
