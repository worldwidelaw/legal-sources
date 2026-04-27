#!/usr/bin/env python3
"""
CK/FSC -- Cook Islands Financial Supervisory Commission

Fetches regulatory doctrine from the Cook Islands FSC website.

Content includes:
  - Banking Prudential Statements (BPS01-BPS14)
  - Insurance Prudential Statements (PS11.x)
  - AML/CFT Practice Guidelines (FTRA)
  - Legislation (Acts, Regulations)
  - Public Bulletins, Reports, Sanctions guidance
  - Prescribed Fee schedules

Strategy:
  - Crawl content.aspx?cn=<page> pages for each section
  - Extract PDF links from /Documentation/FC/ directory
  - Download and extract text from each PDF
  - Deduplicate by filename (same PDF may appear on multiple pages)

Data Coverage:
  - ~200 regulatory documents (PDFs)
  - Primarily English
  - No authentication required

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional, Set
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from bs4 import BeautifulSoup
from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CK.FSC")

BASE_URL = "https://www.fsc.gov.ck"
CONTENT_URL = f"{BASE_URL}/public/content.aspx"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Sections of the FSC website to crawl
SECTIONS = [
    {"cn": "Legislation", "label": "Legislation", "doc_type": "legislation"},
    {"cn": "PrudentialSAndF", "label": "Prudential Statements & Forms", "doc_type": "prudential_statement"},
    {"cn": "LawRegulation", "label": "FIU Laws & Regulations", "doc_type": "regulation"},
    {"cn": "AntiMoneyLaundering", "label": "Anti-Money Laundering", "doc_type": "guidance"},
    {"cn": "PublicBulletin", "label": "Public Bulletins", "doc_type": "bulletin"},
    {"cn": "CompanySanctions", "label": "Sanctions", "doc_type": "guidance"},
    {"cn": "Reports", "label": "Reports", "doc_type": "report"},
    {"cn": "PublicNotice", "label": "Public Notices", "doc_type": "notice"},
    {"cn": "CompanyNews", "label": "News & Media Releases", "doc_type": "news"},
    {"cn": "PrescribedFee", "label": "Prescribed Fees", "doc_type": "fee_schedule"},
    {"cn": "Registry", "label": "Registry", "doc_type": "guidance"},
]

MIN_TEXT_CHARS = 100


class CKFSCScraper(BaseScraper):
    """
    Scraper for CK/FSC -- Cook Islands Financial Supervisory Commission.
    Country: CK
    URL: https://www.fsc.gov.ck/

    Data types: doctrine
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.7",
        })
        self._seen_filenames: Set[str] = set()

    def _fetch_section_links(self, section: Dict[str, str]) -> List[Dict[str, str]]:
        """Fetch a section page and extract all PDF/DOC download links."""
        cn = section["cn"]
        url = f"{CONTENT_URL}?cn={cn}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return []
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        links = []

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            # Only interested in document links (PDFs primarily)
            if "/Documentation/" not in href and not href.lower().endswith(".pdf"):
                continue
            # Skip non-document files
            lower_href = href.lower()
            if any(lower_href.endswith(ext) for ext in [".xlsx", ".xls", ".doc", ".docx", ".jpg", ".png"]):
                continue
            if not lower_href.endswith(".pdf"):
                continue

            full_url = urljoin(BASE_URL, href)
            # Extract filename for dedup and title
            filename = unquote(href.rsplit("/", 1)[-1])

            # Get link text for title
            link_text = a_tag.get_text(strip=True)
            if not link_text or len(link_text) < 3:
                link_text = filename.replace(".pdf", "").replace(".PDF", "")

            links.append({
                "url": full_url,
                "filename": filename,
                "title": link_text,
                "section": section["label"],
                "doc_type": section["doc_type"],
            })

        logger.info(f"  {section['label']}: {len(links)} PDF links found")
        return links

    def _download_and_extract(self, doc: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Download a PDF and extract text."""
        url = doc["url"]
        filename = doc["filename"]

        # Dedup by filename
        if filename in self._seen_filenames:
            return None
        self._seen_filenames.add(filename)

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return None
            if not resp.content or resp.content[:4] != b"%PDF":
                logger.warning(f"Not a PDF: {url}")
                return None
        except Exception as e:
            logger.warning(f"Download failed: {url}: {e}")
            return None

        doc_id = self._make_doc_id(filename)
        text = extract_pdf_markdown(
            source="CK/FSC",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="doctrine",
        )

        if not text or len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text for {filename}: {len(text or '')} chars")
            return None

        logger.info(f"  {filename}: {len(text)} chars")
        return {
            "doc_id": doc_id,
            "title": doc["title"],
            "full_text": text,
            "url": url,
            "filename": filename,
            "section": doc["section"],
            "doc_type": doc["doc_type"],
        }

    @staticmethod
    def _make_doc_id(filename: str) -> str:
        """Create a clean document ID from the filename."""
        name = filename.rsplit(".", 1)[0]  # Remove extension
        clean = re.sub(r"[^\w\d-]", "_", name).strip("_")
        clean = re.sub(r"_+", "_", clean)
        if len(clean) > 120:
            clean = clean[:100] + "_" + hashlib.md5(name.encode()).hexdigest()[:8]
        return clean

    # ── BaseScraper interface ─────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all FSC sections."""
        logger.info("Starting CK/FSC full crawl...")
        self._seen_filenames.clear()

        all_docs = []
        for section in SECTIONS:
            links = self._fetch_section_links(section)
            all_docs.extend(links)

        logger.info(f"Total PDF links found: {len(all_docs)}")

        yielded = 0
        for doc in all_docs:
            result = self._download_and_extract(doc)
            if result:
                yielded += 1
                yield result

        logger.info(f"CK/FSC crawl complete: {yielded} documents with text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all documents (site is small, no date filtering available)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "CK/FSC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("full_text", ""),
            "date": "",
            "url": raw.get("url", ""),
            "doc_type": raw.get("doc_type", ""),
            "section": raw.get("section", ""),
            "language": "en",
        }

    def test_api(self):
        """Quick connectivity and content test."""
        print("Testing CK/FSC sources...\n")

        for section in SECTIONS[:3]:
            print(f"=== {section['label']} ===")
            links = self._fetch_section_links(section)
            print(f"  Found {len(links)} PDF links")

            if links:
                doc = links[0]
                print(f"  First: {doc['title']}")
                print(f"  URL: {doc['url']}")
                result = self._download_and_extract(doc)
                if result:
                    print(f"  Text: {len(result['full_text'])} chars")
                    print(f"  Preview: {result['full_text'][:150]}...")
                else:
                    print("  ERROR: No text extracted")
            print()

        print("Test complete!")


def main():
    scraper = CKFSCScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
