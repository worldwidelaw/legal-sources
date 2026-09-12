#!/usr/bin/env python3
"""
TR/BDDK -- Turkish Banking Regulation and Supervision Agency

Fetches banking regulations, communiques, board decisions, and supervisory
guidance from BDDK via HTML scraping and PDF extraction.

Strategy:
  - GET /Mevzuat/Liste/{categoryId} returns all documents in a category (no pagination)
  - Parse HTML for document links with data-detayTuru attribute:
    - "dokuman": direct PDF at /Mevzuat/DokumanGetir/{id}
    - "info": detail page at /Mevzuat/Detay/{id} with attachment PDFs
    - "webLink": external link to mevzuat.gov.tr (skipped — covered by TR/Mevzuat)
  - Extract text from PDFs via common/pdf_extract

URL patterns:
  - Category list: https://www.bddk.org.tr/Mevzuat/Liste/{categoryId}
  - Document PDF: https://www.bddk.org.tr/Mevzuat/DokumanGetir/{documentId}
  - Attachment PDF: https://www.bddk.org.tr/Mevzuat/EkGetir/{docId}?ekId={attachmentId}
  - Detail page: https://www.bddk.org.tr/Mevzuat/Detay/{documentId}
  - No auth required

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TR.BDDK")

BASE_URL = "https://www.bddk.org.tr"

# BDDK Mevzuat category IDs and their names
CATEGORIES = {
    49: "Kanunlar",                          # Laws
    50: "Bankacılık Kanununa İlişkin Düzenlemeler",  # Banking Law Regulations
    51: "Banka Kartları ve Kredi Kartları Kanununa İlişkin Düzenlemeler",  # Card Payment Regulations
    52: "Finansal Kiralama, Faktoring Kanununa İlişkin Düzenlemeler",  # Leasing/Factoring Regulations
    54: "BDDK'na İlişkin Düzenlemeler",       # BDDK Institutional Regulations
    55: "Resmi Gazetede Yayımlanan Kurul Kararları",  # Published Board Decisions
    56: "Resmi Gazetede Yayımlanmayan Kurul Kararları",  # Unpublished Board Decisions
    58: "Düzenleme Taslakları",               # Draft Regulations
    63: "Mülga Düzenlemeler",                 # Repealed Regulations
}


def parse_tr_date(date_str: str) -> Optional[str]:
    """Parse Turkish date format 'DD.MM.YYYY' to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_list_page(html: str, category_id: int) -> List[Dict]:
    """Parse a category list page and extract document entries."""
    documents = []

    # Find all document links — patterns:
    # <a ... data-detayturu="dokuman" data-id="123" ...>Title</a>
    # <a ... data-detayturu="info" data-id="123" ...>Title</a>
    # <a ... data-detayturu="webLink" href="..." ...>Title</a>

    # Pattern for documents with data-id (dokuman and info types)
    doc_pattern = re.compile(
        r'<a[^>]*data-detayturu=["\'](\w+)["\'][^>]*data-id=["\'](\d+)["\'][^>]*>(.*?)</a>',
        re.DOTALL | re.IGNORECASE
    )
    # Also try reversed order: data-id before data-detayturu
    doc_pattern_rev = re.compile(
        r'<a[^>]*data-id=["\'](\d+)["\'][^>]*data-detayturu=["\'](\w+)["\'][^>]*>(.*?)</a>',
        re.DOTALL | re.IGNORECASE
    )

    seen_ids = set()

    for match in doc_pattern.finditer(html):
        detail_type = match.group(1).strip()
        doc_id = match.group(2).strip()
        title = html_mod.unescape(re.sub(r'<[^>]+>', '', match.group(3)).strip())

        if doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)

        if detail_type == "webLink":
            continue

        documents.append({
            "doc_id": doc_id,
            "detail_type": detail_type,
            "title": title,
            "category_id": category_id,
            "category_name": CATEGORIES.get(category_id, f"Category {category_id}"),
        })

    for match in doc_pattern_rev.finditer(html):
        doc_id = match.group(1).strip()
        detail_type = match.group(2).strip()
        title = html_mod.unescape(re.sub(r'<[^>]+>', '', match.group(3)).strip())

        if doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)

        if detail_type == "webLink":
            continue

        documents.append({
            "doc_id": doc_id,
            "detail_type": detail_type,
            "title": title,
            "category_id": category_id,
            "category_name": CATEGORIES.get(category_id, f"Category {category_id}"),
        })

    # Try a more general pattern if the above yielded nothing
    if not documents:
        # Look for DokumanGetir links
        pdf_links = re.findall(
            r'href=["\'](?:/Mevzuat/)?DokumanGetir/(\d+)["\'][^>]*>(.*?)</a>',
            html, re.DOTALL | re.IGNORECASE
        )
        for doc_id, title_raw in pdf_links:
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)
            title = html_mod.unescape(re.sub(r'<[^>]+>', '', title_raw).strip())
            documents.append({
                "doc_id": doc_id,
                "detail_type": "dokuman",
                "title": title,
                "category_id": category_id,
                "category_name": CATEGORIES.get(category_id, f"Category {category_id}"),
            })

        # Look for Detay links
        detay_links = re.findall(
            r'href=["\'](?:/Mevzuat/)?Detay/(\d+)["\'][^>]*>(.*?)</a>',
            html, re.DOTALL | re.IGNORECASE
        )
        for doc_id, title_raw in detay_links:
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)
            title = html_mod.unescape(re.sub(r'<[^>]+>', '', title_raw).strip())
            documents.append({
                "doc_id": doc_id,
                "detail_type": "info",
                "title": title,
                "category_id": category_id,
                "category_name": CATEGORIES.get(category_id, f"Category {category_id}"),
            })

    # Try to extract dates from titles (board decisions format: (DD.MM.YYYY - NNNNN))
    for doc in documents:
        date_match = re.search(r'\((\d{2}\.\d{2}\.\d{4})\s*-\s*(\d+)\)', doc["title"])
        if date_match:
            doc["date_str"] = date_match.group(1)
            doc["decision_number"] = date_match.group(2)

    return documents


class BDDKScraper(BaseScraper):
    """Scraper for TR/BDDK -- Turkish Banking Regulation and Supervision Agency."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            },
            timeout=60,
            verify=False,
        )

    def _fetch_category(self, category_id: int) -> List[Dict]:
        """Fetch all documents from a single category."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/Mevzuat/Liste/{category_id}")
            if not resp or resp.status_code != 200:
                logger.error(f"Category {category_id} error: {resp.status_code if resp else 'no response'}")
                return []
            docs = parse_list_page(resp.text, category_id)
            logger.info(f"Category {category_id} ({CATEGORIES.get(category_id, '?')}): {len(docs)} documents")
            return docs
        except Exception as e:
            logger.error(f"Error fetching category {category_id}: {e}")
            return []

    def _get_detail_attachments(self, doc_id: str) -> List[str]:
        """Fetch a detail page and return attachment EkGetir URLs."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/Mevzuat/Detay/{doc_id}")
            if not resp or resp.status_code != 200:
                return []
            # Look for EkGetir links
            ek_matches = re.findall(
                r'/Mevzuat/EkGetir/\d+\?ekId=\d+',
                resp.text
            )
            if ek_matches:
                return [f"{BASE_URL}{m}" for m in ek_matches]
            # Also check for DokumanGetir on detail pages
            dok_matches = re.findall(
                r'/Mevzuat/DokumanGetir/(\d+)',
                resp.text
            )
            if dok_matches:
                return [f"{BASE_URL}/Mevzuat/DokumanGetir/{m}" for m in dok_matches]
            return []
        except Exception as e:
            logger.error(f"Error fetching detail {doc_id}: {e}")
            return []

    def _extract_pdf_text(self, doc_id: str, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text."""
        text = extract_pdf_markdown(
            source="TR/BDDK",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="doctrine",
        )
        return text

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all documents across all categories."""
        seen_ids = set()

        for cat_id in CATEGORIES:
            docs = self._fetch_category(cat_id)
            for doc in docs:
                doc_id = doc["doc_id"]
                if doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)
                yield doc
            time.sleep(1)

        logger.info(f"Total unique documents: {len(seen_ids)}")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents from recent categories only."""
        # Board decisions (categories 55, 56) are the most frequently updated
        for cat_id in [55, 56]:
            docs = self._fetch_category(cat_id)
            for doc in docs:
                date_str = parse_tr_date(doc.get("date_str", ""))
                if date_str:
                    try:
                        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        if dt >= since:
                            yield doc
                    except ValueError:
                        yield doc
                else:
                    yield doc
            time.sleep(1)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document metadata into standard schema, fetching PDF text."""
        doc_id_num = raw.get("doc_id", "")
        detail_type = raw.get("detail_type", "")
        title = raw.get("title", "").strip()
        category_name = raw.get("category_name", "")
        date_str = parse_tr_date(raw.get("date_str", ""))
        decision_number = raw.get("decision_number", "")

        if not doc_id_num or not title:
            return None

        doc_id = f"BDDK-{doc_id_num}"

        # Determine PDF URL based on detail type
        text = None
        pdf_url = None

        if detail_type == "dokuman":
            pdf_url = f"{BASE_URL}/Mevzuat/DokumanGetir/{doc_id_num}"
            text = self._extract_pdf_text(doc_id, pdf_url)
        elif detail_type == "info":
            # Fetch detail page for attachment URLs
            attachment_urls = self._get_detail_attachments(doc_id_num)
            if attachment_urls:
                # Try first attachment
                pdf_url = attachment_urls[0]
                text = self._extract_pdf_text(doc_id, pdf_url)
                # If multiple attachments, concatenate text
                if len(attachment_urls) > 1:
                    parts = [text] if text else []
                    for extra_url in attachment_urls[1:3]:  # Limit to 3 total
                        extra_text = self._extract_pdf_text(f"{doc_id}-att", extra_url)
                        if extra_text:
                            parts.append(extra_text)
                    if parts:
                        text = "\n\n---\n\n".join(parts)
            else:
                # Try direct DokumanGetir as fallback
                pdf_url = f"{BASE_URL}/Mevzuat/DokumanGetir/{doc_id_num}"
                text = self._extract_pdf_text(doc_id, pdf_url)

        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
            return None

        url = pdf_url or f"{BASE_URL}/Mevzuat/Detay/{doc_id_num}"

        record = {
            "_id": doc_id,
            "_source": "TR/BDDK",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": url,
            "category": category_name,
            "decision_number": decision_number if decision_number else None,
            "jurisdiction": "TR",
            "language": "tr",
        }

        return record

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BDDK Mevzuat pages...")

        total = 0
        for cat_id, cat_name in CATEGORIES.items():
            docs = self._fetch_category(cat_id)
            count = len(docs)
            total += count
            print(f"  Category {cat_id} ({cat_name}): {count} documents")
            if docs:
                d = docs[0]
                print(f"    First: [{d['detail_type']}] {d['title'][:80]}")
            time.sleep(0.5)

        print(f"\nTotal documents across all categories: {total}")

        # Test PDF extraction on first available document
        for cat_id in CATEGORIES:
            docs = self._fetch_category(cat_id)
            for d in docs:
                if d["detail_type"] == "dokuman":
                    doc_id_num = d["doc_id"]
                    pdf_url = f"{BASE_URL}/Mevzuat/DokumanGetir/{doc_id_num}"
                    print(f"\nTesting PDF extraction: {d['title'][:60]}...")
                    text = self._extract_pdf_text(f"BDDK-{doc_id_num}", pdf_url)
                    if text:
                        print(f"  Extracted {len(text)} chars")
                        print(f"  Sample: {text[:200]}...")
                    else:
                        print("  No text extracted")
                    print("\nTest complete!")
                    return

        print("\nNo dokuman-type documents found for PDF test")
        print("Test complete!")


def main():
    scraper = BDDKScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
