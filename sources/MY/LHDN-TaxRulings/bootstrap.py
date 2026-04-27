#!/usr/bin/env python3
"""
MY/LHDN-TaxRulings -- Malaysia LHDN Public Rulings and Tax Guidelines

Fetches public rulings, guidelines, and practice notes from LHDN (Lembaga
Hasil Dalam Negeri Malaysia / Inland Revenue Board of Malaysia).

Strategy:
  - Scrapes the public rulings page for PDF links with metadata
  - Scrapes the guidelines page (operational + technical + advance ruling)
  - Scrapes the practice notes page
  - Downloads PDFs and extracts full text via common.pdf_extract

Data:
  - ~200+ public rulings (2000–2026)
  - ~30+ guidelines (operational, technical, advance ruling)
  - ~17 practice notes

All documents are _type "doctrine" (official tax guidance/interpretation).

License: Public regulatory data (Malaysia)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import time
import logging
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MY.LHDN-TaxRulings")

BASE_URL = "https://www.hasil.gov.my"

PAGES = {
    "Public Ruling": "/en/legislation/public-rulings/",
    "Guideline": "/en/legislation/guidelines/",
    "Practice Note": "/en/legislation/practice-note/",
}


def _strip_html(html_text: str) -> str:
    """Remove HTML tags and decode entities."""
    import html as html_module
    text = re.sub(r'<br\s*/?\s*>', '\n', html_text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    return text.strip()


def _parse_date_ddmmyyyy(date_str: str) -> Optional[str]:
    """Parse dates in DD.MM.YYYY format used by LHDN."""
    if not date_str:
        return None
    date_str = date_str.strip()

    # DD.MM.YYYY
    m = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
    if m:
        day, month, year = m.groups()
        try:
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # DD/MM/YYYY
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
    if m:
        day, month, year = m.groups()
        try:
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None


class LHDNTaxRulingsScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
        )

    def _extract_table_rows(self, html: str, category: str, page_url: str) -> List[Dict[str, Any]]:
        """Extract document entries from HTML tables on LHDN pages."""
        results = []
        seen_urls = set()

        # Extract table rows
        for row_m in re.finditer(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL):
            row = row_m.group(1)

            # Skip header rows
            if '<th' in row:
                continue

            # Extract cells
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(cells) < 3:
                continue

            # Find PDF link in the row
            pdf_match = re.search(
                r'<a[^>]+href=["\']([^"\']*\.pdf)["\']',
                row, re.IGNORECASE
            )
            if not pdf_match:
                continue

            href = pdf_match.group(1)
            if not href.startswith("http"):
                href = urljoin(BASE_URL, href)

            if href in seen_urls:
                continue
            seen_urls.add(href)

            # Parse cells based on typical LHDN table structure:
            # Col 0: Ref number, Col 1: Title, Col 2: Date, Col 3+: Notes/Link
            ref_number = _strip_html(cells[0]).strip() if len(cells) > 0 else ""
            title = _strip_html(cells[1]).strip() if len(cells) > 1 else ""
            date_str = _strip_html(cells[2]).strip() if len(cells) > 2 else ""

            # Sometimes the title is in cell 0 and date in cell 1 (guidelines)
            if not title and ref_number:
                title = ref_number
                ref_number = ""

            # If title is too short, derive from filename
            if len(title) < 5:
                fname = unquote(href.split("/")[-1])
                fname = re.sub(r'\.pdf$', '', fname, flags=re.IGNORECASE)
                fname = fname.replace("-", " ").replace("_", " ")
                title = fname.strip()

            if not title:
                continue

            results.append({
                "url": href,
                "title": title,
                "ref_number": ref_number,
                "date_str": date_str,
                "category": category,
                "page_url": page_url,
            })

        # Also pick up any PDF links not found in table rows (accordion-style content)
        for pdf_m in re.finditer(
            r'<a[^>]+href=["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>',
            html, re.DOTALL | re.IGNORECASE
        ):
            href = pdf_m.group(1)
            link_text = _strip_html(pdf_m.group(2)).strip()

            if not href.startswith("http"):
                href = urljoin(BASE_URL, href)

            if href in seen_urls:
                continue
            seen_urls.add(href)

            # Derive title
            if len(link_text) >= 5:
                title = link_text
            else:
                fname = unquote(href.split("/")[-1])
                fname = re.sub(r'\.pdf$', '', fname, flags=re.IGNORECASE)
                fname = fname.replace("-", " ").replace("_", " ")
                title = fname.strip()

            if not title or len(title) < 3:
                continue

            # Try to find a date near the link
            idx = html.find(pdf_m.group(0))
            date_str = ""
            if idx > 0:
                context = _strip_html(html[max(0, idx - 300):idx + 300])
                date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', context)
                if date_match:
                    date_str = date_match.group(1)

            results.append({
                "url": href,
                "title": title,
                "ref_number": "",
                "date_str": date_str,
                "category": category,
                "page_url": page_url,
            })

        return results

    def _get_all_documents(self) -> List[Dict[str, Any]]:
        """Fetch all document entries from all LHDN legislation pages."""
        all_docs = []
        seen_urls = set()

        for category, page_path in PAGES.items():
            logger.info(f"Fetching {category} page: {page_path}")
            resp = self.client.get(page_path)
            if not resp or resp.status_code != 200:
                logger.warning(f"  Failed to fetch {page_path}: {resp.status_code if resp else 'no response'}")
                continue

            items = self._extract_table_rows(resp.text, category, page_path)
            for item in items:
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    all_docs.append(item)

            logger.info(f"  Found {len(items)} documents")
            time.sleep(1.5)

        logger.info(f"Total documents to process: {len(all_docs)}")
        return all_docs

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download PDF with proper headers (LHDN rejects bare requests)."""
        import requests
        try:
            resp = requests.get(
                url,
                timeout=(15, 60),
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "application/pdf,*/*",
                },
            )
            resp.raise_for_status()
            if len(resp.content) < 500:
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"  PDF download failed {url}: {e}")
            return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all documents with full text from PDFs."""
        documents = self._get_all_documents()

        for i, doc in enumerate(documents):
            url = doc["url"]
            title = doc["title"]
            logger.info(f"[{i+1}/{len(documents)}] {title[:60]}...")

            source_id = hashlib.md5(url.encode()).hexdigest()

            # Download PDF with browser headers (LHDN returns 500 without them)
            pdf_bytes = self._download_pdf(url)
            if not pdf_bytes:
                logger.warning(f"  Could not download PDF")
                continue

            try:
                text = extract_pdf_markdown(
                    source="MY/LHDN-TaxRulings",
                    source_id=source_id,
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                )
            except Exception as e:
                logger.warning(f"  PDF extraction failed: {e}")
                continue

            if not text or len(text.strip()) < 50:
                logger.warning(f"  Insufficient text ({len(text) if text else 0} chars)")
                continue

            yield self.normalize({
                "url": url,
                "title": title,
                "ref_number": doc.get("ref_number", ""),
                "date": _parse_date_ddmmyyyy(doc.get("date_str", "")),
                "text": text,
                "category": doc.get("category", ""),
                "page_url": doc.get("page_url", ""),
            })

            time.sleep(1.5)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Yield documents updated since the given date."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        url = raw["url"]
        doc_id = hashlib.md5(url.encode()).hexdigest()

        return {
            "_id": f"MY/LHDN-TaxRulings/{doc_id}",
            "_source": "MY/LHDN-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": url,
            "ref_number": raw.get("ref_number", ""),
            "category": raw.get("category", ""),
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="MY/LHDN-TaxRulings scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10+ sample records")
    args = parser.parse_args()

    scraper = LHDNTaxRulingsScraper()

    if args.command == "test":
        logger.info("Testing connectivity to hasil.gov.my...")
        resp = scraper.client.get("/en/legislation/public-rulings/")
        if resp and resp.status_code == 200:
            logger.info(f"OK — got {len(resp.text)} bytes from public rulings page")
        else:
            logger.error(f"FAIL — status {resp.status_code if resp else 'no response'}")
        return

    if args.command in ("bootstrap", "update"):
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else 999999

        for doc in scraper.fetch_all():
            count += 1
            text_len = len(doc.get("text", ""))
            logger.info(
                f"  #{count} {doc['title'][:50]}... "
                f"({text_len} chars, {doc['category']})"
            )

            # Save sample
            if count <= 20:
                fname = re.sub(r'[^\w\-]', '_', doc["_id"])[:80] + ".json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)

            if count >= max_records:
                break

        logger.info(f"Done — {count} records fetched")


if __name__ == "__main__":
    main()
