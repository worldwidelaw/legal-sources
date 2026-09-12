#!/usr/bin/env python3
"""
MD/BNM -- National Bank of Moldova Regulations & Decisions

Fetches regulations from the National Bank of Moldova (Banca Națională
a Moldovei) at https://www.bnm.md/en/content/regulations-list.

Strategy:
  1. Scrape the single-page regulations list (all ~95 regulations)
  2. Visit each regulation detail page to find PDF download links
  3. Download PDFs and extract full text via pdfplumber
  4. Normalize into standard schema

Data source:
  - Listing: https://www.bnm.md/en/content/regulations-list (single page)
  - Detail: https://www.bnm.md/en/content/<slug>
  - PDFs: https://www.bnm.md/files/<filename>.pdf

Coverage:
  - Banking sector, financial-banking market, payment systems,
    insurance sector, non-bank lending
  - ~95 regulations from 1997 to present

License: Public Domain (Government regulations)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Same as bootstrap (single page)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    import docx
except ImportError:
    docx = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MD.BNM")

BASE_URL = "https://www.bnm.md"
LIST_URL = "https://www.bnm.md/en/content/regulations-list"
SOURCE_ID = "MD/BNM"

# Category section headers in the listing page
CATEGORY_HEADERS = [
    "Banking Sector",
    "Financial-Banking Market",
    "Payment Systems",
    "Insurance Sector",
    "Non-Bank Lending",
]


class BNMScraper(BaseScraper):
    """
    Scraper for MD/BNM -- National Bank of Moldova.
    Country: MD
    URL: https://www.bnm.md/en/content/regulations-list

    Data types: legislation, doctrine
    Auth: none
    """

    SOURCE_ID = SOURCE_ID

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            verify=False,
        )

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.http.get(LIST_URL, timeout=15)
            if resp.status_code == 200 and "regulations" in resp.text.lower():
                logger.info("Connectivity OK — regulations list accessible")
                return True
            logger.error(f"Unexpected response: {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def _parse_listing_page(self, html: str) -> list:
        """Parse the regulations listing page to extract regulation links and categories."""
        results = []

        # The content area is inside the body field div:
        # <div class="field field-name-body ...">
        section_headers = [
            ("The banking sector", "Banking Sector"),
            ("The financial-banking market", "Financial-Banking Market"),
            ("Payment systems and financial market infrastructure", "Payment Systems"),
            ("The insurance sector", "Insurance Sector"),
            ("Non-bank lending", "Non-Bank Lending"),
        ]

        # Isolate the body field content area
        body_match = re.search(
            r'<div class="field field-name-body[^"]*"[^>]*>(.*)',
            html,
            re.DOTALL,
        )
        if not body_match:
            logger.error("Could not find body field div in HTML")
            return results
        content_html = body_match.group(1)

        # Split by section headers to assign categories
        # Build a regex that matches any section header
        header_pattern = "|".join(
            re.escape(h) for h, _ in section_headers
        )
        parts = re.split(
            f"({header_pattern})",
            content_html,
            flags=re.IGNORECASE,
        )

        link_pattern = re.compile(
            r'<a\s+href="(/en/content/[^"]+)"[^>]*>(.*?)</a>',
            re.DOTALL,
        )

        # Map lowercase header text to category name
        header_map = {h.lower(): cat for h, cat in section_headers}
        current_category = "Banking Sector"

        seen_slugs = set()
        for part in parts:
            part_lower = part.strip().lower()
            if part_lower in header_map:
                current_category = header_map[part_lower]
                continue

            for match in link_pattern.finditer(part):
                url_path = match.group(1)
                title = re.sub(r"<[^>]+>", "", match.group(2)).strip()

                if not title or len(title) < 10:
                    continue
                if "regulations-list" in url_path:
                    continue

                slug = url_path.split("/en/content/")[-1]
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)

                results.append(
                    {
                        "url_path": url_path,
                        "url_slug": slug,
                        "title": title,
                        "category": current_category,
                    }
                )

        return results

    # Sidebar PDF that appears on every page — must be skipped
    SKIP_PDFS = {"residential property price index"}

    def _get_download_urls(self, detail_url: str) -> Dict[str, list]:
        """Visit a regulation detail page and extract PDF/DOCX download URLs.

        Returns dict with keys 'docx' and 'pdf', each a list of URLs.
        Filters out known sidebar/unrelated PDFs.
        """
        result = {"docx": [], "pdf": []}
        try:
            resp = self.http.get(detail_url, timeout=20)
            if resp.status_code != 200:
                logger.warning(f"Detail page returned {resp.status_code}: {detail_url}")
                return result
        except Exception as e:
            logger.warning(f"Failed to fetch detail page {detail_url}: {e}")
            return result

        pattern = re.compile(r'href="([^"]+\.(pdf|docx?))"', re.IGNORECASE)
        seen = set()
        for match in pattern.finditer(resp.text):
            raw_url = match.group(1)
            ext = match.group(2).lower()
            if not raw_url.startswith("http"):
                full_url = urljoin(BASE_URL, raw_url)
            else:
                full_url = raw_url
            if full_url in seen:
                continue
            seen.add(full_url)

            # Skip known sidebar PDFs
            decoded = unquote(full_url).lower()
            if any(skip in decoded for skip in self.SKIP_PDFS):
                continue

            if ext in ("docx", "doc"):
                result["docx"].append(full_url)
            else:
                result["pdf"].append(full_url)

        return result

    def _extract_docx_text(self, docx_url: str) -> Optional[str]:
        """Download a DOCX and extract text via python-docx."""
        if docx is None:
            logger.debug("python-docx not installed — skipping DOCX extraction")
            return None
        try:
            resp = self.http.get(docx_url, timeout=60)
            if resp.status_code != 200:
                logger.warning(f"DOCX download failed: {resp.status_code} for {docx_url}")
                return None

            with tempfile.NamedTemporaryFile(suffix=".docx") as tmp:
                tmp.write(resp.content)
                tmp.flush()
                doc = docx.Document(tmp.name)
                paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
                return "\n\n".join(paragraphs) if paragraphs else None
        except Exception as e:
            logger.warning(f"DOCX extraction failed for {docx_url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        if pdfplumber is None:
            logger.error("pdfplumber not installed — cannot extract PDF text")
            return None
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning(f"PDF download failed: {resp.status_code} for {pdf_url}")
                return None

            with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
                tmp.write(resp.content)
                tmp.flush()
                with pdfplumber.open(tmp.name) as pdf:
                    pages_text = []
                    for page in pdf.pages:
                        t = page.extract_text()
                        if t:
                            pages_text.append(t)
                        # Release per-page layout + cached textmap to cap peak
                        # RSS on large PDFs (prevents OOM exit 137 on the fleet).
                        page.flush_cache()
                        try:
                            page.get_textmap.cache_clear()
                        except AttributeError:
                            pass
                    return "\n\n".join(pages_text) if pages_text else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def _extract_date_from_title(self, title: str) -> Optional[str]:
        """Try to extract a date from the regulation title."""
        # Common patterns: "No 60 of 12 March 2026", "No.328 of December 13, 2019"
        # DD.MM.YYYY
        m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", title)
        if m:
            d, mo, y = m.groups()
            return f"{y}-{mo}-{d}"

        # "of DD Month YYYY"
        months = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        m = re.search(
            r"(\d{1,2})\s+(january|february|march|april|may|june|july|august|"
            r"september|october|november|december)\s*,?\s*(\d{4})",
            title,
            re.IGNORECASE,
        )
        if m:
            d, mo_name, y = m.groups()
            mo = months[mo_name.lower()]
            return f"{y}-{mo}-{int(d):02d}"

        # "Month DD, YYYY"
        m = re.search(
            r"(january|february|march|april|may|june|july|august|"
            r"september|october|november|december)\s+(\d{1,2})\s*,?\s*(\d{4})",
            title,
            re.IGNORECASE,
        )
        if m:
            mo_name, d, y = m.groups()
            mo = months[mo_name.lower()]
            return f"{y}-{mo}-{int(d):02d}"

        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw regulation record into standard schema."""
        slug = raw.get("url_slug", "unknown")
        doc_id = f"MD-BNM-{slug[:80]}"
        date_str = self._extract_date_from_title(raw.get("title", ""))

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str or "",
            "url": f"{BASE_URL}{raw.get('url_path', '')}",
            "url_slug": slug,
            "category": raw.get("category", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all regulations from the BNM listing page."""
        logger.info("Fetching regulations list...")
        resp = self.http.get(LIST_URL, timeout=20)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch listing: {resp.status_code}")
            return

        items = self._parse_listing_page(resp.text)
        logger.info(f"Found {len(items)} regulation links")

        if not items:
            logger.error("No regulation links found — HTML structure may have changed")
            return

        count = 0
        for i, item in enumerate(items):
            detail_url = f"{BASE_URL}{item['url_path']}"
            logger.info(f"[{i+1}/{len(items)}] Visiting: {item['title'][:70]}...")

            time.sleep(1)  # rate limit

            downloads = self._get_download_urls(detail_url)
            if not downloads["docx"] and not downloads["pdf"]:
                logger.warning(f"  No download links found on detail page")
                continue

            # Try DOCX first (cleaner text), then PDF
            text = None
            used_url = None
            for docx_url in downloads["docx"]:
                text = self._extract_docx_text(docx_url)
                if text and len(text) > 200:
                    used_url = docx_url
                    break
            if not text or len(text) < 200:
                for pdf_url in downloads["pdf"]:
                    text = self._extract_pdf_text(pdf_url)
                    if text and len(text) > 200:
                        used_url = pdf_url
                        break

            if not text or len(text) < 200:
                logger.warning(f"  No usable text extracted from downloads")
                continue

            item["text"] = text
            item["pdf_url"] = used_url or ""
            record = self.normalize(item)
            yield record
            count += 1
            logger.info(f"  -> {len(text)} chars extracted")

            if sample and count >= 12:
                logger.info(f"Sample complete: {count} records")
                return

        logger.info(f"Fetch complete: {count} records")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates — same as full fetch for this small dataset."""
        yield from self.fetch_all(sample=False)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MD/BNM Bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--since", type=str, help="Date for incremental update")
    args = parser.parse_args()

    scraper = BNMScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            if args.sample:
                out_file = sample_dir / f"{count:04d}.json"
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(
                    f"[{count+1}] {record['title'][:50]} — "
                    f"{len(record.get('text',''))} chars"
                )
            else:
                print(json.dumps(record, ensure_ascii=False))
            count += 1
        logger.info(f"Done: {count} records")

    elif args.command == "update":
        since = args.since or "2024-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))
            count += 1
        logger.info(f"Update done: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
