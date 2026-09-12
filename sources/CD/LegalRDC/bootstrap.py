#!/usr/bin/env python3
"""
CD/LegalRDC -- DRC Court Decisions from legalrdc.com

Fetches case law (CCJA, Cour de Cassation, Conseil d'État, Cour Constitutionnelle)
via the WordPress REST API, downloads attached PDFs, and extracts full text.

Strategy:
  - Query WP REST API for posts in jurisprudence categories
  - Extract PDF download URL from post content HTML
  - Download PDF and extract full text with pdfplumber
  - Parse metadata (court, case number, date) from post title

Usage:
  python bootstrap.py bootstrap          # Fetch all ~242 decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import tempfile
import unicodedata
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CD.LegalRDC")

API_BASE = "https://legalrdc.com/wp-json/wp/v2/posts"

# Jurisprudence category IDs from the WP taxonomy
# 89=CCJA Arrêts, 90=CCJA Ordonnances, 91=CCJA Règlements,
# 97=Cour de Cassation Arrêts, 92=Conseil d'État Arrêts,
# 93=Conseil d'État Ordonnances, 94=Cour Constitutionnelle,
# 95=CC Arrêts, 96=CC Ordonnances
JURISPRUDENCE_CATEGORIES = "89,90,91,97,92,93,94,95,96"

# Map category IDs to court names
COURT_MAP = {
    89: "CCJA",
    90: "CCJA",
    91: "CCJA",
    3: "CCJA",
    97: "Cour de Cassation",
    5: "Cour de Cassation",
    92: "Conseil d'État",
    93: "Conseil d'État",
    4: "Conseil d'État",
    94: "Cour Constitutionnelle",
    95: "Cour Constitutionnelle",
    96: "Cour Constitutionnelle",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


class LegalRDCScraper(BaseScraper):
    """Scraper for CD/LegalRDC -- DRC court decisions via WordPress API + PDF."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _request(self, url: str, timeout: int = 60, **kwargs) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout, **kwargs)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _extract_pdf_url(self, content_html: str) -> Optional[str]:
        """Extract PDF download URL from post content HTML."""
        # Pattern 1: wp-block-file download link
        match = re.search(r'href="([^"]+\.pdf)"', content_html, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text with pdfplumber."""
        resp = self._request(pdf_url, timeout=120, stream=True)
        if resp is None:
            return None

        try:
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
                for chunk in resp.iter_content(chunk_size=65536):
                    tmp.write(chunk)
                tmp.flush()

                pages_text = []
                with pdfplumber.open(tmp.name) as pdf:
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            pages_text.append(text)
                        try:
                            page.flush_cache(); page.get_textmap.cache_clear()
                        except Exception:
                            pass

                return "\n\n".join(pages_text) if pages_text else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url[:80]}: {e}")
            return None

    def _determine_court(self, categories: List[int]) -> str:
        """Determine court name from WP category IDs."""
        for cat_id in categories:
            if cat_id in COURT_MAP:
                return COURT_MAP[cat_id]
        return "Unknown"

    def _parse_case_number(self, title: str) -> Optional[str]:
        """Extract case/arrêt number from title."""
        # Normalize Unicode to precomposed form (NFC) for consistent matching
        t = unicodedata.normalize("NFC", title)
        # e.g. "arrêt n° 001/2021" or "arrêt RP 0001" or "RConst 1550"
        patterns = [
            r'(?:arrêt|arret)\s+(?:n°?\s*)?(\S+(?:/\d{4})?)',
            r'(R(?:Const|P|CE)\s*\d+(?:/\d+)*)',
        ]
        for pat in patterns:
            m = re.search(pat, t, re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None

    def _parse_date_from_title(self, title: str) -> Optional[str]:
        """Try to extract date from title like 'du 14 janvier 2021'."""
        t = unicodedata.normalize("NFC", title)
        match = re.search(
            r'du\s+(\d{1,2})\s+(\w+)\s+(\d{4})', t, re.IGNORECASE
        )
        if match:
            day, month_str, year = match.groups()
            month_map = {
                "janvier": "01", "fevrier": "02", "février": "02", "mars": "03",
                "avril": "04", "mai": "05", "juin": "06", "juillet": "07",
                "aout": "08", "août": "08", "septembre": "09", "octobre": "10",
                "novembre": "11", "decembre": "12", "décembre": "12",
            }
            month = month_map.get(month_str.lower())
            if month:
                return f"{year}-{month}-{day.zfill(2)}"
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw WP post + extracted text into standard schema."""
        title = unicodedata.normalize("NFC", raw.get("title", ""))
        court = raw.get("court", "Unknown")
        case_number = self._parse_case_number(title)
        date = raw.get("parsed_date") or raw.get("date", "")[:10]

        doc_id = f"CD-LegalRDC-{raw['id']}"

        return {
            "_id": doc_id,
            "_source": "CD/LegalRDC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("link", ""),
            "court": court,
            "case_number": case_number,
            "language": "fr",
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all jurisprudence decisions via WP API + PDF extraction."""
        max_records = 15 if sample else None
        page = 1
        per_page = 20
        fetched = 0

        while True:
            params = {
                "categories": JURISPRUDENCE_CATEGORIES,
                "per_page": per_page,
                "page": page,
                "_fields": "id,title,content,date,link,categories",
            }

            resp = self._request(f"{API_BASE}?{self._encode_params(params)}")
            if resp is None:
                logger.error(f"Failed to fetch page {page}")
                break

            try:
                posts = resp.json()
            except Exception:
                logger.error(f"Invalid JSON on page {page}")
                break

            if not posts:
                break

            for post in posts:
                post_id = post["id"]
                title = post.get("title", {}).get("rendered", "")
                content_html = post.get("content", {}).get("rendered", "")
                categories = post.get("categories", [])
                post_date = post.get("date", "")
                link = post.get("link", "")

                # Extract PDF URL
                pdf_url = self._extract_pdf_url(content_html)
                if not pdf_url:
                    logger.warning(f"No PDF found for post {post_id}: {title[:60]}")
                    continue

                # Extract text from PDF
                logger.info(f"Extracting PDF for: {title[:60]}...")
                text = self._extract_text_from_pdf(pdf_url)
                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text from PDF for post {post_id}")
                    continue

                court = self._determine_court(categories)
                parsed_date = self._parse_date_from_title(title) or post_date[:10]

                raw = {
                    "id": post_id,
                    "title": title,
                    "text": text,
                    "date": post_date,
                    "parsed_date": parsed_date,
                    "link": link,
                    "court": court,
                    "pdf_url": pdf_url,
                }

                yield self.normalize(raw)
                fetched += 1

                if max_records and fetched >= max_records:
                    logger.info(f"Sample limit reached ({max_records} records)")
                    return

            # Check if more pages
            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            page += 1

        logger.info(f"Fetched {fetched} records total")

    def _encode_params(self, params: Dict[str, Any]) -> str:
        """Encode query parameters."""
        from urllib.parse import urlencode
        return urlencode(params)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch posts modified after a given date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        # WP API supports 'after' parameter for date filtering
        page = 1
        per_page = 20

        while True:
            params = {
                "categories": JURISPRUDENCE_CATEGORIES,
                "per_page": per_page,
                "page": page,
                "after": since + "T00:00:00",
                "_fields": "id,title,content,date,link,categories",
                "orderby": "date",
                "order": "desc",
            }

            resp = self._request(f"{API_BASE}?{self._encode_params(params)}")
            if resp is None or not resp.json():
                break

            posts = resp.json()
            for post in posts:
                post_id = post["id"]
                title = post.get("title", {}).get("rendered", "")
                content_html = post.get("content", {}).get("rendered", "")
                categories = post.get("categories", [])
                post_date = post.get("date", "")
                link = post.get("link", "")

                pdf_url = self._extract_pdf_url(content_html)
                if not pdf_url:
                    continue

                text = self._extract_text_from_pdf(pdf_url)
                if not text or len(text) < 100:
                    continue

                court = self._determine_court(categories)
                parsed_date = self._parse_date_from_title(title) or post_date[:10]

                raw = {
                    "id": post_id,
                    "title": title,
                    "text": text,
                    "date": post_date,
                    "parsed_date": parsed_date,
                    "link": link,
                    "court": court,
                    "pdf_url": pdf_url,
                }

                yield self.normalize(raw)

            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            page += 1

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(f"{API_BASE}?per_page=1&categories={JURISPRUDENCE_CATEGORIES}")
        if resp and resp.status_code == 200:
            data = resp.json()
            if data:
                logger.info(f"API OK — {resp.headers.get('X-WP-Total', '?')} total posts")
                return True
        logger.error("API connectivity test failed")
        return False


def main():
    scraper = LegalRDCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample = "--sample" in sys.argv
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=sample):
            count += 1
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"[{count}] Saved: {record['title'][:60]}")

        logger.info(f"Done. {count} records saved to {sample_dir}")

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
