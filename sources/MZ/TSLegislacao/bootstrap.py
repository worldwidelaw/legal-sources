#!/usr/bin/env python3
"""
MZ/TSLegislacao -- Mozambique Supreme Court Legislation Portal

Fetches legislation PDFs from the Tribunal Supremo de Moçambique website.
The legislation page (WordPress page ID 95) contains ~112 PDF links covering
constitutions, laws, decrees, resolutions, and directives (1975-2026).

Strategy:
  1. Fetch WP page 95 via REST API to get HTML content
  2. Parse HTML to extract PDF links and their display titles
  3. Download each PDF and extract full text via pdfplumber

Usage:
  python bootstrap.py bootstrap          # Fetch all legislation
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import io
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Tuple, Optional
from html import unescape

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MZ.TSLegislacao")

BASE_URL = "https://www.ts.gov.mz"
PAGE_ID = 95

# CID artifact pattern from PDF extraction
CID_RE = re.compile(r"\(cid:\d+\)")

MONTH_MAP = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03",
    "abril": "04", "maio": "05", "junho": "06", "julho": "07",
    "agosto": "08", "setembro": "09", "outubro": "10",
    "novembro": "11", "dezembro": "12",
}

# Document type detection
DOC_TYPE_PATTERNS = [
    (re.compile(r"Constitui[cç][aã]o", re.IGNORECASE), "constitution"),
    (re.compile(r"\bLei\b", re.IGNORECASE), "law"),
    (re.compile(r"Decreto[\s-]Lei", re.IGNORECASE), "decree-law"),
    (re.compile(r"Decreto[\s-]Presidencial", re.IGNORECASE), "presidential_decree"),
    (re.compile(r"\bDecreto\b", re.IGNORECASE), "decree"),
    (re.compile(r"Resolu[cç][aã]o", re.IGNORECASE), "resolution"),
    (re.compile(r"Direct?iva", re.IGNORECASE), "directive"),
    (re.compile(r"Despacho", re.IGNORECASE), "dispatch"),
    (re.compile(r"Circular", re.IGNORECASE), "circular"),
    (re.compile(r"Diploma\s+Ministerial", re.IGNORECASE), "ministerial_diploma"),
    (re.compile(r"Aviso", re.IGNORECASE), "notice"),
    (re.compile(r"Regulamento", re.IGNORECASE), "regulation"),
    (re.compile(r"Estatuto", re.IGNORECASE), "statute"),
]

# Law number extraction
LAW_NUM_RE = re.compile(
    r"(?:Lei|Decreto|Resolu[cç][aã]o|Despacho|Direct?iva)\s+(?:n[.ºo°]*\s*)?(\d+[\-/]\d{2,4})",
    re.IGNORECASE,
)

TAG_RE = re.compile(r"<[^>]+>")


def _extract_date(title: str, pdf_url: str) -> Optional[str]:
    """Extract a date from the title or fall back to upload path year."""
    month_re = re.compile(
        r"(\d{1,2})\s+de\s+(Janeiro|Fevereiro|Mar[cç\u00e7]o|Abril|Maio|Junho|"
        r"Julho|Agosto|Setembro|Outubro|Novembro|Dezembro)",
        re.IGNORECASE,
    )

    # Try full date: "NN de Month de YYYY"
    full_re = re.compile(
        r"(\d{1,2})\s+de\s+(Janeiro|Fevereiro|Mar[cç\u00e7]o|Abril|Maio|Junho|"
        r"Julho|Agosto|Setembro|Outubro|Novembro|Dezembro)\s+de\s+(\d{4})",
        re.IGNORECASE,
    )
    m = full_re.search(title)
    if m:
        day = int(m.group(1))
        month = MONTH_MAP.get(m.group(2).lower().replace("\u00e7", "ç"), "01")
        year = m.group(3)
        return f"{year}-{month}-{day:02d}"

    # Try partial date with year from law number: "N/YYYY, de DD de Month"
    m = month_re.search(title)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower().replace("\u00e7", "ç")
        month = MONTH_MAP.get(month_name, "01")
        # Extract year from law number pattern like "4/2024" or "15/2023"
        year_m = re.search(r"/(\d{4})", title)
        if year_m:
            year = year_m.group(1)
            if 1900 <= int(year) <= 2030:
                return f"{year}-{month}-{day:02d}"

    # Try year-only from "de YYYY" pattern
    year_m = re.search(r"de\s+(\d{4})", title)
    if year_m:
        year = year_m.group(1)
        if 1900 <= int(year) <= 2030:
            return f"{year}-01-01"

    # Fall back to upload path year: /uploads/2023/09/...
    m = re.search(r"/uploads/(\d{4})/(\d{2})/", pdf_url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"

    return None


def _detect_doc_type(title: str) -> str:
    """Detect document type from title."""
    for pattern, doc_type in DOC_TYPE_PATTERNS:
        if pattern.search(title):
            return doc_type
    return "legislation"


def _extract_doc_number(title: str) -> str:
    """Extract law/decree number from title."""
    m = LAW_NUM_RE.search(title)
    if m:
        return m.group(1).strip()
    return ""


class MZTSLegislacaoScraper(BaseScraper):
    """Scraper for MZ/TSLegislacao -- Mozambique legislation portal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

    def _fetch_pdf_links(self) -> List[Tuple[str, str]]:
        """Fetch the legislation page and extract PDF links with titles."""
        resp = self.session.get(
            f"{BASE_URL}/wp-json/wp/v2/pages/{PAGE_ID}",
            timeout=60,
        )
        resp.raise_for_status()
        page = resp.json()
        html = page.get("content", {}).get("rendered", "")

        # Extract <a href="...pdf">Title</a> pairs
        pattern = r'<a\s+[^>]*href="([^"]*\.pdf)"[^>]*>(.*?)</a>'
        links = re.findall(pattern, html, re.DOTALL)

        # Deduplicate by normalized URL
        seen = set()
        unique = []
        for url, text in links:
            norm_url = url.replace("http://", "https://")
            if norm_url not in seen:
                seen.add(norm_url)
                clean_text = TAG_RE.sub("", text).strip()
                clean_text = unescape(clean_text)
                unique.append((norm_url, clean_text))

        logger.info(f"Found {len(unique)} unique PDF links on legislation page")
        return unique

    def _download_pdf_text(self, url: str) -> str:
        """Download a PDF and extract text using pdfplumber."""
        if not url:
            return ""
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=90)
                if resp.status_code != 200:
                    logger.warning(f"PDF download HTTP {resp.status_code}: {url}")
                    return ""
                if len(resp.content) > 50_000_000:
                    logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
                    return ""
                with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                    pages_text = []
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            pages_text.append(text)
                        try:
                            page.flush_cache(); page.get_textmap.cache_clear()
                        except Exception:
                            pass
                    full_text = "\n\n".join(pages_text)
                    # Clean up CID artifacts and excessive whitespace
                    full_text = CID_RE.sub("", full_text)
                    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
                    return full_text.strip()
            except Exception as e:
                logger.warning(f"PDF extraction attempt {attempt + 1}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        title = raw.get("title", "")
        pdf_url = raw.get("url", "")
        text = raw.get("_full_text", "")

        # Generate stable ID from URL hash
        url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:12]
        doc_id = f"MZ/TSLegislacao/{url_hash}"

        date = _extract_date(title, pdf_url)
        doc_type = _detect_doc_type(title)
        doc_number = _extract_doc_number(title)

        return {
            "_id": doc_id,
            "_source": "MZ/TSLegislacao",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url,
            "doc_id": url_hash,
            "doc_type": doc_type,
            "doc_number": doc_number,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        try:
            pdf_links = self._fetch_pdf_links()
        except Exception as e:
            logger.error(f"Failed to fetch legislation page: {e}")
            return

        for pdf_url, title in pdf_links:
            if limit and count >= limit:
                break

            logger.info(f"Downloading [{count + 1}]: {title[:70]}")
            text = self._download_pdf_text(pdf_url)

            if len(text) < 100:
                logger.warning(f"  Skipping: text too short ({len(text)} chars) for: {title[:60]}")
                continue

            raw = {
                "title": title,
                "url": pdf_url,
                "_full_text": text,
            }
            yield raw
            count += 1
            logger.info(f"  OK: {len(text)} chars")

        logger.info(f"Fetched {count} legislation documents total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # Re-fetch all — the page is small enough that differential isn't needed
        yield from self.fetch_all(sample=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = MZTSLegislacaoScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
