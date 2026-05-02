#!/usr/bin/env python3
"""
MX/Banxico-Disposiciones -- Banco de México Circulars and Provisions

Fetches regulatory circulars and general provisions from Mexico's central bank
(Banco de México) with full text extracted from PDF documents.

Strategy:
  - Scrape the year-by-year listing page for all unique regulation landing pages.
  - For each landing page, find PDF links (compiled/consolidated text first).
  - Download the first (compiled) PDF and extract text via pdf_extract.

Data:
  - ~319 unique regulations from 1969 to present
  - Full text in Spanish, extracted from PDF
  - Covers: credit institutions, payment systems, FX, interest rates, etc.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental (not implemented)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.Banxico-Disposiciones")

BASE_URL = "https://www.banxico.org.mx"
YEAR_INDEX_URL = f"{BASE_URL}/marco-normativo/normativa-agrupada-por-ano-cr.html"
DELAY = 2.0


class BanxicoDisposicionesScraper(BaseScraper):
    """Scraper for MX/Banxico-Disposiciones."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
        })

    def _get_unique_landing_pages(self) -> List[Dict[str, str]]:
        """Scrape year-by-year listing to find all unique regulation landing pages."""
        logger.info("Fetching year-by-year regulation index...")
        r = self.session.get(YEAR_INDEX_URL, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        seen = set()
        pages = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if (
                "/marco-normativo/normativa-emitida-por-el-banco-de-mexico/" in href
                and href.endswith(".html")
            ):
                if href not in seen:
                    seen.add(href)
                    label = a.get_text(strip=True)
                    full_url = urljoin(BASE_URL, href)
                    # Extract slug from URL path
                    # e.g., /marco-normativo/.../circular-3-2012/operaciones-instituciones-cre.html
                    parts = href.rstrip("/").split("/")
                    slug = parts[-2] + "/" + parts[-1].replace(".html", "") if len(parts) >= 2 else href
                    pages.append({
                        "url": full_url,
                        "slug": slug,
                        "label": label,
                    })

        logger.info(f"Found {len(pages)} unique regulation pages")
        return pages

    def _get_pdf_links(self, page_url: str) -> List[Tuple[str, str]]:
        """Get PDF links from a regulation landing page. Returns [(label, url)]."""
        r = self.session.get(page_url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        pdfs = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if ".pdf" in href.lower():
                label = a.get_text(strip=True)
                full_url = urljoin(BASE_URL, href)
                pdfs.append((label, full_url))

        return pdfs

    def _extract_title_from_page(self, page_url: str, soup: BeautifulSoup = None) -> str:
        """Extract the regulation title from the landing page."""
        if soup is None:
            r = self.session.get(page_url, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

        # Try the page title
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Remove "Banco de México" suffix
            title = re.sub(r',?\s*Banco de México$', '', title)
            if title and len(title) > 5:
                return title

        # Fallback: first h1 or h2
        for tag in soup.find_all(["h1", "h2"]):
            text = tag.get_text(strip=True)
            if text and len(text) > 5:
                return text

        return ""

    def _extract_date_from_text(self, text: str) -> Optional[str]:
        """Try to extract a date from the PDF text (usually near the top)."""
        # Spanish date patterns: "Lunes 12 de febrero de 2024"
        months = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
        }
        # Match "DD de MONTH de YYYY"
        m = re.search(r'(\d{1,2})\s+de\s+(' + '|'.join(months.keys()) + r')\s+de\s+(\d{4})',
                       text[:2000], re.IGNORECASE)
        if m:
            day = int(m.group(1))
            month = months[m.group(2).lower()]
            year = m.group(3)
            return f"{year}-{month}-{day:02d}"
        return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Banxico regulations with full text from PDFs."""
        pages = self._get_unique_landing_pages()

        for i, page in enumerate(pages):
            try:
                logger.info(f"[{i+1}/{len(pages)}] Processing: {page['slug']}")
                time.sleep(DELAY)

                # Get landing page and PDF links
                r = self.session.get(page["url"], timeout=30)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")

                title = self._extract_title_from_page(page["url"], soup)
                if not title:
                    title = page["label"]

                # Find PDF links
                pdfs = []
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if ".pdf" in href.lower():
                        label = a.get_text(strip=True)
                        full_url = urljoin(BASE_URL, href)
                        pdfs.append((label, full_url))

                if not pdfs:
                    logger.warning(f"  No PDFs found for {page['slug']}, skipping")
                    continue

                # Download first (compiled/consolidated) PDF
                pdf_label, pdf_url = pdfs[0]
                logger.info(f"  Downloading PDF: {pdf_label}")
                time.sleep(DELAY)

                try:
                    pdf_resp = self.session.get(pdf_url, timeout=60)
                    pdf_resp.raise_for_status()
                except requests.RequestException as e:
                    logger.warning(f"  Failed to download PDF: {e}")
                    continue

                pdf_bytes = pdf_resp.content
                if len(pdf_bytes) < 1000:
                    logger.warning(f"  PDF too small ({len(pdf_bytes)} bytes), skipping")
                    continue

                # Extract text
                text = extract_pdf_markdown(
                    source="MX/Banxico-Disposiciones",
                    source_id=page["slug"],
                    pdf_bytes=pdf_bytes,
                    table="case_law",
                    force=True,
                )

                if not text or len(text) < 100:
                    logger.warning(f"  Insufficient text extracted ({len(text) if text else 0} chars)")
                    continue

                date = self._extract_date_from_text(text)

                yield self.normalize({
                    "slug": page["slug"],
                    "title": title,
                    "text": text,
                    "date": date,
                    "url": page["url"],
                    "pdf_url": pdf_url,
                    "pdf_count": len(pdfs),
                })

            except Exception as e:
                logger.error(f"  Error processing {page['slug']}: {e}")
                continue

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Incremental update - fetch all (small corpus, no date filtering on index)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        return {
            "_id": f"MX/Banxico-Disposiciones/{raw['slug']}",
            "_source": "MX/Banxico-Disposiciones",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "pdf_count": raw.get("pdf_count", 0),
            "regulation_slug": raw.get("slug", ""),
        }


# ── CLI ──────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="MX/Banxico-Disposiciones bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    scraper = BanxicoDisposicionesScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        r = scraper.session.get(YEAR_INDEX_URL, timeout=15)
        logger.info(f"Year index status: {r.status_code}")
        pages = scraper._get_unique_landing_pages()
        logger.info(f"Found {len(pages)} unique regulation pages")
        logger.info("Test passed!")
        return

    if args.command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if args.sample else 999999

        for record in scraper.fetch_all():
            text_len = len(record.get("text", ""))
            logger.info(
                f"  => {record['_id']} | {record.get('title', '')[:60]} | "
                f"text={text_len} chars | date={record.get('date', 'N/A')}"
            )

            # Save sample
            safe_name = re.sub(r'[^\w\-]', '_', record["regulation_slug"])[:80]
            out_path = sample_dir / f"{safe_name}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            count += 1
            if count >= limit:
                break

        logger.info(f"Done. Saved {count} records to {sample_dir}")


if __name__ == "__main__":
    main()
