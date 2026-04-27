#!/usr/bin/env python3
"""
PA/DGI-TaxGuidance -- Panama DGI Tax Resolutions and Guidance

Fetches tax resolutions, decrees, laws, e-invoicing normativa, and legal
opinions (consultas juridicas) from Panama's Direccion General de Ingresos.

Strategy:
  1. Parse listing pages (R1.php, D1.php, L1.php, Blegales.php) for PDF links
  2. Parse Consultas Juridicas 2020-2024 page for PDF-based legal opinions
  3. Download each PDF and extract full text via common.pdf_extract

Coverage:
  - Tax resolutions (Resoluciones)
  - Executive decrees (Decretos Ejecutivos)
  - Tax laws (Leyes)
  - E-invoicing normativa (Factura Electronica)
  - Legal opinions 2020-2024 (Consultas Juridicas)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test-api              # Test connectivity
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

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
logger = logging.getLogger("legal-data-hunter.PA.DGI-TaxGuidance")

BASE_URL = "https://dgi.mef.gob.pa"

# Listing pages with their categories
LISTING_PAGES = [
    {"path": "/LDRd/R1.php", "category": "resoluciones", "doc_type": "resolution"},
    {"path": "/LDRd/D1.php", "category": "decretos", "doc_type": "decree"},
    {"path": "/LDRd/L1.php", "category": "leyes", "doc_type": "law"},
    {"path": "/_7FacturaElectronica/Blegales.php", "category": "factura_electronica", "doc_type": "resolution"},
]

# Consultas Juridicas page (2020-2024 with PDF documents)
CONSULTAS_URL = f"{BASE_URL}/Juridico/consulta_juridica/html-generado/consultajuridica2024.php"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
    "Accept": "text/html,application/xhtml+xml",
}


class DGITaxGuidanceScraper(BaseScraper):
    """Scraper for Panama DGI tax resolutions and guidance."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _extract_pdfs_from_listing(self, page_info: dict) -> Generator[dict, None, None]:
        """Parse a listing page and yield PDF metadata."""
        url = BASE_URL + page_info["path"]
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find all PDF links
        seen_urls = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith(".pdf"):
                continue

            # Resolve relative URLs
            pdf_url = urljoin(url, href)

            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            # Extract title from link text or filename
            link_text = a.get_text(strip=True)
            # Use filename as title if link text is generic
            generic_texts = {"ver documento", "ver", "descargar", "pdf", "enlace", "link", ""}
            if not link_text or link_text.lower().strip() in generic_texts or len(link_text) < 5:
                filename = unquote(pdf_url.split("/")[-1])
                link_text = filename.replace(".pdf", "").replace("-", " ").replace("_", " ").strip()

            # Try to extract date from filename or link text
            date = self._extract_date(pdf_url, link_text)

            yield {
                "title": link_text,
                "pdf_url": pdf_url,
                "date": date,
                "category": page_info["category"],
                "doc_type": page_info["doc_type"],
                "source_page": url,
            }

    def _extract_consultas(self) -> Generator[dict, None, None]:
        """Parse the Consultas Juridicas 2020-2024 page for PDF opinions."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(CONSULTAS_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch consultas page: {e}")
            return

        soup = BeautifulSoup(resp.text, "html.parser")

        # The page has a table with columns: TEMA, SUBTEMA, NUMERO, FECHA, ANO, FECHA DOC, Enlace
        table = soup.find("table")
        if not table:
            logger.warning("No table found on consultas page")
            return

        rows = table.find_all("tr")
        seen_urls = set()

        for row in rows[1:]:  # Skip header
            cells = row.find_all("td")
            if len(cells) < 6:
                continue

            tema = cells[0].get_text(strip=True) if len(cells) > 0 else ""
            subtema = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            numero = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            fecha_recep = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            ano = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            fecha_doc = cells[5].get_text(strip=True) if len(cells) > 5 else ""

            # Find PDF link in the last cell (Enlace) — links use onClick="window.open('...')"
            link_cell = cells[-1] if len(cells) > 6 else cells[-1]
            a_tag = link_cell.find("a")
            if not a_tag:
                continue

            # Extract PDF path from onClick attribute
            onclick = a_tag.get("onclick", "") or a_tag.get("onClick", "")
            pdf_path = None
            if onclick:
                m = re.search(r"window\.open\('([^']+\.pdf)'", onclick, re.IGNORECASE)
                if m:
                    pdf_path = m.group(1)

            # Fallback to href if onClick didn't work
            if not pdf_path:
                href = a_tag.get("href", "")
                if href and href != "#" and href.lower().endswith(".pdf"):
                    pdf_path = href

            if not pdf_path:
                continue

            pdf_url = urljoin(CONSULTAS_URL, pdf_path)
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            # Build title from metadata
            title = f"Consulta Juridica {numero}" if numero else f"Consulta {tema}"
            if subtema:
                title += f" - {subtema}"

            # Parse date
            date = self._parse_date_string(fecha_doc) or self._parse_date_string(fecha_recep)

            yield {
                "title": title,
                "pdf_url": pdf_url,
                "date": date,
                "category": "consultas_juridicas",
                "doc_type": "opinion",
                "tema": tema,
                "subtema": subtema,
                "numero": numero,
                "source_page": CONSULTAS_URL,
            }

    def _extract_date(self, url: str, text: str) -> Optional[str]:
        """Try to extract a date from URL or text."""
        # Pattern: YYYY in filename or text
        combined = url + " " + text

        # Try DD-month-YYYY patterns (e.g., "21-mayo-2021")
        months_es = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
        }
        for month_name, month_num in months_es.items():
            pattern = rf"(\d{{1,2}})-?{month_name}-?(\d{{4}})"
            m = re.search(pattern, combined, re.IGNORECASE)
            if m:
                day = m.group(1).zfill(2)
                year = m.group(2)
                return f"{year}-{month_num}-{day}"

        # Try just YYYY
        m = re.search(r"(20[12]\d)", combined)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def _parse_date_string(self, s: str) -> Optional[str]:
        """Parse various date formats to ISO."""
        if not s:
            return None
        s = s.strip()

        # Try DD/MM/YYYY
        m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
        if m:
            return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

        # Try YYYY-MM-DD
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
        if m:
            return s

        # Try DD-MM-YYYY
        m = re.match(r"(\d{1,2})-(\d{1,2})-(\d{4})", s)
        if m:
            return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

        return None

    def _make_id(self, raw: dict) -> str:
        """Generate a unique ID from the PDF URL."""
        pdf_url = raw.get("pdf_url", "")
        # Use the filename as base for ID
        filename = unquote(pdf_url.split("/")[-1]).replace(".pdf", "")
        # Clean for ID
        clean = re.sub(r"[^a-zA-Z0-9_-]", "_", filename)
        clean = re.sub(r"_+", "_", clean).strip("_")
        return f"PA_DGI_{raw['category']}_{clean}"[:200]

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all DGI tax guidance documents."""
        # 1. Listing pages (Resoluciones, Decretos, Leyes, FE)
        for page_info in LISTING_PAGES:
            logger.info(f"Parsing {page_info['category']} from {page_info['path']}...")
            count = 0
            for raw in self._extract_pdfs_from_listing(page_info):
                count += 1
                # Download PDF and extract text
                self.rate_limiter.wait()
                doc_id = self._make_id(raw)
                text = extract_pdf_markdown(
                    source="PA/DGI-TaxGuidance",
                    source_id=doc_id,
                    pdf_url=raw["pdf_url"],
                    table="doctrine",
                )
                if text and len(text) >= 50:
                    raw["text"] = text
                    yield raw
                else:
                    logger.debug(f"  Skipping {raw['title'][:60]} — no text extracted")
                time.sleep(1)
            logger.info(f"  {page_info['category']}: {count} PDFs found")

        # 2. Consultas Juridicas (2020-2024)
        logger.info("Parsing Consultas Juridicas 2020-2024...")
        consulta_count = 0
        for raw in self._extract_consultas():
            consulta_count += 1
            self.rate_limiter.wait()
            doc_id = self._make_id(raw)
            text = extract_pdf_markdown(
                source="PA/DGI-TaxGuidance",
                source_id=doc_id,
                pdf_url=raw["pdf_url"],
                table="doctrine",
            )
            if text and len(text) >= 50:
                raw["text"] = text
                yield raw
            else:
                logger.debug(f"  Skipping consulta {raw['title'][:60]} — no text extracted")
            time.sleep(1)
        logger.info(f"  Consultas: {consulta_count} PDFs found")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since a date."""
        for raw in self.fetch_all():
            date_str = raw.get("date", "")
            if date_str:
                try:
                    doc_date = datetime.fromisoformat(date_str)
                    if doc_date.replace(tzinfo=None) >= since.replace(tzinfo=None):
                        yield raw
                except (ValueError, TypeError):
                    yield raw
            else:
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw DGI record into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        _id = self._make_id(raw)

        return {
            "_id": _id,
            "_source": "PA/DGI-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "category": raw.get("category"),
            "doc_type": raw.get("doc_type"),
            "tema": raw.get("tema"),
            "subtema": raw.get("subtema"),
            "numero": raw.get("numero"),
            "language": "es",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="PA/DGI-TaxGuidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = DGITaxGuidanceScraper()

    if args.command == "test-api":
        logger.info("Testing DGI connectivity...")
        try:
            resp = scraper.session.get(f"{BASE_URL}/LDRd/R1.php", timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            pdf_links = [a for a in soup.find_all("a", href=True) if a["href"].lower().endswith(".pdf")]
            logger.info(f"OK: {len(pdf_links)} PDF links found on Resoluciones page")
            if pdf_links:
                logger.info(f"Sample: {pdf_links[0].get_text(strip=True)[:80]}")
        except Exception as e:
            logger.error(f"FAIL: {e}")
            sys.exit(1)
    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.run_sample(n=15)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
