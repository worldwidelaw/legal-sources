#!/usr/bin/env python3
"""
CL/TDPI -- Tribunal de Propiedad Industrial (Chile IP Court)

Fetches patent rulings and trademark jurisprudence bulletins from TDPI.

Sources:
  - Patent rulings: https://www.tdpi.cl/fallos-relevantes-de-patentes/
    ~102 individual PDF case decisions (2015-2026)
  - Trademark bulletins: https://www.tdpi.cl/category/documentos/boletin-de-jurisprudencia-marcaria/
    ~21+ quarterly PDF bulletins (2021-2026)

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Tuple
from urllib.parse import urljoin

import requests
import pdfplumber
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.TDPI")

SOURCE_ID = "CL/TDPI"
PATENT_URL = "https://www.tdpi.cl/fallos-relevantes-de-patentes/"
TRADEMARK_BASE = "https://www.tdpi.cl/category/documentos/boletin-de-jurisprudencia-marcaria/"

ROL_PATTERN = re.compile(r"ROL[- ](?:TdPI|TDPI)[- ](?:N[°º]?\s*)?(\d{3,4}-\d{4})", re.IGNORECASE)
DATE_PATTERN = re.compile(r"Elaborada[- ](\d{2}-\d{2}-\d{4})")


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        pdf = pdfplumber.open(io.BytesIO(content))
        pages_text = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages_text.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        pdf.close()
        return "\n\n".join(pages_text)
    except Exception as e:
        logger.warning("PDF extraction failed: %s", e)
        return ""


def _parse_date_from_filename(filename: str) -> str:
    """Extract date from filename like 'Elaborada-12-05-2026'."""
    m = DATE_PATTERN.search(filename)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%d-%m-%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def _parse_rol_from_text(text: str) -> str:
    """Extract ROL case number from filename or text."""
    m = ROL_PATTERN.search(text)
    return f"TDPI-{m.group(1)}" if m else None


class TDPIScraper(BaseScraper):
    def __init__(self):
        super().__init__(str(Path(__file__).resolve().parent))
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (academic research)",
        })

    def _get_page(self, url: str) -> BeautifulSoup:
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")

    def _collect_patent_pdfs(self) -> List[Tuple[str, str]]:
        """Scrape patent rulings page for PDF links. Returns [(url, title), ...]."""
        logger.info("Collecting patent ruling PDFs from %s", PATENT_URL)
        soup = self._get_page(PATENT_URL)
        results = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf") and "wp-content/uploads" in href:
                url = href if href.startswith("http") else urljoin(PATENT_URL, href)
                title = a.get_text(strip=True) or Path(href).stem
                results.append((url, title))
        logger.info("Found %d patent ruling PDFs", len(results))
        return results

    def _collect_trademark_pdfs(self) -> List[Tuple[str, str]]:
        """Scrape trademark bulletin pages for PDF links."""
        logger.info("Collecting trademark bulletin PDFs")
        results = []
        page_url = TRADEMARK_BASE

        for page_num in range(1, 35):
            try:
                soup = self._get_page(page_url)
            except Exception as e:
                logger.warning("Failed to fetch trademark page %d: %s", page_num, e)
                break

            found_any = False
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if not a:
                    continue
                post_url = a["href"]
                title = a.get_text(strip=True)
                if not title:
                    continue
                # fetch the post page to find the PDF link
                results.append((post_url, title))
                found_any = True

            if not found_any:
                break

            # Find next page link
            next_link = soup.find("a", class_="next")
            if not next_link or not next_link.get("href"):
                break
            page_url = next_link["href"]
            time.sleep(1)

        logger.info("Found %d trademark bulletin posts", len(results))
        return results

    def _get_pdf_from_post(self, post_url: str) -> str:
        """Extract PDF URL from a WordPress post page."""
        try:
            soup = self._get_page(post_url)
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.lower().endswith(".pdf") and "wp-content/uploads" in href:
                    return href if href.startswith("http") else urljoin(post_url, href)
        except Exception as e:
            logger.warning("Failed to get PDF from post %s: %s", post_url, e)
        return None

    def _download_pdf(self, url: str) -> bytes:
        """Download PDF content."""
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.content

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        # Phase 1: Patent rulings (individual cases)
        patent_pdfs = self._collect_patent_pdfs()
        for i, (pdf_url, link_text) in enumerate(patent_pdfs):
            time.sleep(1.5)
            try:
                content = self._download_pdf(pdf_url)
            except Exception as e:
                logger.warning("Failed to download patent PDF %s: %s", pdf_url, e)
                continue

            text = _extract_pdf_text(content)
            if not text or len(text) < 50:
                logger.warning("Insufficient text from %s (%d chars)", pdf_url, len(text))
                continue

            filename = Path(pdf_url).stem
            rol = _parse_rol_from_text(filename) or _parse_rol_from_text(text)
            date = _parse_date_from_filename(filename)

            yield {
                "doc_type": "patent_ruling",
                "pdf_url": pdf_url,
                "link_text": link_text,
                "filename": filename,
                "rol": rol,
                "date": date,
                "text": text,
                "pdf_size": len(content),
            }

            if (i + 1) % 10 == 0:
                logger.info("Patent progress: %d/%d", i + 1, len(patent_pdfs))

        # Phase 2: Trademark bulletins
        trademark_posts = self._collect_trademark_pdfs()
        for i, (post_url, title) in enumerate(trademark_posts):
            time.sleep(1.5)

            pdf_url = self._get_pdf_from_post(post_url)
            if not pdf_url:
                logger.warning("No PDF found in post: %s", post_url)
                continue

            time.sleep(1)
            try:
                content = self._download_pdf(pdf_url)
            except Exception as e:
                logger.warning("Failed to download trademark PDF %s: %s", pdf_url, e)
                continue

            text = _extract_pdf_text(content)
            if not text or len(text) < 50:
                logger.warning("Insufficient text from %s (%d chars)", pdf_url, len(text))
                continue

            filename = Path(pdf_url).stem

            yield {
                "doc_type": "trademark_bulletin",
                "pdf_url": pdf_url,
                "link_text": title,
                "filename": filename,
                "rol": None,
                "date": None,
                "text": text,
                "pdf_size": len(content),
            }

            if (i + 1) % 10 == 0:
                logger.info("Trademark progress: %d/%d", i + 1, len(trademark_posts))

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_type = raw.get("doc_type", "patent_ruling")
        filename = raw.get("filename", "")
        link_text = raw.get("link_text", "")
        rol = raw.get("rol")

        if doc_type == "patent_ruling":
            title = link_text or filename.replace("-", " ")
            if rol:
                title = f"ROL {rol}: {title}"
            _id = rol or filename
        else:
            title = link_text or filename.replace("-", " ")
            _id = f"bulletin-{filename}"

        return {
            "_id": _id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "doc_type": doc_type,
            "rol": rol,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = TDPIScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
