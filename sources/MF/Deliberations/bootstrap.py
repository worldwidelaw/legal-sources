#!/usr/bin/env python3
"""
MF/Deliberations -- Saint-Martin Territorial Council Deliberations

Fetches deliberations from the Collectivité de Saint-Martin via the
Prismic CMS REST API. Each deliberation includes a PDF with the full
text of the official decision.

Endpoint: comsaintmartin.cdn.prismic.io/api/v2
Document type: deliberation_page
Total: ~2,675 deliberations (executive + territorial council)

Usage:
  python bootstrap.py bootstrap          # Fetch all deliberations
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import shutil
import subprocess
import sys
import time
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MF.Deliberations")

PRISMIC_API = "https://comsaintmartin.cdn.prismic.io/api/v2"
PAGE_SIZE = 100

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


class DeliberationsScraper(BaseScraper):
    """Scraper for MF/Deliberations -- Saint-Martin council deliberations."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._master_ref = None

    def _get_master_ref(self) -> str:
        """Fetch the current Prismic master ref."""
        if self._master_ref:
            return self._master_ref
        resp = self.session.get(PRISMIC_API, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        for ref in data.get("refs", []):
            if ref.get("isMasterRef"):
                self._master_ref = ref["ref"]
                logger.info(f"Prismic master ref: {self._master_ref}")
                return self._master_ref
        raise RuntimeError("No master ref found in Prismic API")

    def _search_documents(self, page: int = 1) -> Dict[str, Any]:
        """Query Prismic for deliberation_page documents."""
        ref = self._get_master_ref()
        params = {
            "ref": ref,
            "q": '[[at(document.type,"deliberation_page")]]',
            "pageSize": PAGE_SIZE,
            "page": page,
            "orderings": "[document.first_publication_date desc]",
        }
        for attempt in range(3):
            try:
                time.sleep(0.5)
                resp = self.session.get(
                    f"{PRISMIC_API}/documents/search",
                    params=params,
                    timeout=30,
                )
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"API attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(3 * (attempt + 1))
        return {"results": [], "total_pages": 0}

    def _extract_pdf_url(self, doc: Dict[str, Any]) -> Optional[str]:
        """Extract the main deliberation PDF URL from slices2."""
        data = doc.get("data", {})
        for s in data.get("slices2", []):
            if s.get("variation") == "deliberationPdf":
                for item in s.get("items", []):
                    link = item.get("link", {})
                    if link.get("url") and link.get("kind") == "file":
                        return link["url"]
        # Fallback: any PDF in slices2
        for s in data.get("slices2", []):
            for item in s.get("items", []):
                link = item.get("link", {})
                url = link.get("url", "")
                if url.endswith(".pdf") and link.get("kind") == "file":
                    return url
        return None

    def _extract_council_name(self, doc: Dict[str, Any]) -> str:
        """Extract council type from linked conseil_ct."""
        data = doc.get("data", {})
        link = data.get("conseil_link", {})
        slug = link.get("slug", "")
        if "executif" in slug:
            return "Conseil Exécutif"
        elif "territorial" in slug:
            return "Conseil Territorial"
        return slug.replace("-", " ").title() if slug else ""

    def _extract_department(self, doc: Dict[str, Any]) -> str:
        """Extract department/direction name from linked direction_ct."""
        data = doc.get("data", {})
        link = data.get("direction_link", {})
        slug = link.get("slug", "")
        return slug.replace("-", " ").title() if slug else ""

    def _ocr_pdf(self, pdf_bytes: bytes) -> Optional[str]:
        """Extract text from scanned PDF via OCR (fitz render + tesseract)."""
        try:
            import fitz
        except ImportError:
            logger.warning("PyMuPDF (fitz) not installed — cannot OCR")
            return None

        tesseract_cmd = shutil.which("tesseract")
        if not tesseract_cmd:
            logger.warning("tesseract not found in PATH — cannot OCR")
            return None

        # Detect available languages
        ocr_lang = "eng"
        try:
            result = subprocess.run(
                [tesseract_cmd, "--list-langs"],
                capture_output=True, text=True, timeout=5,
            )
            if "fra" in result.stdout:
                ocr_lang = "fra+eng"
        except Exception:
            pass

        try:
            from PIL import Image
            import pytesseract
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
        except ImportError:
            logger.warning("pytesseract/Pillow not installed — cannot OCR")
            return None

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        parts = []
        for i in range(len(doc)):
            page = doc[i]
            mat = fitz.Matrix(2, 2)  # 2x zoom for OCR quality
            pix = page.get_pixmap(matrix=mat)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            text = pytesseract.image_to_string(img, lang=ocr_lang)
            if text and text.strip():
                parts.append(text.strip())
        doc.close()
        return "\n\n".join(parts) if parts else None

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text, trying text extraction then OCR."""
        try:
            resp = self.session.get(pdf_url, timeout=60)
            resp.raise_for_status()
            pdf_bytes = resp.content
        except Exception as e:
            logger.warning(f"PDF download failed: {e}")
            return None

        # Try standard text extraction first (for non-scanned PDFs)
        try:
            from common.pdf_extract import _extract
            text = _extract(pdf_bytes)
            if text and len(text.strip()) > 100 and "(cid:" not in text:
                return text.strip()
        except Exception:
            pass

        # Fall back to OCR for scanned PDFs
        return self._ocr_pdf(pdf_bytes)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title = raw.get("title", "")
        pub_date = raw.get("publication")
        if not pub_date:
            pub_date = raw.get("first_publication_date", "")[:10]

        return {
            "_id": raw.get("document_id", ""),
            "_source": "MF/Deliberations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": pub_date or None,
            "url": raw.get("url", ""),
            "council": raw.get("council", ""),
            "department": raw.get("department", ""),
            "is_annulled": raw.get("is_annulled", False),
            "is_modified": raw.get("is_modified", False),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all deliberations from Prismic API."""
        page = 1
        total = 0
        skipped = 0

        while True:
            logger.info(f"Fetching page {page}...")
            result = self._search_documents(page)
            docs = result.get("results", [])
            total_pages = result.get("total_pages", 0)

            if not docs:
                break

            for doc in docs:
                data = doc.get("data", {})
                doc_id = doc.get("id", "")
                title = data.get("titre", "")
                pdf_url = self._extract_pdf_url(doc)

                if not pdf_url:
                    logger.debug(f"No PDF for {doc_id}: {title[:60]}")
                    skipped += 1
                    continue

                # Extract text from PDF (OCR for scanned documents)
                text = None
                try:
                    text = self._extract_text_from_pdf(pdf_url)
                except Exception as e:
                    logger.warning(f"PDF extraction failed for {doc_id}: {e}")

                if not text or len(text.strip()) < 50:
                    logger.debug(f"Insufficient text for {doc_id}")
                    skipped += 1
                    continue

                uid = doc.get("uid", "")
                raw = {
                    "document_id": doc_id,
                    "title": title,
                    "text": text.strip(),
                    "publication": data.get("publication"),
                    "first_publication_date": doc.get("first_publication_date", ""),
                    "url": f"https://www.com-saint-martin.fr/deliberations_actes/deliberations/{uid}" if uid else "",
                    "council": self._extract_council_name(doc),
                    "department": self._extract_department(doc),
                    "is_annulled": data.get("isannule", False),
                    "is_modified": data.get("ismodifie", False),
                }
                total += 1
                yield raw

            logger.info(f"Page {page}/{total_pages}: {total} docs fetched, {skipped} skipped")

            if page >= total_pages:
                break
            page += 1

        logger.info(f"Completed: {total} documents fetched, {skipped} skipped")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        """Test connectivity and PDF extraction."""
        try:
            ref = self._get_master_ref()
            logger.info(f"Prismic API OK, master ref: {ref}")
        except Exception as e:
            logger.error(f"Cannot connect to Prismic API: {e}")
            return False

        result = self._search_documents(page=1)
        total = result.get("total_results_size", 0)
        docs = result.get("results", [])
        logger.info(f"Found {total} deliberations, {len(docs)} on first page")

        if not docs:
            logger.error("No documents returned")
            return False

        # Test PDF OCR extraction on first document with a PDF
        for doc in docs[:5]:
            pdf_url = self._extract_pdf_url(doc)
            if pdf_url:
                try:
                    text = self._extract_text_from_pdf(pdf_url)
                    if text and len(text.strip()) > 50:
                        logger.info(f"PDF extraction OK: {len(text)} chars")
                        return True
                except Exception as e:
                    logger.warning(f"PDF extraction failed: {e}")

        logger.error("Could not extract text from any PDF")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MF/Deliberations data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = DeliberationsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
