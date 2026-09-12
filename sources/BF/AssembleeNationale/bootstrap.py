#!/usr/bin/env python3
"""
BF/AssembleeNationale -- Burkina Faso Assemblée Nationale Legislation

Fetches promulgated laws from the Burkina Faso National Assembly (an.bf).
Paginates through /loip listing, fetches detail pages for PDF links,
extracts full text via PyMuPDF (fitz).

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Connectivity test
"""

import sys
import io
import re
import html as html_lib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BF.AssembleeNationale")

BASE_URL = "https://www.an.bf"
LIST_URL = f"{BASE_URL}/loip"
SOURCE_ID = "BF/AssembleeNationale"

FR_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}


def _strip_tags(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_french_date(text: str) -> Optional[str]:
    """Extract and parse a French date like '23 décembre 2024' from text."""
    m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if m:
        day, month_name, year = m.groups()
        month_num = FR_MONTHS.get(month_name.lower())
        if month_num:
            try:
                return datetime(int(year), month_num, int(day)).date().isoformat()
            except ValueError:
                pass
    # Try DD/MM/YYYY
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if m:
        day, month, year = m.groups()
        try:
            return datetime(int(year), int(month), int(day)).date().isoformat()
        except ValueError:
            pass
    return None


class AssembleeNationaleScraper(BaseScraper):
    """Scraper for BF/AssembleeNationale."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _get_total_pages(self) -> int:
        """Determine total number of pages from the listing."""
        try:
            resp = self.session.get(LIST_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch listing: {e}")
            return 0

        pages = re.findall(r'\?page=(\d+)', resp.text)
        if pages:
            return max(int(p) for p in pages)
        return 1

    def _get_law_ids_from_page(self, page: int) -> list[str]:
        """Extract law IDs from a listing page."""
        url = f"{LIST_URL}?page={page}"
        try:
            time.sleep(2.0)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch page {page}: {e}")
            return []

        ids = list(set(re.findall(r'/loip/(\d+)', resp.text)))
        logger.info(f"Page {page}: found {len(ids)} law entries")
        return ids

    def _fetch_law_detail(self, law_id: str) -> Optional[dict]:
        """Fetch a law detail page and extract metadata + PDF URL."""
        url = f"{BASE_URL}/loip/{law_id}"
        try:
            time.sleep(2.0)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch law {law_id}: {e}")
            return None

        html = resp.text

        # Extract title: "de la loi : Loi n°..." or "Télécharger la Loi n°..."
        title = None
        m = re.search(r'(?:de la loi|Intitul[eé])\s*:\s*(Loi[^<"]{10,250})', html, re.I)
        if m:
            title = _strip_tags(m.group(1)).strip()
        if not title:
            m = re.search(r'T[ée]l[ée]charger la (Loi[^<"]{10,250})', html, re.I)
            if m:
                title = _strip_tags(m.group(1)).strip()
        if not title:
            # Try to find any "Loi n°..." pattern
            m = re.search(r'(Loi\s+(?:organique\s+)?n[°o]?\s*[\d\-/]+\w*[^<"]{0,200})', html, re.I)
            if m:
                title = _strip_tags(m.group(1)).strip()

        # Extract date from title
        date_iso = _parse_french_date(title) if title else None

        # Extract PDF URL - the law text PDF (in /storage/Loi/ path)
        pdf_urls = re.findall(
            r'href="(/storage/Loi/[^"]+\.pdf)"',
            html,
            re.I,
        )
        if not pdf_urls:
            # Broader: any PDF in storage
            pdf_urls = re.findall(
                r'href="(/storage/[^"]+\.pdf)"',
                html,
                re.I,
            )

        # Extract law number
        law_number = None
        if title:
            m = re.match(r'(Loi\s+(?:organique\s+)?n[°o]?\s*[\d\-/]+\w*)', title, re.I)
            if m:
                law_number = m.group(1).strip()

        return {
            "law_id": law_id,
            "title": title or f"Loi (ID {law_id})",
            "law_number": law_number,
            "date": date_iso,
            "url": url,
            "pdf_urls": pdf_urls,
        }

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text via PyMuPDF."""
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) < 200:
                return None
            doc = fitz.open(stream=resp.content, filetype="pdf")
            pages = []
            for page in doc:
                t = page.get_text()
                if t and t.strip():
                    pages.append(t.strip())
            doc.close()
            text = "\n\n".join(pages)
            return text if len(text) > 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all promulgated laws with full text."""
        total_pages = self._get_total_pages()
        if total_pages == 0:
            logger.error("Could not determine page count")
            return

        logger.info(f"Found {total_pages} pages of laws to process")

        for page in range(1, total_pages + 1):
            law_ids = self._get_law_ids_from_page(page)
            for law_id in law_ids:
                detail = self._fetch_law_detail(law_id)
                if not detail:
                    continue

                # Try to extract text from the law PDF
                text = None
                for pdf_path in detail.get("pdf_urls", []):
                    full_url = f"{BASE_URL}{pdf_path}"
                    text = self._extract_pdf_text(full_url)
                    if text:
                        break

                if not text:
                    logger.debug(f"No text extracted for law {law_id} (scanned PDF)")
                    continue

                yield self.normalize({
                    **detail,
                    "text": text,
                })

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch laws updated since a given date (checks first 2 pages)."""
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for page in range(1, 3):
            law_ids = self._get_law_ids_from_page(page)
            for law_id in law_ids:
                detail = self._fetch_law_detail(law_id)
                if not detail:
                    continue
                if since and detail.get("date") and detail["date"] < since:
                    return
                text = None
                for pdf_path in detail.get("pdf_urls", []):
                    full_url = f"{BASE_URL}{pdf_path}"
                    text = self._extract_pdf_text(full_url)
                    if text:
                        break
                if not text:
                    continue
                yield self.normalize({**detail, "text": text})

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        law_id = raw.get("law_id", "unknown")
        return {
            "_id": f"BF-AN-{law_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", f"{BASE_URL}/loip/{law_id}"),
            "law_number": raw.get("law_number"),
            "law_id": law_id,
        }

    def test_connection(self) -> bool:
        """Test connectivity to the data source."""
        try:
            resp = self.session.get(LIST_URL, timeout=15)
            resp.raise_for_status()
            return "loip" in resp.text.lower() or "loi" in resp.text.lower()
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import json

    scraper = AssembleeNationaleScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        ok = scraper.test_connection()
        print(f"Connection test: {'PASSED' if ok else 'FAILED'}")
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        max_records = 15 if sample_mode else None
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all():
            if sample_mode:
                out_path = sample_dir / f"{record['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                print(f"  [{count + 1}] {record['title'][:80]}")

            count += 1
            if max_records and count >= max_records:
                break

        print(f"\nTotal: {count} records fetched")
        if sample_mode:
            print(f"Samples saved to: {sample_dir}")
        sys.exit(0 if count > 0 else 1)

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
