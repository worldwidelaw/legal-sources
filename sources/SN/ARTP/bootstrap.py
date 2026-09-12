#!/usr/bin/env python3
"""
SN/ARTP -- Senegal Autorité de Régulation des Télécommunications et des Postes

Fetches regulatory documents (laws, reports, observatory, dashboards, decisions)
from the ARTP Drupal site. Documents are PDFs; scanned/image-only PDFs are skipped.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
import hashlib
import io
import warnings
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
import pdfplumber
from bs4 import BeautifulSoup

# Suppress SSL warnings (ARTP has cert issues)
warnings.filterwarnings("ignore", message=".*certificate.*")
warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SN.ARTP")

BASE_URL = "https://artp.sn"
LISTING_URL = f"{BASE_URL}/documentation"
MAX_PAGES = 60  # Safety limit


class ARTPScraper(BaseScraper):
    """
    Scraper for SN/ARTP -- Senegal telecom regulatory documents.
    Country: SN
    URL: https://artp.sn/documentation

    Data types: doctrine, legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data research project)",
        })

    # ------------------------------------------------------------------
    # Data collection
    # ------------------------------------------------------------------

    def _get_all_doc_slugs(self) -> list[str]:
        """Paginate through /documentation to collect all document slugs."""
        all_slugs = []
        seen = set()

        for page_num in range(MAX_PAGES):
            url = f"{LISTING_URL}?page={page_num}"
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch page {page_num}: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            found = 0

            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "/votre-documentation/" not in href:
                    continue
                # Skip "Lire la suite" duplicates
                if "lire" in link.get_text(strip=True).lower():
                    continue
                slug = href.rstrip("/")
                if slug not in seen:
                    seen.add(slug)
                    all_slugs.append(slug)
                    found += 1

            if found == 0:
                break
            time.sleep(0.5)

        logger.info(f"Collected {len(all_slugs)} unique document slugs")
        return all_slugs

    def _get_doc_info(self, slug: str) -> Optional[dict]:
        """Visit a document page and extract title + PDF URL."""
        url = urljoin(BASE_URL, slug)
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {slug}: {e}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Title
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""
        if not title:
            title_tag = soup.find("title")
            title = title_tag.get_text(strip=True).split("|")[0].strip() if title_tag else ""
        if not title or len(title) < 5:
            return None

        # PDF URL from iframe viewer
        pdf_url = None
        for iframe in soup.find_all("iframe"):
            src = iframe.get("src") or iframe.get("data-src") or ""
            m = re.search(r"file=([^#&]+)", src)
            if m:
                pdf_url = unquote(m.group(1))
                break

        if not pdf_url:
            return None

        # Classify document type from slug
        slug_lower = slug.lower()
        if "decision" in slug_lower:
            doc_type = "decision"
        elif "code" in slug_lower or "loi" in slug_lower:
            doc_type = "law"
        elif "catalogue" in slug_lower:
            doc_type = "catalogue"
        elif "rapport" in slug_lower:
            doc_type = "report"
        elif "observatoire" in slug_lower:
            doc_type = "observatory"
        elif "tableau" in slug_lower:
            doc_type = "dashboard"
        elif "note" in slug_lower:
            doc_type = "note"
        elif "formulaire" in slug_lower or "procedure" in slug_lower:
            doc_type = "form"
        else:
            doc_type = "other"

        # Extract date from title
        date = _extract_date(title)

        # Generate stable ID from slug
        slug_clean = slug.replace("/votre-documentation/", "")
        doc_id = f"SN-ARTP-{hashlib.md5(slug_clean.encode()).hexdigest()[:12]}"

        return {
            "doc_id": doc_id,
            "title": title,
            "pdf_url": pdf_url,
            "page_url": url,
            "date": date,
            "document_type": doc_type,
            "slug": slug_clean,
        }

    # ------------------------------------------------------------------
    # PDF extraction
    # ------------------------------------------------------------------

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text via pdfplumber."""
        try:
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
            return None

        if len(resp.content) < 500:
            return None

        try:
            pdf = pdfplumber.open(io.BytesIO(resp.content))
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
            full_text = "\n\n".join(pages_text)
            return full_text if len(full_text) >= 100 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    # ------------------------------------------------------------------
    # Normalize
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 100:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        doc_type = raw.get("document_type", "other")
        _type = "legislation" if doc_type == "law" else "doctrine"

        return {
            "_id": raw["doc_id"],
            "_source": "SN/ARTP",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("page_url", raw["pdf_url"]),
            "document_type": doc_type,
        }

    # ------------------------------------------------------------------
    # Main fetch methods
    # ------------------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all ARTP documents with full PDF text."""
        slugs = self._get_all_doc_slugs()

        yielded = 0
        skipped = 0
        no_pdf = 0

        for i, slug in enumerate(slugs):
            logger.info(f"[{i+1}/{len(slugs)}] Processing: {slug[-60:]}")

            doc_info = self._get_doc_info(slug)
            if not doc_info:
                no_pdf += 1
                continue

            # Skip forms (not regulatory content)
            if doc_info["document_type"] == "form":
                skipped += 1
                continue

            text = self._extract_pdf_text(doc_info["pdf_url"])
            if not text:
                skipped += 1
                logger.info(f"  Skipped (no extractable text): {doc_info['title'][:50]}")
                continue

            doc_info["text"] = text
            # Yield RAW per BaseScraper contract; the framework calls normalize().
            # (Previously yielded self.normalize(...) → double-normalize → 0 records.)
            yielded += 1
            yield doc_info

            time.sleep(1.5)

        logger.info(
            f"Done. Yielded: {yielded}, Skipped (no text): {skipped}, "
            f"No PDF: {no_pdf}"
        )

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recently added documents."""
        yield from self.fetch_all()

    def test(self) -> dict:
        """Quick connectivity test."""
        try:
            resp = self.session.get(LISTING_URL, timeout=30)
            soup = BeautifulSoup(resp.text, "html.parser")
            doc_links = [
                a for a in soup.find_all("a", href=True)
                if "/votre-documentation/" in a["href"]
            ]
            return {
                "status": "ok" if resp.status_code == 200 else "error",
                "http_status": resp.status_code,
                "doc_links_on_page": len(doc_links),
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _extract_date(text: str) -> Optional[str]:
    """Extract date from title text."""
    if not text:
        return None

    # Try "au 31 Mars 2026" pattern
    month_map = {
        "janvier": "01", "février": "02", "mars": "03", "avril": "04",
        "mai": "05", "juin": "06", "juillet": "07", "août": "08",
        "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
    }
    for month_name, month_num in month_map.items():
        m = re.search(
            rf"(\d{{1,2}})\s+{month_name}\s+(20\d{{2}}|19\d{{2}})",
            text.lower(),
        )
        if m:
            day = m.group(1).zfill(2)
            year = m.group(2)
            return f"{year}-{month_num}-{day}"

    # Try "trimestre YYYY" pattern
    m = re.search(r"(\d)(?:er|ème|e)?\s*trimestre\s*(20\d{2}|19\d{2})", text.lower())
    if m:
        quarter = int(m.group(1))
        year = m.group(2)
        month = str(quarter * 3).zfill(2)
        return f"{year}-{month}-01"

    # Fall back to year only
    m = re.search(r"\b(20\d{2}|19\d{2})\b", text)
    if m:
        return f"{m.group(1)}-01-01"

    return None


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ARTPScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        result = scraper.test()
        print(json.dumps(result, indent=2))
    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample_mode else 99999

        gen = scraper.fetch_all() if command == "bootstrap" else scraper.fetch_updates()

        for raw in gen:
            # fetch_all() yields RAW docs; normalize here to match the VPS
            # bootstrap_fast() path and write proper sample records.
            record = scraper.normalize(raw)
            if not record:
                continue
            count += 1
            if sample_mode:
                outpath = sample_dir / f"{count:04d}.json"
                outpath.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                print(f"[{count}] {record['title'][:60]} ({len(record['text'])} chars)")
            else:
                print(json.dumps(record, ensure_ascii=False))

            if count >= limit:
                break

        print(f"\nTotal records: {count}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
