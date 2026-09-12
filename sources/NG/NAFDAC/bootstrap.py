#!/usr/bin/env python3
"""
NG/NAFDAC -- Nigeria National Agency for Food and Drug Administration

Fetches regulations, guidelines, and regulatory directives from NAFDAC.
Documents are PDFs covering drug regulation, food safety, medical devices,
chemicals, cosmetics, veterinary products, clinical trials, and more.

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
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NG.NAFDAC")

BASE_URL = "https://nafdac.gov.ng"
REGULATIONS_AJAX = (
    f"{BASE_URL}/wp-admin/admin-ajax.php"
    "?action=wp_ajax_ninja_tables_public_action"
    "&table_id=9904&target_action=get-all-data&default_sorting=new_first"
)
GUIDELINES_URL = f"{BASE_URL}/regulatory-resources/guidelines/"
DIRECTIVES_URL = f"{BASE_URL}/regulatory-resources/regulatory-directive/"


class NAFDACScraper(BaseScraper):
    """
    Scraper for NG/NAFDAC -- NAFDAC regulations, guidelines, and directives.
    Country: NG
    URL: https://nafdac.gov.ng/regulatory-resources/nafdac-regulations/

    Data types: legislation, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data research project)",
        })

    # ------------------------------------------------------------------
    # Data collection: three sub-sources
    # ------------------------------------------------------------------

    def _get_regulations(self) -> list[dict]:
        """Fetch gazetted regulations from Ninja Tables AJAX endpoint."""
        try:
            resp = self.session.get(REGULATIONS_AJAX, timeout=30)
            resp.raise_for_status()
            rows = resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch regulations AJAX: {e}")
            return []

        documents = []
        for row in rows:
            # Ninja Tables nests data under row["value"]
            val = row.get("value", row)

            title_html = val.get("title", "")
            title = _strip_html(title_html)
            if not title:
                continue

            pdf_url = _extract_href(title_html)
            if not pdf_url:
                continue

            if not pdf_url.startswith("http"):
                pdf_url = urljoin(BASE_URL, pdf_url)

            product_type = _strip_html(val.get("producttype", ""))
            category = _strip_html(val.get("category", ""))

            date = _extract_year_date(title)

            documents.append({
                "title": title,
                "pdf_url": pdf_url,
                "date": date,
                "document_type": "regulation",
                "product_type": product_type,
                "category": category,
            })

        logger.info(f"Found {len(documents)} gazetted regulations")
        return documents

    def _get_guidelines(self) -> list[dict]:
        """Scrape guideline PDF links from the guidelines page."""
        from bs4 import BeautifulSoup

        try:
            resp = self.session.get(GUIDELINES_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch guidelines page: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        seen_urls = set()
        documents = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if ".pdf" not in href.lower():
                continue

            pdf_url = urljoin(GUIDELINES_URL, href)
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            link_text = link.get_text(strip=True)
            product_type = ""
            category = ""

            # Extract metadata from table row cells:
            # [title, product_type, category, status, date]
            parent_row = link.find_parent("tr")
            if parent_row:
                cells = parent_row.find_all("td")
                if len(cells) >= 3:
                    product_type = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    category = cells[2].get_text(strip=True) if len(cells) > 2 else ""

            if not link_text or len(link_text) < 10 or link_text.lower() in ("view", "download", "pdf", "click here"):
                filename = unquote(pdf_url.split("/")[-1])
                link_text = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
                link_text = re.sub(r'[-_]+', ' ', link_text).strip()

            if not category:
                category = _category_from_path(pdf_url)
            date = _extract_year_date(link_text) or _extract_year_date(pdf_url)

            documents.append({
                "title": link_text,
                "pdf_url": pdf_url,
                "date": date,
                "document_type": "guideline",
                "product_type": product_type,
                "category": category,
            })

        logger.info(f"Found {len(documents)} guidelines")
        return documents

    def _get_directives(self) -> list[dict]:
        """Scrape regulatory directive PDF links from the directives page."""
        from bs4 import BeautifulSoup

        try:
            resp = self.session.get(DIRECTIVES_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch directives page: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        seen_urls = set()
        documents = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if ".pdf" not in href.lower():
                continue

            pdf_url = urljoin(DIRECTIVES_URL, href)
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            link_text = link.get_text(strip=True)

            # Try parent row for better title
            parent_row = link.find_parent("tr")
            if parent_row:
                cells = parent_row.find_all("td")
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    if len(cell_text) > 20:
                        link_text = cell_text
                        break

            if not link_text or len(link_text) < 10 or link_text.lower() in ("view", "download", "pdf"):
                filename = unquote(pdf_url.split("/")[-1])
                link_text = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
                link_text = re.sub(r'[-_]+', ' ', link_text).strip()

            date = _extract_year_date(link_text) or _extract_year_date(pdf_url)

            documents.append({
                "title": link_text,
                "pdf_url": pdf_url,
                "date": date,
                "document_type": "directive",
                "product_type": "",
                "category": "Regulatory Directive",
            })

        logger.info(f"Found {len(documents)} regulatory directives")
        return documents

    # ------------------------------------------------------------------
    # PDF extraction
    # ------------------------------------------------------------------

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text via pdfplumber."""
        try:
            resp = self.session.get(pdf_url, timeout=90)
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
            return full_text if len(full_text) >= 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    # ------------------------------------------------------------------
    # Normalize
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        url_hash = hashlib.md5(raw["pdf_url"].encode()).hexdigest()[:12]
        doc_id = f"NG-NAFDAC-{url_hash}"

        doc_type = raw.get("document_type", "guideline")
        _type = "legislation" if doc_type == "regulation" else "doctrine"

        return {
            "_id": doc_id,
            "_source": "NG/NAFDAC",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "document_type": doc_type,
            "product_type": raw.get("product_type", ""),
            "category": raw.get("category", ""),
        }

    # ------------------------------------------------------------------
    # Main fetch methods
    # ------------------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all NAFDAC documents with full PDF text."""
        regulations = self._get_regulations()
        guidelines = self._get_guidelines()
        directives = self._get_directives()

        all_docs = regulations + guidelines + directives

        # Deduplicate by PDF URL
        seen = set()
        unique_docs = []
        for doc in all_docs:
            url = doc["pdf_url"]
            if url not in seen:
                seen.add(url)
                unique_docs.append(doc)

        logger.info(
            f"Total unique documents: {len(unique_docs)} "
            f"(regulations={len(regulations)}, guidelines={len(guidelines)}, "
            f"directives={len(directives)})"
        )

        yielded = 0
        skipped = 0

        for i, doc in enumerate(unique_docs):
            logger.info(f"[{i+1}/{len(unique_docs)}] Downloading: {doc['title'][:70]}")

            text = self._extract_pdf_text(doc["pdf_url"])
            if not text:
                skipped += 1
                logger.warning(f"Skipped (no text): {doc['title'][:70]}")
                continue

            doc["text"] = text
            normalized = self.normalize(doc)
            if normalized:
                yielded += 1
                yield normalized

            time.sleep(1.5)

        logger.info(f"Done. Yielded: {yielded}, Skipped: {skipped}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recently added documents (re-fetches all since no date filter available)."""
        yield from self.fetch_all()

    def test(self) -> dict:
        """Quick connectivity test."""
        results = {}
        for name, url in [
            ("regulations_ajax", REGULATIONS_AJAX),
            ("guidelines_page", GUIDELINES_URL),
            ("directives_page", DIRECTIVES_URL),
        ]:
            try:
                resp = self.session.get(url, timeout=30)
                results[name] = {"status": resp.status_code, "ok": resp.status_code == 200}
            except Exception as e:
                results[name] = {"status": "error", "ok": False, "error": str(e)}

        all_ok = all(r.get("ok") for r in results.values())
        return {"status": "ok" if all_ok else "partial", "endpoints": results}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r'<[^>]+>', '', text)
    return clean.strip()


def _extract_href(html: str) -> Optional[str]:
    """Extract first href from an HTML snippet."""
    match = re.search(r'href=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _extract_year_date(text: str) -> Optional[str]:
    """Extract a year from text and return as ISO date."""
    match = re.search(r'(20[12]\d)', text)
    if match:
        return f"{match.group(1)}-01-01"
    return None


def _category_from_path(pdf_url: str) -> str:
    """Derive a category from the PDF URL path."""
    path = pdf_url.lower()
    if "drug_guidelines" in path or "dr&r" in path or "dr_and_r" in path:
        return "Drugs"
    if "clinical" in path:
        return "Clinical Trials"
    if "food" in path or "fsan" in path or "fr_and_r" in path:
        return "Food"
    if "vbm" in path or "vaccine" in path or "biologics" in path:
        return "Vaccines & Biologicals"
    if "chemical" in path or "cer" in path:
        return "Chemicals"
    if "veterinary" in path or "vmap" in path:
        return "Veterinary"
    if "pvg" in path or "pharmacovigilance" in path:
        return "Pharmacovigilance"
    if "narcotics" in path or "ncs" in path:
        return "Narcotics"
    if "pms" in path or "post-market" in path:
        return "Post-Market Surveillance"
    if "portinspection" in path or "pid" in path:
        return "Port Inspection"
    if "medical_devices" in path:
        return "Medical Devices"
    if "der" in path:
        return "Drug Evaluation"
    if "inspection" in path:
        return "Inspection"
    if "laboratory" in path:
        return "Laboratory"
    if "quality" in path or "qms" in path:
        return "Quality Management"
    if "reforms" in path:
        return "Reforms"
    if "traceability" in path:
        return "Traceability"
    return "General"


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = NAFDACScraper()

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

        for record in gen:
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
