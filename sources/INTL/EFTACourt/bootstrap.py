#!/usr/bin/env python3
"""
INTL/EFTACourt -- EFTA Court Judgments and Advisory Opinions

Fetches judgments and advisory opinions from the EFTA Court at eftacourt.int.

Strategy:
  - Paginate the WordPress REST API (/wp-json/wp/v2/cases) to list all ~455 cases
  - For each case, fetch the case page HTML to extract metadata and PDF links
  - Download judgment/opinion PDFs and extract full text via common/pdf_extract

Data Coverage:
  - ~455 cases from 1994 to present
  - EEA Agreement interpretation for Iceland, Liechtenstein, Norway
  - Judgments, advisory opinions, orders

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.EFTACourt")

BASE_URL = "https://eftacourt.int"
WP_API = f"{BASE_URL}/wp-json/wp/v2/cases"
PER_PAGE = 100
MAX_PDF_BYTES = 50 * 1024 * 1024  # 50 MB


class EFTACourtScraper(BaseScraper):
    """Scraper for EFTA Court judgments and advisory opinions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/json",
            "Accept-Language": "en",
        })

    # ── Case listing via WP REST API ─────────────────────────────────

    def _list_all_cases(self) -> list[dict]:
        """Paginate WP REST API to get all case slugs and IDs."""
        all_cases = []
        page = 1
        while True:
            url = WP_API
            params = {
                "per_page": PER_PAGE,
                "page": page,
                "orderby": "date",
                "order": "asc",
                "_fields": "id,slug,title,link,date",
            }
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 400:
                # Past last page
                break
            resp.raise_for_status()
            items = resp.json()
            if not items:
                break
            for item in items:
                all_cases.append({
                    "wp_id": item["id"],
                    "slug": item["slug"],
                    "case_number": item.get("title", {}).get("rendered", ""),
                    "link": item.get("link", ""),
                    "wp_date": item.get("date", ""),
                })
            logger.info(f"Listed page {page}: {len(items)} cases (total {len(all_cases)})")
            page += 1
            time.sleep(0.5)
        return all_cases

    # ── Case detail page parsing ─────────────────────────────────────

    def _fetch_case_page(self, url: str) -> str:
        """Fetch a single case detail page HTML."""
        time.sleep(1)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _parse_case_page(self, html: str, case_info: dict) -> dict:
        """Extract metadata and PDF links from a case detail page."""
        meta = {
            "case_number": case_info["case_number"],
            "url": case_info["link"],
            "slug": case_info["slug"],
        }

        # Extract parties from page title or content
        parties_match = re.search(
            r'<h1[^>]*class="[^"]*entry-title[^"]*"[^>]*>(.*?)</h1>',
            html, re.DOTALL
        )
        if parties_match:
            title_text = re.sub(r'<[^>]+>', '', parties_match.group(1)).strip()
            meta["page_title"] = unescape(title_text)

        # Extract structured metadata from c-case-meta-type spans
        # Format: <span class=c-case-meta-type>Label: </span>Value</p>
        meta_fields = re.findall(
            r'c-case-meta-type[^>]*>([^<]+)</span>\s*(.*?)</p>',
            html, re.DOTALL
        )
        for label, value in meta_fields:
            label = label.strip().rstrip(":").strip().lower()
            value = re.sub(r'<[^>]+>', '', value).strip()
            if label == "judgment date" and value:
                meta["judgment_date_raw"] = value
            elif label == "date submitted" and value:
                meta["date_submitted_raw"] = value
            elif label == "hearing date" and value:
                meta["hearing_date_raw"] = value
            elif label == "status":
                meta["case_status"] = value
            elif label == "type":
                meta["type_code"] = value
            elif label == "subjects":
                meta["subjects"] = value
            elif label == "published in":
                meta["published_in"] = value

        # Case type from type code or class list
        type_code = meta.get("type_code", "").upper()
        if type_code == "AO":
            meta["case_type"] = "Advisory opinion"
        elif type_code == "DA":
            meta["case_type"] = "Direct action"
        elif "action-for-a-declaration" in html.lower():
            meta["case_type"] = "Action for declaration"
        elif "advisory-opinion" in html.lower():
            meta["case_type"] = "Advisory opinion"
        elif "direct-action" in html.lower():
            meta["case_type"] = "Direct action"
        else:
            meta["case_type"] = "Other"

        # Extract all download links (wpdmdl pattern)
        download_links = re.findall(
            r'href="(https?://eftacourt\.int/download/[^"]*wpdmdl=\d+[^"]*)"[^>]*>(.*?)</a>',
            html, re.DOTALL
        )

        meta["documents"] = []
        for link_url, link_text in download_links:
            clean_text = re.sub(r'<[^>]+>', '', link_text).strip()
            meta["documents"].append({
                "url": unescape(link_url.replace("&amp;", "&")),
                "label": clean_text,
            })

        return meta

    def _find_judgment_pdf(self, documents: list[dict]) -> Optional[str]:
        """Find the best document to use as full text (prefer judgment/opinion)."""
        # Priority: Judgment > Advisory Opinion > Order > any other
        for keyword in ["judgment", "advisory opinion", "opinion", "order", "ruling", "decision"]:
            for doc in documents:
                if keyword in doc["label"].lower():
                    return doc["url"]
        # Fallback: first document
        if documents:
            return documents[0]["url"]
        return None

    def _parse_date(self, raw: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not raw:
            return None
        raw = raw.strip()
        for fmt in ("%d/%m/%Y", "%d.%m.%Y", "%d-%m-%Y", "%Y-%m-%d",
                     "%d/%m/%y", "%d.%m.%y"):
            try:
                dt = datetime.strptime(raw, fmt)
                if dt.year < 100:
                    dt = dt.replace(year=dt.year + 2000)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    # ── Main fetch methods ───────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all cases with full text from judgment PDFs."""
        cases = self._list_all_cases()
        logger.info(f"Total cases to process: {len(cases)}")

        for i, case_info in enumerate(cases):
            try:
                logger.info(f"[{i+1}/{len(cases)}] Processing {case_info['case_number']} ...")
                html = self._fetch_case_page(case_info["link"])
                meta = self._parse_case_page(html, case_info)

                if not meta["documents"]:
                    logger.warning(f"  No documents found for {case_info['case_number']}, skipping")
                    continue

                pdf_url = self._find_judgment_pdf(meta["documents"])
                if not pdf_url:
                    logger.warning(f"  No judgment PDF for {case_info['case_number']}, skipping")
                    continue

                # Download PDF with session (WPDM requires cookies)
                pdf_bytes = self._download_pdf_bytes(pdf_url)
                if not pdf_bytes:
                    logger.warning(f"  Failed to download PDF for {case_info['case_number']}, skipping")
                    continue

                # Extract text from PDF bytes
                text = extract_pdf_markdown(
                    source="INTL/EFTACourt",
                    source_id=case_info["slug"],
                    pdf_bytes=pdf_bytes,
                    table="case_law",
                )

                if not text:
                    # Fallback: try pdfplumber/pypdf directly
                    text = self._extract_text_from_bytes(pdf_bytes)

                if not text or len(text.strip()) < 100:
                    logger.warning(f"  Insufficient text for {case_info['case_number']}, skipping")
                    continue

                meta["text"] = text
                meta["pdf_url"] = pdf_url
                meta["wp_date"] = case_info.get("wp_date", "")
                yield meta

            except Exception as e:
                logger.error(f"  Error processing {case_info['case_number']}: {e}")
                continue

    def _download_pdf_bytes(self, url: str) -> Optional[bytes]:
        """Download PDF bytes using the session (preserves WPDM cookies)."""
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) > MAX_PDF_BYTES:
                logger.warning(f"  PDF too large ({len(resp.content)} bytes), skipping")
                return None
            if len(resp.content) < 500:
                logger.warning(f"  PDF too small ({len(resp.content)} bytes), likely error page")
                return None
            return resp.content
        except Exception as e:
            logger.error(f"  PDF download failed: {e}")
            return None

    def _extract_text_from_bytes(self, pdf_bytes: bytes) -> Optional[str]:
        """Fallback: extract text from PDF bytes with pdfplumber/pypdf."""
        import io
        # Try pdfplumber
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n\n".join(p for p in pages if p)
                if text:
                    return text
        except Exception:
            pass
        # Try pypdf
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages = [p.extract_text() or "" for p in reader.pages]
            text = "\n\n".join(p for p in pages if p)
            if text:
                return text
        except Exception:
            pass
        return None

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield cases modified since the given date."""
        cases = self._list_all_cases()
        for case_info in cases:
            wp_date = case_info.get("wp_date", "")
            if wp_date:
                try:
                    case_dt = datetime.fromisoformat(wp_date.replace("Z", "+00:00"))
                    if case_dt.replace(tzinfo=None) < since.replace(tzinfo=None):
                        continue
                except (ValueError, TypeError):
                    pass
            # Re-process recent cases
            try:
                html = self._fetch_case_page(case_info["link"])
                meta = self._parse_case_page(html, case_info)
                if not meta["documents"]:
                    continue
                pdf_url = self._find_judgment_pdf(meta["documents"])
                if not pdf_url:
                    continue
                pdf_bytes = self._download_pdf_bytes(pdf_url)
                if not pdf_bytes:
                    continue
                text = extract_pdf_markdown(
                    source="INTL/EFTACourt",
                    source_id=case_info["slug"],
                    pdf_bytes=pdf_bytes,
                    table="case_law",
                )
                if not text:
                    text = self._extract_text_from_bytes(pdf_bytes)
                if not text or len(text.strip()) < 100:
                    continue
                meta["text"] = text
                meta["pdf_url"] = pdf_url
                meta["wp_date"] = case_info.get("wp_date", "")
                yield meta
            except Exception as e:
                logger.error(f"  Error updating {case_info['case_number']}: {e}")
                continue

    def normalize(self, raw: dict) -> dict:
        """Transform raw case data into standard schema."""
        case_number = raw.get("case_number", "").strip()
        slug = raw.get("slug", case_number.lower().replace("/", "-"))

        # Determine date: judgment date > date submitted > hearing date > wp_date > case number year
        date = self._parse_date(raw.get("judgment_date_raw"))
        if not date:
            date = self._parse_date(raw.get("date_submitted_raw"))
        if not date:
            date = self._parse_date(raw.get("hearing_date_raw"))
        if not date and raw.get("wp_date") and not raw["wp_date"].startswith("1970"):
            try:
                date = raw["wp_date"][:10]
            except (IndexError, TypeError):
                date = None
        if not date:
            # Extract year from case number (e.g., E-03/13 → 2013)
            year_match = re.search(r'/(\d{2})$', case_number)
            if year_match:
                yr = int(year_match.group(1))
                date = f"20{yr:02d}-01-01" if yr < 70 else f"19{yr:02d}-01-01"

        # Build title
        page_title = raw.get("page_title", "")
        if page_title and page_title != case_number:
            title = f"{case_number} — {page_title}"
        else:
            title = case_number

        return {
            "_id": f"eftacourt-{slug}",
            "_source": "INTL/EFTACourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "case_number": case_number,
            "case_type": raw.get("case_type", ""),
            "parties": page_title if page_title != case_number else "",
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = EFTACourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        # Quick connectivity test
        cases = scraper._list_all_cases()
        print(f"Found {len(cases)} cases via WP REST API")
        if cases:
            print(f"First: {cases[0]['case_number']} ({cases[0]['slug']})")
            print(f"Last:  {cases[-1]['case_number']} ({cases[-1]['slug']})")
        sys.exit(0)

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
