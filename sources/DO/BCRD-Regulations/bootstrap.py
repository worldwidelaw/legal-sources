#!/usr/bin/env python3
"""
DO/BCRD-Regulations — Banco Central de la República Dominicana

Fetches Monetary Board resolutions (Resoluciones JM), current regulations
(Reglamentos Vigentes), and instructivos from the Dominican Central Bank.

Strategy:
  1. POST /Home/GetJmResolutions to get all JM resolutions (JSON + PDF URLs)
  2. POST /Home/GetContentForRender for article 2571 (reglamentos) and 2573
     (instructivos) to extract PDF links from HTML content
  3. Download each PDF and extract text with pdfplumber

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (by date)
  python bootstrap.py test-api            # Quick API connectivity test
"""

import io
import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DO.BCRD-Regulations")

BASE_URL = "https://www.bancentral.gov.do"
CDN_BASE = "https://cdn.bancentral.gov.do"
SOURCE_ID = "DO/BCRD-Regulations"

# Article IDs for normativa pages
REGLAMENTOS_ARTICLE_ID = "2571"
INSTRUCTIVOS_ARTICLE_ID = "2573"


class BCRDRegulationsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open legal data research)",
                "Accept": "application/json, text/html, */*",
            },
        )

    # ── Resoluciones JM ─────────────────────────────────────────────

    def _fetch_resolutions(self) -> list[dict]:
        """Fetch all JM resolutions via the POST API."""
        logger.info("Fetching JM resolutions...")
        resp = self.http.post(
            f"{BASE_URL}/Home/GetJmResolutions",
            data={
                "dateFromString": "",
                "dateToString": "",
                "filter": "",
                "maxResultCount": "1000",
                "sortBy": "date",
                "orderBy": "desc",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        payload = resp.json()
        if isinstance(payload, str):
            payload = json.loads(payload)
        items = payload.get("result", {}).get("items", [])
        logger.info("Found %d JM resolutions", len(items))
        return items

    # ── Reglamentos / Instructivos from article pages ────────────────

    def _fetch_article_pdfs(self, article_id: str, doc_type: str) -> list[dict]:
        """Fetch PDF links from a CMS article page."""
        logger.info("Fetching %s (article %s)...", doc_type, article_id)
        resp = self.http.post(
            f"{BASE_URL}/Home/GetContentForRender",
            data={"id": article_id, "languageName": "es"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        payload = resp.json()
        if isinstance(payload, str):
            payload = json.loads(payload)
        content = payload.get("result", {}).get("article", {}).get("content", "")

        # Extract PDF links with their labels
        pattern = r'<a[^>]*href="(https://cdn\.bancentral\.gov\.do[^"]*\.pdf)"[^>]*>(.*?)</a>'
        matches = re.findall(pattern, content, re.DOTALL)

        results = []
        seen_urls = set()
        for url, label in matches:
            clean_label = re.sub(r"<[^>]+>", "", label).strip()
            # Deduplicate by URL
            norm_url = url.split("?")[0]
            if norm_url in seen_urls:
                continue
            seen_urls.add(norm_url)
            results.append({
                "url": url,
                "label": clean_label or Path(unquote(url)).stem,
                "doc_type": doc_type,
            })

        logger.info("Found %d unique %s PDFs", len(results), doc_type)
        return results

    # ── PDF text extraction ──────────────────────────────────────────

    def _extract_pdf_text(self, pdf_url: str, source_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            text = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id=source_id,
                pdf_url=pdf_url,
                table="doctrine",
            )
            if text and len(text.strip()) > 50:
                return text.strip()
        except Exception as e:
            logger.warning("extract_pdf_markdown failed for %s: %s", pdf_url, e)

        # Fallback: direct pdfplumber extraction
        try:
            import pdfplumber
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("PDF download failed (%d): %s", resp.status_code, pdf_url)
                return None
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages = []
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        pages.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                text = "\n\n".join(pages)
                if len(text.strip()) > 50:
                    return text.strip()
        except Exception as e:
            logger.warning("pdfplumber failed for %s: %s", pdf_url, e)

        return None

    # ── Normalization ────────────────────────────────────────────────

    def _normalize_resolution(self, item: dict) -> Optional[dict]:
        """Normalize a JM resolution record."""
        title = item.get("title", "").strip()
        pdf_url = item.get("customUrl", "")
        descriptors = item.get("descriptors", "").strip()
        date_str = item.get("auxiliarDatetime1", "")
        doc_id = f"JM-{item.get('id', title)}"

        if not pdf_url:
            logger.warning("No PDF URL for resolution %s", title)
            return None

        # Append cache-buster parameter
        if "?" not in pdf_url:
            pdf_url = pdf_url + f"?v={int(time.time())}"

        text = self._extract_pdf_text(pdf_url, doc_id)
        if not text:
            logger.warning("No text extracted for resolution %s", title)
            return None

        # Parse date
        date_iso = None
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_iso = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": pdf_url.split("?")[0],
            "document_type": "resolución_jm",
            "descriptors": descriptors,
            "resolution_number": item.get("previewContent", ""),
            "category": item.get("category_Name", "Resoluciones JM"),
        }

    def _normalize_article_pdf(self, item: dict) -> Optional[dict]:
        """Normalize a reglamento/instructivo PDF."""
        pdf_url = item["url"]
        label = item["label"]
        doc_type = item["doc_type"]

        # Create a stable ID from the URL path
        url_path = pdf_url.split("?")[0]
        filename = Path(unquote(unquote(url_path))).stem
        doc_id = f"{doc_type}-{filename}"

        text = self._extract_pdf_text(pdf_url, doc_id)
        if not text:
            logger.warning("No text extracted for %s: %s", doc_type, label)
            return None

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": label or filename,
            "text": text,
            "date": None,
            "url": url_path,
            "document_type": doc_type,
            "descriptors": "",
            "category": doc_type.replace("_", " ").title(),
        }

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BCRD regulatory documents."""
        # 1. JM Resolutions
        resolutions = self._fetch_resolutions()
        for item in resolutions:
            record = self._normalize_resolution(item)
            if record:
                yield record

        # 2. Reglamentos Vigentes
        reglamentos = self._fetch_article_pdfs(REGLAMENTOS_ARTICLE_ID, "reglamento")
        for item in reglamentos:
            record = self._normalize_article_pdf(item)
            if record:
                yield record

        # 3. Instructivos
        instructivos = self._fetch_article_pdfs(INSTRUCTIVOS_ARTICLE_ID, "instructivo")
        for item in instructivos:
            record = self._normalize_article_pdf(item)
            if record:
                yield record

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch only recent resolutions (reglamentos/instructivos are static)."""
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        resolutions = self._fetch_resolutions()
        for item in resolutions:
            if since and item.get("auxiliarDatetime1", "") < since:
                continue
            record = self._normalize_resolution(item)
            if record:
                yield record

    def normalize(self, raw: dict) -> dict:
        """Pass-through — normalization is done in fetch methods."""
        return raw


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="DO/BCRD-Regulations scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BCRDRegulationsScraper()

    if args.command == "test-api":
        logger.info("Testing JM Resolutions API...")
        resolutions = scraper._fetch_resolutions()
        logger.info("OK: %d resolutions found", len(resolutions))
        if resolutions:
            sample = resolutions[0]
            logger.info("Sample: %s — %s", sample.get("title"), sample.get("customUrl"))

        logger.info("Testing Reglamentos article...")
        reglamentos = scraper._fetch_article_pdfs(REGLAMENTOS_ARTICLE_ID, "reglamento")
        logger.info("OK: %d reglamentos found", len(reglamentos))

        logger.info("Testing Instructivos article...")
        instructivos = scraper._fetch_article_pdfs(INSTRUCTIVOS_ARTICLE_ID, "instructivo")
        logger.info("OK: %d instructivos found", len(instructivos))
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        limit = 15 if args.sample else None
        count = 0
        for record in scraper.fetch_all():
            count += 1
            if args.sample or count <= 15:
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(
                    "[%d] %s — %d chars",
                    count,
                    record["title"][:60],
                    len(record.get("text", "")),
                )
            else:
                logger.info(
                    "[%d] %s — %d chars",
                    count,
                    record["title"][:60],
                    len(record.get("text", "")),
                )
            if limit and count >= limit:
                break

        logger.info("Done: %d records fetched", count)

    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
            logger.info("[%d] %s", count, record["title"][:60])
        logger.info("Done: %d updates fetched", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
