#!/usr/bin/env python3
"""
INTL/UEMOACourt -- UEMOA Court of Justice Decisions

Fetches decisions from the Cour de Justice de l'Union Économique et Monétaire
Ouest Africaine (UEMOA) via the WordPress AJAX API at courdejusticeuemoa.org.

Strategy:
  - AJAX API: /wp-admin/admin-ajax.php with actions:
    - search_arret_files (judgments)
    - search_avis_files (advisory opinions)
    - search_ordonnance_files (orders)
  - Download PDFs and extract full text using pdfplumber

Data Coverage:
  - ~86 decisions (58 arrêts, 19 avis, 9 ordonnances)
  - Member states: Benin, Burkina Faso, Côte d'Ivoire, Guinea-Bissau,
    Mali, Niger, Senegal, Togo
  - French language

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
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
logger = logging.getLogger("legal-data-hunter.INTL.UEMOACourt")

BASE_URL = "https://courdejusticeuemoa.org"
AJAX_URL = f"{BASE_URL}/wp-admin/admin-ajax.php"

DOCUMENT_TYPES = [
    {
        "action": "search_arret_files",
        "type": "judgment",
        "type_label": "Arrêt",
        "id_field": "numero_de_la_decision",
        "date_field": "date_de_la_decision",
        "pdf_field": "fichier_arrets",
        "parties_field": "partie_concernee",
    },
    {
        "action": "search_avis_files",
        "type": "advisory_opinion",
        "type_label": "Avis",
        "id_field": "numero_de_lavis",
        "date_field": "date_de_lavis",
        "pdf_field": "fichiers_avis",
        "parties_field": "demandeur",
    },
    {
        "action": "search_ordonnance_files",
        "type": "order",
        "type_label": "Ordonnance",
        "id_field": "numero_de_lordonnance",
        "date_field": "date_de_lordonnance",
        "pdf_field": "fichier_ordonnance",
        "parties_field": "affaire_concernee",
    },
]


class UEMOACourtScraper(BaseScraper):
    """Scraper for INTL/UEMOACourt -- UEMOA Court of Justice."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/worldwidelaw/legal-sources)",
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.3",
        })

    def _request_json(self, action: str) -> List[Dict[str, Any]]:
        """Fetch document list via WordPress AJAX."""
        for attempt in range(3):
            try:
                time.sleep(1)
                resp = self.session.post(
                    AJAX_URL,
                    data={"action": action},
                    timeout=30,
                )
                if resp.status_code == 200:
                    return resp.json()
                logger.warning(f"AJAX {action}: HTTP {resp.status_code}")
            except (requests.exceptions.RequestException, ValueError) as e:
                logger.warning(f"AJAX {action} attempt {attempt+1}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return []

    def _download_pdf_text(self, url: str) -> str:
        """Download a PDF and extract text using pdfplumber."""
        if not url or not url.startswith("http"):
            return ""
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=60)
                if resp.status_code != 200:
                    logger.warning(f"PDF download HTTP {resp.status_code}: {url}")
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
                    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
                    return full_text.strip()
            except Exception as e:
                logger.warning(f"PDF extraction attempt {attempt+1}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return ""

    def _parse_date(self, raw_date: str) -> str:
        """Parse DD/MM/YYYY to ISO 8601 format."""
        if not raw_date:
            return ""
        raw_date = raw_date.strip()
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})", raw_date)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw_date)
        if m:
            return raw_date
        return ""

    def _clean_html(self, text: str) -> str:
        """Remove HTML entities and clean text."""
        if not text:
            return ""
        text = unescape(text)
        text = re.sub(r"<[^>]+>", "", text)
        return text.strip()

    def _make_id(self, doc_type: Dict, record: Dict) -> str:
        """Generate a unique ID from document type and number."""
        num = record.get(doc_type["id_field"], "").strip()
        if num:
            clean = re.sub(r"[^\w/.-]", "_", num).strip("_")
            return f"{doc_type['type_label']}_{clean}"
        title = record.get("title", "")[:60]
        clean_title = re.sub(r"[^\w]", "_", title).strip("_")
        return f"{doc_type['type_label']}_{clean_title}"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("decision_id", ""),
            "_source": "INTL/UEMOACourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "decision_number": raw.get("decision_number", ""),
            "decision_type": raw.get("decision_type", ""),
            "keywords": raw.get("keywords", ""),
            "parties": raw.get("parties", ""),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all UEMOA Court of Justice decisions."""
        count = 0

        for doc_type in DOCUMENT_TYPES:
            if max_records and count >= max_records:
                return

            records = self._request_json(doc_type["action"])
            if not records:
                logger.warning(f"No records for {doc_type['action']}")
                continue

            logger.info(f"{doc_type['type_label']}: {len(records)} records from API")

            for record in records:
                if max_records and count >= max_records:
                    return

                pdf_url = record.get(doc_type["pdf_field"], "")
                text = self._download_pdf_text(pdf_url)

                if not text or len(text) < 100:
                    logger.warning(
                        f"Insufficient text ({len(text)} chars): "
                        f"{self._clean_html(record.get('title', '?'))[:60]}"
                    )
                    continue

                decision_id = self._make_id(doc_type, record)
                title = self._clean_html(record.get("title", ""))
                date = self._parse_date(record.get(doc_type["date_field"], ""))
                decision_number = record.get(doc_type["id_field"], "").strip()
                keywords = record.get("mots_cles", "").replace(";", ", ").replace("-", " ").strip()
                parties = record.get(doc_type["parties_field"], "").strip()
                link = record.get("link", "")

                raw = {
                    "decision_id": decision_id,
                    "title": title,
                    "text": text,
                    "date": date,
                    "decision_number": decision_number,
                    "decision_type": doc_type["type_label"],
                    "keywords": keywords,
                    "parties": parties,
                    "url": link or f"{BASE_URL}/arrets/",
                    "pdf_url": pdf_url,
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions (all, since the corpus is small)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        records = self._request_json("search_arret_files")
        if not records:
            logger.error("Cannot fetch arrêts from AJAX API")
            return False

        logger.info(f"AJAX API OK: {len(records)} arrêts")

        pdf_url = records[0].get("fichier_arrets", "")
        if pdf_url:
            text = self._download_pdf_text(pdf_url)
            logger.info(f"PDF extraction OK: {len(text)} chars from first arrêt")
        else:
            logger.warning("First arrêt has no PDF URL")

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/UEMOACourt data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UEMOACourtScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all(max_records=max_records):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            text_len = len(normalized.get("text", ""))
            logger.info(
                f"[{count + 1}] {normalized.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to sample/")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
