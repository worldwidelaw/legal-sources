#!/usr/bin/env python3
"""
MG/CNLEGIS -- Madagascar National Legislation Center Fetcher

Fetches legislation from Centre National LEGIS (cnlegis.gov.mg) via
sessionless JSON API. ~39,863 documents with full text in French.

Strategy:
  - POST /page_data_result_direct_mots with var_pg_limit=OFFSET,LIMIT
  - Returns JSON {"aaData": [...]} with full text in html_fichier_fr
  - Clean HTML tags from text fields

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MG.CNLEGIS")

BASE_URL = "https://cnlegis.gov.mg"
DATA_URL = f"{BASE_URL}/page_data_result_direct_mots"
PAGE_SIZE = 500

# Map CNLEGIS types to our types
TYPE_MAP = {
    "Loi": "legislation",
    "Loi organique": "legislation",
    "Loi constitutionnelle": "legislation",
    "Constitution": "legislation",
    "Décret": "legislation",
    "Ordonnance": "legislation",
    "Arrêté": "legislation",
    "Circulaire": "legislation",
    "Décision": "legislation",
    "Instruction": "legislation",
    "Arrêt": "case_law",
    "Jugement": "case_law",
    "Délibération": "legislation",
    "Avis": "legislation",
    "Note": "legislation",
    "Déclaration": "legislation",
    "Exposé des motifs de la loi": "legislation",
    "Procès verbal": "legislation",
}


def clean_html(html_text: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def parse_date(date_str: str) -> str:
    """Parse DD-MM-YYYY to YYYY-MM-DD."""
    if not date_str:
        return ""
    try:
        parts = date_str.strip().split("-")
        if len(parts) == 3 and len(parts[2]) == 4:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
    except (ValueError, IndexError):
        pass
    return ""


class CNLEGISScraper(BaseScraper):
    """Scraper for MG/CNLEGIS -- Madagascar legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        })

    def _fetch_page(self, offset: int, limit: int = PAGE_SIZE) -> Optional[list]:
        """Fetch a page of documents from the JSON API."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.post(
                    DATA_URL,
                    data={"var_pg_limit": f"{offset},{limit}"},
                    timeout=60,
                )
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()

                if not resp.text or len(resp.text.strip()) == 0:
                    return None

                data = resp.json()
                return data.get("aaData", [])
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = str(raw.get("id", ""))
        type_txt = raw.get("type_txt", "")
        doc_type = TYPE_MAP.get(type_txt, "legislation")

        # Clean HTML from subject and text fields
        title = clean_html(raw.get("objet_txt", ""))
        text = clean_html(raw.get("html_fichier_fr", ""))

        # If no text body, try Malagasy version
        if not text:
            text = clean_html(raw.get("html_fichier_mg", ""))

        date = parse_date(raw.get("date_txt", ""))
        num = raw.get("num_txt", "")

        return {
            "_id": f"MG-CNLEGIS-{doc_id}",
            "_source": "MG/CNLEGIS",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}/page_acces_pdf/{self._encode_id(doc_id)}",
            "type_txt": type_txt,
            "num_txt": num,
            "status": raw.get("etat_txt", ""),
            "ministry": raw.get("ministere", ""),
        }

    @staticmethod
    def _encode_id(doc_id: str) -> str:
        """Encode document ID for PDF URL."""
        mapping = {
            "0": "akZ", "1": "blY", "2": "cmX", "3": "dnW", "4": "eoV",
            "5": "fpU", "6": "gqT", "7": "hrS", "8": "isR", "9": "jtQ",
        }
        return "".join(mapping.get(c, c) for c in str(doc_id))

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents from the API."""
        offset = 0
        total = 0
        while True:
            records = self._fetch_page(offset)
            if not records:
                break

            for raw in records:
                normalized = self.normalize(raw)
                if normalized["text"] and len(normalized["text"]) >= 50:
                    total += 1
                    yield normalized

            logger.info(f"Offset {offset}: {len(records)} records, {total} with text so far")

            if len(records) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        logger.info(f"Fetch complete: {total} documents with full text")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (API returns newest first)."""
        count = 0
        max_pages = 5
        for page in range(max_pages):
            records = self._fetch_page(page * PAGE_SIZE)
            if not records:
                break
            for raw in records:
                normalized = self.normalize(raw)
                if normalized["text"] and len(normalized["text"]) >= 50:
                    if since and normalized["date"] and normalized["date"] < since:
                        return
                    count += 1
                    yield normalized
        logger.info(f"Update complete: {count} recent documents")

    def test(self) -> bool:
        """Quick connectivity test."""
        records = self._fetch_page(0, limit=5)
        if records is None:
            logger.error("Cannot reach CNLEGIS API")
            return False

        logger.info(f"API OK: {len(records)} records returned")
        if records:
            sample = self.normalize(records[0])
            text_len = len(sample["text"])
            logger.info(f"Sample: {sample['title'][:80]} ({text_len} chars text)")
            if text_len > 50:
                logger.info("Full text extraction OK")
                return True
            else:
                logger.warning("Text too short - may not have full text")
                return False
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MG/CNLEGIS data fetcher")
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
    args = parser.parse_args()

    scraper = CNLEGISScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            count = 0
            records = scraper._fetch_page(0, limit=20)
            if records:
                for raw in records:
                    normalized = scraper.normalize(raw)
                    if normalized["text"] and len(normalized["text"]) >= 50:
                        out_path = sample_dir / f"{count:04d}.json"
                        with open(out_path, "w", encoding="utf-8") as f:
                            json.dump(normalized, f, ensure_ascii=False, indent=2)
                        logger.info(
                            f"[{count+1}] {normalized['title'][:80]} "
                            f"({len(normalized['text']):,} chars)"
                        )
                        count += 1
                        if count >= 15:
                            break

            logger.info(f"Bootstrap sample complete: {count} records saved to {sample_dir}")
        else:
            count = 0
            for record in scraper.fetch_all():
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 500 == 0:
                    logger.info(f"Saved {count} records...")
            logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
