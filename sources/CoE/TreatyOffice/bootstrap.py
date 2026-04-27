#!/usr/bin/env python3
"""
CoE/TreatyOffice — Council of Europe Treaty Office

Fetches all CoE treaties (conventions, charters, protocols) via the
conventions-ws.coe.int JSON API, downloads English PDF full texts from
rm.coe.int, and extracts text via common/pdf_extract.

Data coverage: ~232 treaties with full text in English and French.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import sys
import logging
import re
import html as html_mod
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CoE.TreatyOffice")

SEARCH_URL = "https://conventions-ws.coe.int/WS_LFRConventions/api/traites/search"
API_TOKEN = "hfghhgp2q5vgwg1hbn532kw71zgtww7e"
API_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "token": API_TOKEN,
}
SOURCE_ID = "CoE/TreatyOffice"


def _parse_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str[:10] if len(date_str) >= 10 else date_str


def fetch_all_treaties() -> list:
    """Fetch all treaty metadata from the API in a single POST."""
    body = {
        "NumsSte": [],
        "CodePays": None,
        "AnneeOuverture": None,
        "AnneeVigueur": None,
        "CodeLieuSTE": None,
        "CodeMatieres": [],
        "TitleKeywords": [],
    }
    resp = requests.post(SEARCH_URL, json=body, headers=API_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


class TreatyOfficeScraper(BaseScraper):
    """Scraper for CoE/TreatyOffice - Council of Europe Treaty Office."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        treaty_num = raw.get("Numero_traite", "")
        title_en = raw.get("Libelle_titre_ENG", "") or raw.get("Nom_commun_ENG", "")
        text = raw.get("text", "")

        if not text or len(text) < 50:
            return None

        return {
            "_id": f"CETS-{treaty_num}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title_en,
            "title_fr": raw.get("Libelle_titre_FRE", ""),
            "common_name": raw.get("Nom_commun_ENG", ""),
            "text": text,
            "date": _parse_date(raw.get("Date_ste")),
            "date_entry_into_force": _parse_date(raw.get("Date_vigueur_ste")),
            "treaty_number": treaty_num,
            "place_of_signature": raw.get("Lieu_ste", ""),
            "url": f"https://www.coe.int/en/web/conventions/full-list/-/conventions/treaty/{treaty_num}",
            "pdf_url_en": raw.get("Lien_pdf_traite_ENG", ""),
            "pdf_url_fr": raw.get("Lien_pdf_traite_FRE", ""),
            "language": "en",
        }

    def _extract_text_from_html(self, pdf_url: str, treaty_num: str) -> Optional[str]:
        """Extract treaty text via rm.coe.int ?format=html (no PDF libs needed)."""
        html_url = f"{pdf_url}?format=html"
        try:
            resp = requests.get(html_url, timeout=60, headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            })
            if resp.status_code != 200:
                return None
            if "text/html" not in resp.headers.get("content-type", ""):
                return None
            text = re.sub(r"<[^>]+>", " ", resp.text)
            text = html_mod.unescape(text)
            text = re.sub(r"[ \t]+", " ", text)
            text = re.sub(r"\n\s*\n", "\n\n", text)
            text = text.strip()
            return text if len(text) >= 50 else None
        except Exception as e:
            logger.warning(f"HTML extraction failed for CETS {treaty_num}: {e}")
            return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        logger.info("Fetching treaty list from API...")
        treaties = fetch_all_treaties()
        logger.info(f"Found {len(treaties)} treaties in API")

        for treaty in treaties:
            treaty_num = treaty.get("Numero_traite", "unknown")
            pdf_url = treaty.get("Lien_pdf_traite_ENG", "")
            if not pdf_url:
                logger.debug(f"Skipping CETS {treaty_num}: no English PDF URL")
                continue

            title = treaty.get("Libelle_titre_ENG", "")
            logger.info(f"Fetching CETS {treaty_num}: {title[:60]}...")
            self.rate_limiter.wait()

            # Primary: HTML format from rm.coe.int (no PDF libs needed)
            text = self._extract_text_from_html(pdf_url, treaty_num)

            # Fallback: PDF extraction (requires pdfplumber/pypdf)
            if not text:
                try:
                    from common.pdf_extract import extract_pdf_markdown
                    text = extract_pdf_markdown(
                        source=SOURCE_ID,
                        source_id=f"CETS-{treaty_num}",
                        pdf_url=pdf_url,
                        table="legislation",
                    )
                except Exception as e:
                    logger.debug(f"PDF fallback failed for CETS {treaty_num}: {e}")

            if not text or len(text) < 50:
                logger.warning(f"No text extracted for CETS {treaty_num}")
                continue

            treaty["text"] = text
            yield treaty

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = TreatyOfficeScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing CoE Treaty Office API...")
        try:
            treaties = fetch_all_treaties()
            logger.info(f"API returned {len(treaties)} treaties")
            if treaties:
                t = treaties[0]
                logger.info(f"First: CETS {t.get('Numero_traite')}: {t.get('Libelle_titre_ENG', '')[:60]}")
            print("Test PASSED")
        except Exception as e:
            print(f"Test FAILED: {e}")
            sys.exit(1)

    elif command in ("bootstrap", "validate", "fetch"):
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
