#!/usr/bin/env python3
"""
PF/JOPF -- French Polynesia Legislation (LEXPOL)

Fetches legislation from lexpol.cloud.pf, the official legal information
service for French Polynesia.

Strategy:
  - Search API: GET /action.php?module=liste&action=charger&data={base64_json}
    Returns paginated list of legislation with id_texte identifiers.
  - Full text API: POST /action.php?module=jopf&action=affiche_texte_num
    Returns HTML body of consolidated text for each document.
  - Covers: Lois du pays (LP), Délibérations, Arrêtés, Décisions, Décrets,
    Lois, Codes, and other text types — 340K+ documents total.

Data:
  - All legislation of French Polynesia from LEXPOL
  - Full text in HTML (cleaned to plain text)
  - License: Open Government Data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import base64
import html
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PF.JOPF")

BASE_URL = "https://lexpol.cloud.pf"
PAGE_SIZE = 25

# All legislation sub-source types
ALL_SOUS_SOURCES = [
    "LP", "DELIB", "ARR", "AVIS", "DEC", "DECRET", "LOI", "CODE",
    "ORD", "CONV", "CIRC", "CONST", "ARRETE_MIN",
]


class LexpolScraper(BaseScraper):
    """
    Scraper for PF/JOPF -- French Polynesia Legislation.
    Country: PF
    URL: https://lexpol.cloud.pf

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept-Language": "fr",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=90,
        )

    def _search(self, offset: int = 0, limit: int = PAGE_SIZE,
                recherche: str = "", sous_sources: list = None) -> dict:
        """Search the LEXPOL legislation index."""
        payload = {
            "page": "recherche_texte",
            "filtres": {
                "SOURCE": ["TEXTES"],
            },
            "tri": "dated",
            "offset": offset,
            "limit": limit,
        }
        if sous_sources:
            payload["filtres"]["SOUS_SOURCE"] = sous_sources
        if recherche:
            payload["filtres"]["RECHERCHE"] = recherche

        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        url = f"/action.php?module=liste&action=charger&data={encoded}"

        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()
        return resp.json()

    def _get_full_text(self, text_id: int) -> Optional[Dict[str, Any]]:
        """Fetch full text of a legislation document via POST API."""
        params = {"id_texte": text_id}
        encoded = base64.b64encode(json.dumps(params).encode()).decode()

        self.rate_limiter.wait()
        resp = self.client.session.post(
            f"{BASE_URL}/action.php?module=jopf&action=affiche_texte_num",
            data={"d": encoded},
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=90,
        )
        resp.raise_for_status()

        try:
            data = json.loads(resp.text, strict=False)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON for text {text_id}")
            return None

        contenu = data.get("contenu")
        if not contenu:
            return None

        return {
            "contenu": contenu,
            "abroge": data.get("abroge", False),
        }

    @staticmethod
    def _clean_html(raw_html: str) -> str:
        """Strip HTML tags and decode entities to plain text."""
        text = re.sub(r'<br\s*/?>', '\n', raw_html)
        text = re.sub(r'</(p|div|li|tr|h\d)>', '\n', text, flags=re.I)
        text = re.sub(r'<[^>]+>', '', text)
        text = html.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    @staticmethod
    def _extract_date(title: str) -> Optional[str]:
        """Extract date from title like 'Loi du pays n° 2026-3 du 26 mars 2026 ...'."""
        months = {
            'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
            'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
            'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12',
        }
        m = re.search(r'(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})', title, re.I)
        if m:
            day = int(m.group(1))
            month = months.get(m.group(2).lower(), '01')
            year = m.group(3)
            return f"{year}-{month}-{day:02d}"

        # Try dd/mm/yyyy pattern from lien field
        m2 = re.search(r'(\d{2})/(\d{2})/(\d{4})', title)
        if m2:
            return f"{m2.group(3)}-{m2.group(2)}-{m2.group(1)}"

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation documents from LEXPOL."""
        offset = 0
        total = None
        empty_pages = 0

        while True:
            try:
                data = self._search(offset=offset, limit=PAGE_SIZE)
            except Exception as e:
                logger.error(f"Search failed at offset {offset}: {e}")
                if empty_pages > 3:
                    break
                empty_pages += 1
                offset += PAGE_SIZE
                time.sleep(5)
                continue

            if total is None:
                total = data.get("count", 0)
                logger.info(f"Total legislation documents: {total}")

            records = data.get("data", [])
            if not records:
                break

            empty_pages = 0

            for rec in records:
                text_id = rec.get("id_texte")
                if not text_id:
                    continue

                # Fetch full text
                full = self._get_full_text(text_id)
                if not full or not full.get("contenu"):
                    logger.debug(f"No full text for {text_id}, skipping")
                    continue

                yield {
                    "id_texte": text_id,
                    "titre": rec.get("titre", ""),
                    "lien": rec.get("lien", ""),
                    "public": rec.get("public", True),
                    "annulation": rec.get("annulation", False),
                    "complement": rec.get("complement"),
                    "sous_titre": rec.get("sous_titre"),
                    "contenu_html": full["contenu"],
                    "abroge": full.get("abroge", False),
                }

            offset += PAGE_SIZE
            if offset >= (total or 0):
                break

            logger.info(f"Progress: {offset}/{total} ({100*offset/total:.1f}%)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recently modified legislation (sorted newest first)."""
        offset = 0
        since_str = since.strftime("%Y-%m-%d") if since else None

        while True:
            try:
                data = self._search(offset=offset, limit=PAGE_SIZE)
            except Exception as e:
                logger.error(f"Update search failed at offset {offset}: {e}")
                break

            records = data.get("data", [])
            if not records:
                break

            for rec in records:
                text_id = rec.get("id_texte")
                if not text_id:
                    continue

                # Extract date from title to check if it's recent
                date_str = self._extract_date(rec.get("lien", "") or rec.get("titre", ""))
                if since_str and date_str and date_str < since_str:
                    return

                full = self._get_full_text(text_id)
                if not full or not full.get("contenu"):
                    continue

                yield {
                    "id_texte": text_id,
                    "titre": rec.get("titre", ""),
                    "lien": rec.get("lien", ""),
                    "public": rec.get("public", True),
                    "annulation": rec.get("annulation", False),
                    "complement": rec.get("complement"),
                    "sous_titre": rec.get("sous_titre"),
                    "contenu_html": full["contenu"],
                    "abroge": full.get("abroge", False),
                }

            offset += PAGE_SIZE

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw LEXPOL data into standard schema."""
        text_id = raw.get("id_texte")
        titre = raw.get("titre", "").strip()
        contenu_html = raw.get("contenu_html", "")

        if not contenu_html:
            return None

        text = self._clean_html(contenu_html)
        if len(text) < 20:
            return None

        date = self._extract_date(raw.get("lien", "") or titre)
        url = f"{BASE_URL}/LexpolAfficheTexte.php?texte={text_id}"

        return {
            "_id": f"PF-JOPF-{text_id}",
            "_source": "PF/JOPF",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": titre,
            "text": text,
            "date": date,
            "url": url,
            "is_repealed": raw.get("abroge", False),
            "subtitle": raw.get("sous_titre"),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = LexpolScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        print("Testing LEXPOL API connectivity...")
        try:
            data = scraper._search(offset=0, limit=1)
            count = data.get("count", 0)
            print(f"OK — {count} legislation documents available")

            rec = data["data"][0]
            text_id = rec["id_texte"]
            full = scraper._get_full_text(text_id)
            if full and full.get("contenu"):
                text = scraper._clean_html(full["contenu"])
                print(f"Full text OK — {len(text)} chars for '{rec['titre'][:60]}...'")
            else:
                print("WARNING: Full text endpoint returned empty content")
        except Exception as e:
            print(f"FAIL — {e}")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample)

    elif command == "update":
        scraper.update()

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
