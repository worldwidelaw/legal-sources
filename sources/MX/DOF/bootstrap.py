#!/usr/bin/env python3
"""
MX/DOF -- Mexico Diario Oficial de la Federación (Official Federal Gazette)

Fetches federal legislation, decrees, regulations, and agreements from the DOF.

Strategy:
  - Uses the official SIDOF Open Data API (JSON endpoints).
  - List notes by date: /dof/sidof/notas/{DD-MM-YYYY} returns JSON with metadata.
  - Full text HTML: /notas/docFuente/{codNota} returns the complete document.
  - Strip HTML to get clean text content.

API:
  - Base: https://sidof.segob.gob.mx
  - Notes by date: /dof/sidof/notas/{DD-MM-YYYY}
  - Full text: /notas/docFuente/{codNota}
  - Diaries by year: /dof/sidof/diarios/{year}
  - No auth required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as htmlmod
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.DOF")

SIDOF_URL = "https://sidof.segob.gob.mx"
START_YEAR = 2005
SAMPLE_DAYS = 5


class DOFScraper(BaseScraper):
    """
    Scraper for MX/DOF -- Mexico's Official Federal Gazette.
    Uses the SIDOF Open Data API for structured JSON access.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=SIDOF_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "*/*",
                "Accept-Language": "es-MX,es;q=0.9",
            },
            timeout=60,
        )

    def _get_notes_for_date(self, d: date) -> List[Dict[str, Any]]:
        """Fetch all notes for a specific date from SIDOF API."""
        self.rate_limiter.wait()
        date_str = d.strftime("%d-%m-%Y")
        url = f"/dof/sidof/notas/{date_str}"

        try:
            resp = self.client.get(url)
            if not resp or resp.status_code != 200:
                logger.warning("Failed to fetch notes for %s (status %s)",
                               d.isoformat(), getattr(resp, 'status_code', 'N/A'))
                return []

            data = resp.json()
            if data.get("messageCode") != 200:
                logger.warning("API error for %s: %s", d.isoformat(), data.get("response"))
                return []

            notes = []
            for edition_key in ("NotasMatutinas", "NotasVespertinas", "NotasExtraordinarias"):
                edition_notes = data.get(edition_key, [])
                for note in edition_notes:
                    cod_nota = note.get("codNota")
                    titulo = note.get("titulo", "")
                    has_content = (
                        note.get("existeHtml") == "S"
                        or note.get("existeDoc") == "S"
                    )
                    if cod_nota and titulo and has_content:
                        notes.append({
                            "codNota": str(cod_nota),
                            "titulo": titulo,
                            "fecha": note.get("fecha", date_str),
                            "codSeccion": note.get("codSeccion", ""),
                            "codDiario": note.get("codDiario"),
                            "nombreCodOrgaUno": note.get("nombreCodOrgaUno", ""),
                            "codOrgaDos": note.get("codOrgaDos", ""),
                            "pagina": note.get("pagina"),
                        })

            return notes

        except Exception as e:
            logger.warning("Error fetching notes for %s: %s", d.isoformat(), e)
            return []

    def _fetch_full_text(self, cod_nota: str) -> Optional[str]:
        """Fetch full text HTML from docFuente endpoint and clean to plain text."""
        self.rate_limiter.wait()
        url = f"/notas/docFuente/{cod_nota}"

        try:
            resp = self.client.get(url)
            if not resp or resp.status_code != 200:
                return None

            content = resp.text
            if not content or len(content) < 200:
                return None

            # Remove style blocks
            text = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
            # Remove script blocks
            text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
            # Replace block elements with newlines
            text = re.sub(r'<(?:p|div|br|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
            # Strip remaining HTML tags
            text = re.sub(r'<[^>]+>', ' ', text)
            # Decode HTML entities
            text = htmlmod.unescape(text)
            # Clean whitespace
            lines = [line.strip() for line in text.split('\n')]
            lines = [line for line in lines if line]
            text = '\n'.join(lines).strip()

            if len(text) < 50:
                return None

            return text

        except Exception as e:
            logger.warning("Error fetching text for note %s: %s", cod_nota, e)
            return None

    def _parse_dof_date(self, date_str: str) -> Optional[str]:
        """Parse DOF date format DD-MM-YYYY to ISO 8601 YYYY-MM-DD."""
        if not date_str:
            return None
        try:
            parts = date_str.split("-")
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1]}-{parts[0]}"
        except Exception:
            pass
        return date_str

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all DOF notes with metadata (full text fetched in normalize)."""
        today = date.today()
        d = today
        start = date(START_YEAR, 1, 1)

        while d >= start:
            if d.weekday() < 5:  # DOF doesn't publish on weekends
                notes = self._get_notes_for_date(d)
                if notes:
                    logger.info("Found %d notes for %s", len(notes), d.isoformat())
                    for note in notes:
                        note["_pub_date"] = d.isoformat()
                        yield note
            d -= timedelta(days=1)

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Yield notes published since the given datetime."""
        if isinstance(since, str):
            since_date = date.fromisoformat(since[:10])
        else:
            since_date = since.date() if hasattr(since, 'date') else since

        today = date.today()
        d = today
        while d >= since_date:
            if d.weekday() < 5:
                notes = self._get_notes_for_date(d)
                for note in notes:
                    note["_pub_date"] = d.isoformat()
                    yield note
            d -= timedelta(days=1)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw SIDOF note into standard schema, fetching full text."""
        cod_nota = raw.get("codNota")
        titulo = raw.get("titulo", "")
        if not cod_nota or not titulo:
            return None

        # Fetch full text
        text = self._fetch_full_text(cod_nota)
        if not text:
            return None

        pub_date = raw.get("_pub_date") or self._parse_dof_date(raw.get("fecha"))

        return {
            "_id": f"DOF_{cod_nota}",
            "_source": "MX/DOF",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": titulo,
            "text": text,
            "date": pub_date,
            "url": f"{SIDOF_URL}/notas/docFuente/{cod_nota}",
            "codNota": cod_nota,
            "section": raw.get("codSeccion", ""),
            "issuing_body": raw.get("nombreCodOrgaUno", ""),
            "issuing_department": raw.get("codOrgaDos", ""),
            "codDiario": str(raw.get("codDiario", "")),
            "page": raw.get("pagina"),
        }


def main():
    scraper = DOFScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing DOF SIDOF API connectivity...")
        today = date.today()
        d = today
        while d.weekday() >= 5:
            d -= timedelta(days=1)

        notes = scraper._get_notes_for_date(d)
        if notes:
            logger.info("SUCCESS: Found %d notes for %s", len(notes), d.isoformat())
            note = notes[0]
            text = scraper._fetch_full_text(note["codNota"])
            if text:
                logger.info("SUCCESS: Full text (%d chars) for: %s",
                            len(text), note["titulo"][:80])
            else:
                logger.error("FAILED: Could not fetch full text for note %s", note["codNota"])
        else:
            logger.error("FAILED: No notes found for %s", d.isoformat())

    elif command == "bootstrap":
        if sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
            logger.info("Sample bootstrap complete: %s", json.dumps(stats, indent=2))
        else:
            stats = scraper.bootstrap()
            logger.info("Full bootstrap complete: %s", json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        logger.info("Update complete: %s", json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
