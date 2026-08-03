#!/usr/bin/env python3
"""
IT/Trento -- Trentino Provincial Legislation (Codice provinciale)

Provincial laws and regulations of the Autonomous Province of Trento (1951-present)
from the official "Codice provinciale" of the Consiglio della Provincia autonoma di
Trento.

Strategy:
  - Download the official open-data XML index (CC BY) that lists every act:
        https://www.consiglio.provincia.tn.it/normattiva/normattiva.xml
    Each <LEGGE> element carries metadata (TIPO, TESTO, NUMERO, DATA, TITOLO)
    plus <URNTESTO>, a plain-text URL to the full consolidated text.
  - For each act, download the .TXT full text (cp1252-encoded) and emit a record.
  - Covers both in-force (vigente) and repealed (abrogato) acts (~2,700 total).

fetch_all() yields RAW metadata dicts (no per-item network); normalize() performs
the full-text download so BaseScraper.bootstrap_fast can parallelize it.

Usage:
  python bootstrap.py bootstrap --sample   # 15 sample records
  python bootstrap.py bootstrap            # full pull (streams to data/records.jsonl)
  python bootstrap.py bootstrap-fast       # full pull, concurrent downloads
  python bootstrap.py update 2024          # acts changed since a year
  python bootstrap.py test                 # connectivity/parse test
"""

import re
import sys
import json
import time
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Trento")

INDEX_XML = "https://www.consiglio.provincia.tn.it/normattiva/normattiva.xml"

# TIPO codes used in the Codice provinciale XML
TIPO_LABELS = {
    "legger": "Legge provinciale",
    "rego": "Regolamento",
}


class TrentoScraper(BaseScraper):
    SOURCE_ID = "IT/Trento"

    def __init__(self, source_dir=None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "text/html,application/xhtml+xml,text/plain,*/*",
        })

    # ── HTTP helpers ──────────────────────────────────────────────────
    def _get(self, url: str) -> requests.Response:
        last_exc = None
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                last_exc = e
                logger.warning("GET attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 ** attempt)
        raise last_exc

    def _fetch_text(self, url: str) -> str:
        """Download a plain-text act body and decode it (cp1252 with fallbacks)."""
        resp = self._get(url)
        raw = resp.content
        for enc in ("cp1252", "latin-1", "utf-8"):
            try:
                text = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            text = raw.decode("utf-8", errors="replace")
        # Normalize whitespace: collapse runs of blank lines, strip trailing spaces
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ── Enumeration ───────────────────────────────────────────────────
    def _load_index(self):
        logger.info("Downloading Codice provinciale index XML...")
        resp = self._get(INDEX_XML)
        root = ET.fromstring(resp.content)
        leggi = root.findall(".//LEGGE")
        logger.info("Index lists %d acts", len(leggi))
        return leggi

    @staticmethod
    def _el_text(el, tag) -> str:
        child = el.find(tag)
        return (child.text or "").strip() if child is not None and child.text else ""

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        for el in self._load_index():
            yield {
                "tipo": self._el_text(el, "TIPO"),
                "testo": self._el_text(el, "TESTO"),
                "numero": self._el_text(el, "NUMERO"),
                "data": self._el_text(el, "DATA"),
                "titolo": self._el_text(el, "TITOLO"),
                "datains": self._el_text(el, "DATAINS"),
                "urnpagina": self._el_text(el, "URNPAGINA"),
                "urntesto": self._el_text(el, "URNTESTO"),
            }

    def fetch_updates(self, since) -> Generator[Dict[str, Any], None, None]:
        since_str = str(since)[:10]
        for raw in self.fetch_all():
            marker = raw.get("datains") or raw.get("data") or ""
            if marker and marker >= since_str:
                yield raw

    # ── Normalization (downloads full text) ───────────────────────────
    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        urntesto = raw.get("urntesto", "")
        if not urntesto:
            return None

        try:
            text = self._fetch_text(urntesto)
        except Exception as e:
            logger.error("Failed to fetch text %s: %s", urntesto, e)
            return None

        if len(text) < 30:
            logger.warning("Short/empty text for %s (%d chars), skipping", urntesto, len(text))
            return None

        tipo = raw.get("tipo", "")
        numero = raw.get("numero", "")
        data = raw.get("data", "") or None
        year = data[:4] if data else ""
        label = TIPO_LABELS.get(tipo, tipo or "Atto")

        if year and numero:
            law_id = f"IT/Trento/{tipo}-{year}-{numero}"
            law_number = f"{label} {numero}/{year}"
        else:
            # Fall back to a stable id from the text URL
            slug = re.sub(r"[^A-Za-z0-9]+", "-", urntesto.rsplit("/", 1)[-1]).strip("-")
            law_id = f"IT/Trento/{slug}"
            law_number = label

        return {
            "_id": law_id,
            "_source": "IT/Trento",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("titolo", "") or law_number,
            "text": text,
            "date": data,
            "url": raw.get("urnpagina", "") or urntesto,
            "law_number": law_number,
            "act_type": label,
            "in_force": raw.get("testo", "").lower() == "vigente",
            "jurisdiction": "IT-TN",
        }

    def test(self) -> bool:
        try:
            leggi = self._load_index()
            if not leggi:
                return False
            # Verify one full-text fetch works
            for el in leggi:
                url = self._el_text(el, "URNTESTO")
                if url:
                    txt = self._fetch_text(url)
                    return len(txt) > 30
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    scraper = TrentoScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        sys.exit(0 if ok else 1)

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            count = 0
            for raw in scraper.fetch_all():
                scraper.rate_limiter.wait()
                record = scraper.normalize(raw)
                if not record:
                    continue
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info("[%d] %s — %d chars", count,
                            record.get("law_number", record["_id"]), len(record["text"]))
                if count >= 15:
                    break
            logger.info("Done: %d sample records saved", count)
        elif command == "bootstrap-fast":
            stats = scraper.bootstrap_fast()
            logger.info("bootstrap-fast done: %s", stats)
        else:
            stats = scraper.bootstrap()
            logger.info("bootstrap done: %s", stats)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else str(datetime.now().year - 1)
        count = 0
        for raw in scraper.fetch_updates(since):
            record = scraper.normalize(raw)
            if record:
                count += 1
                logger.info("[%d] %s", count, record.get("law_number", record["_id"]))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
