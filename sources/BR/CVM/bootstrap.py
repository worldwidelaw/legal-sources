#!/usr/bin/env python3
"""
BR/CVM -- Brazilian Securities Commission (Comissão de Valores Mobiliários)

Fetches administrative sanctioning proceedings and declaratory acts from
CVM's open data portal (CKAN) at dados.cvm.gov.br.

Data sources:
  1. Sanctioning proceedings (PAS): CSV with Objeto + Ementa full text fields
     ~533 proceedings with detailed legal descriptions
  2. Declaratory acts: CSV with links to .doc files on sistemas.cvm.gov.br
     ~898 acts suspending irregular securities intermediation

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import csv
import sys
import json
import time
import logging
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.CVM")

SOURCE_ID = "BR/CVM"
SAMPLE_DIR = Path(__file__).parent / "sample"

SANCTIONS_ZIP_URL = "https://dados.cvm.gov.br/dados/PROCESSO/SANCIONADOR/DADOS/processo_sancionador.zip"
DECLARATORY_CSV_URL = "https://dados.cvm.gov.br/dados/ATO_DECLR/INTERMED/DADOS/ato_declr.csv"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "*/*",
}

DELAY = 1.5


class CVMScraper(BaseScraper):
    """Scraper for BR/CVM -- Brazilian Securities Commission open data."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _download_csv(self, url: str, encoding: str = "latin-1") -> list[dict]:
        """Download a CSV and return list of row dicts."""
        logger.info("Downloading %s", url)
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        text = resp.content.decode(encoding, errors="replace")
        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        return list(reader)

    def _download_sanctions_csv(self) -> tuple[list[dict], list[dict]]:
        """Download and extract sanctions ZIP containing two CSVs."""
        logger.info("Downloading sanctions ZIP: %s", SANCTIONS_ZIP_URL)
        resp = self.session.get(SANCTIONS_ZIP_URL, timeout=120)
        resp.raise_for_status()

        rows_main = []
        rows_accused = []
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                raw = zf.read(name).decode("latin-1", errors="replace")
                reader = csv.DictReader(io.StringIO(raw), delimiter=";")
                if "acusado" in name.lower():
                    rows_accused = list(reader)
                else:
                    rows_main = list(reader)

        logger.info("Sanctions: %d proceedings, %d accused entries", len(rows_main), len(rows_accused))
        return rows_main, rows_accused

    def _build_accused_map(self, rows_accused: list[dict]) -> dict[str, list[str]]:
        """Build NUP -> list of accused names."""
        accused_map: dict[str, list[str]] = {}
        for row in rows_accused:
            nup = row.get("NUP", "").strip()
            name = row.get("Nome_Acusado", "").strip()
            if nup and name:
                accused_map.setdefault(nup, []).append(name)
        return accused_map

    def _normalize_sanction(self, row: dict, accused: list[str]) -> dict:
        """Normalize a sanctions proceeding record."""
        nup = row.get("NUP", "").strip()
        objeto = row.get("Objeto", "").strip()
        ementa = row.get("Ementa", "").strip()

        # Combine Objeto + Ementa for full text
        parts = []
        if objeto:
            parts.append(f"Objeto: {objeto}")
        if ementa:
            parts.append(f"Ementa: {ementa}")
        text = "\n\n".join(parts)

        date_str = row.get("Data_Abertura", "").strip() or None
        fase = row.get("Fase_Atual", "").strip()
        subfase = row.get("Subfase_Atual", "").strip()

        title = f"PAS {nup}"
        if ementa and len(ementa) < 200:
            title = f"PAS {nup} - {ementa[:150]}"

        return {
            "_id": f"BR-CVM-PAS-{nup}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": f"https://sistemas.cvm.gov.br/?PAS",
            "language": "pt",
            "process_number": nup,
            "phase": fase,
            "subphase": subfase,
            "accused": accused,
            "instruction_unit": row.get("Componente_Organizacional_Instrucao", "").strip(),
        }

    def _normalize_declaratory_act(self, row: dict, doc_text: str = "") -> dict:
        """Normalize a declaratory act record."""
        num = row.get("Numero_Ato_Declaratorio", "").strip()
        date_str = row.get("Data_Ato_Declaratorio", "").strip() or None
        denomination = row.get("Denominacao", "").strip()
        cpf_cnpj = row.get("CNPJ_CPF", "").strip()
        link = row.get("Link_Download", "").strip()
        obs = row.get("Observacao", "").strip()

        # Build text from available fields + downloaded doc text
        parts = []
        parts.append(f"Ato Declaratório nº {num}")
        if denomination:
            parts.append(f"Entidade: {denomination}")
        if cpf_cnpj:
            parts.append(f"CNPJ/CPF: {cpf_cnpj}")
        if obs:
            parts.append(f"Observação: {obs}")
        if doc_text:
            parts.append(f"\n{doc_text}")

        text = "\n".join(parts)

        title = f"Ato Declaratório CVM nº {num}"
        if denomination:
            title += f" - {denomination[:100]}"

        return {
            "_id": f"BR-CVM-AD-{num}-{cpf_cnpj}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": link or "https://dados.cvm.gov.br/dataset/ato_declr-intermed",
            "language": "pt",
            "act_number": num,
            "entity_name": denomination,
            "entity_id": cpf_cnpj,
            "dou_publication_date": row.get("Data_Publicacao_DOU", "").strip() or None,
        }

    def _try_download_doc(self, url: str) -> str:
        """Try to download a .doc file and extract raw text."""
        if not url:
            return ""
        try:
            time.sleep(DELAY)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            content = resp.content
            # .doc files: extract text by finding printable strings
            # Legacy .doc format - extract readable text between null bytes
            text = ""
            try:
                # Try decoding as latin-1 and extracting readable portions
                raw = content.decode("latin-1", errors="replace")
                # Find text segments (sequences of printable chars)
                import re
                segments = re.findall(r'[\w\s.,;:!\-\(\)\/áàâãéêíóôõúüçÁÀÂÃÉÊÍÓÔÕÚÜÇ]{20,}', raw)
                text = " ".join(segments)
                # Clean up
                text = re.sub(r'\s+', ' ', text).strip()
            except Exception:
                pass
            return text
        except Exception as e:
            logger.debug("Failed to download doc %s: %s", url, e)
            return ""

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all CVM sanctions and declaratory acts."""
        count = 0
        sample_limit = 15 if sample else 999999

        # 1. Sanctions proceedings
        logger.info("=== Fetching sanctions proceedings ===")
        rows_main, rows_accused = self._download_sanctions_csv()
        accused_map = self._build_accused_map(rows_accused)

        for row in rows_main:
            if count >= sample_limit:
                break
            nup = row.get("NUP", "").strip()
            if not nup:
                continue
            accused = accused_map.get(nup, [])
            record = self._normalize_sanction(row, accused)
            if len(record["text"]) < 20:
                logger.warning("Skipping sanction %s: no text content", nup)
                continue
            yield record
            count += 1

        if count >= sample_limit:
            logger.info("Sample limit reached after sanctions (%d records)", count)
            return

        # 2. Declaratory acts
        logger.info("=== Fetching declaratory acts ===")
        rows_decl = self._download_csv(DECLARATORY_CSV_URL)
        logger.info("Declaratory acts: %d rows", len(rows_decl))

        # De-duplicate by act number + entity (same act can appear multiple times)
        seen_ids = set()
        for row in rows_decl:
            if count >= sample_limit:
                break
            num = row.get("Numero_Ato_Declaratorio", "").strip()
            cpf = row.get("CNPJ_CPF", "").strip()
            dedup_key = f"{num}-{cpf}"
            if dedup_key in seen_ids:
                continue
            seen_ids.add(dedup_key)

            # Try to download .doc for full text (only in sample mode for a few)
            doc_text = ""
            link = row.get("Link_Download", "").strip()
            if link and (sample or count < 50):
                doc_text = self._try_download_doc(link)

            record = self._normalize_declaratory_act(row, doc_text)
            yield record
            count += 1

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch updates since a date. Re-downloads full dataset and filters."""
        for record in self.fetch_all():
            if record.get("date") and record["date"] >= since:
                yield record

    def normalize(self, raw: dict) -> dict:
        """Already normalized in fetch methods."""
        return raw


def main():
    scraper = CVMScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity...")
        resp = requests.get(
            "https://dados.cvm.gov.br/api/3/action/package_list",
            headers=HEADERS, timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        logger.info("CKAN API OK — %d datasets available", len(data.get("result", [])))
        return

    if command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=sample):
            count += 1
            fname = SAMPLE_DIR / f"{record['_id'].replace('/', '_')}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            if count % 50 == 0:
                logger.info("Saved %d records...", count)
        logger.info("Bootstrap complete: %d records saved to %s", count, SAMPLE_DIR)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else "2025-01-01"
        count = sum(1 for _ in scraper.fetch_updates(since))
        logger.info("Update complete: %d records since %s", count, since)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
