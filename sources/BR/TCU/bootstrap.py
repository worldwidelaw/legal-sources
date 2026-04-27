#!/usr/bin/env python3
"""
BR/TCU -- Brazilian Federal Audit Court (Tribunal de Contas da União)

Fetches TCU acórdãos (decisions) with full text from the open data portal.

Two data access methods:
  1. JSON REST API: metadata + summary (no full text)
     https://dados-abertos.apps.tcu.gov.br/api/acordao/recupera-acordaos
  2. CSV bulk downloads: full text included (ACORDAO, RELATORIO, VOTO, DECISAO)
     https://sites.tcu.gov.br/dados-abertos/jurisprudencia/

The bootstrap uses CSV downloads for full text. Each yearly CSV contains
pipe-delimited records with 32 columns including complete decision text.

Coverage: 1992–present, ~100k+ decisions across Plenário, 1ª/2ª Câmara.

Usage:
  python bootstrap.py bootstrap          # Full pull (latest year CSV)
  python bootstrap.py bootstrap --sample # 15 sample records
  python bootstrap.py bootstrap --full   # All years (WARNING: ~7GB total)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import csv
import json
import hashlib
import logging
import io
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.TCU")

CSV_INDEX_URL = "https://sites.tcu.gov.br/dados-abertos/jurisprudencia/arquivos/jurisprudencia-arquivos.csv"
CSV_BASE = "https://sites.tcu.gov.br/dados-abertos/jurisprudencia/arquivos/acordao-completo/acordao-completo-{year}.csv"
JSON_API = "https://dados-abertos.apps.tcu.gov.br/api/acordao/recupera-acordaos"

# CSV columns (pipe-delimited)
CSV_COLUMNS = [
    "KEY", "TIPO", "TITULO", "NUMACORDAO", "ANOACORDAO", "NUMATA",
    "COLEGIADO", "DATASESSAO", "RELATOR", "SITUACAO", "PROC",
    "ACORDAOSRELACIONADOS", "TIPOPROCESSO", "INTERESSADOS", "ENTIDADE",
    "RELATORDELIBERACAORECORRIDA", "MINISTROREVISOR",
    "MINISTROAUTORVOTOVENCEDOR", "REPRESENTANTEMP", "UNIDADETECNICA",
    "ADVOGADO", "ASSUNTO", "SUMARIO", "ACORDAO", "DECISAO", "QUORUM",
    "MINISTROALEGOUIMPEDIMENTOSESSAO", "RECURSOS", "RELATORIO", "VOTO",
    "DECLARACAOVOTO", "VOTOCOMPLEMENTAR", "VOTOMINISTROREVISOR",
]


def _clean_text(text: str) -> str:
    """Clean text: strip HTML tags, decode entities, normalize whitespace."""
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _parse_date(date_str: str) -> Optional[str]:
    """Parse TCU date (DD/MM/YYYY) to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip().strip('"')
    m = re.match(r'^(\d{2})/(\d{2})/(\d{4})$', date_str)
    if m:
        try:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        except (ValueError, IndexError):
            pass
    return None


def _strip_quotes(val: str) -> str:
    """Strip surrounding double quotes from CSV field."""
    val = val.strip()
    if val.startswith('"') and val.endswith('"'):
        val = val[1:-1]
    return val


class TCUScraper(BaseScraper):
    """Scraper for BR/TCU -- Federal Audit Court open data."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _get_available_years(self) -> List[int]:
        """Get list of years with CSV data available."""
        import requests

        resp = requests.get(CSV_INDEX_URL, timeout=30)
        resp.raise_for_status()
        years = []
        for line in resp.text.strip().split('\n'):
            if line.startswith('Acórdãos,'):
                parts = line.split(',')
                if len(parts) >= 2 and parts[1].strip().isdigit():
                    years.append(int(parts[1].strip()))
        return sorted(years)

    def _stream_csv_year(self, year: int, limit: Optional[int] = None) -> Generator[Dict[str, str], None, None]:
        """Stream and parse a yearly CSV file, yielding row dicts."""
        import requests

        url = CSV_BASE.format(year=year)
        logger.info(f"Downloading CSV for {year}: {url}")

        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        count = 0
        buffer = ""
        first_line = True

        for chunk in resp.iter_content(chunk_size=1024 * 256, decode_unicode=False):
            buffer += chunk.decode('utf-8', errors='replace')

            while '\n' in buffer:
                line, buffer = buffer.split('\n', 1)

                if first_line:
                    first_line = False
                    # Skip header row
                    continue

                line = line.strip()
                if not line:
                    continue

                # Parse pipe-delimited row
                fields = line.split('|', len(CSV_COLUMNS) - 1)
                if len(fields) < 10:
                    continue

                row = {}
                for i, col_name in enumerate(CSV_COLUMNS):
                    if i < len(fields):
                        row[col_name] = _strip_quotes(fields[i])
                    else:
                        row[col_name] = ""

                count += 1
                yield row

                if limit and count >= limit:
                    resp.close()
                    return

        # Process remaining buffer
        if buffer.strip() and not first_line:
            line = buffer.strip()
            fields = line.split('|', len(CSV_COLUMNS) - 1)
            if len(fields) >= 10:
                row = {}
                for i, col_name in enumerate(CSV_COLUMNS):
                    if i < len(fields):
                        row[col_name] = _strip_quotes(fields[i])
                    else:
                        row[col_name] = ""
                yield row

        logger.info(f"Parsed {count} records from {year}")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all records from the latest year's CSV."""
        current_year = datetime.now().year
        yield from self._stream_csv_year(current_year)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch records from the current year's CSV."""
        current_year = datetime.now().year
        yield from self._stream_csv_year(current_year)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw CSV row into standardized schema."""
        # Build full text from main text fields
        text_parts = []
        for field in ["RELATORIO", "VOTO", "ACORDAO", "DECISAO"]:
            val = _clean_text(raw.get(field, ""))
            if val:
                text_parts.append(val)

        # Also include supplementary votes
        for field in ["DECLARACAOVOTO", "VOTOCOMPLEMENTAR", "VOTOMINISTROREVISOR"]:
            val = _clean_text(raw.get(field, ""))
            if val:
                text_parts.append(val)

        text = "\n\n".join(text_parts)
        if not text:
            return None

        key = raw.get("KEY", "").strip()
        titulo = raw.get("TITULO", "").strip()
        num = raw.get("NUMACORDAO", "").strip()
        ano = raw.get("ANOACORDAO", "").strip()

        if key:
            doc_id = f"BR-TCU-{key}"
        elif num and ano:
            doc_id = f"BR-TCU-ACORDAO-{num}-{ano}"
        else:
            text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
            doc_id = f"BR-TCU-{text_hash}"

        title = titulo or f"Acórdão {num}/{ano}" if num and ano else "TCU Decision"
        date = _parse_date(raw.get("DATASESSAO", ""))
        colegiado = raw.get("COLEGIADO", "").strip()
        relator = raw.get("RELATOR", "").strip()
        proc = raw.get("PROC", "").strip()
        sumario = _clean_text(raw.get("SUMARIO", ""))
        tipo_processo = raw.get("TIPOPROCESSO", "").strip()

        # Determine type: most TCU decisions are case_law (audit proceedings)
        doc_type = "case_law"

        return {
            "_id": doc_id,
            "_source": "BR/TCU",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"https://pesquisa.apps.tcu.gov.br/pesquisa/jurisprudencia/#/detalhamento/11/*/{key}" if key else "https://sites.tcu.gov.br/dados-abertos/jurisprudencia/",
            "summary": sumario,
            "panel": colegiado,
            "judge_relator": relator,
            "process_number": proc,
            "process_type": tipo_processo,
            "entity": raw.get("ENTIDADE", "").strip(),
            "subject": raw.get("ASSUNTO", "").strip(),
        }


if __name__ == "__main__":
    scraper = TCUScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--full]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv
    full = "--full" in sys.argv

    if cmd == "test":
        print("Testing TCU API connectivity...")
        try:
            import requests
            # Test JSON API
            resp = requests.get(JSON_API, params={"inicio": 0, "quantidade": 1}, timeout=15)
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                print(f"OK: JSON API works. Latest: {data[0].get('titulo', 'N/A')}")
            else:
                print("WARN: JSON API returned unexpected format")

            # Test CSV index
            years = scraper._get_available_years()
            print(f"OK: CSV data available for {len(years)} years ({years[0]}–{years[-1]})")
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd == "bootstrap":
        stats = scraper.bootstrap(sample_mode="--sample" in sys.argv, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif cmd == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
