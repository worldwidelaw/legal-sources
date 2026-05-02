#!/usr/bin/env python3
"""
MX/SAT-Criterios -- Mexican SAT Tax Criteria Fetcher

Fetches normative criteria (Anexo 7) and non-binding criteria (Anexo 3) from
the Mexican Resolución Miscelánea Fiscal, published in the DOF and accessible
via SIDOF (Sistema de Información del Diario Oficial de la Federación).

Criterios Normativos (N): Binding interpretations of tax law by SAT.
Criterios No Vinculativos (NV): Descriptions of improper tax practices.

Source: https://sidof.segob.gob.mx/notas/docFuente/{codigo}
Volume: ~240 criteria per year (2019-2026), covering CFF, ISR, IVA, IEPS,
        LFD, LISH, LIF, LIGIE.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.SAT-Criterios")

SIDOF_BASE = "https://sidof.segob.gob.mx"
DELAY = 2.0  # seconds between requests

# Known SIDOF codes for RMF Annexes 3+7 by year
# These are the combined Anexo publications containing both N and NV criteria
ANEXO_CODES = {
    "2022": {"codigo": "5640230", "date": "2021-12-27"},
    "2023": {"codigo": "5676651", "date": "2022-12-27"},
    "2024": {"codigo": "5713708", "date": "2024-01-05"},
}

# Law codes referenced in criteria
LAW_CODES = {
    "CFF": "Código Fiscal de la Federación",
    "ISR": "Ley del Impuesto sobre la Renta",
    "IVA": "Ley del Impuesto al Valor Agregado",
    "IEPS": "Ley del Impuesto Especial sobre Producción y Servicios",
    "LFD": "Ley Federal de Derechos",
    "LISH": "Ley del Impuesto sobre Hidrocarburos",
    "LIF": "Ley de Ingresos de la Federación",
    "LIGIE": "Ley de los Impuestos Generales de Importación y Exportación",
}


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_criteria_from_html(html: str) -> List[Dict[str, str]]:
    """Parse individual tax criteria from an Anexo 3+7 SIDOF HTML document.

    The document contains criteria twice: first as a table of contents (titles only),
    then as full text in the body. We parse the body section (second occurrence of
    each criterion ID) to get the full explanatory text.
    """
    # Pattern to match criterion IDs like 1/CFF/N, 1/ISR/NV, etc.
    crit_pattern = r'(\d+)/(' + '|'.join(LAW_CODES.keys()) + r')/(NV?)'

    # Find all criterion positions in the document
    positions = []
    for m in re.finditer(crit_pattern, html):
        crit_id = m.group(0)
        positions.append((m.start(), crit_id, m.group(1), m.group(2), m.group(3)))

    if not positions:
        return []

    # The document has a TOC section (short titles) followed by a body section
    # (full text). The body section is marked by "Vigentes" header.
    # Find the body start by looking for the "Vigentes" marker.
    vigentes_match = re.search(r'Vigentes', html)
    if vigentes_match:
        body_start_pos = vigentes_match.start()
    else:
        # Fallback: use document midpoint
        body_start_pos = len(html) // 3

    # Filter positions to only include those in the body section
    unique_body = [(pos, cid, num, law, ctype)
                   for pos, cid, num, law, ctype in positions
                   if pos >= body_start_pos]

    # First pass: extract all criterion text chunks
    raw_criteria = []
    all_positions = unique_body
    for i, (pos, cid, num, law, ctype) in enumerate(all_positions):
        start = pos
        if i + 1 < len(all_positions):
            end = all_positions[i + 1][0]
        else:
            end = min(pos + 20000, len(html))

        chunk = html[start:end]
        text = strip_html(chunk)

        # Clean up leading style artifacts
        text = re.sub(r'^[^A-Za-z0-9/]*', '', text)

        # Remove trailing section headers like "II. Criterios de la Ley..."
        text = re.sub(r'\n+[IVXLC]+\.\s+Criterios de la Ley.*$', '', text, flags=re.DOTALL)
        text = re.sub(r'\n+Anexo\s+\d+.*$', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = text.strip()

        if len(text) < 20:
            continue

        raw_criteria.append((cid, num, law, ctype, text))

    # Second pass: for criteria that appear multiple times, keep the longest
    # version (body entry, not TOC entry). TOC entries are typically <200 chars.
    best_by_id: Dict[str, Tuple] = {}
    for cid, num, law, ctype, text in raw_criteria:
        if cid not in best_by_id or len(text) > len(best_by_id[cid][4]):
            best_by_id[cid] = (cid, num, law, ctype, text)

    criteria = []
    for cid, num, law, ctype, text in best_by_id.values():
        # Extract title: first sentence or line after the criterion ID
        title_match = re.match(
            r'\d+/[A-Z]+/NV?\s+(.*?)(?:\.\s|$)',
            text,
            re.DOTALL,
        )
        title = title_match.group(1).strip() if title_match else cid

        criteria.append({
            "criterion_id": cid,
            "number": num,
            "law_code": law,
            "criterion_type": "normativo" if ctype == "N" else "no_vinculativo",
            "type_abbrev": ctype,
            "title": title[:300],
            "text": text,
            "law_name": LAW_CODES.get(law, law),
        })

    # Sort by law code and number for consistent ordering
    criteria.sort(key=lambda c: (c["type_abbrev"], c["law_code"], int(c["number"])))
    return criteria


class SATCriterios(BaseScraper):
    SOURCE_ID = "MX/SAT-Criterios"

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=SIDOF_BASE,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
                "User-Agent": "LegalDataHunter/1.0 (academic research; open legal data)",
            },
        )

    def fetch_anexo(self, year: str, codigo: str) -> List[Dict[str, str]]:
        """Fetch and parse criteria from a specific year's Anexo publication."""
        logger.info("Fetching SIDOF document %s (year %s)...", codigo, year)
        resp = self.http.get(f"/notas/docFuente/{codigo}")
        time.sleep(DELAY)

        if resp is None or resp.status_code != 200:
            logger.warning("Failed to fetch SIDOF %s (status=%s)",
                           codigo, resp.status_code if resp else "None")
            return []

        html = resp.text
        if len(html) < 1000:
            logger.warning("SIDOF %s response too small (%d bytes)", codigo, len(html))
            return []

        criteria = parse_criteria_from_html(html)
        logger.info("Parsed %d criteria from year %s (SIDOF %s)", len(criteria), year, codigo)
        return criteria

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a parsed criterion into the standard schema.

        Expects raw dict with criterion fields plus `_year` and `_pub_date`
        embedded by fetch_all().
        """
        cid = raw["criterion_id"]
        year = raw.get("_year") or raw.get("rmf_year", "")
        pub_date = raw.get("_pub_date") or raw.get("date", "")
        return {
            "_id": f"MX-SAT-{year}-{cid.replace('/', '-')}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{cid} {raw['title']}",
            "text": raw["text"],
            "date": pub_date,
            "url": f"{SIDOF_BASE}/notas/docFuente/{ANEXO_CODES.get(year, {}).get('codigo', '')}",
            "language": "es",
            "criterion_id": cid,
            "criterion_type": raw["criterion_type"],
            "law_code": raw["law_code"],
            "law_name": raw["law_name"],
            "rmf_year": year,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all criteria from all known RMF years."""
        total = 0
        sample_limit = 15 if sample else None

        # Process years from newest to oldest
        years = sorted(ANEXO_CODES.keys(), reverse=True)
        if sample:
            years = years[:1]  # Only latest year for sample

        for year in years:
            if sample_limit and total >= sample_limit:
                break

            info = ANEXO_CODES[year]
            criteria = self.fetch_anexo(year, info["codigo"])

            for criterion in criteria:
                if sample_limit and total >= sample_limit:
                    break

                # Embed year and pub_date; BaseScraper will call normalize(raw)
                criterion["_year"] = year
                criterion["_pub_date"] = info["date"]
                yield criterion
                total += 1

                if total % 50 == 0:
                    logger.info("  Progress: %d criteria fetched", total)

        logger.info("Fetch complete. Total criteria: %d", total)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch criteria from the most recent year."""
        latest_year = max(ANEXO_CODES.keys())
        info = ANEXO_CODES[latest_year]

        if info["date"] < since:
            logger.info("No updates since %s (latest is %s)", since, info["date"])
            return

        criteria = self.fetch_anexo(latest_year, info["codigo"])
        for criterion in criteria:
            criterion["_year"] = latest_year
            criterion["_pub_date"] = info["date"]
            yield criterion

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            latest_year = max(ANEXO_CODES.keys())
            info = ANEXO_CODES[latest_year]
            resp = self.http.get(f"/notas/docFuente/{info['codigo']}")
            if resp is None or resp.status_code != 200:
                logger.error("Test failed: HTTP %s", resp.status_code if resp else "None")
                return False

            criteria = parse_criteria_from_html(resp.text)
            if not criteria:
                logger.error("Test failed: no criteria parsed")
                return False

            logger.info("Test passed: %d criteria found in year %s, first: %s (%d chars)",
                        len(criteria), latest_year, criteria[0]["criterion_id"],
                        len(criteria[0]["text"]))
            return True
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/SAT-Criterios bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SATCriterios()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))
        count = stats.get("sample_records_saved") or stats.get("records_fetched", 0)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        stats = scraper.update()
        logger.info("Update complete: %s", json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
