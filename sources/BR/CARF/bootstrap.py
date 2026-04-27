#!/usr/bin/env python3
"""
BR/CARF -- Brazilian Tax Appeals Council (Conselho Administrativo de Recursos Fiscais)

Fetches tax tribunal decisions (acórdãos) from CARF via their public Solr index.
571,000+ decisions covering all federal tax disputes.

API: Apache Solr at https://acordaos.economia.gov.br/solr/acordaos2/select
  - Standard Solr query parameters (q, start, rows, fl, sort, wt=json)
  - Full text in conteudo_txt field (Tika-extracted from PDF, strip metadata prefix)
  - Summary in ementa_s field

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.CARF")

SOLR_URL = "https://acordaos.economia.gov.br/solr/acordaos2/select"
SOURCE_ID = "BR/CARF"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

FIELDS = (
    "id,numero_processo_s,numero_decisao_s,ementa_s,conteudo_txt,"
    "decisao_txt,dt_sessao_tdt,nome_relator_s,secao_s,camara_s,"
    "turma_s,ano_sessao_s,anomes_sessao_s"
)

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/json",
}

DELAY = 1.5
ROWS_PER_PAGE = 100


def fix_doubled_chars(text: str) -> str:
    """Fix doubled characters from PDF extraction artifacts (e.g., 'MMiinniissttéérriioo' -> 'Ministério')."""
    # Detect doubled-char pattern: check if removing every other char produces valid text
    # Heuristic: if >30% of char pairs are identical adjacent pairs, it's doubled
    if len(text) < 20:
        return text
    sample = text[:200].replace('\n', '').replace(' ', '')
    if len(sample) < 10:
        return text
    pairs = sum(1 for i in range(0, len(sample) - 1, 2) if sample[i] == sample[i + 1])
    ratio = pairs / (len(sample) / 2) if len(sample) > 0 else 0
    if ratio < 0.4:
        return text
    # De-duplicate: process character by character
    result = []
    i = 0
    while i < len(text):
        result.append(text[i])
        # Skip next char if it's identical (doubled) and not a whitespace/newline
        if i + 1 < len(text) and text[i] == text[i + 1] and text[i] not in '\n\r':
            i += 2
        else:
            i += 1
    return ''.join(result)


def extract_content(raw_text: str) -> str:
    """Extract actual text content from Tika-extracted field, stripping metadata prefix."""
    if not raw_text:
        return ""
    # Content starts after "Conteúdo =>" or "Conteudo =>"
    for marker in ["Conteúdo =>", "Conteudo =>", "Content =>"]:
        idx = raw_text.find(marker)
        if idx >= 0:
            text = raw_text[idx + len(marker):]
            text = re.sub(r'[ \t]+', ' ', text)
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = fix_doubled_chars(text)
            return text.strip()
    # Fallback: if no marker found, return everything after first newline block
    parts = raw_text.split('\n\n', 1)
    if len(parts) > 1 and len(parts[1]) > 100:
        text = parts[1].strip()
        return fix_doubled_chars(text)
    return fix_doubled_chars(raw_text.strip())


class CARFScraper(BaseScraper):
    """Scraper for BR/CARF -- Brazilian tax tribunal decisions via Solr."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        return {"last_start": 0}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _solr_query(self, q: str = "*:*", start: int = 0, rows: int = ROWS_PER_PAGE,
                    sort: str = "dt_sessao_tdt desc", fq: Optional[str] = None) -> Optional[dict]:
        """Execute a Solr query and return the response."""
        params = {
            "q": q,
            "start": start,
            "rows": rows,
            "fl": FIELDS,
            "sort": sort,
            "wt": "json",
        }
        if fq:
            params["fq"] = fq

        for attempt in range(4):
            try:
                time.sleep(DELAY)
                resp = self.session.get(SOLR_URL, params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait = 3 * (attempt + 1)
                logger.warning("Attempt %d failed: %s. Retrying in %ds...", attempt + 1, e, wait)
                time.sleep(wait)
            except Exception as e:
                logger.error("Solr query failed: %s", e)
                return None
        logger.error("All retries exhausted for Solr query")
        return None

    def normalize(self, doc: dict) -> dict:
        """Transform a Solr document into the standard schema."""
        # Extract full text from conteudo_txt
        raw_content = doc.get("conteudo_txt", "")
        if isinstance(raw_content, list):
            raw_content = raw_content[0] if raw_content else ""
        text = extract_content(raw_content)

        # If conteudo_txt is empty/short, use ementa as fallback
        ementa = doc.get("ementa_s", "")
        if len(text) < 50 and ementa:
            text = ementa

        # Extract decision text
        decisao = doc.get("decisao_txt", "")
        if isinstance(decisao, list):
            decisao = " ".join(decisao)

        # Format date
        raw_date = doc.get("dt_sessao_tdt", "")
        date = raw_date[:10] if raw_date else None

        # Build title from decision number and process number
        decision_num = doc.get("numero_decisao_s", "")
        process_num = doc.get("numero_processo_s", "")
        title = f"Acórdão {decision_num}" if decision_num else f"Processo {process_num}"

        doc_id = doc.get("id", "")

        return {
            "_id": f"BR-CARF-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"https://acordaos.economia.gov.br/solr/acordaos2/browse/?q=id:{doc_id}",
            "language": "pt",
            "decision_number": decision_num,
            "process_number": process_num,
            "rapporteur": doc.get("nome_relator_s", ""),
            "section": doc.get("secao_s", ""),
            "chamber": doc.get("camara_s", ""),
            "panel": doc.get("turma_s", ""),
            "ementa": ementa,
            "decision_text": decisao,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all CARF decisions via Solr pagination.

        Yields raw Solr documents (not normalized). BaseScraper.bootstrap()
        calls self.normalize() on each yielded record.
        """
        checkpoint = self._load_checkpoint()
        start = checkpoint.get("last_start", 0)
        total_yielded = 0

        # Get total count
        result = self._solr_query(start=0, rows=0)
        if not result:
            logger.error("Failed to get total count")
            return
        total_docs = result["response"]["numFound"]
        logger.info("Total CARF decisions: %d. Starting from offset %d", total_docs, start)

        while start < total_docs:
            result = self._solr_query(start=start, rows=ROWS_PER_PAGE,
                                      sort="dt_sessao_tdt asc")
            if not result:
                logger.error("Failed at offset %d", start)
                break

            docs = result["response"]["docs"]
            if not docs:
                break

            for doc in docs:
                yield doc
                total_yielded += 1

            start += ROWS_PER_PAGE
            checkpoint["last_start"] = start
            self._save_checkpoint(checkpoint)

            if total_yielded % 500 == 0:
                logger.info("Progress: %d documents fetched (offset %d/%d)",
                            total_yielded, start, total_docs)

        logger.info("Fetch complete. Total: %d documents", total_yielded)

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        """Fetch decisions from the last 90 days. Yields raw Solr docs."""
        if not since:
            since_str = "NOW-90DAYS"
        elif isinstance(since, str):
            since_str = since + "T00:00:00Z"
        else:
            since_str = since.isoformat() + "Z"
        fq = f"dt_sessao_tdt:[{since_str} TO NOW]"
        start = 0
        total = 0

        while True:
            result = self._solr_query(start=start, rows=ROWS_PER_PAGE, fq=fq,
                                      sort="dt_sessao_tdt desc")
            if not result:
                break
            docs = result["response"]["docs"]
            if not docs:
                break
            for doc in docs:
                yield doc
                total += 1
            start += ROWS_PER_PAGE

        logger.info("Update complete: %d new records", total)

    def test(self) -> bool:
        """Quick connectivity test."""
        logger.info("Testing connectivity to CARF Solr...")
        try:
            result = self._solr_query(q="*:*", start=0, rows=1)
            if not result:
                logger.error("Test failed: no response")
                return False
            total = result["response"]["numFound"]
            docs = result["response"]["docs"]
            if not docs:
                logger.error("Test failed: no documents returned")
                return False
            record = self.normalize(docs[0])
            logger.info("OK: %d total decisions. Sample: '%s' (%d chars text)",
                        total, record["title"][:50], len(record["text"]))
            logger.info("Test PASSED")
            return True
        except Exception as e:
            logger.error("Test FAILED: %s", e)
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description='BR/CARF fetcher')
    parser.add_argument('command', choices=['bootstrap', 'update', 'test'])
    parser.add_argument('--sample', action='store_true', help='Fetch sample records')
    parser.add_argument('--since', type=str, help='Date for update (YYYY-MM-DD)')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CARFScraper()

    if args.command == 'test':
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == 'bootstrap':
        if args.sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
            count = stats.get("sample_records_saved", 0)
        else:
            stats = scraper.bootstrap()
            count = stats.get("records_new", 0)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2, default=str))
        sys.exit(0 if count >= 10 else 1)

    elif args.command == 'update':
        stats = scraper.update()
        logger.info("Update complete: %s", json.dumps(stats, indent=2, default=str))


if __name__ == '__main__':
    main()
