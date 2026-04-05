#!/usr/bin/env python3
"""
BR/TJAM -- Amazonas State Court (Tribunal de Justiça do Estado do Amazonas)

Fetches court decisions (acordãos) from TJAM's eSAJ jurisprudence search.
~92k acordãos with ementas (legal summaries containing holdings and reasoning).

Endpoint: POST https://consultasaj.tjam.jus.br/cjsg/resultadoCompleta.do
  - HTML response with inline ementas, parsed via regex
  - Requires session cookie from initial GET to consultaCompleta.do
  - No authentication required
  - Pagination via nuPagina parameter

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
import html
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
logger = logging.getLogger("legal-data-hunter.BR.TJAM")

SOURCE_ID = "BR/TJAM"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

BASE_URL = "https://consultasaj.tjam.jus.br"
SEARCH_URL = f"{BASE_URL}/cjsg/resultadoCompleta.do"
FORM_URL = f"{BASE_URL}/cjsg/consultaCompleta.do"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml",
}

DELAY = 2.0
PAGE_SIZE = 10  # eSAJ returns 10 results per page

# Regex patterns to extract data from eSAJ HTML
RE_BLOCK = re.compile(
    r'class="fundocinza\d".*?(?=class="fundocinza\d"|<div\s+id="paginacaoSuperior")',
    re.DOTALL,
)
RE_CD_ACORDAO = re.compile(r'cdAcordao="(\d+)"')
RE_PROCESS_NUMBER = re.compile(
    r'cdAcordao="\d+"[^>]*>\s*([\d.\-]+)\s*</a>', re.DOTALL
)
RE_EMENTA = re.compile(
    r'id="textAreaDados_\d+"[^>]*>\s*(.*?)\s*</div>', re.DOTALL
)
RE_FIELD = re.compile(
    r'<strong>\s*(.*?)\s*</strong>\s*(.*?)\s*</td>', re.DOTALL
)
RE_TOTAL = re.compile(r'id="totalResultadoAba-A"[^>]*value="(\d+)"')


def decode_html(text: str) -> str:
    """Decode HTML entities and strip tags."""
    if not text:
        return ""
    text = re.sub(r"<br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date_br(date_str: str) -> Optional[str]:
    """Convert DD/MM/YYYY to ISO 8601 date."""
    if not date_str:
        return None
    date_str = date_str.strip()
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


class TJAMScraper(BaseScraper):
    """Scraper for BR/TJAM -- Amazonas State Court decisions via eSAJ."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _init_session(self):
        """Get a session cookie by visiting the search form."""
        try:
            resp = self.session.get(FORM_URL, timeout=30)
            resp.raise_for_status()
            logger.info("Session initialized (cookie set)")
        except Exception as e:
            logger.warning("Failed to init session: %s", e)

    def _search_page(self, page: int = 1, date_start: str = "",
                     date_end: str = "") -> Optional[str]:
        """Fetch a page of search results. Returns HTML string or None."""
        data = {
            "conversationId": "",
            "dados.buscaEmenta": "",
            "dados.pesquisarComSinonimos": "S",
            "tipoDecisaoSelecionados": "A",
            "dados.dtJulgamentoInicio": date_start,
            "dados.dtJulgamentoFim": date_end,
            "dados.origensSelecionadas": "2",
            "dados.ordenarPor": "dtPublicacao",
            "nuPagina": str(page),
            "totalRegistrosPorPagina": str(PAGE_SIZE),
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": FORM_URL,
        }

        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.post(
                    SEARCH_URL, data=data, headers=headers, timeout=60
                )
                resp.raise_for_status()
                return resp.text
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 5 * (attempt + 1)
                logger.warning("Attempt %d failed: %s. Retrying in %ds...",
                               attempt + 1, e, wait)
                time.sleep(wait)
                # Re-init session on failure
                self._init_session()
            except Exception as e:
                logger.error("Search request failed: %s", e)
                return None
        logger.error("All retries exhausted for page %d", page)
        return None

    def _parse_results(self, html_text: str) -> list:
        """Parse search results HTML into a list of raw record dicts."""
        records = []
        seen_ids = set()

        # Split into individual result blocks
        blocks = RE_BLOCK.findall(html_text)
        if not blocks:
            # Fallback: find by cdAcordao markers
            blocks = re.split(r'(?=class="fundocinza)', html_text)
            blocks = [b for b in blocks if "cdAcordao" in b]

        for block in blocks:
            record = {}

            # Extract cdAcordao
            m = RE_CD_ACORDAO.search(block)
            if not m:
                continue
            cd = m.group(1)
            if cd in seen_ids:
                continue
            seen_ids.add(cd)
            record["cd_acordao"] = cd

            # Extract process number
            m = RE_PROCESS_NUMBER.search(block)
            if m:
                record["process_number"] = m.group(1).strip()

            # Extract ementa text
            m = RE_EMENTA.search(block)
            if m:
                record["ementa"] = decode_html(m.group(1))

            # Extract metadata fields
            for fm in RE_FIELD.finditer(block):
                label = decode_html(fm.group(1)).rstrip(":")
                value = decode_html(fm.group(2)).strip()
                if not value:
                    continue

                label_lower = label.lower()
                if "classe" in label_lower or "assunto" in label_lower:
                    record["classe_assunto"] = value
                elif "relator" in label_lower:
                    record["relator"] = value
                elif "comarca" in label_lower:
                    record["comarca"] = value
                elif "órgão" in label_lower or "orgao" in label_lower:
                    record["orgao_julgador"] = value
                elif "julgamento" in label_lower:
                    record["data_julgamento"] = value
                elif "publicação" in label_lower or "publicacao" in label_lower:
                    record["data_publicacao"] = value

            if record.get("ementa") and len(record["ementa"]) > 30:
                records.append(record)

        return records

    def _get_total(self, html_text: str) -> int:
        """Extract total result count from HTML."""
        m = RE_TOTAL.search(html_text)
        return int(m.group(1)) if m else 0

    def normalize(self, doc: dict) -> dict:
        """Transform a parsed eSAJ record into the standard schema."""
        cd = doc.get("cd_acordao", "")
        proc = doc.get("process_number", "")
        ementa = doc.get("ementa", "")

        # Clean ementa prefix
        text = ementa
        if text.lower().startswith("ementa:"):
            text = text[7:].strip()

        # Date (prefer julgamento, fall back to publicação)
        date = parse_date_br(doc.get("data_julgamento", ""))
        pub_date = parse_date_br(doc.get("data_publicacao", ""))

        # Title
        classe = doc.get("classe_assunto", "")
        orgao = doc.get("orgao_julgador", "")
        title = f"{classe} - {proc}" if classe else f"Acórdão {proc}"
        if orgao:
            title += f" ({orgao})"

        return {
            "_id": f"BR-TJAM-{cd}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date or pub_date,
            "url": f"{BASE_URL}/cjsg/resultadoCompleta.do",
            "language": "pt",
            "cd_acordao": cd,
            "process_number": proc,
            "classe_assunto": classe,
            "rapporteur": doc.get("relator", ""),
            "court_body": orgao,
            "comarca": doc.get("comarca", ""),
            "judgment_date": date,
            "publication_date": pub_date,
        }

    def _fetch_year(self, year: int, sample_limit: int,
                    count_so_far: int,
                    global_seen: set) -> Generator[dict, None, None]:
        """Fetch all acordãos for a given year."""
        date_start = f"01/01/{year}"
        date_end = f"31/12/{year}"
        count = 0

        html_text = self._search_page(page=1, date_start=date_start,
                                       date_end=date_end)
        if not html_text:
            return

        total = self._get_total(html_text)
        if total == 0:
            return
        logger.info("Year %d: %d acordãos", year, total)

        records = self._parse_results(html_text)
        for doc in records:
            if count_so_far + count >= sample_limit:
                return
            cd = doc.get("cd_acordao", "")
            if cd in global_seen:
                continue
            global_seen.add(cd)
            record = self.normalize(doc)
            if len(record["text"]) < 30:
                continue
            yield record
            count += 1

        max_pages = min((total + PAGE_SIZE - 1) // PAGE_SIZE, 999999)
        for page in range(2, max_pages + 1):
            if count_so_far + count >= sample_limit:
                return

            html_text = self._search_page(page=page, date_start=date_start,
                                           date_end=date_end)
            if not html_text:
                break

            records = self._parse_results(html_text)
            if not records:
                break

            for doc in records:
                if count_so_far + count >= sample_limit:
                    return
                cd = doc.get("cd_acordao", "")
                if cd in global_seen:
                    continue
                global_seen.add(cd)
                record = self.normalize(doc)
                if len(record["text"]) < 30:
                    continue
                yield record
                count += 1

            if page % 100 == 0:
                logger.info("Year %d: page %d/%d, %d records",
                            year, page, max_pages, count)

        logger.info("Year %d complete: %d records", year, count)

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all TJAM acordãos via eSAJ search, iterating by year."""
        self._init_session()
        count = 0
        sample_limit = 15 if sample else 999999
        current_year = datetime.now().year
        global_seen = set()

        # Iterate from current year backwards
        for year in range(current_year, 1999, -1):
            if count >= sample_limit:
                return
            for record in self._fetch_year(year, sample_limit, count,
                                            global_seen):
                yield record
                count += 1
            if sample and count >= sample_limit:
                return

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch decisions judged since a date."""
        self._init_session()

        # Convert ISO date to BR format
        try:
            dt = datetime.strptime(since, "%Y-%m-%d")
            date_start = dt.strftime("%d/%m/%Y")
        except ValueError:
            date_start = since

        today = datetime.now().strftime("%d/%m/%Y")
        logger.info("Fetching updates from %s to %s", date_start, today)
        page = 1
        while True:
            html_text = self._search_page(page=page, date_start=date_start,
                                           date_end=today)
            if not html_text:
                break
            records = self._parse_results(html_text)
            if not records:
                break
            for doc in records:
                record = self.normalize(doc)
                if len(record["text"]) >= 30:
                    yield record
            page += 1


def main():
    scraper = TJAMScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity...")
        scraper._init_session()
        html_text = scraper._search_page(page=1, date_start="01/01/2024",
                                          date_end="31/12/2024")
        if html_text:
            total = scraper._get_total(html_text)
            records = scraper._parse_results(html_text)
            logger.info("eSAJ OK — %d total acordãos (2024), %d on first page",
                        total, len(records))
            if records:
                sample_rec = scraper.normalize(records[0])
                logger.info("Sample: %s | text length: %d",
                            sample_rec["title"], len(sample_rec["text"]))
        else:
            logger.error("eSAJ test failed")
            sys.exit(1)
        return

    if command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=sample):
            count += 1
            safe_id = record["_id"].replace("/", "_")[:100]
            fname = SAMPLE_DIR / f"{safe_id}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            if count % 50 == 0:
                logger.info("Saved %d records...", count)
        logger.info("Bootstrap complete: %d records saved to %s",
                     count, SAMPLE_DIR)

    elif command == "update":
        since = (sys.argv[2] if len(sys.argv) > 2
                 and not sys.argv[2].startswith("-") else "2025-01-01")
        count = sum(1 for _ in scraper.fetch_updates(since))
        logger.info("Update complete: %d records since %s", count, since)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
