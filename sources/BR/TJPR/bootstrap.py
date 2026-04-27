#!/usr/bin/env python3
"""
BR/TJPR -- Paraná State Court (Tribunal de Justiça do Estado do Paraná)

Fetches court decisions from TJPR's jurisprudence portal search.
2M+ decisions (acordãos, monocratic, turma recursal) with full text HTML.

Endpoint: POST https://portal.tjpr.jus.br/jurisprudencia/publico/pesquisa.do
  - Session init via GET with actionType=iniciar (provides jsessionid)
  - POST search with jsessionid in URL path
  - Full text inline in <div id="texto{ID}"> when mostrarCompleto=true
  - 50 results per page, pagination via pageNumber field
  - No authentication required

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
logger = logging.getLogger("legal-data-hunter.BR.TJPR")

SOURCE_ID = "BR/TJPR"
SAMPLE_DIR = Path(__file__).parent / "sample"

BASE_URL = "https://portal.tjpr.jus.br"
INIT_URL = f"{BASE_URL}/jurisprudencia/publico/pesquisa.do?actionType=iniciar"
SEARCH_ACTION = "pesquisar"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DELAY = 2.0
PAGE_SIZE = 50  # TJPR returns 50 results per page

# Java-style date month map (date format: "Mon Mar 31 00:00:00 BRT 2025")
JAVA_MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

# Regex patterns
RE_JSESSIONID = re.compile(r"jsessionid=([a-f0-9]+)")
RE_TOTAL = re.compile(r"(\d+)\s*registro\(s\)\s*encontrado")
RE_EMENTA_ID = re.compile(r'id="ementa(\d+)"')
RE_EMENTA_TEXT = re.compile(r'id="ementa(\d+)"[^>]*>(.*?)</div>', re.DOTALL)
RE_TEXTO = re.compile(r'id="texto(\d+)"[^>]*>(.*?)</div>', re.DOTALL)
RE_JAVA_DATE = re.compile(
    r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
    r"(\d{2})\s+\d{2}:\d{2}:\d{2}\s+\w+\s+(\d{4})"
)
RE_BR_DATE = re.compile(r"(\d{2})/(\d{2})/(\d{4})")


def strip_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r"<br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<p[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_java_date(date_str: str) -> Optional[str]:
    """Convert Java toString date to ISO 8601 (YYYY-MM-DD)."""
    if not date_str:
        return None
    m = RE_JAVA_DATE.search(date_str)
    if m:
        month = JAVA_MONTHS.get(m.group(1), "01")
        return f"{m.group(3)}-{month}-{m.group(2)}"
    # Try DD/MM/YYYY
    m = RE_BR_DATE.search(date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def extract_metadata(block: str) -> dict:
    """Extract metadata fields from a result block's <td> elements."""
    meta = {}
    tds = re.findall(r"<td[^>]*>(.*?)</td>", block, re.DOTALL)
    for td in tds:
        clean = strip_html(td).strip()
        if not clean:
            continue
        if clean.startswith("Processo:"):
            rest = clean[9:].strip()
            # Extract process number and type
            proc_match = re.match(r"([\d.\-]+)\s*\(([^)]+)\)", rest)
            if proc_match:
                meta["process_number"] = proc_match.group(1).strip()
                meta["decision_type"] = proc_match.group(2).strip()
            else:
                meta["process_number"] = rest.split()[0] if rest else ""
        elif clean.startswith("Relator(a):"):
            meta["relator"] = clean[11:].strip()
        elif clean.startswith("Órgão Julgador:") or clean.startswith("Orgão Julgador:"):
            meta["orgao_julgador"] = clean.split(":", 1)[1].strip()
        elif clean.startswith("Comarca:"):
            meta["comarca"] = clean[8:].strip()
        elif clean.startswith("Data do Julgamento:"):
            meta["data_julgamento"] = clean[19:].strip()
        elif clean.startswith("Fonte/Data da Publicação:") or clean.startswith("Fonte/Data da Publicacao:"):
            meta["data_publicacao"] = clean.split(":", 1)[1].strip()
        elif clean.startswith("Segredo de Justiça:"):
            pass  # skip
    return meta


class TJPRScraper(BaseScraper):
    """Scraper for BR/TJPR -- Paraná State Court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.jsessionid = ""

    def _init_session(self):
        """Initialize session and get jsessionid."""
        try:
            resp = self.session.get(INIT_URL, timeout=30)
            resp.raise_for_status()
            m = RE_JSESSIONID.search(resp.text)
            if m:
                self.jsessionid = m.group(1)
                logger.info("Session initialized (jsessionid=%s...)", self.jsessionid[:8])
            else:
                logger.warning("Could not extract jsessionid from init page")
        except Exception as e:
            logger.warning("Failed to init session: %s", e)

    def _search_url(self) -> str:
        """Build the search URL with jsessionid."""
        base = f"{BASE_URL}/jurisprudencia/publico/pesquisa.do"
        if self.jsessionid:
            return f"{base};jsessionid={self.jsessionid}?actionType={SEARCH_ACTION}"
        return f"{base}?actionType={SEARCH_ACTION}"

    def _search_page(self, page: int = 1, date_start: str = "",
                     date_end: str = "", ambito: str = "-1") -> Optional[str]:
        """Fetch a page of search results."""
        data = {
            "criterioPesquisa": "",
            "dataJulgamentoInicio": date_start,
            "dataJulgamentoFim": date_end,
            "ambito": ambito,
            "segredoJustica": "false",
            "mostrarCompleto": "true",
            "pageNumber": str(page),
            "sortColumn": "processo_sDataJulgamento",
            "sortOrder": "DESC",
        }

        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.post(
                    self._search_url(), data=data,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Referer": INIT_URL,
                    },
                    timeout=90,
                )
                resp.raise_for_status()
                if len(resp.text) < 5000:
                    # Likely error page, re-init session
                    logger.warning("Short response (%d bytes), re-init session",
                                   len(resp.text))
                    self._init_session()
                    continue
                return resp.text
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 5 * (attempt + 1)
                logger.warning("Attempt %d failed: %s. Retrying in %ds...",
                               attempt + 1, e, wait)
                time.sleep(wait)
                self._init_session()
            except Exception as e:
                logger.error("Search request failed: %s", e)
                return None
        logger.error("All retries exhausted for page %d", page)
        return None

    def _parse_results(self, html_text: str) -> list:
        """Parse search results HTML into raw record dicts."""
        records = []
        seen_ids = set()

        # Get all decision IDs
        ids = RE_EMENTA_ID.findall(html_text)
        if not ids:
            return records

        # Build lookup of ementas and full texts
        ementas = {m.group(1): strip_html(m.group(2))
                   for m in RE_EMENTA_TEXT.finditer(html_text)}
        textos = {m.group(1): strip_html(m.group(2))
                  for m in RE_TEXTO.finditer(html_text)}

        # Split HTML into blocks per result
        for i, doc_id in enumerate(ids):
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            # Find the block for this result
            start = html_text.find(f'ementa{doc_id}')
            if start < 0:
                continue

            # Find block boundaries
            block_start = html_text.rfind('resultTable', 0, start)
            if block_start > 0:
                block_start = html_text.rfind('<table', 0, block_start + 10)
            else:
                block_start = max(0, start - 2000)

            if i + 1 < len(ids):
                next_start = html_text.find(f'ementa{ids[i + 1]}')
                block_end = html_text.rfind('<table', 0, next_start)
                if block_end <= block_start:
                    block_end = next_start
            else:
                block_end = len(html_text)

            block = html_text[block_start:block_end]
            meta = extract_metadata(block)

            record = {
                "id_processo": doc_id,
                "ementa": ementas.get(doc_id, ""),
                "texto": textos.get(doc_id, ""),
                **meta,
            }
            records.append(record)

        return records

    def _get_total(self, html_text: str) -> int:
        """Extract total result count."""
        m = RE_TOTAL.search(html_text)
        return int(m.group(1)) if m else 0

    def normalize(self, doc: dict) -> Optional[dict]:
        """Transform a parsed record into the standard schema."""
        doc_id = doc.get("id_processo", "")
        proc = doc.get("process_number", "")
        ementa = doc.get("ementa", "")
        texto = doc.get("texto", "")

        # Prefer full text, fall back to ementa
        text = texto if len(texto) > len(ementa) else ementa
        if not text or len(text) < 30:
            return None

        # Date
        date = parse_java_date(doc.get("data_julgamento", ""))
        pub_date = parse_java_date(doc.get("data_publicacao", ""))

        # Title
        dtype = doc.get("decision_type", "Acórdão")
        orgao = doc.get("orgao_julgador", "")
        title = f"{dtype} {proc}" if proc else f"{dtype} {doc_id}"
        if orgao:
            title += f" - {orgao}"

        return {
            "_id": f"BR-TJPR-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date or pub_date,
            "url": f"{BASE_URL}/jurisprudencia/j/{doc_id}/",
            "language": "pt",
            "id_processo": doc_id,
            "process_number": proc,
            "decision_type": dtype,
            "rapporteur": doc.get("relator", ""),
            "court_body": orgao,
            "comarca": doc.get("comarca", ""),
            "judgment_date": date,
            "publication_date": pub_date,
        }

    def _fetch_quarter(self, year: int, quarter: int, sample_limit: int,
                       count_so_far: int, global_seen: set) -> Generator[dict, None, None]:
        """Fetch all decisions for a given quarter."""
        q_start_month = (quarter - 1) * 3 + 1
        q_end_month = quarter * 3
        date_start = f"01/{q_start_month:02d}/{year}"
        # End day depends on month
        end_days = {3: 31, 6: 30, 9: 30, 12: 31}
        date_end = f"{end_days[q_end_month]:02d}/{q_end_month:02d}/{year}"

        count = 0
        html_text = self._search_page(page=1, date_start=date_start,
                                       date_end=date_end)
        if not html_text:
            return

        total = self._get_total(html_text)
        if total == 0:
            return
        logger.info("Q%d/%d: %d records", quarter, year, total)

        records = self._parse_results(html_text)
        for doc in records:
            if count_so_far + count >= sample_limit:
                return
            did = doc.get("id_processo", "")
            if did in global_seen:
                continue
            global_seen.add(did)
            record = self.normalize(doc)
            if record and len(record["text"]) >= 30:
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
                did = doc.get("id_processo", "")
                if did in global_seen:
                    continue
                global_seen.add(did)
                record = self.normalize(doc)
                if record and len(record["text"]) >= 30:
                    yield record
                    count += 1

            if page % 50 == 0:
                logger.info("Q%d/%d: page %d/%d, %d records",
                            quarter, year, page, max_pages, count)

        logger.info("Q%d/%d complete: %d records", quarter, year, count)

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all TJPR decisions, iterating by quarter."""
        self._init_session()
        count = 0
        sample_limit = 15 if sample else 999999999
        current_year = datetime.now().year
        global_seen = set()

        for year in range(current_year, 1999, -1):
            if count >= sample_limit:
                return
            for quarter in range(4, 0, -1):
                if count >= sample_limit:
                    return
                for record in self._fetch_quarter(year, quarter, sample_limit,
                                                   count, global_seen):
                    yield record
                    count += 1
                if sample and count >= sample_limit:
                    return

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch decisions judged since a date."""
        self._init_session()

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
                if record and len(record["text"]) >= 30:
                    yield record
            page += 1


def main():
    scraper = TJPRScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity...")
        scraper._init_session()
        html_text = scraper._search_page(page=1, date_start="01/01/2025",
                                          date_end="31/03/2025")
        if html_text:
            total = scraper._get_total(html_text)
            records = scraper._parse_results(html_text)
            logger.info("Portal OK — %d total records (Q1 2025), %d on first page",
                        total, len(records))
            if records:
                sample_rec = scraper.normalize(records[0])
                if sample_rec:
                    logger.info("Sample: %s | text length: %d",
                                sample_rec["title"], len(sample_rec["text"]))
        else:
            logger.error("Portal test failed")
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
