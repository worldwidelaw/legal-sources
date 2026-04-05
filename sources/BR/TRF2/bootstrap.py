#!/usr/bin/env python3
"""
BR/TRF2 -- Federal Regional Court 2nd Region (Tribunal Regional Federal da 2ª Região)

Fetches court decisions from TRF2's Solr-backed jurisprudence portal at juris.trf2.jus.br.
TRF2 covers Rio de Janeiro (RJ) and Espírito Santo (ES).

Endpoints:
  - Search: GET https://juris.trf2.jus.br/consulta.php?q=*&rows=10&start=0
    Returns HTML with ementa UUIDs and process numbers.
  - Full text: GET https://juris.trf2.jus.br/documento.php?uuid=<uuid>
    Returns HTML with complete decision text (inteiro teor).
  - Ementa JSON: GET https://juris.trf2.jus.br/ementa.php?id=<uuid>
    Returns Solr JSON with ementa text.

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
import html as html_mod
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
logger = logging.getLogger("legal-data-hunter.BR.TRF2")

SOURCE_ID = "BR/TRF2"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://juris.trf2.jus.br"
SEARCH_URL = f"{BASE_URL}/consulta.php"
DOC_URL = f"{BASE_URL}/documento.php"
EMENTA_URL = f"{BASE_URL}/ementa.php"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DELAY = 2.0
PAGE_SIZE = 10

# Regex patterns
RE_EMENTA_UUID = re.compile(r'data-id="([a-f0-9]{32})"')
RE_DOC_UUID = re.compile(r'documento\.php\?uuid=([a-f0-9]{32})')
RE_PROCESS_NUM = re.compile(r'(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})')
RE_TOTAL = re.compile(r'([\d.]+)\s*resultado', re.IGNORECASE)
RE_EMENTA_BLOCK = re.compile(
    r'content_ementa"[^>]*id="content_([a-f0-9]{32})"[^>]*>(.*?)</div>',
    re.DOTALL,
)
RE_DATE = re.compile(r'(\d{2})/(\d{2})/(\d{4})')


def clean_html(text: str) -> str:
    """Strip HTML tags and clean text."""
    if not text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_mod.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class TRF2Scraper(BaseScraper):
    """Scraper for BR/TRF2 -- Federal Regional Court 2nd Region decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _search_page(self, start: int = 0, query: str = "*") -> Optional[str]:
        """Fetch a page of search results."""
        params = {"q": query, "rows": str(PAGE_SIZE), "start": str(start)}
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.get(SEARCH_URL, params=params, timeout=60)
                resp.raise_for_status()
                return resp.text
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 5 * (attempt + 1)
                logger.warning("Search attempt %d failed: %s. Retry in %ds",
                               attempt + 1, e, wait)
                time.sleep(wait)
            except Exception as e:
                logger.error("Search failed: %s", e)
                return None
        return None

    def _fetch_full_text(self, uuid: str) -> Optional[str]:
        """Fetch the full decision text from documento.php."""
        params = {"uuid": uuid, "options": "#page=1"}
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.get(DOC_URL, params=params, timeout=60)
                resp.raise_for_status()
                return clean_html(resp.text)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 5 * (attempt + 1)
                logger.warning("Doc fetch attempt %d failed: %s. Retry in %ds",
                               attempt + 1, e, wait)
                time.sleep(wait)
            except Exception as e:
                logger.error("Failed to fetch doc %s: %s", uuid, e)
                return None
        return None

    def _parse_results(self, page_html: str) -> list:
        """
        Parse search results page.
        Returns list of dicts with uuid, doc_uuid, process_number, ementa.
        """
        results = []

        ementa_uuids = RE_EMENTA_UUID.findall(page_html)
        doc_uuids = RE_DOC_UUID.findall(page_html)
        ementa_blocks = RE_EMENTA_BLOCK.findall(page_html)

        # Build ementa map by UUID
        ementa_map = {}
        for uid, content in ementa_blocks:
            ementa_map[uid] = clean_html(content)

        # Extract process numbers per result block
        # Each result has a unique ementa UUID; find the process number near it
        for i, e_uuid in enumerate(ementa_uuids):
            d_uuid = doc_uuids[i] if i < len(doc_uuids) else None

            # Find process number near this UUID in the HTML
            idx = page_html.find(f'data-id="{e_uuid}"')
            if idx >= 0:
                block = page_html[idx:idx + 3000]
                proc_match = RE_PROCESS_NUM.search(block)
                proc = proc_match.group(1) if proc_match else ""
            else:
                proc = ""

            ementa = ementa_map.get(e_uuid, "")

            results.append({
                "ementa_uuid": e_uuid,
                "doc_uuid": d_uuid or e_uuid,
                "process_number": proc,
                "ementa": ementa,
            })

        return results

    def _get_total(self, page_html: str) -> int:
        """Extract total result count."""
        m = RE_TOTAL.search(page_html)
        if m:
            return int(m.group(1).replace(".", ""))
        return 0

    def normalize(self, doc: dict) -> dict:
        """Transform a parsed record into the standard schema."""
        proc = doc.get("process_number", "")
        text = doc.get("text", "")
        ementa = doc.get("ementa", "")

        # Extract case class from ementa (first line often contains it)
        classe = ""
        if ementa:
            first_line = ementa.split("\n")[0].strip()
            # Common patterns: "Agravo de Instrumento Nº ...", "Apelação Cível Nº ..."
            class_match = re.match(r'^(.+?)\s+N[ºo°]\s', first_line)
            if class_match:
                classe = class_match.group(1).strip()

        title = f"{classe} - {proc}" if classe and proc else f"TRF2 {proc}"

        # Extract date from text
        date = None
        date_match = RE_DATE.search(text[-500:] if len(text) > 500 else text)
        if date_match:
            d, m, y = date_match.groups()
            try:
                date = f"{y}-{m}-{d}"
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                date = None

        safe_proc = re.sub(r'[^0-9]', '', proc)
        doc_id = f"BR-TRF2-{safe_proc}" if safe_proc else f"BR-TRF2-{doc.get('doc_uuid', '')[:16]}"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{DOC_URL}?uuid={doc.get('doc_uuid', '')}",
            "language": "pt",
            "process_number": proc,
            "classe": classe,
            "court": "TRF2",
            "ementa": ementa[:500] if ementa else "",
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch TRF2 decisions with pagination."""
        sample_limit = 15 if sample else 999999
        count = 0
        start = 0
        seen = set()

        # First page to get total
        page_html = self._search_page(start=0)
        if not page_html:
            logger.error("Failed to fetch first page")
            return

        total = self._get_total(page_html)
        logger.info("Total results: %d", total)

        while count < sample_limit:
            if start > 0:
                page_html = self._search_page(start=start)
                if not page_html:
                    break

            results = self._parse_results(page_html)
            if not results:
                logger.info("No more results at start=%d", start)
                break

            for result in results:
                if count >= sample_limit:
                    break

                proc = result["process_number"]
                if proc in seen:
                    continue
                seen.add(proc)

                # Fetch full text
                doc_uuid = result["doc_uuid"]
                full_text = self._fetch_full_text(doc_uuid)
                if not full_text or len(full_text) < 50:
                    logger.warning("No/short full text for %s (uuid=%s)", proc, doc_uuid)
                    # Fall back to ementa
                    full_text = result.get("ementa", "")
                    if len(full_text) < 50:
                        continue

                result["text"] = full_text
                record = self.normalize(result)
                yield record
                count += 1

                if count % 10 == 0:
                    logger.info("Fetched %d records...", count)

            start += PAGE_SIZE

            if start > total:
                break

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch recent decisions."""
        logger.info("Fetching recent TRF2 decisions")
        yield from self.fetch_all()


def main():
    scraper = TRF2Scraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to TRF2 juris portal...")
        page_html = scraper._search_page(start=0, query="*")
        if not page_html:
            logger.error("Search test FAILED")
            sys.exit(1)

        total = scraper._get_total(page_html)
        results = scraper._parse_results(page_html)
        logger.info("TRF2 OK — %d total results, %d on first page", total, len(results))

        if results:
            # Test full text fetch
            doc_uuid = results[0]["doc_uuid"]
            full_text = scraper._fetch_full_text(doc_uuid)
            if full_text:
                logger.info("Full text OK — %d chars for uuid=%s", len(full_text), doc_uuid)
                logger.info("Preview: %.200s", full_text[:200])
            else:
                logger.error("Full text fetch FAILED for uuid=%s", doc_uuid)
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
        logger.info("Bootstrap complete: %d records saved to %s", count, SAMPLE_DIR)

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
