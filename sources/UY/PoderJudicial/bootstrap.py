#!/usr/bin/env python3
"""
UY/PoderJudicial -- Uruguay National Case Law Database (BJN)

Fetches full text of court decisions from the Base de Jurisprudencia Nacional.

Strategy:
  - Establish session with JSF/Seam web app
  - Search via AJAX POST (date ranges or keywords)
  - Click each result to load it in the server conversation
  - Fetch full decision page with metadata and full text
  - Extract and clean text from panelTextoSent_body

Data: Public (Poder Judicial de Uruguay, free access).
Rate limit: 3 sec between requests (no robots.txt).

Coverage: ~31,000+ Supreme Court and appellate decisions.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample decisions
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UY.PoderJudicial")

BJN_BASE = "https://bjn.poderjudicial.gub.uy/BJNPUBLICA"
SEARCH_URL = f"{BJN_BASE}/busquedaSimple.seam"
DECISION_URL = f"{BJN_BASE}/hojaInsumo2.seam"
DELAY = 3


def html_to_text(html_content: str) -> str:
    """Extract clean text from BJN decision HTML."""
    if not html_content:
        return ""

    content = html_content

    # Remove highlight spans (search term highlighting)
    content = re.sub(r'<span[^>]*class="highlight"[^>]*>(.*?)</span>', r'\1',
                     content, flags=re.DOTALL | re.IGNORECASE)

    # Remove script/style
    content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)

    # Preserve paragraph breaks
    content = re.sub(r'<p[^>]*>', '\n\n', content, flags=re.IGNORECASE)
    content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
    content = re.sub(r'<div[^>]*>', '\n', content, flags=re.IGNORECASE)

    # Remove all remaining tags
    content = re.sub(r'<[^>]+>', ' ', content)

    # Decode entities
    content = html_module.unescape(content)

    # Clean whitespace
    content = re.sub(r'[ \t]+', ' ', content)
    content = re.sub(r'\n[ \t]+', '\n', content)
    content = re.sub(r'\n{3,}', '\n\n', content)

    return content.strip()


class PoderJudicialScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = None

    def _init_session(self):
        """Initialize a requests session with the BJN app."""
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
            "Accept": "text/html, application/xhtml+xml, application/xml",
        })
        resp = self.session.get(SEARCH_URL, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to init session: HTTP {resp.status_code}")
        logger.info(f"Session initialized: {self.session.cookies.get('JSESSIONID', 'N/A')[:15]}...")

    def _search(self, query: str) -> tuple:
        """Execute a search and return (response_text, cid)."""
        data = {
            "AJAXREQUEST": "_viewRoot",
            "formBusqueda": "formBusqueda",
            "formBusqueda:cajaQuery": query,
            "formBusqueda:Search": "formBusqueda:Search",
            "javax.faces.ViewState": "j_id1",
            "AJAX:EVENTS_COUNT": "1",
        }
        resp = self.session.post(
            SEARCH_URL,
            data=data,
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=30,
        )
        if resp.status_code != 200:
            return None, None

        cid_match = re.search(r'cid=(\d+)', resp.text)
        cid = cid_match.group(1) if cid_match else None

        # Extract result count
        count_match = re.search(r'(\d+)\s*resultado', resp.text)
        total = int(count_match.group(1)) if count_match else 0
        logger.info(f"  Search '{query}': {total} results, cid={cid}")

        return resp.text, cid

    def _count_results_on_page(self, page_html: str) -> list:
        """Extract result indices from the search result page."""
        indices = sorted(set(re.findall(r'formResultados:grid:(\d+):j_id63', page_html)))
        return indices

    def _click_result(self, idx: str) -> bool:
        """Click on a search result to load it in the conversation."""
        click_data = {
            "AJAXREQUEST": "_viewRoot",
            "formResultados": "formResultados",
            f"formResultados:grid:{idx}:j_id63": f"formResultados:grid:{idx}:j_id63",
            "javax.faces.ViewState": "j_id1",
            "AJAX:EVENTS_COUNT": "1",
        }
        resp = self.session.post(
            SEARCH_URL,
            data=click_data,
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=30,
        )
        return resp.status_code == 200

    def _next_page(self) -> Optional[str]:
        """Navigate to the next page of results."""
        data = {
            "AJAXREQUEST": "_viewRoot",
            "formResultados": "formResultados",
            "formResultados:sigLink": "formResultados:sigLink",
            "javax.faces.ViewState": "j_id1",
            "AJAX:EVENTS_COUNT": "1",
        }
        resp = self.session.post(
            SEARCH_URL,
            data=data,
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.text
        return None

    def _fetch_decision(self, cid: str) -> Optional[dict]:
        """Fetch a full decision page and extract metadata + text."""
        resp = self.session.get(f"{DECISION_URL}?cid={cid}", timeout=60)
        if resp.status_code != 200:
            return None

        page = resp.text

        # Extract metadata from RichFaces tables
        def get_cell(cell_id_suffix: str) -> str:
            m = re.search(rf'id="[^"]*{re.escape(cell_id_suffix)}"[^>]*>(.*?)</td>',
                         page, re.DOTALL)
            if m:
                return re.sub(r'<[^>]+>', '', m.group(1)).strip()
            return ""

        # First table: Numero, Sede, Importancia, Tipo
        numero = get_cell("0:j_id13")
        sede = get_cell("0:j_id15")
        importancia = get_cell("0:j_id17")
        tipo = get_cell("0:j_id19")

        # Second table: Fecha, Ficha, Procedimiento
        fecha = get_cell("0:j_id29")
        ficha = get_cell("0:j_id31")
        procedimiento = get_cell("0:j_id33")

        # Materias (may have multiple rows)
        materias = re.findall(r'id="j_id35:\d+:j_id39"[^>]*>(.*?)</td>', page, re.DOTALL)
        materias = [re.sub(r'<[^>]+>', '', m).strip() for m in materias]

        # Firmantes (signers)
        firmantes = re.findall(r'id="gridFirmantes:\d+:j_id48:\d+:j_id49"[^>]*>(.*?)</td>',
                              page, re.DOTALL)
        firmantes = [re.sub(r'<[^>]+>', '', f).strip() for f in firmantes if f.strip()]

        # Full text from panelTextoSent_body
        text_match = re.search(
            r'id="panelTextoSent_body"[^>]*>(.*?)</div>\s*</div>\s*(?:<div|<table|$)',
            page, re.DOTALL
        )
        text = ""
        if text_match:
            text = html_to_text(text_match.group(1))

        if not text or len(text) < 50:
            # Fallback: grab all large text blocks after panelTextoSent
            start = page.find('panelTextoSent_body')
            if start > 0:
                section = page[start:start + 100000]
                text = html_to_text(section)

        # Parse date
        iso_date = None
        if fecha:
            try:
                parts = fecha.split("/")
                if len(parts) == 3:
                    iso_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except Exception:
                pass

        return {
            "decision_number": numero,
            "court": sede,
            "importance": importancia,
            "decision_type": tipo,
            "date_raw": fecha,
            "date": iso_date,
            "ficha": ficha,
            "procedure": procedimiento,
            "subjects": materias,
            "signers": firmantes,
            "text": text,
        }

    def test_api(self):
        """Test connectivity to BJN."""
        logger.info("Testing BJN connectivity...")
        try:
            self._init_session()
            page_html, cid = self._search("constitucional")
            if cid and page_html:
                logger.info("Connectivity test PASSED")
                return True
            logger.error("Connectivity test FAILED: no results or cid")
            return False
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw decision data into standard schema."""
        if not raw or not raw.get("text") or len(raw["text"]) < 50:
            return None

        numero = raw.get("decision_number", "unknown")
        court = raw.get("court", "unknown")
        _id = f"UY-BJN-{numero}".replace("/", "-").replace(" ", "_")

        title_parts = [f"Sentencia {numero}"]
        if court:
            title_parts.append(court)
        if raw.get("procedure"):
            title_parts.append(raw["procedure"])
        title = " — ".join(title_parts)

        return {
            "_id": _id,
            "_source": "UY/PoderJudicial",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "url": f"{BJN_BASE}/busquedaSimple.seam",
            "decision_number": numero,
            "court": court,
            "decision_type": raw.get("decision_type", ""),
            "procedure": raw.get("procedure", ""),
            "subjects": raw.get("subjects", []),
            "signers": raw.get("signers", []),
            "ficha": raw.get("ficha", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch court decisions with full text."""
        sample_limit = 15 if sample else None
        count = 0

        self._init_session()

        # Use diverse search terms to get varied results
        if sample:
            queries = ["constitucional", "penal", "laboral", "civil", "familia"]
        else:
            queries = ["a"]  # Very broad search for full fetch

        for query in queries:
            time.sleep(DELAY)
            page_html, cid = self._search(query)
            if not cid or not page_html:
                logger.warning(f"  No results for query '{query}'")
                continue

            pages_processed = 0
            max_pages = 3 if sample else 500

            while pages_processed < max_pages:
                indices = self._count_results_on_page(page_html)
                if not indices:
                    break

                for idx in indices:
                    time.sleep(DELAY)
                    if not self._click_result(idx):
                        continue

                    time.sleep(DELAY)
                    raw = self._fetch_decision(cid)
                    if not raw:
                        continue

                    record = self.normalize(raw)
                    if record:
                        count += 1
                        text_len = len(record["text"])
                        logger.info(f"  [{count}] {record['decision_number']} ({record['court']}) — {text_len} chars")
                        yield record

                        if sample_limit and count >= sample_limit:
                            logger.info(f"Sample limit ({sample_limit}) reached")
                            return

                pages_processed += 1
                if pages_processed < max_pages:
                    time.sleep(DELAY)
                    next_html = self._next_page()
                    if next_html:
                        page_html = next_html
                    else:
                        break

        logger.info(f"Total decisions fetched: {count}")

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Fetch decisions since a date (re-fetches all)."""
        yield from self.fetch_all(sample=False)

    def bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = Path(self.source_dir) / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in self.fetch_all(sample=sample):
            out_file = sample_dir / f"{record['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)
            count += 1
            logger.info(f"Saved: {out_file.name}")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="UY/PoderJudicial bootstrapper")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 decisions)")
    args = parser.parse_args()

    scraper = PoderJudicialScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = scraper.bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        sys.exit(0)


if __name__ == "__main__":
    main()
