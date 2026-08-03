#!/usr/bin/env python3
"""
BR/TRF2 -- Federal Regional Court 2nd Region (Tribunal Regional Federal da 2ª Região)

TRF2 covers Rio de Janeiro (RJ) and Espírito Santo (ES).

The old Solr-backed portal at juris.trf2.jus.br is GONE (host is NXDOMAIN). TRF2
migrated its jurisprudence into the eproc / InfraTela system. Jurisprudence is now
searched through a stateful PHP form (issue #1123):

  - Search form (establishes PHPSESSID):
      GET  https://eproc.trf2.jus.br/eproc/externo_controlador.php
             ?acao=jurisprudencia@jurisprudencia/pesquisar
  - Result list (POST the search, returns page 1 + total):
      POST https://eproc.trf2.jus.br/eproc/externo_controlador.php
             ?acao=jurisprudencia@jurisprudencia/listar_resultados
  - Pagination (AJAX, same session):
      GET  https://eproc.trf2.jus.br/eproc/externo_controlador.php
             ?acao=jurisprudencia@jurisprudencia/ajax_paginar_resultado&pagina=N

Each result item carries the FULL inteiro-teor text inline (rdoCampo=I) inside a
`resValue` block, together with resLabel/resValue metadata pairs (PROCESSO, UF,
ÓRGÃO JULGADOR, DATA DO JULGAMENTO, DATA DA PUBLICAÇÃO, RELATOR, DECISÃO). No
separate document fetch is required.

Usage:
  python bootstrap.py bootstrap          # Full initial pull (checkpoint/resume)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # VPS fleet entrypoint (alias of full)
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
CHECKPOINT_FILE = Path(__file__).parent / "data" / "trf2_checkpoint.json"

BASE_URL = "https://eproc.trf2.jus.br/eproc"
CONTROLLER = f"{BASE_URL}/externo_controlador.php"
FORM_URL = f"{CONTROLLER}?acao=jurisprudencia@jurisprudencia/pesquisar"
LIST_URL = f"{CONTROLLER}?acao=jurisprudencia@jurisprudencia/listar_resultados"
PAGE_URL = f"{CONTROLLER}?acao=jurisprudencia@jurisprudencia/ajax_paginar_resultado"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

DELAY = 2.0
PAGE_SIZE = 10  # items per result page

# Regex patterns -----------------------------------------------------------
RE_ITEM = re.compile(r'<div class="card mb-3 resultadoItem"')
RE_TIPO = re.compile(r'resValueTipoJurisprudencia[^>]*>(.*?)<', re.DOTALL)
RE_FIELD = re.compile(
    r'resLabel[^>]*>\s*([^<]+?)\s*</[^>]+>\s*<div[^>]*resValue[^>]*>(.*?)</div>',
    re.DOTALL,
)
RE_DECISAO_OPEN = re.compile(
    r'resLabel[^>]*>\s*(?:DECIS[ÃA]O|EMENTA|VOTO)\s*</[^>]+>\s*<div[^>]*resValue[^>]*>',
)
RE_INTEIRO_LINK = re.compile(
    r'inteiroTeor"[^>]*data-link="([^"]*download_inteiro_teor[^"]*)"'
)
RE_PROCESS_NUM = re.compile(r'(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})')
RE_TOTAL = re.compile(r'id="hdnTotalResultado"[^>]*value="(\d+)"')
RE_TOTAL_PAGES = re.compile(r'id="hdnTotalPaginas"[^>]*value="(\d+)"')
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
        self._session_ready = False

    # -- HTTP helpers ------------------------------------------------------
    def _establish_session(self) -> bool:
        """GET the search form to obtain a PHPSESSID cookie."""
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.get(FORM_URL, timeout=60)
                resp.raise_for_status()
                self._session_ready = True
                return True
            except Exception as e:
                wait = 5 * (attempt + 1)
                logger.warning("Session init attempt %d failed: %s. Retry in %ds",
                               attempt + 1, e, wait)
                time.sleep(wait)
        return False

    def _post_search(self) -> Optional[str]:
        """POST the search form (empty query = full corpus, inteiro teor)."""
        data = {
            "txtPesquisa": "",
            "rdoCampo": "I",            # I = Inteiro Teor (full text)
            "chkAgruparResultados": "1",
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": FORM_URL,
        }
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.post(LIST_URL, data=data, headers=headers, timeout=90)
                resp.raise_for_status()
                resp.encoding = "iso-8859-1"
                return resp.text
            except Exception as e:
                wait = 5 * (attempt + 1)
                logger.warning("Search POST attempt %d failed: %s. Retry in %ds",
                               attempt + 1, e, wait)
                time.sleep(wait)
        return None

    def _fetch_inteiro_teor(self, data_link: str) -> Optional[str]:
        """Fetch and clean the full inteiro-teor HTML document for an item."""
        url = data_link
        if not url.startswith("http"):
            url = f"{BASE_URL}/{data_link.lstrip('/')}"
        url = html_mod.unescape(url)
        headers = {"Referer": LIST_URL}
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.get(url, headers=headers, timeout=90)
                resp.raise_for_status()
                resp.encoding = "iso-8859-1"
                text = clean_html(resp.text)
                return text if text else None
            except Exception as e:
                wait = 5 * (attempt + 1)
                logger.warning("Inteiro-teor attempt %d failed: %s. Retry in %ds",
                               attempt + 1, e, wait)
                time.sleep(wait)
        return None

    def _fetch_page(self, pagina: int) -> Optional[str]:
        """Fetch a specific result page via the AJAX pagination endpoint."""
        headers = {"Referer": LIST_URL}
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.get(PAGE_URL, params={"pagina": str(pagina)},
                                        headers=headers, timeout=90)
                resp.raise_for_status()
                resp.encoding = "iso-8859-1"
                return resp.text
            except Exception as e:
                wait = 5 * (attempt + 1)
                logger.warning("Page %d attempt %d failed: %s. Retry in %ds",
                               pagina, attempt + 1, e, wait)
                time.sleep(wait)
        return None

    # -- Parsing -----------------------------------------------------------
    def _parse_page(self, page_html: str) -> list:
        """Parse a result page into a list of raw record dicts."""
        results = []
        starts = [m.start() for m in RE_ITEM.finditer(page_html)]
        for i, st in enumerate(starts):
            en = starts[i + 1] if i + 1 < len(starts) else len(page_html)
            block = page_html[st:en]

            tipo_m = RE_TIPO.search(block)
            tipo = clean_html(tipo_m.group(1)) if tipo_m else ""

            fields = {}
            for m in RE_FIELD.finditer(block):
                lbl = clean_html(m.group(1)).upper().rstrip(":")
                fields[lbl] = clean_html(m.group(2))

            # Full text: the DECISÃO/EMENTA/VOTO resValue is the last field and
            # may contain nested <div>s, so capture from its open tag to the end
            # of the item block (rather than a naive </div> match).
            full_text = ""
            open_m = RE_DECISAO_OPEN.search(block)
            if open_m:
                full_text = clean_html(block[open_m.end():])
            if not full_text:
                # fall back to whichever labelled field holds the body
                for k in ("DECISÃO", "DECISAO", "EMENTA", "VOTO"):
                    if fields.get(k):
                        full_text = fields[k]
                        break

            proc_raw = fields.get("PROCESSO", "")
            proc_m = RE_PROCESS_NUM.search(proc_raw)
            proc = proc_m.group(1) if proc_m else ""

            link_m = RE_INTEIRO_LINK.search(block)
            inteiro_link = link_m.group(1) if link_m else ""

            results.append({
                "tipo": tipo,
                "process_number": proc,
                "uf": fields.get("UF", ""),
                "orgao": fields.get("ÓRGÃO JULGADOR", fields.get("ORGAO JULGADOR", "")),
                "data_julgamento": fields.get("DATA DO JULGAMENTO", ""),
                "data_publicacao": fields.get("DATA DA PUBLICAÇÃO", ""),
                "relator": fields.get("RELATOR", ""),
                "inteiro_link": inteiro_link,
                "text": full_text,          # short dispositivo (fallback)
            })
        return results

    # -- Checkpoint --------------------------------------------------------
    def _load_checkpoint(self) -> int:
        try:
            if CHECKPOINT_FILE.exists():
                return int(json.loads(CHECKPOINT_FILE.read_text()).get("last_page", 0))
        except Exception:
            pass
        return 0

    def _save_checkpoint(self, page: int) -> None:
        try:
            CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
            CHECKPOINT_FILE.write_text(json.dumps({"last_page": page}))
        except Exception as e:
            logger.debug("Checkpoint save failed: %s", e)

    # -- Normalization -----------------------------------------------------
    def normalize(self, doc: dict) -> dict:
        proc = doc.get("process_number", "")
        text = doc.get("text", "")
        tipo = doc.get("tipo", "")
        orgao = doc.get("orgao", "")

        title_parts = [p for p in (tipo, proc) if p]
        title = " - ".join(title_parts) if title_parts else f"TRF2 {proc or 'decisão'}"

        # Prefer judgment date; fall back to publication date.
        date = None
        for raw_date in (doc.get("data_julgamento", ""), doc.get("data_publicacao", "")):
            m = RE_DATE.search(raw_date or "")
            if m:
                d, mo, y = m.groups()
                try:
                    cand = f"{y}-{mo}-{d}"
                    datetime.strptime(cand, "%Y-%m-%d")
                    date = cand
                    break
                except ValueError:
                    continue

        safe_proc = re.sub(r'[^0-9]', '', proc)
        if safe_proc:
            doc_id = f"BR-TRF2-{safe_proc}"
        else:
            import hashlib
            doc_id = "BR-TRF2-" + hashlib.md5(text[:200].encode("utf-8", "ignore")).hexdigest()[:16]

        url = FORM_URL
        if proc:
            num = re.sub(r'[^0-9]', '', proc)
            url = (f"{CONTROLLER}?acao=processo_seleciona_publica"
                   f"&acao_origem=processo_consulta_publica"
                   f"&acao_retorno=processo_consulta_publica&num_processo={num}")

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "language": "pt",
            "process_number": proc,
            "tipo": tipo,
            "orgao_julgador": orgao,
            "relator": doc.get("relator", ""),
            "uf": doc.get("uf", ""),
            "data_publicacao": doc.get("data_publicacao", ""),
            "court": "TRF2",
        }

    # -- Fetch loops -------------------------------------------------------
    def fetch_all(self) -> Generator[dict, None, None]:
        if not self._session_ready and not self._establish_session():
            logger.error("Could not establish eproc session")
            return

        page_html = self._post_search()
        if not page_html:
            logger.error("Failed to POST search")
            return

        total = 0
        total_pages = 0
        m = RE_TOTAL.search(page_html)
        if m:
            total = int(m.group(1))
        m = RE_TOTAL_PAGES.search(page_html)
        if m:
            total_pages = int(m.group(1))
        logger.info("Total documents: %d across %d pages", total, total_pages)
        if total_pages <= 0:
            total_pages = 1

        resume_page = self._load_checkpoint()
        seen = set()
        count = 0

        for page in range(1, total_pages + 1):
            if page <= resume_page:
                continue
            if page == 1:
                items = self._parse_page(page_html)
            else:
                html = self._fetch_page(page)
                if not html:
                    logger.warning("Skipping page %d (fetch failed)", page)
                    continue
                items = self._parse_page(html)

            if not items:
                logger.info("No items on page %d — stopping", page)
                break

            for it in items:
                key = it["process_number"] or (it["text"][:80] if it["text"] else "")
                if not key or key in seen:
                    continue
                seen.add(key)

                # Prefer the full inteiro-teor document (relatório + voto +
                # dispositivo); the inline DECISÃO block is only a short summary.
                if it.get("inteiro_link"):
                    full = self._fetch_inteiro_teor(it["inteiro_link"])
                    if full and len(full) > len(it.get("text") or ""):
                        it["text"] = full

                if not it["text"] or len(it["text"]) < 50:
                    continue
                yield it
                count += 1

            if page % 25 == 0:
                self._save_checkpoint(page)
                logger.info("Fetched %d records through page %d", count, page)

        self._save_checkpoint(total_pages)
        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        logger.info("Fetching recent TRF2 decisions (since %s)", since)
        yield from self.fetch_all()


def main():
    scraper = TRF2Scraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to TRF2 eproc jurisprudence...")
        if not scraper._establish_session():
            logger.error("Session init FAILED")
            sys.exit(1)
        page_html = scraper._post_search()
        if not page_html:
            logger.error("Search POST FAILED")
            sys.exit(1)
        m = RE_TOTAL.search(page_html)
        total = int(m.group(1)) if m else 0
        items = scraper._parse_page(page_html)
        logger.info("TRF2 OK — %d total docs, %d items on page 1", total, len(items))
        if items:
            it = items[0]
            logger.info("Sample: proc=%s date=%s text=%d chars",
                        it["process_number"], it["data_julgamento"], len(it["text"]))
            logger.info("Preview: %.200s", it["text"][:200])
        return

    # bootstrap-fast is the VPS fleet entrypoint; alias it to the full bootstrap
    # path so it writes the full corpus to data/records.jsonl (not just sample/).
    if command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        logger.info("Bootstrap complete: %s", stats)

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
