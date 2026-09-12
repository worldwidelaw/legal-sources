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
  - Pagination (AJAX, same session): POST the *result* form back, with the
    wanted page written into its hidden `hdnPaginaAtual` field. There is no
    `pagina` query parameter — sending one is answered HTTP 200 with a page the
    server picks itself, so the walk never advances (#1502).
      POST https://eproc.trf2.jus.br/eproc/externo_controlador.php
             ?acao=jurisprudencia@jurisprudencia/ajax_paginar_resultado

The result form also exposes `selTamanhoPagina` (10/25/50/100 — we ask for 100),
`selOrdenacao` (1 = newest first) and a publication-date range
(`dtPublicacaoInicio`/`dtPublicacaoFim`) that the refresh path uses to fetch
only what has been published since the last run.

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
from datetime import datetime, timedelta, timezone
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

# The result form's own page-size selector offers 10/25/50/100; 100 cuts the
# full walk from ~121K requests to ~12.1K.
PAGE_SIZE = 100
# selOrdenacao: 1 = "mais recentes", 2 = "mais antigos". Newest-first is what
# makes the incremental lane cheap.
ORDER_NEWEST_FIRST = "1"
# How far before `since` to reopen the publication window. A decision can be
# indexed a few days after the publication date it carries, and re-yielding a
# handful of already-ingested documents is much cheaper than missing them --
# the loader dedups on _id.
UPDATE_LOOKBACK_DAYS = 7

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
        # The exact field set of the last search; every paging POST replays it.
        self._search_data = {}

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

    def _post_search(self, pub_from: str = "", pub_to: str = "") -> Optional[str]:
        """POST the search form (empty query = full corpus, inteiro teor).

        `pub_from`/`pub_to` are dd/mm/yyyy strings bound to the advanced form's
        publication-date range; leaving them empty searches the whole corpus.
        The form data is kept on the instance because every subsequent page has
        to be POSTed back with the identical field set (see `_fetch_page`).
        """
        data = {
            "txtPesquisa": "",
            "rdoCampo": "I",            # I = Inteiro Teor (full text)
            "chkAgruparResultados": "1",
            "selOrdenacao": ORDER_NEWEST_FIRST,
            "selTamanhoPagina": str(PAGE_SIZE),
        }
        if pub_from:
            data["dtPublicacaoInicio"] = pub_from
        if pub_to:
            data["dtPublicacaoFim"] = pub_to
        self._search_data = data

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": FORM_URL,
        }
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.post(LIST_URL, data=data, headers=headers, timeout=180)
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
        """Fetch a specific result page via the AJAX pagination endpoint.

        The endpoint takes no `pagina` query parameter, which is what the old
        implementation sent: `paginar()` in modulos/jurisprudencia/js/jurisprudencia.js
        POSTs `$('#frmJurisprudenciaResultado').serializeArray()` to
        `hdnUrlPaginar` after writing the wanted page into the hidden
        `hdnPaginaAtual` field. A GET with `?pagina=N` is accepted with HTTP 200
        and silently answered with a page the server picks itself, so the walk
        kept re-reading the head of the result set instead of advancing.
        """
        data = dict(self._search_data)
        data["hdnPaginaAtual"] = str(pagina)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": LIST_URL,
            "X-Requested-With": "XMLHttpRequest",
        }
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.post(PAGE_URL, data=data,
                                         headers=headers, timeout=180)
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
        """Resume page for the full walk, or 0 to start over.

        A page number only means anything alongside the page size it was
        recorded under, so a checkpoint written at a different `selTamanhoPagina`
        is discarded rather than silently skipping ten times too far.
        """
        try:
            if CHECKPOINT_FILE.exists():
                state = json.loads(CHECKPOINT_FILE.read_text())
                if int(state.get("page_size", 0)) != PAGE_SIZE:
                    logger.info("Ignoring checkpoint written at page size %s "
                                "(now %d) — restarting the walk",
                                state.get("page_size"), PAGE_SIZE)
                    return 0
                return int(state.get("last_page", 0))
        except Exception:
            pass
        return 0

    def _save_checkpoint(self, page: int) -> None:
        try:
            CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
            CHECKPOINT_FILE.write_text(json.dumps({"last_page": page,
                                                   "page_size": PAGE_SIZE}))
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
    def _walk_search(self, pub_from: str = "", pub_to: str = "",
                     resume_page: int = 0,
                     checkpoint: bool = False) -> Generator[dict, None, None]:
        """Walk every page of one search and yield raw records with full text.

        Shared by the full crawl and the refresh; the only difference between
        them is the publication-date window bound to the search.
        """
        if not self._session_ready and not self._establish_session():
            logger.error("Could not establish eproc session")
            return

        page_html = self._post_search(pub_from, pub_to)
        if not page_html:
            logger.error("Failed to POST search")
            return

        total = 0
        m = RE_TOTAL.search(page_html)
        if m:
            total = int(m.group(1))
        if total <= 0:
            m = RE_TOTAL_PAGES.search(page_html)
            total_pages = int(m.group(1)) if m else 1
        else:
            # hdnTotalPaginas is computed for the page size the form was
            # rendered with, so derive the count from the one we asked for.
            total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
        window = f" published {pub_from}–{pub_to}" if pub_from else ""
        logger.info("Search returned %d documents across %d pages of %d%s",
                    total, total_pages, PAGE_SIZE, window)
        total_pages = max(total_pages, 1)

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

            if checkpoint and page % 25 == 0:
                self._save_checkpoint(page)
                logger.info("Fetched %d records through page %d", count, page)

        if checkpoint:
            self._save_checkpoint(total_pages)
        logger.info("Total records yielded: %d", count)

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._walk_search(resume_page=self._load_checkpoint(),
                                     checkpoint=True)

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Yield only decisions published since the last run.

        The old body was `yield from self.fetch_all()`, so every refresh slot
        re-walked the entire 1.2M-document result set to surface the handful of
        genuinely new decisions (#1502).

        The advanced search form carries a publication-date range
        (`dtPublicacaoInicio`/`dtPublicacaoFim`) and the server honours it — an
        August-2026 window returns 15,003 documents against a 1,209,328 corpus.
        Publication date is the right comparator here because it is when the
        decision became available to us, not when it was judged: DATA DO
        JULGAMENTO routinely runs ahead of DATA DA PUBLICAÇÃO on this portal
        (page one currently shows judgments dated 03/09/2026 published
        27/08/2026), so a judgment-date cutoff would let documents appear on the
        wrong side of the window.

        The window is reopened `UPDATE_LOOKBACK_DAYS` before `since` to cover
        decisions the portal indexes a few days late; the loader dedups the
        overlap on `_id`.
        """
        cutoff = self._parse_since(since)
        if cutoff is None:
            logger.warning("Unparseable since=%r — falling back to a full walk", since)
            yield from self.fetch_all()
            return

        pub_from = (cutoff - timedelta(days=UPDATE_LOOKBACK_DAYS)).strftime("%d/%m/%Y")
        pub_to = datetime.now(timezone.utc).strftime("%d/%m/%Y")
        logger.info("Fetching TRF2 decisions published %s–%s (since=%s)",
                    pub_from, pub_to, since)
        yield from self._walk_search(pub_from=pub_from, pub_to=pub_to)

    @staticmethod
    def _parse_since(since) -> Optional[datetime]:
        """Accept the several shapes the runner passes as `since`."""
        if isinstance(since, datetime):
            return since
        if not since:
            return None
        text = str(since).strip()
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None


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
        default_since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        since = (sys.argv[2] if len(sys.argv) > 2
                 and not sys.argv[2].startswith("-") else default_since)
        count = sum(1 for _ in scraper.fetch_updates(since))
        logger.info("Update complete: %d records since %s", count, since)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
