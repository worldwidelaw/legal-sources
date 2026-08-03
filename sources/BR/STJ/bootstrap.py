#!/usr/bin/env python3
"""
BR/STJ -- Superior Tribunal de Justiça (Brazil), full inteiro-teor acórdãos

Fetches the FULL TEXT (inteiro teor) of STJ acórdãos from the official
jurisprudence search (SCON). Distinct from BR/STJDadosAbertos, which only
carries the "espelhos" (ementa/summary) datasets from May 2022 onward — this
source retrieves the complete decision PDF (relatório + votos + certidão).

Access recipe (the SCON search sits behind a BIG-IP ASM + Cloudflare WAF that
rejects naked programmatic requests):
  1. Prime a session on https://scon.stj.jus.br/SCON/ to obtain the BIG-IP
     ``TS...`` cookie + ``JSESSIONID``.
  2. Query pesquisar.jsp with a browser User-Agent, the primed cookies and a
     Referer header. Enumerate by publication date using the field operator
     ``livre=DTPB="YYYYMMDD"`` and paginate with ``&l=<n>&i=<offset>``.
  3. Each result row exposes an inteiro-teor link
     ``/SCON/GetInteiroTeorDoAcordao?num_registro=<reg>&dt_publicacao=<dd/mm/yyyy>``
     which returns the decision as a text-based PDF (extractable, not scanned).

Enumeration walks publication dates backwards from today to START_YEAR, paging
through every acórdão published on each date and downloading its full-text PDF.

Usage:
  python bootstrap.py bootstrap --sample   # 15 sample records (recent dates)
  python bootstrap.py bootstrap            # full backfill
  python bootstrap.py bootstrap-fast       # full backfill (threaded)
  python bootstrap.py test                 # connectivity test
"""

import sys
import re
import time
import html
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.STJ")

BASE = "https://scon.stj.jus.br/SCON"
HOME = f"{BASE}/"
SEARCH = f"{BASE}/pesquisar.jsp"
ITA = f"{BASE}/GetInteiroTeorDoAcordao"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# Earliest publication year to backfill down to. STJ acórdãos with full-text
# PDFs go back into the 1990s; keep a generous floor.
START_YEAR = 1995
PER_PAGE = 50
REQUEST_TIMEOUT = 60


def _clean(text: str) -> str:
    """Collapse whitespace and unescape HTML entities."""
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _iso_from_ddmmyyyy(s: str) -> Optional[str]:
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", s or "")
    if not m:
        return None
    d, mo, y = m.groups()
    try:
        datetime(int(y), int(mo), int(d))
    except ValueError:
        return None
    return f"{y}-{mo}-{d}"


class STJScraper(BaseScraper):
    """Scraper for BR/STJ -- full inteiro-teor acórdãos from SCON."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._session: Optional[requests.Session] = None

    # -- session / WAF handling ------------------------------------------

    def _new_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update(
            {
                "User-Agent": UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            }
        )
        # Prime the BIG-IP / Cloudflare session on the homepage.
        s.get(HOME, timeout=REQUEST_TIMEOUT)
        return s

    def _ensure_session(self) -> requests.Session:
        if self._session is None:
            self._session = self._new_session()
        return self._session

    def _get(self, url: str, params: dict, referer: str, retries: int = 3) -> Optional[requests.Response]:
        for attempt in range(retries):
            s = self._ensure_session()
            try:
                r = s.get(
                    url,
                    params=params,
                    headers={"Referer": referer},
                    timeout=REQUEST_TIMEOUT,
                )
            except requests.RequestException as e:
                logger.warning("request error (%s): %s", attempt, e)
                self._session = None
                time.sleep(2)
                continue
            if r.status_code == 200 and "Request Rejected" not in r.text[:400]:
                return r
            # WAF rejected or non-200 -> rebuild session and retry.
            logger.warning("WAF/HTTP issue (status %s, attempt %s) -> re-priming session", r.status_code, attempt)
            self._session = None
            time.sleep(2 + attempt)
        return None

    # -- search / parse --------------------------------------------------

    def _search_date(self, yyyymmdd: str, offset: int) -> Optional[requests.Response]:
        params = {
            "livre": f'DTPB="{yyyymmdd}"',
            "b": "ACOR",
            "l": str(PER_PAGE),
            "i": str(offset),
        }
        return self._get(SEARCH, params, referer=SEARCH)

    @staticmethod
    def _parse_results(html_text: str) -> List[Dict[str, Any]]:
        """Extract per-document metadata from a SCON results page."""
        # Split the page into per-document chunks anchored on the doc header.
        anchors = [m.start() for m in re.finditer(r"clsIdentificacaoDocumento", html_text)]
        if not anchors:
            return []
        anchors.append(len(html_text))
        out: List[Dict[str, Any]] = []
        for k in range(len(anchors) - 1):
            chunk = html_text[anchors[k]: anchors[k + 1]]

            ita = re.search(
                r"GetInteiroTeorDoAcordao\?num_registro=(\d+)&dt_publicacao=([\d/]+)",
                chunk,
            )
            if not ita:
                continue  # no full-text PDF for this row
            num_registro = ita.group(1)
            dt_publicacao = ita.group(2)

            # Document identification / class sigla (e.g. "RESP 1971993").
            ident = re.search(
                r'clsIdentificacaoDocumento"?>\s*(.*?)\s*</div>', chunk, re.S
            )
            identificacao = _clean(re.sub(r"<[^>]+>", " ", ident.group(1))) if ident else ""

            # Labelled metadata blocks: <div class="paragrafoBRS">
            #   <div class="docTitulo">LABEL</div><div class="docTexto">VALUE</div></div>
            fields: Dict[str, str] = {}
            for pm in re.finditer(
                r'docTitulo">(.*?)</div>\s*<div class="docTexto">(.*?)</div>',
                chunk,
                re.S,
            ):
                label = _clean(re.sub(r"<[^>]+>", " ", pm.group(1)))
                # Preserve line breaks within a value for the Processo descriptor.
                raw_val = pm.group(2).replace("<br>", "\n").replace("<br/>", "\n")
                value = html.unescape(re.sub(r"[ \t]*\n[ \t]*", "\n", re.sub(r"<[^>]+>", "", raw_val))).strip()
                if label and label not in fields:
                    fields[label] = value

            def _f(*labels: str) -> str:
                for lab in labels:
                    for key, val in fields.items():
                        if key.startswith(lab):
                            return val
                return ""

            processo = _f("Processo")
            # First line of the Processo block is the clean citation (class + number / UF).
            processo_num = processo.split("\n", 1)[0].strip() if processo else identificacao

            # Ementa: prefer the clean copy-to-clipboard textarea, fall back to field.
            em = re.search(r'textareaSemformatacao"[^>]*>(.*?)</textarea>', chunk, re.S)
            ementa = _clean(re.sub(r"<[^>]+>", " ", em.group(1))) if em else _f("Ementa")

            out.append(
                {
                    "num_registro": num_registro,
                    "dt_publicacao": dt_publicacao,
                    "identificacao": identificacao,
                    "processo": processo_num,
                    "ementa": ementa,
                    "relator": _f("Relator"),
                    "orgao_julgador": _f("Órgão Julgador"),
                    "data_julgamento": _f("Data do Julgamento"),
                }
            )
        return out

    def _fetch_inteiro_teor_text(self, num_registro: str, dt_publicacao: str) -> Optional[str]:
        r = self._get(
            ITA,
            {"num_registro": num_registro, "dt_publicacao": dt_publicacao},
            referer=SEARCH,
        )
        if r is None:
            return None
        ctype = r.headers.get("Content-Type", "")
        if "pdf" not in ctype.lower() and not r.content[:4] == b"%PDF":
            return None
        try:
            return pdf_extract.extract_pdf_markdown(
                "BR/STJ", num_registro, pdf_bytes=r.content, force=True
            )
        except Exception as e:
            logger.warning("PDF extract failed for %s: %s", num_registro, e)
            return None

    # -- iteration -------------------------------------------------------

    def _iter_dates(self) -> Generator[str, None, None]:
        """Yield publication dates YYYYMMDD backwards from today to START_YEAR."""
        day = datetime.now(timezone.utc).date()
        floor = datetime(START_YEAR, 1, 1).date()
        while day >= floor:
            # Skip weekends — STJ does not publish on Sat/Sun.
            if day.weekday() < 5:
                yield day.strftime("%Y%m%d")
            day -= timedelta(days=1)

    def fetch_all(self) -> Generator[dict, None, None]:
        seen = set()
        for yyyymmdd in self._iter_dates():
            offset = 1
            while True:
                resp = self._search_date(yyyymmdd, offset)
                if resp is None:
                    logger.warning("skip date %s offset %s (no response)", yyyymmdd, offset)
                    break
                rows = self._parse_results(resp.text)
                if not rows:
                    break
                for row in rows:
                    reg = row["num_registro"]
                    if reg in seen:
                        continue
                    seen.add(reg)
                    text = self._fetch_inteiro_teor_text(reg, row["dt_publicacao"])
                    if not text or len(text) < 200:
                        continue
                    row["_text"] = text
                    yield row
                    time.sleep(1)
                if len(rows) < PER_PAGE:
                    break
                offset += PER_PAGE
                time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch acórdãos published on/after `since` (walks recent dates)."""
        since_date = since.date() if isinstance(since, datetime) else since
        day = datetime.now(timezone.utc).date()
        seen = set()
        while day >= since_date:
            if day.weekday() < 5:
                yyyymmdd = day.strftime("%Y%m%d")
                offset = 1
                while True:
                    resp = self._search_date(yyyymmdd, offset)
                    if resp is None:
                        break
                    rows = self._parse_results(resp.text)
                    if not rows:
                        break
                    for row in rows:
                        reg = row["num_registro"]
                        if reg in seen:
                            continue
                        seen.add(reg)
                        text = self._fetch_inteiro_teor_text(reg, row["dt_publicacao"])
                        if not text or len(text) < 200:
                            continue
                        row["_text"] = text
                        yield row
                        time.sleep(1)
                    if len(rows) < PER_PAGE:
                        break
                    offset += PER_PAGE
            day -= timedelta(days=1)

    def normalize(self, raw: dict) -> Optional[dict]:
        text = (raw.get("_text") or "").strip()
        if not text:
            return None

        num_registro = str(raw.get("num_registro", "")).strip()
        identificacao = (raw.get("identificacao") or "").strip()
        processo = (raw.get("processo") or "").strip()
        ementa = (raw.get("ementa") or "").strip()

        title = processo or identificacao or (ementa[:150] + ("..." if len(ementa) > 150 else ""))
        if not title:
            title = f"STJ acórdão {num_registro}"

        date = _iso_from_ddmmyyyy(raw.get("data_julgamento", "")) or _iso_from_ddmmyyyy(
            raw.get("dt_publicacao", "")
        )

        if num_registro:
            doc_id = f"BR-STJ-{num_registro}"
        else:
            doc_id = "BR-STJ-" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

        url = f"{ITA}?num_registro={num_registro}&dt_publicacao={raw.get('dt_publicacao','')}"

        return {
            "_id": doc_id,
            "_source": "BR/STJ",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "process_number": processo or identificacao,
            "class_sigla": identificacao,
            "numero_registro": num_registro,
            "orgao_julgador": (raw.get("orgao_julgador") or "").strip(),
            "judge_relator": (raw.get("relator") or "").strip(),
            "ementa": ementa,
            "publication_date": _iso_from_ddmmyyyy(raw.get("dt_publicacao", "")),
        }


if __name__ == "__main__":
    scraper = STJScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test":
        print("Testing STJ SCON connectivity...")
        try:
            resp = scraper._search_date(
                (datetime.now(timezone.utc).date() - timedelta(days=7)).strftime("%Y%m%d"),
                1,
            )
            if resp is None:
                print("FAIL: WAF blocked / no response")
                sys.exit(1)
            rows = scraper._parse_results(resp.text)
            print(f"OK: parsed {len(rows)} result rows")
            if rows:
                r0 = rows[0]
                print(f"  sample: {r0['identificacao']} reg={r0['num_registro']} pub={r0['dt_publicacao']}")
                txt = scraper._fetch_inteiro_teor_text(r0["num_registro"], r0["dt_publicacao"])
                print(f"  inteiro teor chars: {len(txt) if txt else 0}")
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd == "bootstrap":
        stats = scraper.bootstrap(sample_mode="--sample" in sys.argv, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif cmd == "bootstrap-fast":
        workers = 5
        batch_size = 100
        for i, arg in enumerate(sys.argv):
            if arg == "--workers" and i + 1 < len(sys.argv):
                workers = int(sys.argv[i + 1])
            if arg == "--batch-size" and i + 1 < len(sys.argv):
                batch_size = int(sys.argv[i + 1])
        stats = scraper.bootstrap_fast(max_workers=workers, batch_size=batch_size)
        fetched = stats.get("records_fetched", 0)
        logger.info(f"Bootstrap-fast complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif cmd == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
