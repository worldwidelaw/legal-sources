#!/usr/bin/env python3
"""
MX/PoderJudicialQuintanaRoo -- Quintana Roo State Court Decisions
(Módulo de Consulta Ciudadana de Sentencias Públicas)

Fetches public-version court decisions (sentencias públicas) from the Poder
Judicial del Estado de Quintana Roo via the public "Módulo de Consulta
Ciudadana de Sentencias Públicas" portal.

Portal:  https://gestionchetumal.tsjqroo.gob.mx/sentenciaspublicas
Per court: /sentenciaspublicas/Home/Consultas?id={N}   (N = 1..92)

Discovery
---------
The landing page lists every juzgado / sala as a link to
``Home/Consultas?id={N}``. Each Consultas page server-renders a full table
(``#tablaJuzgado``) of that court's public sentences with columns:

  expediente | juzgado | materia | juicio | tema | publicación | archivo

The "archivo" cell carries an ``onclick="download('//<host>/<path>/X.pdf')"``
handler. The PDF lives on an internal file host that is only reachable through
the portal's own proxy endpoint:

  Home/GetPdf?FilePath=<dir>&fileName=<file.pdf>

where ``<dir>`` / ``<file.pdf>`` come from splitting the onclick path. The PDF
body is downloaded through that proxy and its full text extracted.

A few courts (ids 17, 90, 91) load their rows dynamically from separate JSON
APIs (gestionjudicial / lexcifam) rather than server-rendering them; those are
covered too via their documented endpoints.

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast --full
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote, urlencode
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialQuintanaRoo")

BASE = "https://gestionchetumal.tsjqroo.gob.mx"
PORTAL = f"{BASE}/sentenciaspublicas"
CONSULTAS = f"{PORTAL}/Home/Consultas"
GETPDF = f"{PORTAL}/Home/GetPdf"

# Courts whose rows are loaded dynamically from JSON APIs.
LEXCIFAM_API = "https://lexcifam.tsjqroo.gob.mx/api/servicios-sentencias-publicas/getSentenciasPublicas/"
GESTIONJUDICIAL_API = "https://gestionjudicial.tsjqroo.gob.mx/apitdj/api/SentenciaPublica/GetSentenciaPublica"
SYSTEM_CONFIG = {
    "17": {"apiType": "gestionjudicial", "organoJudicialId": 4, "tipoJuicio": "MERCANTIL ORAL"},
    "90": {"apiType": "gestionjudicial", "organoJudicialId": 5, "tipoJuicio": "FAMILIAR"},
    "91": {"apiType": "lexcifam", "organoJudicialId": "28a77984-b4fc-4d89-8e48-0624ae75106c", "tipoJuicio": "FAMILIAR"},
}


def _clean(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _norm_date(value: str) -> Optional[str]:
    """Accept D/MM/YYYY or DD/MM/YYYY (and ISO) -> ISO YYYY-MM-DD."""
    value = (value or "").strip()
    if not value:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", value)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", value)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    return None


def _pdf_proxy_url(onclick_path: str) -> Optional[str]:
    """Translate an onclick download('//host/dir/file.pdf') path into the
    portal's GetPdf proxy URL. Returns None for absolute http(s) URLs which the
    portal opens directly (handled separately)."""
    p = onclick_path.strip()
    if p.lower().startswith("http"):
        return p  # absolute -> direct
    name = p.split("/")[-1]
    directory = "/".join(p.split("/")[:-1])
    return f"{GETPDF}?FilePath={quote(directory, safe='')}&fileName={quote(name, safe='')}"


class QuintanaRooCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialQuintanaRoo public-version court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; Legal-Data-Hunter/1.0; "
                          "+https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html, application/json, */*",
            "Referer": PORTAL,
        })

    # ---- HTTP helpers -------------------------------------------------

    def _get(self, url: str, timeout: int = 90, **kw) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(1.2)
                resp = self.session.get(url, timeout=timeout, **kw)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    # ---- Discovery ----------------------------------------------------

    def _court_ids(self) -> List[int]:
        resp = self._get(PORTAL)
        if not resp:
            return []
        ids = sorted({int(x) for x in re.findall(r"Consultas\?id=(\d+)", resp.text)})
        return ids

    def _parse_court_page(self, court_id: int) -> List[Dict[str, Any]]:
        """Parse the server-rendered #tablaJuzgado rows for one court."""
        resp = self._get(f"{CONSULTAS}?id={court_id}")
        if not resp:
            return []
        h = resp.text
        # Grab the tablaJuzgado <tbody> (the precharged, server-rendered table).
        tb = re.search(
            r'id="tablaJuzgado".*?<tbody[^>]*>(.*?)</tbody>', h, re.S | re.I
        )
        body = tb.group(1) if tb else ""
        out = []
        for tr in re.findall(r"<tr>(.*?)</tr>", body, re.S | re.I):
            onclick = re.search(r"download\(\s*['\"]([^'\"]+\.pdf)['\"]", tr, re.I)
            if not onclick:
                continue
            cells = [_clean(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S | re.I)]
            proxy = _pdf_proxy_url(onclick.group(1))
            if not proxy:
                continue
            out.append({
                "court_id": court_id,
                "expediente": cells[0] if len(cells) > 0 else "",
                "juzgado": cells[1] if len(cells) > 1 else "",
                "materia": cells[2] if len(cells) > 2 else "",
                "juicio": cells[3] if len(cells) > 3 else "",
                "tema": cells[4] if len(cells) > 4 else "",
                "fecha": cells[5] if len(cells) > 5 else "",
                "raw_pdf": onclick.group(1),
                "pdf": proxy,
            })
        return out

    def _api_court(self, court_id: int) -> List[Dict[str, Any]]:
        """Fetch rows for the dynamic (JSON-API) courts 17/90/91."""
        cfg = SYSTEM_CONFIG.get(str(court_id))
        if not cfg:
            return []
        out: List[Dict[str, Any]] = []
        try:
            if cfg["apiType"] == "gestionjudicial":
                params = {"organoJudicialId": cfg["organoJudicialId"]}
                resp = self._get(f"{GESTIONJUDICIAL_API}?{urlencode(params)}", timeout=120)
            else:
                params = {"organoJudicialId": cfg["organoJudicialId"]}
                resp = self._get(f"{LEXCIFAM_API}?{urlencode(params)}", timeout=120)
            if not resp:
                return []
            data = resp.json()
        except Exception as e:
            logger.warning(f"API court {court_id} failed: {e}")
            return []
        items = data if isinstance(data, list) else data.get("data") or data.get("result") or []
        for it in items:
            if not isinstance(it, dict):
                continue
            archivo = it.get("archivo") or it.get("url") or it.get("pdf") or ""
            if not archivo:
                continue
            proxy = _pdf_proxy_url(archivo)
            if not proxy:
                continue
            out.append({
                "court_id": court_id,
                "expediente": str(it.get("expediente") or ""),
                "juzgado": str(it.get("organoJudicial") or cfg.get("tipoJuicio") or ""),
                "materia": str(it.get("materia") or ""),
                "juicio": str(it.get("juicio") or cfg.get("tipoJuicio") or ""),
                "tema": str(it.get("accion") or it.get("tema") or ""),
                "fecha": str(it.get("fecha") or it.get("fechaImprimir") or ""),
                "raw_pdf": archivo,
                "pdf": proxy,
            })
        return out

    def _discover(self) -> Generator[Dict[str, Any], None, None]:
        seen = set()
        ids = self._court_ids()
        logger.info(f"Discovered {len(ids)} courts")
        for cid in ids:
            if str(cid) in SYSTEM_CONFIG:
                rows = self._api_court(cid)
            else:
                rows = self._parse_court_page(cid)
            if rows:
                logger.info(f"Court id={cid}: {len(rows)} sentences")
            for r in rows:
                key = r["raw_pdf"]
                if key in seen:
                    continue
                seen.add(key)
                yield r

    # ---- Schema -------------------------------------------------------

    @staticmethod
    def _doc_id(raw: Dict[str, Any]) -> str:
        stem = raw["raw_pdf"].rsplit("/", 1)[-1]
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        return re.sub(r"[^A-Za-z0-9_\-]+", "_", stem).strip("_") or f"c{raw['court_id']}"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = self._doc_id(raw)
        expediente = raw.get("expediente", "")
        juzgado = raw.get("juzgado", "")
        materia = raw.get("materia", "")
        title_bits = [b for b in [
            f"Expediente {expediente}" if expediente else "",
            juzgado,
            f"({materia})" if materia else "",
        ] if b]
        title = " — ".join(title_bits[:2]) + (f" {title_bits[2]}" if len(title_bits) > 2 else "")
        title = title.strip() or f"Sentencia {doc_id}"

        return {
            "_id": f"MX-ROO-{doc_id}",
            "_source": "MX/PoderJudicialQuintanaRoo",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": _norm_date(raw.get("fecha", "")),
            "url": raw["pdf"],
            "court": juzgado,
            "subject_area": materia,
            "case_type": raw.get("juicio", ""),
            "case_topic": raw.get("tema", ""),
            "case_number": expediente,
        }

    # ---- Fetch --------------------------------------------------------

    def _extract_text(self, raw: Dict[str, Any], doc_id: str) -> str:
        # Download the PDF through our own session (carries Referer) then hand the
        # raw bytes to the shared extractor, which OOM-hardens / cleans the text.
        resp = self._get(
            raw["pdf"], timeout=120,
            headers={"Referer": f"{CONSULTAS}?id={raw['court_id']}"},
        )
        if not resp or not resp.content:
            return ""
        try:
            text = extract_pdf_markdown(
                "MX/PoderJudicialQuintanaRoo", f"MX-ROO-{doc_id}",
                pdf_bytes=resp.content,
            )
            return text or ""
        except Exception as e:
            logger.warning(f"PDF extract failed for {raw['pdf']}: {e}")
            return ""

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        count = 0
        skipped = 0
        for raw in self._discover():
            doc_id = self._doc_id(raw)
            text = self._extract_text(raw, doc_id)
            if not text or len(text) < 50:
                skipped += 1
                continue
            raw["_text"] = text
            count += 1
            yield raw
            if count % 50 == 0:
                logger.info(f"Fetched {count} decisions ({skipped} skipped)")
        logger.info(f"Completed: {count} decisions fetched, {skipped} skipped")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        # Tables are publicación-desc ordered; re-scan and let the loader dedup on _id.
        yield from self.fetch_all()

    def test(self) -> bool:
        ids = self._court_ids()
        if not ids:
            logger.error("Cannot reach Quintana Roo portal / no courts found")
            return False
        logger.info(f"Portal OK: {len(ids)} courts")
        # Find a court with server-rendered rows.
        for cid in ids:
            if str(cid) in SYSTEM_CONFIG:
                continue
            rows = self._parse_court_page(cid)
            if rows:
                logger.info(f"Court id={cid}: {len(rows)} rows")
                sample = rows[0]
                txt = self._extract_text(sample, self._doc_id(sample))
                logger.info(f"Sample PDF {sample['pdf']} -> {len(txt)} chars")
                return len(txt) > 50
        logger.error("No server-rendered sentences found")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/PoderJudicialQuintanaRoo data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = QuintanaRooCourtScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
