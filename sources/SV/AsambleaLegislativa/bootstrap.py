"""
Legal Data Hunter — SV/AsambleaLegislativa

Asamblea Legislativa de la República de El Salvador — Leyes y Decretos.

Source site (Drupal): https://www.asamblea.gob.sv/leyes-y-decretos/busqueda-decretos

Access strategy (no API; structured HTML + per-document PDF):
  1. The "Decretos por año" index lists every year that has decrees:
       /leyes-y-decretos/decretos-por-anios            -> list of years
       /leyes-y-decretos/decretos-por-anios/{year}/0   -> all decree cards for a year
     Each card links to a node page /leyes-y-decretos/view/{id}.
  2. The node page carries structured metadata (decree number, dates, materia,
     rama del derecho, resumen) and a link to the official PDF under
       /sites/default/files/documents/decretos/{uuid}.pdf
  3. The PDF holds the FULL TEXT of the law/decree. Recent decrees are
     digital-native (extractable text); we extract it with pdfplumber.

Records whose PDF yields no extractable text (older scanned-image decrees) are
skipped — this source only contributes full-text records.
"""

import io
import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import urllib3
import pdfplumber

# asamblea.gob.sv serves a valid Sectigo certificate but omits the intermediate
# CA from the chain, so strict verification fails ("unable to get local issuer
# certificate"). The cert itself is legitimate; disable verification for this
# open, public-data host and silence the resulting warning noise.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter")

BASE = "https://www.asamblea.gob.sv"
YEAR_INDEX = BASE + "/leyes-y-decretos/decretos-por-anios"
YEAR_LIST = BASE + "/leyes-y-decretos/decretos-por-anios/{year}/0"
NODE_URL = BASE + "/leyes-y-decretos/view/{node_id}"

# Minimum extracted characters for a PDF to count as "full text" (filters
# out scanned-image decrees that yield little/no text).
MIN_TEXT_CHARS = 400


class SourceScraper(BaseScraper):
    """Scraper for SV/AsambleaLegislativa (legislation, full text via PDF)."""

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open legal data research)"
            ),
            "Accept-Language": "es-SV,es;q=0.9",
        })
        # See note above: incomplete server-side cert chain.
        self.session.verify = False

    # ── HTTP helpers ──────────────────────────────────────────────

    def _get(self, url: str, binary: bool = False) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=90)
                if resp.status_code == 200:
                    return resp
                logger.debug(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.debug(f"Request error for {url}: {e}")
            time.sleep(2 * (attempt + 1))
        return None

    # ── Enumeration ───────────────────────────────────────────────

    def _list_years(self) -> list:
        resp = self._get(YEAR_INDEX)
        if not resp:
            return []
        years = sorted(
            {int(y) for y in re.findall(r"decretos-por-anios/(\d{4})/0", resp.text)},
            reverse=True,
        )
        return years

    def _list_node_ids(self, year: int) -> list:
        resp = self._get(YEAR_LIST.format(year=year))
        if not resp:
            return []
        ids, seen = [], set()
        for nid in re.findall(r"leyes-y-decretos/view/(\d+)", resp.text):
            if nid not in seen:
                seen.add(nid)
                ids.append(nid)
        return ids

    # ── Per-document fetch ────────────────────────────────────────

    def _fetch_node(self, node_id: str, year: int) -> Optional[dict]:
        resp = self._get(NODE_URL.format(node_id=node_id))
        if not resp:
            return None
        html = resp.text

        pdf_match = re.search(
            r"sites/default/files/documents/decretos/[A-Za-z0-9._%-]+\.pdf", html
        )
        if not pdf_match:
            return None
        pdf_url = BASE + "/" + pdf_match.group(0)

        # Title: first <h1> on the page is the decree title.
        title = ""
        h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S)
        if h1:
            title = _clean(h1.group(1))

        # Metadata table: <th>Label:</th> <td>value</td> pairs.
        # Some rows (e.g. "Tipo") are wrapped in HTML comments on the page;
        # drop comment delimiters so those rows are parsed too.
        meta_html = html.replace("<!--", "").replace("-->", "")
        meta = {}
        for label, value in re.findall(
            r"<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>", meta_html, re.S
        ):
            key = _clean(label).rstrip(":").strip()
            val = _clean(value)
            if key and val:
                meta[key] = val

        # Resumen (summary) lives in its own card.
        resumen = ""
        res = re.search(
            r"Resumen:.*?<div class=\"card-body[^\"]*\">(.*?)</div>", html, re.S
        )
        if res:
            resumen = _clean(res.group(1))

        # Publication table: three <th> headers then three <td> values.
        diario, tomo, fecha_pub = "", "", ""
        pub = re.search(r"Datos de Publicaci.*?</table>", html, re.S)
        if pub:
            tds = [_clean(t) for t in re.findall(r"<td[^>]*>(.*?)</td>", pub.group(0), re.S)]
            tds = [t for t in tds if t]
            if len(tds) >= 3:
                diario, tomo, fecha_pub = tds[0], tds[1], tds[2]

        return {
            "node_id": node_id,
            "year": year,
            "url": NODE_URL.format(node_id=node_id),
            "pdf_url": pdf_url,
            "title": title,
            "numero_decreto": meta.get("Numero del decreto", ""),
            "fecha_emision": meta.get("Fecha de Emisión", ""),
            "materia": meta.get("Materia", ""),
            "sub_materia": meta.get("Sub-Materia", ""),
            "tipo": meta.get("Tipo", ""),
            "rama_derecho": meta.get("Rama del derecho", ""),
            "resumen": resumen,
            "diario_oficial": diario,
            "tomo": tomo,
            "fecha_publicacion": fecha_pub,
        }

    def _extract_pdf_text(self, pdf_url: str) -> str:
        resp = self._get(pdf_url)
        if not resp:
            return ""
        data = resp.content
        # Some PDFs are served with a few junk bytes before the %PDF header.
        head = data.find(b"%PDF")
        if head > 0:
            data = data[head:]
        try:
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                parts = [(page.extract_text() or "") for page in pdf.pages]
        except Exception as e:
            logger.debug(f"PDF parse failed for {pdf_url}: {e}")
            return ""
        text = "\n".join(parts)
        # Collapse excessive whitespace while keeping paragraph breaks.
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text

    # ── Abstract methods ──────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        for year in self._list_years():
            node_ids = self._list_node_ids(year)
            logger.info(f"Year {year}: {len(node_ids)} decrees")
            for node_id in node_ids:
                meta = self._fetch_node(node_id, year)
                if meta:
                    yield meta
                time.sleep(0.5)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        # The site exposes no modified-since filter; restrict the year sweep to
        # the year of `since` onward, dedup is handled by the base class.
        start_year = since.year
        for year in self._list_years():
            if year < start_year:
                break
            node_ids = self._list_node_ids(year)
            for node_id in node_ids:
                meta = self._fetch_node(node_id, year)
                if meta:
                    yield meta
                time.sleep(0.5)

    def normalize(self, raw: dict) -> Optional[dict]:
        text = self._extract_pdf_text(raw["pdf_url"])
        if len(text) < MIN_TEXT_CHARS:
            # Scanned-image decree without OCR-able text — skip (no full text).
            return None

        return {
            "_id": f"SV-AL-{raw['node_id']}",
            "_source": "SV/AsambleaLegislativa",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": text,
            "date": _to_iso(raw.get("fecha_emision")),
            "date_publication": _to_iso(raw.get("fecha_publicacion")),
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "decree_number": raw.get("numero_decreto"),
            "materia": raw.get("materia"),
            "sub_materia": raw.get("sub_materia"),
            "rama_derecho": raw.get("rama_derecho"),
            "summary": raw.get("resumen"),
            "diario_oficial": raw.get("diario_oficial"),
            "tomo": raw.get("tomo"),
            "language": "es",
            "jurisdiction": "SV",
        }


def _clean(html_fragment: str) -> str:
    """Strip tags and decode common entities from an HTML fragment."""
    import html as _html

    text = re.sub(r"<[^>]+>", " ", html_fragment)
    text = _html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _to_iso(value: Optional[str]) -> Optional[str]:
    """Convert dd/mm/yyyy -> yyyy-mm-dd; return None if not parseable."""
    if not value:
        return None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", value.strip())
    if not m:
        return None
    d, mo, y = m.groups()
    return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = SourceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    import json
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
