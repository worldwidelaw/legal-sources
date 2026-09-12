#!/usr/bin/env python3
"""
ES/REGCON -- Spanish collective bargaining agreements (convenios colectivos)

Source request: issue #1624 (upstream worldwidelaw/legal-sources#251).

What this collects
------------------
Every collective bargaining agreement (convenio colectivo de trabajo) and its
related instruments -- revisions, wage tables, adhesions, extensions,
"acuerdos de modificacion", arbitration awards and "actas" -- that the
Direccion General de Trabajo registers in REGCON and publishes in the BOE,
with the FULL TEXT of the agreement.

Access strategy (official open data, no scraping of the JSF registry)
---------------------------------------------------------------------
REGCON itself (https://expinterweb.mites.gob.es/regcon/) is a stateful JSF
consultation form that only exposes registry metadata -- the authoritative
*text* of every state-scope agreement is the BOE publication that REGCON
registers.  So we enumerate through the BOE Open Data API, which is
documented, unauthenticated and stable:

  1. Daily summary (index):
       GET https://www.boe.es/datosabiertos/api/boe/sumario/YYYYMMDD
     Section III ("Otras disposiciones") groups items by department and
     epigraph.  Agreements sit under the epigraph
     "Convenios colectivos de trabajo" (Ministerio de Trabajo y Economia
     Social).  This is an exact epigraph match, so it does not pick up the
     unrelated administrative "Convenios" between public bodies.

  2. Full text (per document):
       GET https://www.boe.es/diario_boe/xml.php?id=BOE-A-YYYY-NNNNN
     Returns <metadatos> plus a <texto> element holding the complete
     agreement -- articles, annexes and wage tables -- as XHTML.

Metadata requested by the consumer (sector, geographic scope, dates,
publication reference, agreement code) is derived from the BOE metadata plus
the "codigo de convenio n.o NNNNNNNNNNNNNN" that every registration
resolution cites in its opening paragraph.  The first two digits of that code
are the territorial scope (99/90 = state-wide, otherwise the INE province
code), which is what REGCON keys its own records on.

Scope note: this covers the STATE-scope register (BOE).  Province- and
region-scope agreements are published in the boletines provinciales /
autonomicos and are covered by ES/ProvincialGazettes and the per-region ES
sources.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py update             # Incremental (since last run)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.REGCON")

BASE_URL = "https://www.boe.es"
SUMARIO_URL = BASE_URL + "/datosabiertos/api/boe/sumario/{day}"
XML_URL = BASE_URL + "/diario_boe/xml.php?id={doc_id}"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "application/json, application/xml;q=0.9",
    "Accept-Language": "es",
}

# The BOE epigraph under which the Direccion General de Trabajo publishes
# every REGCON registration. Matched case/accent-insensitively.
EPIGRAPH = "convenios colectivos de trabajo"

# BOE has published under this epigraph since the 1980s; the JSON summary API
# is reliable from 1990 onwards. Overridable via config (`coverage.start_date`).
DEFAULT_START = "1990-01-01"

# INE province codes -> province name. Used to turn the first two digits of a
# convenio code into the agreement's territorial scope.
PROVINCES = {
    "01": "Araba/Alava", "02": "Albacete", "03": "Alicante/Alacant",
    "04": "Almeria", "05": "Avila", "06": "Badajoz", "07": "Illes Balears",
    "08": "Barcelona", "09": "Burgos", "10": "Caceres", "11": "Cadiz",
    "12": "Castellon/Castello", "13": "Ciudad Real", "14": "Cordoba",
    "15": "A Coruna", "16": "Cuenca", "17": "Girona", "18": "Granada",
    "19": "Guadalajara", "20": "Gipuzkoa", "21": "Huelva", "22": "Huesca",
    "23": "Jaen", "24": "Leon", "25": "Lleida", "26": "La Rioja", "27": "Lugo",
    "28": "Madrid", "29": "Malaga", "30": "Murcia", "31": "Navarra",
    "32": "Ourense", "33": "Asturias", "34": "Palencia", "35": "Las Palmas",
    "36": "Pontevedra", "37": "Salamanca", "38": "Santa Cruz de Tenerife",
    "39": "Cantabria", "40": "Segovia", "41": "Sevilla", "42": "Soria",
    "43": "Tarragona", "44": "Teruel", "45": "Toledo", "46": "Valencia/Valencia",
    "47": "Valladolid", "48": "Bizkaia", "49": "Zamora", "50": "Zaragoza",
    "51": "Ceuta", "52": "Melilla",
}

CODE_RE = re.compile(r"c[oó]digo\s+de\s+conven\w*[^0-9]{0,40}(\d{11,14})", re.I)

# Instrument type detected from the title, most specific first.
INSTRUMENT_PATTERNS = [
    ("wage_tables", r"tabla[s]?\s+salarial"),
    ("revision", r"revisi[oó]n"),
    ("amendment", r"modificaci[oó]n|acuerdo\s+de\s+modificaci"),
    ("extension", r"pr[oó]rroga|ultraactividad"),
    ("adhesion", r"adhesi[oó]n"),
    ("arbitration_award", r"laudo\s+arbitral"),
    ("minutes", r"\bacta[s]?\b"),
    ("denunciation", r"denuncia"),
    ("agreement", r"convenio\s+colectivo|acuerdo"),
]

# Company-scope tells, checked before the sector tells because a company
# agreement may still name the sector it operates in.
COMPANY_RE = re.compile(
    r"convenio\s+colectivo\s+(de\s+la\s+|de\s+)?empresa\b|"
    r"\b(s\.?a\.?u?\.?|s\.?l\.?u?\.?|s\.?a\.?e\.?|s\.?c\.?a\.?)\s*[,.]|"
    r"\bpara\s+los\s+centros\s+de\s+trabajo\b",
    re.I,
)
SECTOR_RE = re.compile(
    r"\bsector\w*\b|convenio\s+colectivo\s+(general|estatal|marco|nacional)\b|"
    r"\bacuerdo\s+(marco|estatal)\b|\bindustria[s]?\b|\bgremio\b",
    re.I,
)


def _strip_accents(value: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", value)
        if unicodedata.category(c) != "Mn"
    )


def _as_list(value) -> list:
    """BOE JSON collapses single-element arrays into objects."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _iso_date(compact: Optional[str]) -> Optional[str]:
    """'20250305' -> '2025-03-05'. Returns None for empty/odd values."""
    if not compact:
        return None
    digits = re.sub(r"\D", "", str(compact))
    if len(digits) != 8:
        return None
    try:
        return date(int(digits[:4]), int(digits[4:6]), int(digits[6:])).isoformat()
    except ValueError:
        return None


def _clean_text(fragment: str) -> str:
    """Turn the <texto> XHTML fragment into readable plain text."""
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", fragment)
    # Table cells must not be glued together, and block elements are newlines.
    text = re.sub(r"(?i)</(td|th)>", "\t", text)
    text = re.sub(r"(?i)</(p|div|tr|h[1-6]|li|table|blockquote)>", "\n", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ").replace(" ", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n[ \n]*", "\n", text)
    return text.strip()


class REGCONScraper(BaseScraper):
    """
    Scraper for ES/REGCON -- Spanish collective bargaining agreements.
    Country: ES
    URL: https://expinterweb.mites.gob.es/regcon/

    Data types: legislation (collective agreements)
    Auth: none (BOE open data)
    """

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir or str(Path(__file__).parent))
        self.client = HttpClient(headers=HEADERS, timeout=60)
        coverage = self.config.get("coverage", {}) or {}
        self.start_date = str(coverage.get("start_date") or DEFAULT_START)

    # ── Index: walk BOE daily summaries ───────────────────────────────

    def _day_items(self, day: date) -> List[Dict[str, Any]]:
        """Return the convenio-colectivo items published on `day`."""
        url = SUMARIO_URL.format(day=day.strftime("%Y%m%d"))
        resp = self.client.get(url, rate_limiter=self.rate_limiter)
        if resp.status_code == 404:
            # No edition that day (Sundays, holidays).
            return []
        resp.raise_for_status()
        payload = resp.json()

        if str(payload.get("status", {}).get("code")) not in ("200", "ok", "OK"):
            # BOE answers 200 with an error envelope for non-publication days.
            return []

        found = []
        sumario = payload.get("data", {}).get("sumario", {})
        for diario in _as_list(sumario.get("diario")):
            issue = diario.get("numero")
            for seccion in _as_list(diario.get("seccion")):
                for dep in _as_list(seccion.get("departamento")):
                    for epigrafe in _as_list(dep.get("epigrafe")):
                        name = _strip_accents(epigrafe.get("nombre") or "").lower()
                        if name != EPIGRAPH:
                            continue
                        for item in _as_list(epigrafe.get("item")):
                            found.append({
                                "identifier": item.get("identificador"),
                                "title": item.get("titulo"),
                                "url_xml": item.get("url_xml"),
                                "url_html": item.get("url_html"),
                                "url_pdf": (item.get("url_pdf") or {}).get("texto"),
                                "department": dep.get("nombre"),
                                "boe_issue": issue,
                                "publication_date": day.isoformat(),
                            })
        return found

    def _walk_days(self, start: date, end: date) -> Generator[dict, None, None]:
        """Yield agreement items from `end` back to `start`, newest first.

        Newest-first means a sample or an interrupted crawl carries the most
        recent agreements rather than 1990's, and an incremental run reaches
        the new material immediately.
        """
        day = end
        total = 0
        while day >= start:
            try:
                items = self._day_items(day)
            except Exception as exc:
                logger.warning("Summary fetch failed for %s: %s", day, exc)
                self.record_coverage_gap(
                    unit=day.isoformat(), reason="summary_fetch_failed", error=str(exc)
                )
                day -= timedelta(days=1)
                continue
            for item in items:
                if item.get("identifier") and item.get("url_xml"):
                    total += 1
                    yield item
            if day.day == 1:
                logger.info("Indexed back to %s (%d agreements so far)", day, total)
            day -= timedelta(days=1)
        logger.info("Index complete: %d agreement publications from %s to %s",
                    total, start, end)

    def fetch_all(self) -> Generator[dict, None, None]:
        start = date.fromisoformat(self.start_date)
        yield from self._walk_days(start, date.today())

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Re-walk only the summaries published on/after `since`.

        `since` is a crawl timestamp, and BOE publication date is exactly when
        a registration became available to us, so it is the correct
        availability comparator. A few days of overlap are re-read so a
        late-arriving correction is not missed; the upsert dedups them.
        """
        since_str = as_date_str(since)
        try:
            start = date.fromisoformat(since_str[:10]) - timedelta(days=7)
        except ValueError:
            start = date.fromisoformat(self.start_date)
        floor = date.fromisoformat(self.start_date)
        if start < floor:
            start = floor
        logger.info("Incremental update: BOE summaries from %s", start)
        yield from self._walk_days(start, date.today())

    # ── Full text ─────────────────────────────────────────────────────

    def _fetch_document(self, doc_id: str) -> Optional[ET.Element]:
        resp = self.client.get(XML_URL.format(doc_id=doc_id),
                               rate_limiter=self.rate_limiter)
        resp.raise_for_status()
        body = resp.content
        try:
            return ET.fromstring(body)
        except ET.ParseError as exc:
            logger.warning("XML parse failed for %s: %s", doc_id, exc)
            return None

    @staticmethod
    def _meta(root: ET.Element, tag: str) -> Optional[str]:
        node = root.find("./metadatos/" + tag)
        if node is None:
            return None
        value = (node.text or "").strip()
        return value or None

    # ── Classification helpers ────────────────────────────────────────

    @staticmethod
    def _convenio_code(text: str) -> Optional[str]:
        match = CODE_RE.search(text)
        return match.group(1) if match else None

    @staticmethod
    def _territorial_scope(code: Optional[str]) -> Dict[str, Optional[str]]:
        """First two digits of a REGCON code carry the territorial scope."""
        if not code or len(code) < 2:
            return {"territorial_scope": None, "province": None}
        prefix = code[:2]
        if prefix in ("99", "90"):
            return {"territorial_scope": "estatal", "province": None}
        if prefix in PROVINCES:
            return {"territorial_scope": "provincial", "province": PROVINCES[prefix]}
        return {"territorial_scope": None, "province": None}

    @staticmethod
    def _scope_type(title: str, code: Optional[str]) -> str:
        """sector | empresa | unknown.

        The code prefix is authoritative where REGCON uses the state pair
        (99 = state sector agreement, 90 = state company agreement); otherwise
        fall back to the wording of the title.
        """
        if code:
            if code.startswith("99"):
                return "sector"
            if code.startswith("90"):
                return "empresa"
        if COMPANY_RE.search(title):
            return "empresa"
        if SECTOR_RE.search(title):
            return "sector"
        return "unknown"

    @staticmethod
    def _instrument(title: str) -> str:
        lowered = title.lower()
        for name, pattern in INSTRUMENT_PATTERNS:
            if re.search(pattern, lowered):
                return name
        return "agreement"

    # ── Normalization ─────────────────────────────────────────────────

    def normalize(self, raw: dict) -> Optional[dict]:
        doc_id = raw["identifier"]
        root = self._fetch_document(doc_id)
        if root is None:
            return None

        texto = root.find("./texto")
        if texto is None:
            logger.debug("No <texto> element for %s", doc_id)
            return None
        fragment = ET.tostring(texto, encoding="unicode", method="xml")
        text = _clean_text(fragment)
        if len(text) < 400:
            # Every real registration resolution carries the agreement body;
            # anything this short is a stub and must not be stored as content.
            logger.debug("Text too short for %s (%d chars)", doc_id, len(text))
            return None

        title = self._meta(root, "titulo") or raw.get("title") or ""
        code = self._convenio_code(text) or self._convenio_code(title)
        scope = self._territorial_scope(code)

        # Subject descriptors and validity notes live under <analisis>, not
        # <metadatos>. `materias` carries the sector and geography tags the
        # consumer needs; `notas` carries the vigencia/prorroga statements
        # that let an agreement be tracked over time.
        materias = [
            (node.text or "").strip()
            for node in root.findall("./analisis/materias/materia")
            if (node.text or "").strip()
        ]
        validity_notes = [
            (node.text or "").strip()
            for node in root.findall("./analisis/notas/nota")
            if (node.text or "").strip()
        ]

        record = {
            "_id": doc_id,
            "_source": "ES/REGCON",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "identifier": doc_id,
            "title": title,
            "text": text,
            "date": _iso_date(self._meta(root, "fecha_disposicion"))
            or raw.get("publication_date"),
            "publication_date": _iso_date(self._meta(root, "fecha_publicacion"))
            or raw.get("publication_date"),
            "url": raw.get("url_html") or f"{BASE_URL}/diario_boe/txt.php?id={doc_id}",
            "url_pdf": raw.get("url_pdf"),
            "url_xml": raw.get("url_xml"),
            "convenio_code": code,
            "territorial_scope": scope["territorial_scope"],
            "scope_type": self._scope_type(title, code),
            "instrument_type": self._instrument(title),
            "departamento": self._meta(root, "departamento") or raw.get("department"),
            "rango": self._meta(root, "rango"),
            "boe_issue": raw.get("boe_issue"),
            "publication_reference": doc_id,
            "materias": materias,
            "validity_notes": validity_notes,
            "language": "es",
        }
        if scope["province"]:
            record["province"] = scope["province"]
        return record

    # ── Diagnostics ───────────────────────────────────────────────────

    def test_connection(self):
        print("Testing ES/REGCON (BOE convenios colectivos)...")
        print("\n1. Daily summary index...")
        probe = date(2025, 3, 5)
        items = self._day_items(probe)
        print(f"   {probe}: {len(items)} agreement publications")
        for item in items[:3]:
            print(f"   - {item['identifier']}: {item['title'][:80]}...")

        print("\n2. Full-text fetch...")
        if items:
            record = self.normalize(items[0])
            if record:
                print(f"   {record['_id']}: {len(record['text'])} chars")
                print(f"   code={record['convenio_code']} "
                      f"scope={record['territorial_scope']}/{record['scope_type']} "
                      f"instrument={record['instrument_type']}")
                print(f"   {record['text'][:200]}...")
            else:
                print("   ERROR: normalize returned None")
        print("\nTest complete!")


def main():
    scraper = REGCONScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] "
              "[--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        sample_size = int(sys.argv[sys.argv.index("--sample-size") + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: "
                  f"{stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2, default=str))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated")
        print(json.dumps(stats, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI dispatches
    # on the literal command name, so alias it (issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
