#!/usr/bin/env python3
"""
EU/Treaties — the primary law of the European Union (CELEX **sector 1**).

Sector 1 of the EUR-Lex taxonomy holds the EU's **primary law**: the founding
Treaties and their consolidated versions (the Treaty on European Union and the
Treaty on the Functioning of the European Union), the Charter of Fundamental
Rights, the Euratom Treaty, the successive amending Treaties (Maastricht,
Amsterdam, Nice, Lisbon), the Accession Treaties by which new Member States
joined, the Protocols and Declarations annexed to the Treaties, and the
withdrawal agreement (the UK Withdrawal Agreement). Their authentic text is
published in the Official Journal and registered in the EU Publications Office
repository (CELLAR) with a CELEX number in **sector 1** — ``1{YYYY}{DESC}...``
(e.g. ``12016E/TXT`` — consolidated TFEU 2016; ``12016M/TXT`` — consolidated
TEU; ``12016A/TXT`` — Charter of Fundamental Rights; ``12020W/TXT`` — UK
Withdrawal Agreement; ``12007L/TXT`` — Treaty of Lisbon).

This is the EU's constitutional layer and a distinct, additive corpus: EU/EUR-Lex
covers only enacted secondary legislation (sector 3, REG/DIR/DEC), and the
sector-2/4 siblings cover external agreements and complementary legislation.

Data flow
---------
1. Enumerate every sector-1 act via the public CELLAR SPARQL endpoint (CELEX
   matching ``^1[0-9]{4}``), retrieving CELEX + document date + English
   expression title. Paged with LIMIT/OFFSET (the sector-1 corpus is ~9,800
   rows, under the ~10 000-row SPARQL OFFSET ceiling, so no year-scoping is
   needed).
2. Fetch the full text of each act from CELLAR via HTTP content negotiation:
       GET http://publications.europa.eu/resource/celex/{CELEX}
       Accept: application/xhtml+xml
       Accept-Language: en
   Sector-1 CELEX numbers carry ``/`` and parenthesised sub-part markers (e.g.
   ``12020W/TXT``, ``12019W/TXT(02)``), so the CELEX is URL-encoded before it is
   appended to the resource path. This serves the OJ Formex/xHTML body and
   bypasses the eur-lex.europa.eu AWS-WAF that 202-challenges datacenter IPs, so
   it is fleet-safe.
3. Normalize to the standard schema (legislation).

Same CELLAR recipe as EU/ComplementaryLegislation and EU/InternationalAgreements,
narrowed to the sector-1 descriptor, with URL-encoding for the slashed CELEX (as
in EU/Council-Positions). Corrigenda (``…R(0N)``) and manifestations with no
xHTML export 404 → skipped by the minimum-length guard.
"""

import sys
import html
import re
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logger = logging.getLogger("legal-data-hunter")

SPARQL_ENDPOINT = "http://publications.europa.eu/webapi/rdf/sparql"
CELLAR_CELEX = "http://publications.europa.eu/resource/celex/{celex}"

# CELEX pattern for sector-1 primary law: 1{YYYY}{DESC}...
CELEX_RE = r"^1[0-9]{4}"

# Leading token in the Formex/xHTML export is the source filename — strip it.
_FILENAME_PREFIX_RE = re.compile(r"^\s*\S+\.(?:docx|xml|fmx\.xml|xhtml[^\s]*)\s+", re.IGNORECASE)

SPARQL_QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?celex ?date ?title WHERE {{
  ?work cdm:resource_legal_id_celex ?celex .
  FILTER(REGEX(STR(?celex), "{pattern}"))
  OPTIONAL {{ ?work cdm:work_date_document ?date . }}
  OPTIONAL {{
    ?exp cdm:expression_belongs_to_work ?work ;
         cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> ;
         cdm:expression_title ?title .
  }}
}}
ORDER BY DESC(?date) ?celex
LIMIT {limit} OFFSET {offset}
"""


# Withdrawn/blank OJ acts carry only the ~245-char masthead boilerplate plus a
# "has been withdrawn and therefore left blank" marker — no substantive content.
_BLANK_MARKER = "withdrawn and therefore left blank"
_MIN_TEXT = 400


def _is_useful(text: str) -> bool:
    return len(text) >= _MIN_TEXT and _BLANK_MARKER not in text.lower()


def _strip_html(raw_html: str) -> str:
    """Strip tags/entities from an xHTML document, returning clean text."""
    text = re.sub(r"<script[\s\S]*?</script>", " ", raw_html)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = _FILENAME_PREFIX_RE.sub("", text)
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class TreatiesScraper(BaseScraper):
    """Scraper for EU primary law (CELEX sector 1) via CELLAR."""

    PAGE_SIZE = 200

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (+https://github.com/ZachLaik) legal-open-data",
        })

    # ---- HTTP helpers -----------------------------------------------------

    def _get(self, url, *, headers=None, params=None, max_retries=4, timeout=60):
        last = None
        for attempt in range(max_retries):
            try:
                r = self.session.get(url, headers=headers, params=params,
                                     timeout=timeout, allow_redirects=True)
                if r.status_code == 200:
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(2 ** attempt, 30))
                    continue
                return r  # 404/303-resolved-error etc. — let caller decide
            except requests.RequestException as e:
                last = e
                time.sleep(min(2 ** attempt, 30))
        if last:
            raise last
        return None

    def _sparql_page(self, offset: int) -> list:
        query = SPARQL_QUERY.format(pattern=CELEX_RE, limit=self.PAGE_SIZE, offset=offset)
        r = self._get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "application/sparql-results+json"},
            timeout=120,
        )
        if r is None or r.status_code != 200:
            logger.warning("SPARQL page offset=%s failed (%s)", offset, getattr(r, "status_code", "?"))
            return []
        bindings = r.json().get("results", {}).get("bindings", [])
        out = []
        for b in bindings:
            celex = b.get("celex", {}).get("value")
            if not celex:
                continue
            out.append({
                "celex": celex,
                "date": b.get("date", {}).get("value"),
                "title": b.get("title", {}).get("value"),
            })
        return out

    def _fetch_text(self, celex: str) -> str:
        # Sector-1 CELEX carry "/" and parenthesised sub-parts — URL-encode.
        r = self._get(
            CELLAR_CELEX.format(celex=quote(celex, safe="")),
            headers={"Accept": "application/xhtml+xml", "Accept-Language": "en"},
            timeout=120,
        )
        if r is None or r.status_code != 200:
            return ""
        return _strip_html(r.text)

    # ---- BaseScraper contract --------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW act dicts (with full text) for every sector-1 act."""
        offset = 0
        seen = set()
        while True:
            page = self._sparql_page(offset)
            if not page:
                break
            for meta in page:
                celex = meta["celex"]
                if celex in seen:
                    continue
                seen.add(celex)
                text = self._fetch_text(celex)
                if not _is_useful(text):
                    logger.debug("Skip %s: not useful (len=%d)", celex, len(text))
                    continue
                time.sleep(1)  # be polite to CELLAR
                yield {
                    "celex": celex,
                    "date": meta.get("date"),
                    "title": meta.get("title"),
                    "text": text,
                }
            if len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield acts with a document date on/after ``since`` (newest-first)."""
        cutoff = since.date().isoformat() if isinstance(since, datetime) else str(since)
        offset = 0
        seen = set()
        while True:
            page = self._sparql_page(offset)
            if not page:
                break
            stop = False
            for meta in page:
                d = meta.get("date")
                if d and d < cutoff:
                    stop = True
                    break
                celex = meta["celex"]
                if celex in seen:
                    continue
                seen.add(celex)
                text = self._fetch_text(celex)
                if not _is_useful(text):
                    continue
                time.sleep(1)
                yield {
                    "celex": celex,
                    "date": meta.get("date"),
                    "title": meta.get("title"),
                    "text": text,
                }
            if stop or len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

    def normalize(self, raw: dict) -> dict:
        celex = raw["celex"]
        title = (raw.get("title") or "").strip()
        if not title:
            title = f"EU primary-law act {celex}"
        date = raw.get("date")
        if date and not re.match(r"^\d{4}-\d{2}-\d{2}", str(date)):
            date = None
        return {
            "_id": celex,
            "_source": "EU/Treaties",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "celex": celex,
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{quote(celex, safe='')}",
        }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scraper = TreatiesScraper()

    command = sys.argv[1] if len(sys.argv) > 1 else "bootstrap"
    sample_mode = "--sample" in sys.argv

    if command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")
    elif command == "test":
        page = scraper._sparql_page(0)
        print(f"SPARQL returned {len(page)} sector-1 acts on page 1")
        if page:
            c = page[0]["celex"]
            txt = scraper._fetch_text(c)
            print(f"  {c}: {(page[0].get('title') or '')[:70]}")
            print(f"  full text length: {len(txt)} chars")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
