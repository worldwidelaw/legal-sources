#!/usr/bin/env python3
"""
EU/ConsolidatedLegislation — the consolidated, in-force text of EU law
(CELEX **sector 0**).

The EUR-Lex taxonomy reserves **sector 0** for *consolidated* texts: the
official, editorially integrated versions of EU legal acts in which every
subsequent amendment has been woven back into the base act, so the reader sees
the law *as it currently stands* (or as it will stand on a future effective
date). A sector-0 CELEX has the form ``0{YYYY}{TYPE}{NNNN}-{YYYYMMDD}`` — the
base act's original year/type/number followed by the date of the consolidated
version, e.g.

    02000L0060-20260510   Water Framework Directive, consolidated to 10.05.2026
    02008R1333-20260818   Food-additives Regulation, consolidated to 18.08.2026
    02015R0035-20270130   Solvency II Delegated Regulation, consolidated

Each base act is re-consolidated every time it is amended, so CELLAR holds many
point-in-time consolidations per act (the WFD alone has nine). This source keeps
the **single most recent consolidation per base act** — i.e. the current
in-force integrated text — which is by far the most useful representation of
"what the law says today" and avoids ingesting a dozen near-identical historical
snapshots of the same act. Across ~11,200 base acts that yields ~11,200 documents.

This is a distinct, additive corpus. EU/EUR-Lex covers sector-3 secondary
legislation in its *as-adopted* form (the base act with none of its later
amendments applied); this source is the *consolidated* counterpart — the same
acts as amended and integrated. The consolidated text is what practitioners
actually read, and it is not captured by any as-adopted sibling. Directly closes
the gap reported in issue #1187 (consolidated Water Framework Directive
amendments missing).

Data flow
---------
1. Enumerate every sector-0 act via the public CELLAR SPARQL endpoint. The full
   corpus (~32,800 consolidations) exceeds the endpoint's ~10 000-row OFFSET
   ceiling, so enumeration is scoped **per original year** (CELEX matches
   ``^0{YYYY}``); the busiest single year is ~1,400 rows, comfortably under the
   ceiling. Within each year the consolidations are reduced to the latest one
   per base act (by the ``-{YYYYMMDD}`` suffix — all consolidations of a given
   base share that base's year, so the reduction is complete within one year
   page set).
2. Fetch the full consolidated text from CELLAR via HTTP content negotiation:
       GET http://publications.europa.eu/resource/celex/{CELEX}
       Accept: application/xhtml+xml
       Accept-Language: en
   This serves the consolidated Formex/xHTML body and bypasses the
   eur-lex.europa.eu AWS-WAF that 202-challenges datacenter IPs, so it is
   fleet-safe. The sector-0 CELEX carries a ``-`` and no problematic characters,
   but it is URL-encoded defensively as in the sibling scrapers.
3. Normalize to the standard schema (legislation).

Same CELLAR recipe as EU/Treaties (sector 1) and EU/ComplementaryLegislation
(sector 4), narrowed to the sector-0 descriptor with per-year scoping and
latest-per-base reduction.
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

# Consolidated acts carry base-act years from the earliest EU law (1950s) up to
# the present; future-effective consolidations are dated ahead but their base
# year is still the original act's year. Scan a generous inclusive range.
YEAR_MIN = 1951
YEAR_MAX = 2030

# Leading token in the Formex/xHTML export is the source filename — strip it.
_FILENAME_PREFIX_RE = re.compile(r"^\s*\S+\.(?:docx|xml|fmx\.xml|xhtml[^\s]*)\s+", re.IGNORECASE)

SPARQL_QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?celex ?date ?title WHERE {{
  ?work cdm:resource_legal_id_celex ?celex .
  FILTER(REGEX(STR(?celex), "^0{year}"))
  OPTIONAL {{ ?work cdm:work_date_document ?date . }}
  OPTIONAL {{
    ?exp cdm:expression_belongs_to_work ?work ;
         cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> ;
         cdm:expression_title ?title .
  }}
}}
ORDER BY ?celex
LIMIT {limit} OFFSET {offset}
"""

# Date-filtered variant used by fetch_updates — returns only the (small) set of
# consolidations dated on/after the cutoff, so no year loop is needed.
SPARQL_UPDATES = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?celex ?date ?title WHERE {{
  ?work cdm:resource_legal_id_celex ?celex .
  FILTER(REGEX(STR(?celex), "^0[0-9]{{4}}"))
  ?work cdm:work_date_document ?date .
  FILTER(?date >= "{cutoff}"^^<http://www.w3.org/2001/XMLSchema#date>)
  OPTIONAL {{
    ?exp cdm:expression_belongs_to_work ?work ;
         cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> ;
         cdm:expression_title ?title .
  }}
}}
ORDER BY DESC(?date) ?celex
LIMIT {limit} OFFSET {offset}
"""

_MIN_TEXT = 400

# Consolidation suffix: 02000L0060-20260510 -> base "02000L0060", date "20260510"
_CELEX_SPLIT_RE = re.compile(r"^(?P<base>0[0-9]{4}[A-Z]+[0-9()]+)-(?P<cdate>\d{8})")


def _base_and_cdate(celex: str):
    m = _CELEX_SPLIT_RE.match(celex)
    if not m:
        # No consolidation-date suffix — treat the whole CELEX as its own base.
        return celex, "00000000"
    return m.group("base"), m.group("cdate")


def _strip_html(raw_html: str) -> str:
    """Strip tags/entities from an xHTML document, returning clean text."""
    text = re.sub(r"<script[\s\S]*?</script>", " ", raw_html)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = _FILENAME_PREFIX_RE.sub("", text)
    text = re.sub(r"[ \t  ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class ConsolidatedLegislationScraper(BaseScraper):
    """Scraper for consolidated EU law (CELEX sector 0) via CELLAR."""

    PAGE_SIZE = 300

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

    def _sparql(self, query: str) -> list:
        r = self._get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "application/sparql-results+json"},
            timeout=180,
        )
        if r is None or r.status_code != 200:
            logger.warning("SPARQL query failed (%s)", getattr(r, "status_code", "?"))
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

    def _year_latest_per_base(self, year: int) -> list:
        """All sector-0 acts for one original year, reduced to the latest
        consolidation per base act."""
        best = {}
        offset = 0
        while True:
            page = self._sparql(SPARQL_QUERY.format(year=year, limit=self.PAGE_SIZE, offset=offset))
            if not page:
                break
            for meta in page:
                base, cdate = _base_and_cdate(meta["celex"])
                cur = best.get(base)
                if cur is None or cdate > cur["_cdate"]:
                    meta = dict(meta)
                    meta["_cdate"] = cdate
                    best[base] = meta
            if len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE
        return list(best.values())

    def _fetch_text(self, celex: str) -> str:
        r = self._get(
            CELLAR_CELEX.format(celex=quote(celex, safe="")),
            headers={"Accept": "application/xhtml+xml", "Accept-Language": "en"},
            timeout=180,
        )
        if r is None or r.status_code != 200:
            return ""
        return _strip_html(r.text)

    # ---- BaseScraper contract --------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW act dicts (with full consolidated text) — the latest
        consolidation of every sector-0 base act."""
        for year in range(YEAR_MAX, YEAR_MIN - 1, -1):
            acts = self._year_latest_per_base(year)
            if not acts:
                continue
            logger.info("Year %s: %d base acts", year, len(acts))
            for meta in acts:
                celex = meta["celex"]
                text = self._fetch_text(celex)
                if len(text) < _MIN_TEXT:
                    logger.debug("Skip %s: short (len=%d)", celex, len(text))
                    continue
                time.sleep(1)  # be polite to CELLAR
                yield {
                    "celex": celex,
                    "date": meta.get("date"),
                    "title": meta.get("title"),
                    "text": text,
                }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield acts whose consolidation date is on/after ``since`` — i.e. acts
        that were (re)consolidated recently."""
        cutoff = since.date().isoformat() if isinstance(since, datetime) else str(since)
        best = {}
        offset = 0
        while True:
            page = self._sparql(SPARQL_UPDATES.format(cutoff=cutoff, limit=self.PAGE_SIZE, offset=offset))
            if not page:
                break
            for meta in page:
                base, cdate = _base_and_cdate(meta["celex"])
                cur = best.get(base)
                if cur is None or cdate > cur["_cdate"]:
                    meta = dict(meta)
                    meta["_cdate"] = cdate
                    best[base] = meta
            if len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE
        for meta in best.values():
            celex = meta["celex"]
            text = self._fetch_text(celex)
            if len(text) < _MIN_TEXT:
                continue
            time.sleep(1)
            yield {
                "celex": celex,
                "date": meta.get("date"),
                "title": meta.get("title"),
                "text": text,
            }

    def normalize(self, raw: dict) -> dict:
        celex = raw["celex"]
        title = (raw.get("title") or "").strip()
        if not title:
            title = f"EU consolidated act {celex}"
        date = raw.get("date")
        if date and not re.match(r"^\d{4}-\d{2}-\d{2}", str(date)):
            date = None
        return {
            "_id": celex,
            "_source": "EU/ConsolidatedLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "celex": celex,
            "title": title,
            "text": raw["text"],
            "date": date,  # date of the consolidated version
            "url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{quote(celex, safe='')}",
        }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scraper = ConsolidatedLegislationScraper()

    command = sys.argv[1] if len(sys.argv) > 1 else "bootstrap"
    sample_mode = "--sample" in sys.argv

    if command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")
    elif command == "test":
        acts = scraper._year_latest_per_base(2000)
        print(f"Year 2000: {len(acts)} latest-per-base consolidated acts")
        if acts:
            c = acts[0]["celex"]
            txt = scraper._fetch_text(c)
            print(f"  {c}: {(acts[0].get('title') or '')[:70]}")
            print(f"  full text length: {len(txt)} chars")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
