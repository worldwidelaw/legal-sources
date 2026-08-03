#!/usr/bin/env python3
"""
EU/AG-Opinions — Opinions of the Advocates General of the Court of Justice
of the European Union (CJEU).

Before the Court of Justice delivers a judgment in most cases, an Advocate
General delivers a reasoned, independent "Opinion" proposing a legal solution.
These Opinions are substantial standalone legal analyses (often the fullest
statement of the reasoning behind EU law) and are published, with full text, in
the EU case-law collection. In EUR-Lex's own taxonomy Advocate General Opinions
belong to the *case-law* sector alongside judgments and orders, addressable by a
CELEX number of the form ``6{YYYY}CC{NNNN}`` (sector 6 = EU case-law;
"CC" = *conclusions* / Opinion of an Advocate General; the 4-digit year is the
case-registration year).

Data flow
---------
1. Enumerate every AG Opinion via the public CELLAR SPARQL endpoint
   (CELEX matching ``^6{YYYY}CC``), retrieving CELEX + document date + English
   expression title + ECLI. Enumeration is scoped **one case-year at a time**
   (1954→present) because the full corpus (~11,000 Opinions) exceeds the
   CELLAR SPARQL 10,000-row OFFSET ceiling; each year is a few hundred rows.
2. Fetch the full text of each Opinion from CELLAR via HTTP content negotiation:
       GET http://publications.europa.eu/resource/celex/{CELEX}
       Accept: application/xhtml+xml      (modern Opinions)
       Accept: text/html                  (fallback for older Opinions)
       Accept-Language: en
   This serves the Formex/xHTML (or legacy HTML) body and, crucially, bypasses
   the eur-lex.europa.eu AWS-WAF that 202-challenges datacenter IPs.
3. Normalize to the standard schema (case_law).

This is the sibling of EU/CURIA (which covers CJEU/General-Court *judgments*,
CELEX type ``CJ``/``TJ``) and of EU/EESC-Opinions / EU/CoR-Opinions (which use
the same CELLAR content-negotiation recipe for OJ C-series advisory opinions).
"""

import sys
import html
import re
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logger = logging.getLogger("legal-data-hunter")

SPARQL_ENDPOINT = "http://publications.europa.eu/webapi/rdf/sparql"
CELLAR_CELEX = "http://publications.europa.eu/resource/celex/{celex}"

# First year the CJEU existed; AG Opinions begin in the early 1960s but we scan
# from 1954 so no year is ever missed (empty years simply yield nothing).
FIRST_YEAR = 1954

SPARQL_QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT DISTINCT ?celex ?date ?title ?ecli WHERE {{
  ?work cdm:resource_legal_id_celex ?celex .
  FILTER(REGEX(STR(?celex), "^6{year}CC[0-9]+$"))
  OPTIONAL {{ ?work cdm:work_date_document ?date . }}
  OPTIONAL {{ ?work cdm:case_law_ecli ?ecli . }}
  OPTIONAL {{
    ?exp cdm:expression_belongs_to_work ?work ;
         cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> ;
         cdm:expression_title ?title .
  }}
}}
ORDER BY ?celex
LIMIT {limit} OFFSET {offset}
"""

# Legacy EUR-Lex HTML pages begin with an inline CSS @import and a boilerplate
# "Important legal notice" banner; strip both so the body starts at the title.
_CSS_IMPORT_RE = re.compile(r"^\s*@import[^;]+;\s*", re.IGNORECASE)
_LEGAL_NOTICE_RE = re.compile(r"^\s*Important legal notice\s*\|?\s*", re.IGNORECASE)
# Leading token in a Formex export is the source filename, e.g.
# "C_202603243EN.000101.fmx.xml" — strip it from the body text.
_FILENAME_PREFIX_RE = re.compile(r"^\s*\S+\.(?:xml|fmx\.xml)\s+", re.IGNORECASE)


def _strip_html(raw_html: str) -> str:
    """Strip tags/entities from an xHTML/HTML document, returning clean text."""
    text = re.sub(r"<script[\s\S]*?</script>", " ", raw_html)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = _CSS_IMPORT_RE.sub("", text)
    text = _FILENAME_PREFIX_RE.sub("", text)
    text = _LEGAL_NOTICE_RE.sub("", text)
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class AGOpinionsScraper(BaseScraper):
    """Scraper for CJEU Advocate General Opinions via CELLAR."""

    PAGE_SIZE = 500

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

    def _sparql_year_page(self, year: int, offset: int) -> list:
        query = SPARQL_QUERY.format(year=year, limit=self.PAGE_SIZE, offset=offset)
        r = self._get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "application/sparql-results+json"},
            timeout=120,
        )
        if r is None or r.status_code != 200:
            logger.warning("SPARQL year=%s offset=%s failed (%s)", year, offset,
                           getattr(r, "status_code", "?"))
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
                "ecli": b.get("ecli", {}).get("value"),
            })
        return out

    def _fetch_text(self, celex: str) -> str:
        """Fetch full text: xHTML first (modern), fall back to legacy HTML."""
        for accept in ("application/xhtml+xml", "text/html"):
            r = self._get(
                CELLAR_CELEX.format(celex=celex),
                headers={"Accept": accept, "Accept-Language": "en"},
                timeout=90,
            )
            if r is not None and r.status_code == 200 and r.text:
                text = _strip_html(r.text)
                if len(text) >= 200:
                    return text
        return ""

    def _iter_meta(self) -> Generator[dict, None, None]:
        """Yield opinion metadata dicts, oldest case-year first, deduped."""
        seen = set()
        this_year = datetime.now(timezone.utc).year
        for year in range(FIRST_YEAR, this_year + 1):
            offset = 0
            while True:
                page = self._sparql_year_page(year, offset)
                if not page:
                    break
                for meta in page:
                    if meta["celex"] in seen:
                        continue
                    seen.add(meta["celex"])
                    yield meta
                if len(page) < self.PAGE_SIZE:
                    break
                offset += self.PAGE_SIZE

    # ---- BaseScraper contract --------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW opinion dicts (with full text) for every AG Opinion."""
        for meta in self._iter_meta():
            text = self._fetch_text(meta["celex"])
            if len(text) < 200:
                logger.debug("Skip %s: text too short", meta["celex"])
                continue
            time.sleep(1)  # be polite to CELLAR
            yield {**meta, "text": text}

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield opinions with a document date on/after ``since``."""
        cutoff = since.date().isoformat() if isinstance(since, datetime) else str(since)
        for meta in self._iter_meta():
            d = meta.get("date")
            if d and str(d) < cutoff:
                continue
            text = self._fetch_text(meta["celex"])
            if len(text) < 200:
                continue
            time.sleep(1)
            yield {**meta, "text": text}

    def normalize(self, raw: dict) -> dict:
        celex = raw["celex"]
        title = (raw.get("title") or "").strip()
        if not title:
            title = f"Opinion of the Advocate General ({celex})"
        date = raw.get("date")
        if date and not re.match(r"^\d{4}-\d{2}-\d{2}", str(date)):
            date = None
        return {
            "_id": celex,
            "_source": "EU/AG-Opinions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "celex": celex,
            "ecli": raw.get("ecli"),
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}",
        }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scraper = AGOpinionsScraper()

    command = sys.argv[1] if len(sys.argv) > 1 else "bootstrap"
    sample_mode = "--sample" in sys.argv

    if command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")
    elif command == "test":
        page = scraper._sparql_year_page(2018, 0)
        print(f"SPARQL returned {len(page)} opinions for 2018 page 1")
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
