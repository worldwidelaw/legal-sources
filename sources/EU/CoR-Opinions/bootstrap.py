#!/usr/bin/env python3
"""
EU/CoR-Opinions — European Committee of the Regions opinions.

The Committee of the Regions (CoR) is the EU's assembly of regional and local
representatives. It adopts advisory opinions on EU legislative proposals
(Commission/Council/Parliament referrals + own-initiative and outlook opinions).
Each adopted opinion is published in the Official Journal C series and is
therefore stored, with full text, in the EU Publications Office repository
(CELLAR), addressable by a CELEX number of the form ``5{YYYY}AR{NNNN}``
(sector 5 = EESC/CoR acts; "AR" = *avis CdR* / CoR opinion).

Data flow
---------
1. Enumerate every CoR opinion via the public CELLAR SPARQL endpoint
   (CELEX matching ``^5[0-9]{4}AR``), retrieving CELEX + document date +
   English expression title. Paged with LIMIT/OFFSET.
2. Fetch the full text of each opinion from CELLAR via HTTP content
   negotiation:
       GET http://publications.europa.eu/resource/celex/{CELEX}
       Accept: application/xhtml+xml
       Accept-Language: en
   This serves the OJ Formex/xHTML body and, crucially, bypasses the
   eur-lex.europa.eu AWS-WAF that 202-challenges datacenter IPs.
3. Normalize to the standard schema (doctrine).

Why not the CoR CDR eGovernment / cor.europa.eu listing? The public listing is
a JavaScript SPA. CELLAR is the authoritative, anonymous, datacenter-friendly
full-text source for the same corpus (identical pattern to EU/EESC-Opinions,
which uses the sibling ``AE`` CELEX code).
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

# CELEX pattern for CoR opinions: 5{YYYY}AR{NNNN}
CELEX_RE = r"^5[0-9]{4}AR"

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

# Leading token in the Formex export is the source filename, e.g.
# "C_202603243EN.000101.fmx.xml" — strip it from the body text.
_FILENAME_PREFIX_RE = re.compile(r"^\s*\S+\.(?:xml|fmx\.xml)\s+", re.IGNORECASE)


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


class CoROpinionsScraper(BaseScraper):
    """Scraper for CoR opinions via CELLAR (Publications Office)."""

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
        r = self._get(
            CELLAR_CELEX.format(celex=celex),
            headers={"Accept": "application/xhtml+xml", "Accept-Language": "en"},
            timeout=90,
        )
        if r is None or r.status_code != 200:
            return ""
        return _strip_html(r.text)

    # ---- BaseScraper contract --------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW opinion dicts (with full text) for every CoR opinion."""
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
                if len(text) < 200:
                    logger.debug("Skip %s: text too short (%d)", celex, len(text))
                    continue
                time.sleep(1)  # be polite to CELLAR
                yield {
                    "celex": celex,
                    "date": meta.get("date"),
                    "title": meta.get("title"),
                    "text": text,
                }
            offset += self.PAGE_SIZE

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield opinions with a document date on/after ``since``.

        Opinions are enumerated newest-first, so we can stop once we cross the
        cutoff date.
        """
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
                if len(text) < 200:
                    continue
                time.sleep(1)
                yield {
                    "celex": celex,
                    "date": meta.get("date"),
                    "title": meta.get("title"),
                    "text": text,
                }
            if stop:
                break
            offset += self.PAGE_SIZE

    def normalize(self, raw: dict) -> dict:
        celex = raw["celex"]
        title = (raw.get("title") or "").strip()
        if not title:
            title = f"CoR Opinion {celex}"
        date = raw.get("date")
        # SPARQL dates are already ISO (YYYY-MM-DD); pass through / null.
        if date and not re.match(r"^\d{4}-\d{2}-\d{2}", str(date)):
            date = None
        return {
            "_id": celex,
            "_source": "EU/CoR-Opinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "celex": celex,
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}",
        }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scraper = CoROpinionsScraper()

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
        print(f"SPARQL returned {len(page)} opinions on page 1")
        if page:
            c = page[0]["celex"]
            txt = scraper._fetch_text(c)
            print(f"  {c}: {page[0].get('title', '')[:70]}")
            print(f"  full text length: {len(txt)} chars")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
