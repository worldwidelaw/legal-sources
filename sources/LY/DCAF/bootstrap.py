#!/usr/bin/env python3
"""
LY/DCAF - Libya DCAF Security Sector Legal Database

Libyan legal texts from DCAF (Geneva Centre for Security Sector Governance)
via the site's WordPress REST API.

Content types: Constitutional Law, Decrees, Laws, Resolutions, Judicial
Decisions, Bylaws, Declarations, International Agreements.

The site is bilingual and WordPress keeps each language as its own post, on
its own endpoint: /ar/wp-json/... (2,175 Arabic originals) and /wp-json/...
(2,155 English posts, of which 786 carry a real translation and the rest are
the "ONLY AVAILABLE IN ARABIC" placeholder). Both are collected; the
placeholders are dropped.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full extraction
  python bootstrap.py update               # Incremental (modified_after)
  python bootstrap.py test                 # Test connectivity
"""

import html
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LY.DCAF")

SITE = "https://security-legislation.ly"

# WordPress keeps the two languages as separate posts on separate endpoints.
LANG_ENDPOINTS = {
    "ar": f"{SITE}/ar/wp-json/wp/v2/latest-laws",
    "en": f"{SITE}/wp-json/wp/v2/latest-laws",
}

TAXONOMY_ROUTES = {
    "text_type": "text-type-categories",
    "status": "status-categories",
    "index": "database-index-categories",
    "institution": "institution-categories",
}

# WPML gives each taxonomy term a separate id per language, so an Arabic post
# references term ids (4218 "قانون") that simply do not exist in the English
# taxonomy listing (4221 "Law") — reading only the English endpoint left
# text_type/status/institution blank on every one of the 2,175 Arabic records.
# The ids are globally unique, so both listings merge into one lookup.
TAXONOMY_PREFIXES = ("", "/ar")

TAXONOMY_FIELDS = {
    "text_type": "text-type-categories",
    "status": "status-categories",
    "index": "database-index-categories",
    "institution": "institution-categories",
}

# WPML picks the language from the URL prefix, but it also honours
# Accept-Language — an unset header lets it redirect /wp-json to /ar/wp-json and
# serve the Arabic corpus under the English walk. State the language we want.
ACCEPT_LANGUAGE = {"ar": "ar,en;q=0.5", "en": "en-US,en;q=0.9,ar;q=0.5"}

# An English post with no translation yet carries this stub instead of the law.
PLACEHOLDER = "ONLY AVAILABLE IN ARABIC"

MIN_TEXT_CHARS = 50

HEADERS = {"User-Agent": "LegalDataHunter/1.0 (research)"}

# 400 is the end-of-pages signal and must never be retried; these are transient.
RETRY_STATUS = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 4


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<p[^>]*>", "\n", text)
    text = re.sub(r"</p>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class DCAFScraper(BaseScraper):
    SOURCE_ID = "LY/DCAF"

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir or str(Path(__file__).parent))
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._taxonomies: Optional[Dict[str, Dict[int, str]]] = None

    # ── HTTP ──────────────────────────────────────────────────────────

    def _get(self, url: str, params: dict,
             headers: Optional[dict] = None) -> requests.Response:
        """GET with backoff on transient failures.

        A single blip used to cost a whole language: the walk is one generator,
        so an exception raised on page 3 of 22 abandoned every page behind it
        (issue #1584).
        """
        last_error: Optional[Exception] = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self.rate_limiter.wait()
            try:
                resp = self.session.get(url, params=params, headers=headers,
                                        timeout=60)
            except requests.RequestException as e:
                last_error = e
            else:
                if resp.status_code not in RETRY_STATUS:
                    resp.raise_for_status()
                    return resp
                last_error = requests.HTTPError(
                    f"HTTP {resp.status_code}", response=resp)
            if attempt < MAX_ATTEMPTS:
                delay = 2 ** attempt
                logger.warning("%s (attempt %d/%d) — retrying in %ds",
                               last_error, attempt, MAX_ATTEMPTS, delay)
                time.sleep(delay)
        raise last_error

    def _taxonomy_map(self) -> Dict[str, Dict[int, str]]:
        if self._taxonomies is not None:
            return self._taxonomies
        mappings: Dict[str, Dict[int, str]] = {}
        for tax_name, route in TAXONOMY_ROUTES.items():
            terms: Dict[int, str] = {}
            for prefix in TAXONOMY_PREFIXES:
                url = f"{SITE}{prefix}/wp-json/wp/v2/{route}"
                try:
                    resp = self._get(url, {"per_page": 100})
                    terms.update({t["id"]: strip_html(t["name"]) for t in resp.json()})
                except requests.RequestException as e:
                    logger.warning("Could not fetch %s taxonomy from %s: %s",
                                   tax_name, url, e)
            mappings[tax_name] = terms
        logger.info(
            "Taxonomies: %s",
            ", ".join(f"{k}={len(v)}" for k, v in mappings.items()) or "none",
        )
        self._taxonomies = mappings
        return mappings

    def _iter_posts(self, lang: str, extra: Optional[dict] = None
                    ) -> Generator[Dict[str, Any], None, None]:
        """Walk one language endpoint page by page, yielding raw WP items.

        WordPress does not echo the page number, so a silently-ignored `page`
        would look like a full walk that keeps re-serving page 1. Guard on the
        post ids instead: a page whose ids we have all already seen means the
        parameter is not being honoured, and continuing would loop to
        X-WP-TotalPages emitting one page's worth of duplicates.
        """
        base = LANG_ENDPOINTS[lang]
        page = 1
        total_pages = None
        seen: set = set()
        yielded = 0

        while True:
            params = {"per_page": 100, "page": page, "orderby": "id", "order": "asc"}
            if extra:
                params.update(extra)
            try:
                resp = self._get(base, params,
                                 {"Accept-Language": ACCEPT_LANGUAGE[lang]})
            except requests.HTTPError as e:
                # WP answers 400 rest_post_invalid_page_number past the end.
                if e.response is not None and e.response.status_code == 400:
                    break
                raise

            # WPML can redirect one language root onto the other's prefix, which
            # would walk the Arabic corpus while labelling it English.
            served_ar = "/ar/wp-json" in resp.url
            if served_ar != (lang == "ar"):
                raise RuntimeError(
                    f"[{lang}] request redirected to {resp.url} — the site "
                    "served the other language's corpus, refusing to mislabel it"
                )

            if total_pages is None:
                total = int(resp.headers.get("X-WP-Total", 0))
                total_pages = int(resp.headers.get("X-WP-TotalPages", 0))
                logger.info("[%s] %d posts across %d pages", lang, total, total_pages)
                if total == 0:
                    break

            items = resp.json()
            if not items:
                break

            ids = {it.get("id") for it in items}
            if ids and ids <= seen:
                raise RuntimeError(
                    f"[{lang}] page {page} re-served posts already seen — the "
                    "`page` parameter is being ignored, refusing to loop"
                )
            seen |= ids

            for item in items:
                yield {"item": item, "lang": lang}
                yielded += 1

            if total_pages and page >= total_pages:
                break
            page += 1

        logger.info("[%s] yielded %d posts", lang, yielded)

    # ── BaseScraper contract ──────────────────────────────────────────

    def _iter_lang(self, lang: str, extra: Optional[dict] = None
                   ) -> Generator[Dict[str, Any], None, None]:
        """Walk one language, turning a mid-walk failure into a coverage gap.

        The two endpoints are independent corpora (2,160 Arabic originals plus
        786 English translations) but `fetch_all` chains them into a single
        generator, so an English-side error aborted the run *after* the Arabic
        half had already streamed to disk. The fleet saw a non-zero exit with
        exactly 2,160 records written and no way to tell that the missing 786
        were a whole language the crawl never reached (issue #1584). Record the
        gap instead: the other language still lands, the run ends cleanly, and
        the shortfall is loud in the log and in status.yaml.
        """
        try:
            yield from self._iter_posts(lang, extra)
        except Exception as e:
            self.record_coverage_gap(
                f"lang:{lang}",
                f"{type(e).__name__}: {e}",
                endpoint=LANG_ENDPOINTS[lang],
            )

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        self._taxonomy_map()
        for lang in ("ar", "en"):
            yield from self._iter_lang(lang)

    def fetch_updates(self, since) -> Generator[Dict[str, Any], None, None]:
        """Posts whose WordPress `modified` stamp is newer than `since`.

        `modified` is when the text became available to us, which is the right
        comparator for an incremental lane — the `date` field holds the law's
        own promulgation date and would never advance.
        """
        # `since` arrives as a datetime from the fleet but as a string from
        # some callers; as_date_str normalises both (issue #1512 class).
        day = as_date_str(since)
        stamp = f"{day}T00:00:00"
        logger.info("Fetching posts modified after %s", stamp)
        self._taxonomy_map()
        for lang in ("ar", "en"):
            yield from self._iter_lang(lang, {"modified_after": stamp})

    def normalize(self, raw: Dict[str, Any]) -> Optional[dict]:
        item = raw["item"]
        lang = raw["lang"]
        taxonomies = self._taxonomy_map()

        wp_id = item.get("id", 0)
        title = strip_html((item.get("title") or {}).get("rendered", ""))
        text = strip_html((item.get("content") or {}).get("rendered", ""))

        # English posts awaiting translation carry a stub, not the law.
        if PLACEHOLDER in text.upper() or len(text) < MIN_TEXT_CHARS:
            return None

        def tax(name: str) -> str:
            ids = item.get(TAXONOMY_FIELDS[name], []) or []
            table = taxonomies.get(name, {})
            return "; ".join(n for n in (table.get(i, "") for i in ids) if n)

        raw_date = item.get("date") or ""
        modified = item.get("modified") or ""

        # Arabic posts keep the original ids the corpus was first ingested
        # under; the English translations are separate WordPress posts whose
        # ids run in the same numeric range, so they need their own suffix.
        ident = f"LY-DCAF-{wp_id}" if lang == "ar" else f"LY-DCAF-{wp_id}-en"

        return {
            "_id": ident,
            "_source": "LY/DCAF",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw_date[:10] or None,
            "url": item.get("link", ""),
            "language": lang,
            "modified": modified[:10] or None,
            "text_type": tax("text_type"),
            "status": tax("status"),
            "institution": tax("institution"),
            "index_category": tax("index"),
        }

    # ── Diagnostics ───────────────────────────────────────────────────

    def test_connection(self) -> bool:
        ok = True
        for lang, base in LANG_ENDPOINTS.items():
            try:
                resp = self._get(base, {"per_page": 1})
                total = resp.headers.get("X-WP-Total", "?")
                logger.info("%s endpoint: HTTP %s, %s posts",
                            lang, resp.status_code, total)
                if not int(total or 0):
                    ok = False
            except requests.RequestException as e:
                logger.error("%s endpoint failed: %s", lang, e)
                ok = False
        return ok


def main():
    import argparse
    parser = argparse.ArgumentParser(description="LY/DCAF Libya Legal Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DCAFScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test_connection() else 1)
    elif args.command == "bootstrap":
        print(f"Bootstrap complete: {scraper.bootstrap(sample_mode=args.sample)}")
    elif args.command == "update":
        print(f"Update complete: {scraper.update()}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
