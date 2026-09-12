#!/usr/bin/env python3
"""
EU/EDAL — European Database of Asylum Law (ECRE).

EDAL publishes structured, English-language summaries of asylum case law from
the CJEU, the ECtHR, UN treaty bodies and the national courts of 20+ EU member
states. Each summary carries the full analytical body of the case as prepared by
the national expert: headnote, facts, decision & reasoning, outcome, subsequent
proceedings and observations.

Site migration (2026)
---------------------
The Drupal 7 site was replaced by a Laravel application. The old paths are gone:

    /en/case-law-search?page=N   -> 404   (old listing, used by the previous scraper)
    /en/case-law/{title-slug}    -> 301 to the site root (old case pages)

The new structure is:

    /summaries                       listing (Livewire table, JS-paginated)
    /summaries/case/{slug}           case page, server-rendered HTML
    /sitemap.xml                     sitemap index
    /sitemap.xml/summaries/{cjeu,ecrthr,national,un}

Discovery therefore goes through the sitemaps rather than the listing: they are
server-generated, complete (~1,830 cases) and cost four requests instead of
~180 paginated ones. Case pages are plain server-rendered HTML — the analytical
body lives in `<section data-section-id="...">` blocks and the metadata in a
two-column table — so no browser automation is needed.

robots.txt allows `User-agent: *` everywhere except /admin, /user and /core, and
no longer declares a Crawl-delay; we still pace requests at 1s.
"""

import html
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logger = logging.getLogger("legal-data-hunter")

BASE_URL = "https://www.asylumlawdatabase.eu"
SITEMAP_INDEX = f"{BASE_URL}/sitemap.xml"
CASE_URL_RE = re.compile(r"^https://www\.asylumlawdatabase\.eu/summaries/case/[^/]+$")
CRAWL_DELAY = 1.0

USER_AGENT = "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)"

# Analytical sections, in the order they should appear in the composed full text.
SECTIONS = [
    ("headnote", "HEADNOTE"),
    ("facts", "FACTS"),
    ("decision", "DECISION & REASONING"),
    ("outcome", "OUTCOME"),
    ("subproc", "SUBSEQUENT PROCEEDINGS"),
    ("observation", "OBSERVATIONS"),
]

# Metadata table labels -> record field names.
META_LABELS = {
    "country of decision": "country",
    "country of applicant": "country_of_applicant",
    "court name": "court",
    "date of decision": "date_str",
    "citation": "citation",
    "additional citation": "additional_citation",
    "ecli": "ecli",
}

_TAG_RE = re.compile(r"(?s)<[^>]+>")
_BLOCK_RE = re.compile(r"(?i)</(p|div|li|tr|h[1-6]|section|table)\s*>|<br\s*/?>")


def _html_to_text(fragment: str) -> str:
    """Strip tags from an HTML fragment, keeping block-level line breaks."""
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", fragment)
    text = _BLOCK_RE.sub("\n", text)
    text = _TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    # Collapse intra-line whitespace, drop empty lines.
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.split("\n")]
    return "\n".join(ln for ln in lines if ln)


class EDALScraper(BaseScraper):
    """Scraper for the European Database of Asylum Law."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    # ------------------------------------------------------------------ HTTP

    def _get(self, url: str, retries: int = 3) -> Optional[str]:
        for attempt in range(retries):
            try:
                r = self.session.get(url, timeout=60)
                if r.status_code == 200:
                    return r.text
                if r.status_code == 404:
                    logger.warning(f"404 for {url}")
                    return None
                logger.warning(f"HTTP {r.status_code} for {url}")
            except Exception as exc:
                logger.warning(f"Request failed ({attempt + 1}/{retries}) {url}: {exc}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- discovery

    def _sitemap_urls(self) -> List[str]:
        """Return every sub-sitemap listed in the sitemap index."""
        xml = self._get(SITEMAP_INDEX)
        if not xml:
            return []
        return re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml)

    def discover_case_urls(self) -> List[str]:
        """Collect all /summaries/case/ URLs from the summary sitemaps.

        The four sitemaps are round-robin interleaved so that any prefix of the
        result (notably the 15 records taken in sample mode) spans CJEU, ECtHR,
        national and UN case law instead of being 100% CJEU.
        """
        groups: List[List[str]] = []
        seen = set()
        for sm in self._sitemap_urls():
            if "/summaries/" not in sm:
                continue
            xml = self._get(sm)
            if not xml:
                logger.warning(f"Could not read sitemap {sm}")
                continue
            group = []
            for loc in re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml):
                loc = html.unescape(loc)
                if CASE_URL_RE.match(loc) and loc not in seen:
                    seen.add(loc)
                    group.append(loc)
            logger.info(f"{sm.rsplit('/', 1)[-1]}: {len(group)} case URLs")
            # Sitemaps list oldest first; walk newest first so an interrupted or
            # sampled run covers the most recent case law.
            groups.append(list(reversed(group)))
            time.sleep(CRAWL_DELAY)

        urls: List[str] = []
        for i in range(max((len(g) for g in groups), default=0)):
            for group in groups:
                if i < len(group):
                    urls.append(group[i])
        logger.info(f"Discovered {len(urls)} unique case URLs")
        return urls

    # ----------------------------------------------------------------- parse

    @staticmethod
    def _parse_metadata(page: str) -> Dict[str, str]:
        """Read the two-column header table (`<strong>Label:</strong> | value`)."""
        meta: Dict[str, str] = {}
        for m in re.finditer(
            r"(?s)<tr>\s*<td>\s*<strong>(.*?)</strong>\s*</td>\s*<td>(.*?)</td>", page
        ):
            label = _html_to_text(m.group(1)).strip().rstrip(":").strip().lower()
            field = META_LABELS.get(label)
            if not field:
                continue
            value = " ".join(_html_to_text(m.group(2)).split("\n")).strip()
            if value:
                meta[field] = value
        return meta

    @staticmethod
    def _parse_section(page: str, section_id: str) -> str:
        """Extract one `<section data-section-id="...">` block, minus its heading."""
        m = re.search(
            r'(?s)<section[^>]*data-section-id="%s"[^>]*>(.*?)</section>' % section_id,
            page,
        )
        if not m:
            return ""
        fragment = re.sub(r"(?is)<h[1-6][^>]*>.*?</h[1-6]>", "", m.group(1), count=1)
        return _html_to_text(fragment).strip()

    @staticmethod
    def _parse_keywords(page: str) -> List[str]:
        """Keyword chips link to the faceted listing (`/summaries?keywords=NN`)."""
        kws: List[str] = []
        for m in re.finditer(
            r'(?s)<a[^>]+href="[^"]*/summaries\?[^"]*keywords[^"]*"[^>]*>(.*?)</a>', page
        ):
            kw = " ".join(_html_to_text(m.group(1)).split())
            if kw and kw not in kws:
                kws.append(kw)
        return kws

    @staticmethod
    def _iso_date(value: str) -> str:
        """EDAL prints dates as DD-MM-YYYY."""
        if not value:
            return ""
        m = re.search(r"(\d{2})-(\d{2})-(\d{4})", value)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", value)
        return m.group(0) if m else ""

    def parse_case(self, url: str) -> Optional[Dict[str, Any]]:
        page = self._get(url)
        if not page:
            return None

        h1 = re.search(r"(?s)<h1[^>]*>(.*?)</h1>", page)
        title = " ".join(_html_to_text(h1.group(1)).split()) if h1 else ""

        parts, fields = [], {}
        for section_id, heading in SECTIONS:
            body = self._parse_section(page, section_id)
            fields[section_id] = body
            if body:
                parts.append(f"{heading}\n{body}")

        text = "\n\n".join(parts)
        if not text:
            logger.warning(f"No analytical body on {url}")
            return None

        doc: Dict[str, Any] = {
            "url": url,
            "slug": url.rstrip("/").rsplit("/", 1)[-1],
            "title": title,
            "text": text,
            "keywords": self._parse_keywords(page),
            "other_sources": self._parse_section(page, "source"),
        }
        doc.update(fields)
        doc.update(self._parse_metadata(page))
        return doc

    # -------------------------------------------------------------- pipeline

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        urls = self.discover_case_urls()
        if not urls:
            raise RuntimeError("Sitemap discovery returned no case URLs")

        fetched = 0
        for i, url in enumerate(urls, 1):
            doc = self.parse_case(url)
            if doc:
                fetched += 1
                yield doc
            if i % 50 == 0:
                logger.info(f"Processed {i}/{len(urls)} case pages ({fetched} with text)")
            time.sleep(CRAWL_DELAY)
        logger.info(f"fetch_all complete: {fetched}/{len(urls)} cases with full text")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """No per-document lastmod on the sitemaps — filter on decision date."""
        cutoff = since.date() if hasattr(since, "date") else since
        for doc in self.fetch_all():
            iso = self._iso_date(doc.get("date_str", ""))
            if not iso:
                yield doc
                continue
            try:
                if datetime.strptime(iso, "%Y-%m-%d").date() >= cutoff:
                    yield doc
            except ValueError:
                yield doc

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = (raw.get("text") or "").strip()
        if not text:
            return None

        return {
            "_id": f"EDAL-{raw['slug'][:120]}",
            "_source": "EU/EDAL",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "headnote": raw.get("headnote", ""),
            "facts": raw.get("facts", ""),
            "decision": raw.get("decision", ""),
            "outcome": raw.get("outcome", ""),
            "observations": raw.get("observation", ""),
            "date": self._iso_date(raw.get("date_str", "")),
            "court": raw.get("court", ""),
            "country": raw.get("country", ""),
            "country_of_applicant": raw.get("country_of_applicant", ""),
            "citation": raw.get("citation", ""),
            "ecli": raw.get("ecli", ""),
            "keywords": raw.get("keywords", []),
            "other_sources": raw.get("other_sources", ""),
            "url": raw["url"],
        }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scraper = EDALScraper()

    command = sys.argv[1] if len(sys.argv) > 1 else "bootstrap"
    sample_mode = "--sample" in sys.argv

    if command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")
    elif command == "test":
        urls = scraper.discover_case_urls()
        print(f"Discovered {len(urls)} case URLs")
        if urls:
            doc = scraper.parse_case(urls[0])
            print(json.dumps(scraper.normalize(doc), ensure_ascii=False, indent=2)[:1500])
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
