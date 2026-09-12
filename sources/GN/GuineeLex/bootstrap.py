#!/usr/bin/env python3
"""
GN/GuineeLex -- Le droit guineen en vigueur (guineelex.com)

Consolidated Guinean legislation, article by article: the Constitution, 60
codes, ordinary and organic laws, ordonnances, decrets/arretes, ministerial
decisions, ratified treaties and the OHADA uniform acts.

Strategy:
  - The site is a statically rendered Astro build: every document page ships
    its complete text in the HTML, so there is no API to reverse and no JS to
    execute. Enumeration comes from the published sitemap index, which is
    authoritative (~5,620 pages across nine per-category sitemaps).
  - Each page carries a schema.org `Legislation` JSON-LD block with
    legislationIdentifier / legislationDate / legislationType /
    legislationLegalForce, so the metadata is read from structured markup
    rather than scraped out of prose.
  - Body text lives in `<article data-pagefind-body>` -- the element the
    site's own search index is built from, i.e. exactly the document and none
    of the chrome.
  - The corpus deliberately keeps "reference documentaire" stubs: pages for
    texts that are cited by other documents but whose full text has not been
    transcribed. They render a placeholder paragraph instead of articles and
    are dropped here rather than emitted as metadata-only records. The tell
    is structural -- a real document has one `article-head` heading per
    article, a stub has none.

Endpoints:
  - Sitemap index: https://guineelex.com/sitemap-index.xml
  - Category:      https://guineelex.com/sitemap-{codes,lois,...}.xml
  - Document:      https://guineelex.com/{category}/{slug}

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py update             # Incremental (new + consolidated texts)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from itertools import zip_longest
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GN.GuineeLex")

SOURCE_ID = "GN/GuineeLex"
BASE_URL = "https://guineelex.com"
SITEMAP_INDEX = "/sitemap-index.xml"

# Category slug -> human label, in the order the site presents them.
CATEGORIES = {
    "constitution": "Constitution",
    "codes": "Code",
    "lois-organiques": "Loi organique",
    "lois": "Loi",
    "ordonnances": "Ordonnance",
    "decrets-arretes": "Décret / Arrêté",
    "decisions": "Décision",
    "traites": "Droit international",
    "ohada": "OHADA",
}

# Consolidated texts are amended in place, so an incremental run re-reads them
# even when the URL is already known. The rest are dated acts that do not change
# once published.
LIVING_CATEGORIES = ("constitution", "codes", "lois-organiques", "ohada")

# Below this a "document" is a placeholder, not a text worth indexing.
MIN_TEXT_CHARS = 200

# Sitemap enumeration is the whole corpus; losing it silently would look like a
# small source rather than a broken one.
MIN_EXPECTED_DOCS = 3000


def strip_html(fragment: str) -> str:
    """Remove markup and decode entities, keeping paragraph breaks."""
    if not fragment:
        return ""
    # Buttons and icon spans carry Material Symbols ligatures ("content_copy",
    # "gavel") that read as words once the tags are gone.
    text = re.sub(
        r"(?is)<(script|style|svg|button)[^>]*>.*?</\1>", "", fragment
    )
    text = re.sub(
        r"(?is)<span[^>]*material-symbols[^>]*>.*?</span>", "", text
    )
    text = re.sub(r"(?i)<(br|p|div|h[1-6]|li|tr|section)[^>]*/?>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text).replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def iso_date(value: Any) -> Optional[str]:
    """schema.org dates are already ISO; keep the day and drop anything else."""
    text = clean(value)
    if not text:
        return None
    match = re.match(r"(\d{4}-\d{2}-\d{2})", text)
    return match.group(1) if match else None


def org_name(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        return clean(value.get("name"))
    return clean(value)


class GuineeLexScraper(BaseScraper):
    """
    Scraper for GN/GuineeLex -- consolidated Guinean law from guineelex.com.
    Country: GN
    URL: https://guineelex.com
    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "fr",
            },
            timeout=60,
        )
        self._seen_path = self.source_dir / "data" / "seen_urls.json"

    # ── Enumeration ──────────────────────────────────────────────────

    def _get_text(self, path: str) -> str:
        self.rate_limiter.wait()
        resp = self.client.get(path)
        if resp.status_code != 200:
            raise RuntimeError(f"GET {path}: HTTP {resp.status_code}")
        return resp.text

    def _sitemap_urls(self, path: str) -> List[str]:
        return re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", self._get_text(path))

    def _enumerate(self) -> List[Dict[str, str]]:
        """
        Walk the sitemap index and return one entry per document page. Each
        category sitemap also lists its own landing page (/lois, /codes, ...),
        which has no document body; those carry a single path segment, so the
        segment count separates them from documents without a extra fetch.
        """
        docs: List[Dict[str, str]] = []
        seen = set()
        for sitemap in self._sitemap_urls(SITEMAP_INDEX):
            category = re.sub(r".*/sitemap-|\.xml$", "", sitemap)
            if category == "core":
                continue  # home, methodology, 3D map — no legal text
            for url in self._sitemap_urls(sitemap):
                path = url[len(BASE_URL):] if url.startswith(BASE_URL) else url
                parts = [p for p in path.split("/") if p]
                if len(parts) < 2 or url in seen:
                    continue
                seen.add(url)
                docs.append({"url": url, "category": parts[0], "slug": parts[-1]})

        grouped: Dict[str, List[dict]] = {}
        for doc in docs:
            grouped.setdefault(doc["category"], []).append(doc)
        logger.info(
            "Sitemaps list %d documents: %s",
            len(docs), {k: len(v) for k, v in grouped.items()},
        )

        # Round-robin the categories rather than crawling them end to end.
        # 4,506 of the 5,620 pages are decrets/arretes, so in sitemap order a
        # crawl that stops early — or a 15-record sample — sees one document
        # type and nothing else.
        docs = [
            doc for row in zip_longest(*grouped.values())
            for doc in row if doc is not None
        ]

        if len(docs) < MIN_EXPECTED_DOCS:
            raise RuntimeError(
                f"sitemap enumeration returned only {len(docs)} documents "
                f"(expected >= {MIN_EXPECTED_DOCS}) — enumeration is broken, "
                "refusing to crawl a truncated corpus"
            )
        return docs

    # ── Seen-URL checkpoint ──────────────────────────────────────────

    def _load_seen(self) -> set:
        try:
            return set(json.loads(self._seen_path.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            return set()

    def _save_seen(self, urls) -> None:
        self._seen_path.parent.mkdir(parents=True, exist_ok=True)
        self._seen_path.write_text(
            json.dumps(sorted(urls), ensure_ascii=False), encoding="utf-8"
        )

    # ── Fetching ─────────────────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        docs = self._enumerate()
        self._save_seen(doc["url"] for doc in docs)
        for doc in docs:
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        The sitemaps carry no <lastmod>, so there is no server-side signal for
        "changed since". What we can compare is availability: a URL absent from
        the last crawl is new to us. Consolidated texts are amended in place
        under a stable URL, so those categories are always re-read; the loader
        dedups them on content when nothing moved. Revisions to a dated act
        are only caught by a full re-crawl — noted in the README.
        """
        docs = self._enumerate()
        seen = self._load_seen()
        fresh = [
            doc for doc in docs
            if doc["url"] not in seen or doc["category"] in LIVING_CATEGORIES
        ]
        logger.info(
            "%d documents to re-read (%d new since last crawl, %d consolidated)",
            len(fresh),
            sum(1 for d in docs if d["url"] not in seen),
            sum(1 for d in fresh if d["category"] in LIVING_CATEGORIES),
        )
        self._save_seen(doc["url"] for doc in docs)
        for doc in fresh:
            yield doc

    # ── Normalisation ────────────────────────────────────────────────

    @staticmethod
    def _legislation_ld(page: str) -> dict:
        for block in re.findall(
            r"(?is)<script[^>]*application/ld\+json[^>]*>(.*?)</script>", page
        ):
            try:
                data = json.loads(block)
            except ValueError:
                continue
            for entry in data if isinstance(data, list) else [data]:
                if isinstance(entry, dict) and entry.get("@type") == "Legislation":
                    return entry
        return {}

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw.get("url")
        if not url:
            return None

        try:
            page = self._get_text(url[len(BASE_URL):] if url.startswith(BASE_URL) else url)
        except Exception as exc:
            logger.warning("Fetch failed for %s: %s", url, exc)
            return None

        match = re.search(
            r"(?is)<article[^>]*data-pagefind-body[^>]*>(.*?)</article>", page
        )
        if not match:
            logger.debug("No document body element on %s", url)
            return None
        body = match.group(1)

        # A "reference documentaire" stub renders a placeholder paragraph and
        # no articles; only pages with article headings hold real text.
        if not re.search(r'class="article-head"', body):
            return None

        text = strip_html(body)
        # The body opens with the breadcrumb ("Accueil › Lois › <title>");
        # it is navigation, not part of the document.
        text = re.sub(r"\A\s*Accueil\s*›[^\n]*\n+", "", text)
        if len(text) < MIN_TEXT_CHARS:
            return None

        meta = self._legislation_ld(page)
        title = clean(meta.get("name"))
        if not title:
            heading = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", page)
            title = strip_html(heading.group(1)) if heading else None
        if not title:
            return None
        title = re.sub(r"\s+", " ", title)

        category = raw.get("category") or ""
        in_force = str(meta.get("legislationLegalForce", "")).endswith("InForce")

        return {
            "_id": f"{SOURCE_ID}/{category}/{raw.get('slug')}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date(meta.get("legislationDate") or meta.get("datePublished")),
            "url": url,
            "category": category,
            "category_label": CATEGORIES.get(category, category),
            "identifier": clean(meta.get("legislationIdentifier")),
            "legislation_type": clean(meta.get("legislationType")),
            "passed_by": org_name(meta.get("legislationPassedBy")),
            "in_force": in_force,
            "legal_force": clean(meta.get("legislationLegalForce")),
            "article_count": len(re.findall(r'class="article-head"', body)),
            "jurisdiction": org_name(meta.get("legislationJurisdiction")) or "République de Guinée",
            "language": "fr",
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="GN/GuineeLex bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full bootstrap or sample")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--full", action="store_true", help="Full fetch (all documents)")

    sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Connectivity test")

    args = parser.parse_args()
    scraper = GuineeLexScraper()

    if args.command == "test":
        docs = scraper._enumerate()
        logger.info("OK: %d documents listed by the sitemaps", len(docs))
        record = scraper.normalize(docs[0])
        if not record:
            logger.error("FAILED: first document produced no record")
            sys.exit(1)
        logger.info(
            "Newest sample: %s (%s chars, %s articles)",
            record["title"], len(record["text"]), record["article_count"],
        )

    elif args.command == "bootstrap":
        sample_mode = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        logger.info(f"Bootstrap complete: {stats}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")

    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI dispatches
    # on the literal command name, so alias it (issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
