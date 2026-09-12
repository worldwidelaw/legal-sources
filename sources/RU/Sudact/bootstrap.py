#!/usr/bin/env python3
"""
RU/Sudact -- Russian Court Decisions Fetcher (sudact.ru)

Fetches case law from sudact.ru, the largest open database of Russian court
decisions. Covers general jurisdiction courts, arbitration courts, magistrate
courts, and the Supreme Court across all 85+ Russian federal subjects.

Strategy:
  - Bootstrap: Reads sitemap XML index to enumerate decision URLs, then
    fetches individual decision pages and extracts full text from HTML.
  - Update: Diffs the sitemap against a checkpoint of already-fetched doc ids
    and only fetches the newcomers, newest first.
  - Sample: Fetches 15 records from the sitemap for validation.

The sitemap is a rolling window of the ~97K most recently published decisions,
not the whole corpus, so a doc that scrolls out of it can never be re-offered.
That is what makes the checkpoint cheap to bound (see `_save_seen`).

Data source: https://sudact.ru/
Sitemap: https://sudact.ru/sitemap.xml

Usage:
  python bootstrap.py bootstrap            # Full fetch (100K+ records)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental refresh
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from lxml import etree

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RU.Sudact")

SITEMAP_INDEX_URL = "https://sudact.ru/sitemap.xml"
SITEMAP_PART_URLS = [
    "https://sudact.ru/sitemap_part_0.xml.gz",
    "https://sudact.ru/sitemap_part_1.xml.gz",
]
SITEMAP_NS = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Decision URLs end in /doc/{id}/ -- the id is both the record key and the
# checkpoint key, so it is derived in exactly one place.
DOC_ID_RE = re.compile(r"/doc/([A-Za-z0-9]+)/?$")

# Flush the checkpoint this often so a run killed at the fleet's 100h cap
# resumes where it stopped instead of re-fetching from the top.
CHECKPOINT_EVERY = 500

# Court type mapping from URL path
COURT_TYPE_MAP = {
    "regular": "general_jurisdiction",
    "arbitral": "arbitration",
    "magistrate": "magistrate",
    "vsrf": "supreme_court",
}

# Russian month names for date parsing
RUSSIAN_MONTHS = {
    "января": "01", "февраля": "02", "марта": "03",
    "апреля": "04", "мая": "05", "июня": "06",
    "июля": "07", "августа": "08", "сентября": "09",
    "октября": "10", "ноября": "11", "декабря": "12",
}


def doc_id_from_url(url: str) -> str:
    """The /doc/{id}/ segment, or the URL itself if it does not match."""
    m = DOC_ID_RE.search(url)
    return m.group(1) if m else url


def parse_russian_date(text: str) -> Optional[str]:
    """Parse Russian date like '8 октября 2025 г.' to ISO format."""
    if not text:
        return None
    m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if m:
        day, month_name, year = m.group(1), m.group(2), m.group(3)
        month = RUSSIAN_MONTHS.get(month_name.lower())
        if month:
            return f"{year}-{month}-{int(day):02d}"
    # Try DD.MM.YYYY format
    m2 = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text)
    if m2:
        return f"{m2.group(3)}-{m2.group(2)}-{m2.group(1)}"
    return None


class SudactScraper(BaseScraper):
    """Scraper for sudact.ru Russian court decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        })

    def _fetch_sitemap_urls(self, limit: int = 0) -> list:
        """Fetch decision URLs from sitemap XML files."""
        return [e["url"] for e in self._fetch_sitemap_entries(limit)]

    def _fetch_sitemap_entries(self, limit: int = 0) -> list:
        """Fetch decision entries -- {url, doc_id, lastmod} -- from the sitemaps.

        `lastmod` is kept rather than discarded because it is the only signal
        available when there is no checkpoint yet (fresh VPS clone: `data/` is
        gitignored). It is a real discriminator, not a site-template constant --
        verified live at 11 distinct days spread evenly over the window.
        """
        all_urls = []
        for sitemap_url in SITEMAP_PART_URLS:
            logger.info(f"Fetching sitemap: {sitemap_url}")
            try:
                r = self.session.get(sitemap_url, timeout=120)
                r.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch sitemap {sitemap_url}: {e}")
                continue

            # Parse XML (may be raw XML despite .gz extension)
            try:
                root = etree.fromstring(r.content)
            except etree.XMLSyntaxError:
                # Try decompressing
                import gzip
                try:
                    data = gzip.decompress(r.content)
                    root = etree.fromstring(data)
                except Exception:
                    logger.warning(f"Cannot parse sitemap {sitemap_url}")
                    continue

            entries = []
            for el in root.findall(".//s:url", SITEMAP_NS):
                loc = el.findtext("s:loc", namespaces=SITEMAP_NS) or ""
                # Filter to doc pages only
                if "/doc/" not in loc:
                    continue
                entries.append({
                    "url": loc,
                    "doc_id": doc_id_from_url(loc),
                    "lastmod": (el.findtext("s:lastmod", namespaces=SITEMAP_NS) or "")[:10],
                })
            all_urls.extend(entries)
            logger.info(f"  Found {len(entries)} decision URLs in {sitemap_url}")

            if limit and len(all_urls) >= limit:
                all_urls = all_urls[:limit]
                break

        logger.info(f"Total decision URLs from sitemaps: {len(all_urls)}")
        return all_urls

    def _extract_decision(self, url: str) -> Optional[dict]:
        """Fetch a decision page and extract structured data."""
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Extract JSON-LD metadata
        json_ld = {}
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if data.get("@type") == "Article":
                    json_ld = data
                    break
            except (json.JSONDecodeError, TypeError):
                continue

        # Extract title from h1
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""

        # Extract court info from div.b-justice
        court_div = soup.find("div", class_="b-justice")
        court_name = ""
        case_category = ""
        uid = ""
        if court_div:
            # Court name is in the first link or text
            court_link = court_div.find("a")
            if court_link:
                court_name = court_link.get_text(strip=True)
            else:
                court_name = court_div.get_text(strip=True).split("-")[0].strip()

            # Category (Гражданское, Уголовное, etc.)
            cat_span = court_div.find("span", class_="b-doc-category")
            if cat_span:
                case_category = cat_span.get_text(strip=True).strip("- ")

            # UID
            uid_span = court_div.find("span", class_="b-doc-uid")
            if uid_span:
                uid_text = uid_span.get_text(strip=True)
                uid = uid_text.replace("УИД-", "").replace("УИД —", "").strip()

        # Fallback court name from JSON-LD
        if not court_name and json_ld.get("author", {}).get("name"):
            court_name = json_ld["author"]["name"]

        # Extract full text from the main content cell
        text = self._extract_text(soup)

        if not text or len(text) < 100:
            logger.debug(f"Insufficient text ({len(text) if text else 0} chars) at {url}")
            return None

        # Extract date from title or JSON-LD
        date = None
        if title:
            date = parse_russian_date(title)
        if not date and json_ld.get("dateModified"):
            date = json_ld["dateModified"][:10]

        # Extract case number from title
        case_number = ""
        m = re.search(r"по делу\s*№?\s*(.+?)$", title)
        if m:
            case_number = m.group(1).strip()

        # Determine court type from URL
        court_type = "unknown"
        for path_key, ct in COURT_TYPE_MAP.items():
            if f"/{path_key}/" in url:
                court_type = ct
                break

        doc_id = doc_id_from_url(url)

        return {
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "court": court_name,
            "court_type": court_type,
            "case_number": case_number,
            "case_category": case_category,
            "uid": uid,
        }

    def _extract_text(self, soup: BeautifulSoup) -> str:
        """Extract the decision full text from the page HTML."""
        # The text is in td.h-col1 inside the content table
        cell = soup.find("td", class_="h-col1")
        if not cell:
            return ""

        # Remove non-text elements: scripts, styles, ads, navigation
        for tag in cell.find_all(["script", "style", "noscript", "iframe"]):
            tag.decompose()

        # Remove the right-side menu column and action buttons
        for cls in ["h-col2-rightmenu", "b-doc-actions", "b-doc-menu", "b-justice-menu"]:
            for el in cell.find_all(class_=cls):
                el.decompose()

        # Remove the "Документы по делу" section (related docs at the bottom)
        for el in cell.find_all("div", class_="b-case-docs"):
            el.decompose()

        # Remove the "Судебная практика по:" section at the bottom
        for el in cell.find_all("div", class_="b-doc-practice"):
            el.decompose()

        # Remove the metadata table at the bottom (Суд:, Истцы:, etc.)
        for br_tag in cell.find_all("br"):
            sibling_text = br_tag.next_sibling
            if sibling_text and isinstance(sibling_text, str) and "Суд:" in sibling_text:
                # Remove everything from this point
                parent = br_tag.parent
                if parent:
                    for sib in list(br_tag.next_siblings):
                        if hasattr(sib, "decompose"):
                            sib.decompose()
                        else:
                            sib.extract()
                    br_tag.decompose()

        # Get text, preserving paragraph breaks
        # Replace block elements with newlines
        for br in cell.find_all("br"):
            br.replace_with("\n")
        for p in cell.find_all("p"):
            p.insert_before("\n")
            p.insert_after("\n")
        for div in cell.find_all("div"):
            div.insert_before("\n")

        text = cell.get_text()

        # Clean up the text
        # Remove the h1 title (already extracted separately)
        h1 = soup.find("h1")
        if h1:
            h1_text = h1.get_text(strip=True)
            text = text.replace(h1_text, "", 1)

        # Clean whitespace
        lines = [line.strip() for line in text.split("\n")]
        lines = [line for line in lines if line]
        text = "\n".join(lines)

        # Remove ad markers
        text = re.sub(r"adfox_\d+", "", text)
        # Remove remaining JS artifacts
        text = re.sub(r"window\.Ya\.adfoxCode\.create[^;]+;", "", text)

        return text.strip()

    # ── Checkpoint (#1502) ────────────────────────────────────────────

    def _checkpoint_path(self) -> Path:
        return self.source_dir / "data" / "sitemap_checkpoint.json"

    def _load_seen(self) -> set:
        """Doc ids already fetched, as of the last run."""
        try:
            with open(self._checkpoint_path(), encoding="utf-8") as f:
                return set(json.load(f).get("doc_ids") or [])
        except (OSError, ValueError, AttributeError):
            return set()

    def _save_seen(self, seen: set, in_sitemap: set) -> None:
        """Persist the seen set, pruned to ids still in the sitemap.

        The sitemap is a rolling window, so an id that has scrolled out of it
        can never be offered again and remembering it is dead weight. Pruning
        bounds the checkpoint at the window size (~97K ids) instead of letting
        it grow without limit as the window rolls forward.
        """
        keep = sorted(seen & in_sitemap) if in_sitemap else sorted(seen)
        path = self._checkpoint_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"doc_ids": keep,
                       "updated_at": datetime.now(timezone.utc).isoformat()}, f)
        tmp.replace(path)  # atomic: a half-written checkpoint would skip real docs

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions from the sitemap."""
        entries = self._fetch_sitemap_entries()
        logger.info(f"Starting bootstrap: {len(entries)} decisions to fetch")

        in_sitemap = {e["doc_id"] for e in entries}
        seen = set()
        for i, entry in enumerate(entries):
            if i > 0 and i % 100 == 0:
                logger.info(f"Progress: {i}/{len(entries)} decisions fetched")

            decision = self._extract_decision(entry["url"])
            if decision:
                # Recorded on success only -- a failed fetch must stay eligible
                # for the next run rather than being checkpointed away.
                seen.add(entry["doc_id"])
                yield decision

            if len(seen) and len(seen) % CHECKPOINT_EVERY == 0:
                self._save_seen(seen, in_sitemap)

            # Rate limit
            time.sleep(1.0)

        self._save_seen(seen, in_sitemap)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield only decisions this source has not already fetched.

        The previous implementation was `yield from self.fetch_all()`, which
        re-fetched all ~97K sitemap URLs at 1s each -- a ~27h refresh that
        dedups almost entirely away, and reads from the fleet's side as a slow
        host rather than as this scraper (#1502).

        Two filters, in order of precision:

        * the checkpoint of already-fetched doc ids -- exact, and the only one
          that survives the sitemap being re-stamped wholesale
        * the sitemap's own `lastmod` against the cutoff -- used *only* when
          there is no checkpoint, since `data/` is gitignored and a fresh VPS
          clone would otherwise re-fetch the entire window

        With a checkpoint, `lastmod` is deliberately ignored: an unseen doc is
        worth fetching whatever its stamp says, which also closes the gap left
        by a previous run that was truncated at the fleet's time cap.
        """
        entries = self._fetch_sitemap_entries()
        in_sitemap = {e["doc_id"] for e in entries}
        seen = self._load_seen()

        if seen:
            fresh = [e for e in entries if e["doc_id"] not in seen]
            logger.info(
                f"Checkpoint holds {len(seen)} fetched doc(s); "
                f"{len(fresh)} of {len(entries)} sitemap URLs are new"
            )
        else:
            cutoff = as_date_str(since)
            fresh = [e for e in entries
                     if not cutoff or not e["lastmod"] or e["lastmod"] >= cutoff]
            logger.info(
                f"No checkpoint yet; falling back to sitemap lastmod >= {cutoff}: "
                f"{len(fresh)} of {len(entries)} URLs"
            )

        if not fresh:
            logger.info("Nothing new in the sitemap since the last run.")
            return

        # Newest first, so a run cut short at the time cap has still collected
        # the most recent decisions rather than an arbitrary slice.
        fresh.sort(key=lambda e: e["lastmod"], reverse=True)

        fetched = 0
        for i, entry in enumerate(fresh):
            if i > 0 and i % 100 == 0:
                logger.info(f"Progress: {i}/{len(fresh)} new decisions fetched")

            decision = self._extract_decision(entry["url"])
            if decision:
                seen.add(entry["doc_id"])
                fetched += 1
                yield decision

            if fetched and fetched % CHECKPOINT_EVERY == 0:
                self._save_seen(seen, in_sitemap)

            time.sleep(1.0)

        self._save_seen(seen, in_sitemap)
        logger.info(f"Incremental refresh: {fetched} new decision(s) of {len(fresh)} candidates")

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        return {
            "_id": f"RU-Sudact-{raw['doc_id']}",
            "_source": "RU/Sudact",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "court": raw.get("court", ""),
            "court_type": raw.get("court_type", ""),
            "case_number": raw.get("case_number", ""),
            "case_category": raw.get("case_category", ""),
            "uid": raw.get("uid", ""),
            "language": "ru",
        }


# ── CLI entry point ─────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="RU/Sudact data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Sample mode: fetch only 15 records")
    parser.add_argument("--full", action="store_true",
                        help="Full mode: fetch all records")
    args = parser.parse_args()

    scraper = SudactScraper()

    if args.command == "bootstrap":
        if args.sample:
            logger.info("=== SAMPLE MODE: fetching 15 records ===")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            stats = scraper.bootstrap(sample_mode=not args.full)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        # Was: computed `since`, then threw it away and called bootstrap(),
        # so the incremental path was unreachable from the CLI. update()
        # reads last_run itself and falls back to bootstrap if never run.
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
