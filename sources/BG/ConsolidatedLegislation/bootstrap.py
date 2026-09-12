#!/usr/bin/env python3
"""
BG/ConsolidatedLegislation -- Consolidated Bulgarian legislation (кодекси и закони).

The Bulgarian State Gazette (dv.parliament.bg) publishes individual gazette items
(new acts and discrete amendments) but NOT the consolidated, in-force text of
codes and laws. Statutory-framework research (e.g. "which КСО provisions govern a
survivor's pension") therefore needs the CONSOLIDATED text, which is not indexed
by BG/StateGazette (see issue #1190).

This source fills that gap. It fetches the full consolidated text of ~816
Bulgarian normative acts — codes (кодекси), laws (закони), regulations (наредби,
правилници, постановления, инструкции, тарифи), Bulgarian-language EU directives
and regulations, and double-taxation treaties (СИДДО) — from the free legal base
of kik-info.com. The underlying normative acts are public domain under the
Bulgarian Copyright Act (Art. 4 excludes normative and official acts); kik-info
is used only as a reachable host for the consolidated text.

Access (plain HTTP GET, no JS/CAPTCHA/auth):
  1. Category index pages list every document as a leaf link:
       https://kik-info.com/normativna-baza/{category}/
     -> /normativna-baza/{category}/{slug}/   (one page per act)
  2. Each act page carries the full consolidated text in a
       <div class="nb-doc"> container (h1 = act title, then the article body
     with amendment annotations "изм. - ДВ, бр. N от YYYY"). Extracted directly
     from the HTML, no PDF, no OCR.

Usage:
  python bootstrap.py bootstrap            # Full pull (all acts)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BG.ConsolidatedLegislation")

BASE = "https://kik-info.com"

# Document categories in the kik-info "normativna-baza" legal base.
# Each maps to a document _type (all are legislation for our schema).
CATEGORIES = [
    "zakoni",         # laws
    "kodeksi",        # codes
    "pravilnici",     # implementing regulations
    "naredbi",        # ordinances
    "postanovlenia",  # Council of Ministers decrees
    "instrukcii",     # instructions
    "tarifa",         # tariffs
    "siddo",          # double-taxation avoidance treaties
    "direktivi",      # EU directives (Bulgarian text)
    "reglamenti",     # EU regulations (Bulgarian text)
    "nss",            # national accounting standards
    "drugi",          # other normative acts
]

MIN_TEXT_LEN = 300


class ConsolidatedLegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Accept-Language": "bg,en;q=0.5",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=90, allow_redirects=True)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_category(self, category: str) -> list[dict]:
        """Return [{category, slug, url}] leaf documents for one category."""
        r = self._get(f"{BASE}/normativna-baza/{category}/")
        if not r:
            logger.warning("could not fetch category index: %s", category)
            return []
        # Leaf docs are /normativna-baza/{category}/{slug}/ (single extra
        # segment); exclude the category root and deeper/other paths.
        prefix = f"/normativna-baza/{category}/"
        found: dict[str, dict] = {}
        for m in re.finditer(r'href="(' + re.escape(prefix) + r'([^"/]+))/?"', r.text):
            path, slug = m.group(1), m.group(2)
            if not slug:
                continue
            found[slug] = {
                "category": category,
                "slug": slug,
                "url": f"{BASE}{path}/",
            }
        return list(found.values())

    def _discover(self) -> list[dict]:
        docs: dict[str, dict] = {}
        for cat in CATEGORIES:
            entries = self._list_category(cat)
            logger.info("category %-14s -> %d docs", cat, len(entries))
            for e in entries:
                docs[f"{e['category']}/{e['slug']}"] = e
        logger.info("discovered %d unique consolidated acts", len(docs))
        return list(docs.values())

    # ------------------------------------------------------------- extract
    @staticmethod
    def _clean_text(node) -> str:
        for bad in node.find_all(["script", "style"]):
            bad.decompose()
        text = node.get_text("\n")
        # Drop the version-compare UI phrase that sits inside the container.
        text = text.replace("сравни версии", "")
        # Collapse whitespace.
        text = re.sub(r"[ \t ]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _latest_amendment_date(text: str) -> str | None:
        """Best-effort 'current-through' date: the most recent amendment year.

        Consolidated acts have no single date; we surface the latest year seen
        in the "ДВ, бр. N от YYYY" amendment annotations as YYYY-01-01.
        """
        years = [int(y) for y in re.findall(r"ДВ,?\s*бр\.\s*\d+\s*от\s*(\d{4})", text)]
        years += [int(y) for y in re.findall(r"бр\.\s*\d+\s*от\s*(\d{4})\s*г", text)]
        years = [y for y in years if 1980 <= y <= 2100]
        if not years:
            return None
        return f"{max(years)}-01-01"

    def _fetch_one(self, doc: dict) -> dict | None:
        r = self._get(doc["url"])
        if not r:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        container = soup.find("div", class_="nb-doc")
        if container is None:
            logger.warning("no nb-doc container: %s", doc["url"])
            return None

        h1 = container.find("h1")
        title = h1.get_text(strip=True) if h1 else None
        if not title:
            t = soup.find("title")
            title = t.get_text(strip=True) if t else doc["slug"]

        text = self._clean_text(container)
        if len(text) < MIN_TEXT_LEN:
            logger.warning("text too short (%d) for %s", len(text), doc["url"])
            return None

        return {
            "category": doc["category"],
            "slug": doc["slug"],
            "url": doc["url"],
            "title": title,
            "text": text,
            "date": self._latest_amendment_date(text),
        }

    # ------------------------------------------------------------- iterate
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        docs = self._discover()
        emitted = 0
        for doc in docs:
            rec = self._fetch_one(doc)
            if not rec:
                continue
            yield rec
            emitted += 1
            if sample and emitted >= 12:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # Consolidated acts are re-fetched wholesale; upsert dedups unchanged
        # text. There is no cheap "modified since" signal, so re-scan all.
        yield from self.fetch_all()

    # ------------------------------------------------------------ normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"BG/ConsolidatedLegislation/{raw['category']}/{raw['slug']}",
            "_source": "BG/ConsolidatedLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "language": "bg",
            "category": raw["category"],
            "jurisdiction": "BG",
        }

    # ----------------------------------------------------------------- test
    def test_api(self) -> bool:
        try:
            entries = self._list_category("kodeksi")
            logger.info("test: found %d codes in 'kodeksi'", len(entries))
            if not entries:
                return False
            kso = next((e for e in entries if e["slug"] == "kso"), entries[0])
            rec = self._fetch_one(kso)
            if rec and len(rec["text"]) > MIN_TEXT_LEN:
                logger.info("test passed: %s -> %d chars, date=%s",
                            rec["title"][:60], len(rec["text"]), rec["date"])
                return True
            logger.error("test failed: no text extracted")
            return False
        except Exception as e:
            logger.error("test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="BG/ConsolidatedLegislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ConsolidatedLegislationScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
