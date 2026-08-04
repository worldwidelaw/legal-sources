#!/usr/bin/env python3
"""
CA/CanLII-Extended -- Extended keyword browse across all CanLII databases

Browses the full CanLII database network (400+ courts and tribunals) with an
expanded keyword set that covers English and French water law, provincial
statutes, riparian rights, drainage law, and Indigenous water rights.

Complements CA/A2AJ by catching decisions where subject-matter keywords appear
in the case title/keywords metadata (CanLII browse does not search full text).

API notes (empirically discovered):
  - Database-list endpoint returns key `caseDatabases`, not `databases`
  - Browse listing omits `decisionDate`; year parsed from citation string
  - Rate limit: ~2 req/s (use 0.65s sleep conservatively)

Usage:
  python bootstrap.py bootstrap          # Browse all 400+ databases
  python bootstrap.py bootstrap --sample # 15 sample records
  python bootstrap.py test               # API connectivity check
  python bootstrap.py update             # Incremental
"""

import sys
import os
import json
import re
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.CanLII-Extended")

BASE = "https://api.canlii.org/v1"
LANG = "en"
PAGE_SIZE = 10_000
MIN_SLEEP = 0.65

# ── Expanded water-law keyword set ────────────────────────────────────────────
WATER_KEYWORDS = [
    # English — core services
    "water supply","water service","water utility","water rate","water meter",
    "water bill","water connection","drinking water","potable water",
    "water treatment","water distribution","water quality","water contamination",
    "water pollution","water safety","water standard","boil water","water advisory",
    "wastewater","sewage","sewer","stormwater","storm water",
    "water main","water pipe","waterworks","water infrastructure",
    # Resource rights
    "water rights","water licence","water permit","water allocation",
    "water diversion","water extraction","water use","water taking","water withdrawal",
    "groundwater","aquifer","well water","surface water","watershed",
    "watercourse","waterway","water body","water table","water level",
    # Environmental
    "source water","water conservation","water shortage","drought",
    "irrigation","water district","water board","water authority",
    "flood","reservoir","floodplain","riparian","hydroelectric",
    "wetland","wetlands","aquatic habitat","aquatic environment","fishery","fisheries",
    # Statutes
    "Canada Water Act","Safe Drinking Water","Clean Water Act","Water Act",
    "Water Resources Act","Fisheries Act","Navigation Protection",
    "Ontario Water Resources Act","BC Water Sustainability Act",
    "Alberta Water Act","Saskatchewan Water Security Agency",
    "Drainage Act",
    # Riparian / tort / property
    "riparian rights","water damage","flood damage","flooding damage",
    "water intrusion","water seepage","water runoff","drainage","municipal drain",
    "detention pond","retention pond","stormwater management",
    "waterfront","shoreline","lakeshore","navigable water",
    # Indigenous
    "Indigenous water","First Nations water","treaty water",
    "drinking water advisory","long-term drinking water advisory",
    # French
    "eau potable","eau souterraine","eaux usées","eaux de surface",
    "distribution d'eau","service d'eau","approvisionnement en eau",
    "qualité de l'eau","contamination de l'eau",
    "aqueduc","égout","assainissement",
    "inondation","barrage","nappe phréatique","cours d'eau","bassin versant",
    "ressources hydriques","eaux pluviales","zone inondable",
    "Loi sur les pêches","Loi sur la protection de la navigation",
    "milieu humide","milieu aquatique","droit riverain",
]
WATER_KW_LOWER = [k.lower() for k in WATER_KEYWORDS]

WORD_PATTERNS = [
    re.compile(r"\bdam\b", re.I), re.compile(r"\bdams\b", re.I),
    re.compile(r"\blevee\b", re.I), re.compile(r"\bdike\b", re.I),
    re.compile(r"\bdyke\b", re.I), re.compile(r"\bcrue[s]?\b", re.I),
    re.compile(r"\bdrainage\b", re.I),
]


def _is_water(title: str, keywords: str = "") -> bool:
    h = (title + " " + keywords).lower()
    if any(kw in h for kw in WATER_KW_LOWER):
        return True
    return any(p.search(title + " " + keywords) for p in WORD_PATTERNS)


def _year_from_citation(citation: str) -> Optional[int]:
    """Extract year from CanLII citation string (e.g. '2019 SCC 12 (CanLII)')."""
    if not citation:
        return None
    m = re.match(r"(\d{4})", str(citation).strip())
    if not m:
        return None
    yr = int(m.group(1))
    return yr if 1900 <= yr <= 2100 else None


class CanLIIExtendedScraper(BaseScraper):
    """
    Browse all 400+ CanLII databases with water-law keyword filtering.

    Two API quirks (documented in worldwidelaw/legal-sources issues #74, #75):
    - Database list returns key `caseDatabases`, not `databases`
    - Browse listings omit `decisionDate`; year must come from citation string
    """

    def __init__(self, source_dir: str):
        super().__init__(source_dir)
        self.api_key = os.environ.get("CANLII_API_KEY", "")
        if not self.api_key:
            raise EnvironmentError("CANLII_API_KEY environment variable not set. "
                                   "Register free at https://developer.canlii.org/")
        import urllib.request as _req
        self._urlopen = _req.urlopen
        self._Request = _req.Request
        self._hdrs = {
            "User-Agent": ("legal-sources-bot/1.0 "
                           "(research; github.com/worldwidelaw/legal-sources)"),
            "Accept": "application/json",
        }

    def _get(self, path: str, params: dict = None) -> dict:
        """GET a CanLII API endpoint with api_key appended."""
        p = {"api_key": self.api_key}
        if params:
            p.update(params)
        import urllib.parse
        url = f"{BASE}{path}?" + urllib.parse.urlencode(p)
        req = self._Request(url, headers=self._hdrs)
        for attempt in range(4):
            try:
                with self._urlopen(req, timeout=30) as r:
                    data = json.loads(r.read())
                time.sleep(MIN_SLEEP)
                return data
            except Exception as e:
                import urllib.error
                code = getattr(e, "code", 0)
                if code == 429:
                    wait = 30 * (attempt + 1)
                    logger.warning("429 rate limit — sleeping %ds", wait)
                    time.sleep(wait)
                elif code == 404:
                    return {}
                else:
                    logger.warning("Request error (attempt %d): %s", attempt + 1, e)
                    time.sleep(3)
        return {}

    def _list_databases(self) -> list:
        """Return all CanLII database descriptors."""
        resp = self._get(f"/caseBrowse/{LANG}/")
        # NOTE: API returns `caseDatabases`, not `databases` (see issue #74)
        return resp.get("caseDatabases", resp.get("databases", []))

    def fetch_all(self) -> Generator[dict, None, None]:
        """Browse all CanLII databases and yield water-law matching records."""
        date_from = self.config.get("fetch", {}).get("date_from", "2000-01-01")
        date_to   = self.config.get("fetch", {}).get("date_to", "2099-12-31")

        dbs = self._list_databases()
        logger.info("Found %d CanLII databases to browse", len(dbs))

        for db in dbs:
            db_id   = db.get("databaseId", "")
            db_name = db.get("name", db_id)
            offset  = 0

            while True:
                resp = self._get(f"/caseBrowse/{LANG}/{db_id}/", {
                    "decisionDateAfter":  date_from,
                    "decisionDateBefore": date_to,
                    "resultCount": PAGE_SIZE,
                    "offset": offset,
                })
                page = resp.get("cases", [])
                if not page:
                    break

                for c in page:
                    title = c.get("title", "")
                    kw    = c.get("keywords", "")
                    if _is_water(title, kw):
                        yield {**c, "_db_id": db_id, "_db_name": db_name}

                if len(page) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Incremental: same as fetch_all but with date_from set to last_run."""
        if since is None:
            since = (self.status.get("last_run") or "")[:10]
        if since:
            self.config.setdefault("fetch", {})["date_from"] = since
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Map a CanLII browse record to the standard schema."""
        citation = raw.get("citation", "")
        title    = raw.get("title", "")
        if not title:
            return None

        db_id   = raw.get("_db_id", "")
        db_name = raw.get("_db_name", db_id)
        year    = _year_from_citation(citation)

        # NOTE: decisionDate absent from browse response (see issue #75)
        url = raw.get("url", "")
        if not url and db_id:
            case_id = raw.get("caseId", "")
            url = f"https://www.canlii.org/en/{db_id}/{case_id}/doc.html"

        return {
            "_id": citation.strip().lower() or raw.get("caseId", ""),
            "title": title,
            "text": title,  # browse API has no full text; title is the indexed field
            "citation": citation,
            "year": year,
            "date": f"{year}-01-01" if year else "",
            "tribunal": db_id,
            "court_name": db_name,
            "docket": raw.get("docketNumber", ""),
            "keywords": raw.get("keywords", ""),
            "url": url,
            "language": raw.get("language", "en"),
            "source": "CA/CanLII-Extended",
            "country": "CA",
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CA/CanLII-Extended scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch 15 sample records")
    args = parser.parse_args()

    source_dir = Path(__file__).parent
    scraper = CanLIIExtendedScraper(str(source_dir))

    if args.command == "test":
        print("Testing CanLII API connectivity...")
        try:
            dbs = scraper._list_databases()
            print(f"OK — {len(dbs)} databases accessible")
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    else:
        generator = scraper.fetch_all() if args.command == "bootstrap" else scraper.fetch_updates()
        sample_limit = 15 if args.sample else None
        count = 0
        sample_dir = source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        for raw in generator:
            normalized = scraper.normalize(raw)
            if normalized is None:
                continue
            count += 1

            if count <= 15:
                safe = re.sub(r"[^\w-]", "_", normalized["_id"])[:50]
                with open(sample_dir / f"{safe}.json", "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

            if sample_limit and count >= sample_limit:
                print(f"Sample mode: {count} records")
                break
            if count % 100 == 0:
                logger.info("%d records collected...", count)

        print(f"Done: {count} records")
