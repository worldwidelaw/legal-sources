#!/usr/bin/env python3
"""
US/FL-JudicialEthics -- Florida Judicial Ethics Advisory Committee (JEAC)
                        — Judicial Ethics Advisory Opinions

Fetches the full text of the advisory opinions issued by the Florida Supreme
Court's Judicial Ethics Advisory Committee (JEAC). The JEAC renders written
advisory opinions interpreting the Florida Code of Judicial Conduct as applied
to specific circumstances confronting or affecting a judge or judicial candidate
= doctrine (the Committee's official written interpretation of the judicial-
conduct rules).

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  jeac.flcourts.gov is a Next.js front-end backed by an Ibexa (eZ Platform) DXP
  whose public JSON content API is served from the media host:

      https://flcourts-media.ccplatform.net/api/data/fetch

  The opinions are organized under an "Opinions by Year" container
  (location 846113). Its child folders are the per-year containers
  (1972-present); each per-year container's children are the individual
  jeac_opinion content items.

    * List the year containers (folders):
        /api/data/fetch?loadContent=false&limit=100
            &parentLocationID=846113&classFilter=folder
    * List the opinions in a year (with full content fields):
        /api/data/fetch?loadContent=true&limit=200
            &parentLocationID={year_location}
            &sortClause[0]=opinion&sortClause[1]=disposition_date&sortClause[2]=DESC

  Each opinion item carries structured fields: opinion (the "YYYY-NN" number),
  canons, date_of_issue, subject, issue, facts, discussion, references, and
  migrated_html. The full opinion body is born-digital HTML in `migrated_html`
  (real text, no OCR); if that is empty the structured Issue / Facts /
  Discussion / References / Subject sections are assembled instead.

Strategy:
  Enumerate the year containers, then walk each year's opinions, extract the
  full text from migrated_html (de-tagged) with a structured-field fallback,
  and normalize. ~1,391 opinions from 1972 to present.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FL-JudicialEthics")

API = "https://flcourts-media.ccplatform.net/api/data/fetch"
# Ibexa location id of the "Opinions by Year" container on the JEAC siteaccess.
YEAR_PARENT_LOCATION = 846113
# Sort clauses reproduce the site's own call (opinion, then disposition_date DESC).
SORT = "sortClause%5B0%5D=opinion&sortClause%5B1%5D=disposition_date&sortClause%5B2%5D=DESC"

FRONTEND_HOST = "jeac.flcourts-frontend.ccplatform.net"
PUBLIC_HOST = "jeac.flcourts.gov"

# Structured sections (in reading order) used when migrated_html is empty.
SECTIONS = [
    ("issue", "ISSUE"),
    ("facts", "FACTS"),
    ("discussion", "DISCUSSION"),
    ("references", "REFERENCES"),
    ("subject", "SUBJECT"),
]


def _detag(raw_html: str) -> str:
    """Convert an HTML fragment to clean plain text (block tags -> newlines)."""
    if not raw_html:
        return ""
    body = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", "", raw_html)
    body = re.sub(r"(?i)<(br|/p|/div|/td|/tr|/li|/h[1-6])[^>]*>", "\n", body)
    body = re.sub(r"<[^>]+>", " ", body)
    body = _html.unescape(body).replace("\xa0", " ")
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in body.splitlines()]
    return "\n".join(ln for ln in lines if ln).strip()


def _field_html(fields: dict, key: str) -> str:
    """Return the html5 string of a rich-text field (or a plain string field)."""
    v = fields.get(key)
    if isinstance(v, dict):
        return v.get("html5") or ""
    return v or ""


def _opinion_date(fields: dict) -> str | None:
    doi = fields.get("date_of_issue")
    if isinstance(doi, dict):
        inner = doi.get("date")
        if isinstance(inner, dict):
            s = inner.get("date")  # "2021-08-05 00:00:00.000000"
            if s:
                m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
                if m:
                    y, mo, d = m.groups()
                    if "0000" not in (y,) and mo != "00" and d != "00":
                        return f"{y}-{mo}-{d}"
    return None


def _public_url(location_url: str) -> str:
    if not location_url:
        return f"https://{PUBLIC_HOST}/All-Opinions"
    return location_url.replace(FRONTEND_HOST, PUBLIC_HOST)


class FLJudicialEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
    def _get_json(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    data = json.loads(out.stdout.decode("utf-8", "replace"))
                    if data == ["Error"]:
                        logger.warning(f"API returned Error for {url}")
                    else:
                        return data
            except Exception as e:
                logger.warning(f"fetch failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_year_locations(self) -> list[int]:
        """Return every per-year container location id (folders), newest first."""
        url = (f"{API}?loadContent=false&limit=200"
               f"&parentLocationID={YEAR_PARENT_LOCATION}&classFilter=folder")
        data = self._get_json(url)
        if not isinstance(data, list):
            logger.error("could not list year containers")
            return []
        locs = []
        for it in data:
            try:
                locs.append(int(it["location"]["id"]))
            except (KeyError, TypeError, ValueError):
                continue
        return locs

    def _list_year_opinions(self, year_loc: int) -> list[dict]:
        url = (f"{API}?loadContent=true&limit=200"
               f"&parentLocationID={year_loc}&{SORT}")
        data = self._get_json(url)
        if not isinstance(data, list):
            return []
        return [it for it in data if isinstance(it, dict)]

    # -------------------------------------------------------- extraction
    def _extract(self, item: dict) -> dict | None:
        content = item.get("content") or {}
        fields = content.get("fields") or {}
        number = (fields.get("opinion") or content.get("name") or "").strip()
        if not number:
            return None

        text = _detag(_field_html(fields, "migrated_html"))
        if len(text) < 150:
            # Assemble from structured sections.
            parts = []
            for key, label in SECTIONS:
                sec = _detag(_field_html(fields, key))
                if sec:
                    parts.append(f"{label}\n{sec}")
            text = "\n\n".join(parts).strip()
        if len(text) < 100:
            return None

        canons = (fields.get("canons") or "").strip()
        date = _opinion_date(fields)
        if date is None:
            m = re.match(r"(\d{4})-", number)
            date = f"{m.group(1)}-01-01" if m else None

        loc_url = (item.get("location") or {}).get("url", "")
        subject = _detag(_field_html(fields, "subject"))
        return {
            "number": number,
            "canons": canons,
            "subject": subject[:1000] if subject else "",
            "text": text,
            "date": date,
            "url": _public_url(loc_url),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Florida JEAC advisory opinions...")
        locs = self._list_year_locations()
        if not locs:
            logger.error("API test FAILED: no year containers found")
            return False
        logger.info(f"  discovered {len(locs)} year containers")
        ok = 0
        for loc in locs[:3]:
            for item in self._list_year_opinions(loc)[:3]:
                rec = self._extract(item)
                if rec and len(rec["text"]) > 200:
                    logger.info(f"  Opinion {rec['number']} OK "
                                f"({len(rec['text'])} chars) date={rec['date']}")
                    ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        return {
            "_id": f"US/FL-JudicialEthics/{number}",
            "_source": "US/FL-JudicialEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Florida Judicial Ethics Advisory Committee",
            "title": f"Florida JEAC Opinion {number}",
            "canons": raw.get("canons") or None,
            "subject": raw.get("subject") or None,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-FL",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        locs = self._list_year_locations()
        seen: set[str] = set()
        emitted = 0
        for loc in locs:
            for item in self._list_year_opinions(loc):
                rec = self._extract(item)
                if not rec:
                    continue
                if rec["number"] in seen:
                    continue
                seen.add(rec["number"])
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
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/FL-JudicialEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FLJudicialEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
