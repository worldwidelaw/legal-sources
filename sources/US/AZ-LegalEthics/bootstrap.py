#!/usr/bin/env python3
"""
US/AZ-LegalEthics -- State Bar of Arizona — Legal (Attorney) Ethics Opinions

Fetches the full text of the Formal Ethics Opinions issued by the State Bar of
Arizona's (former) Committee on the Rules of Professional Conduct (and, for the
most recent opinions, the Arizona Supreme Court Attorney Ethics Advisory
Committee). Each opinion interprets the Arizona Rules of Professional Conduct
(Ethical Rules, "ER ...") and answers a specific inquiry about a lawyer's
professional-responsibility obligations = doctrine (the Bar's official written
interpretation of the attorney-conduct rules). Opinions span 1985-present.

Access (public JSON API, no CAPTCHA, no per-user auth):
  The azbar.org ethics-opinions page is a JS listing backed by a public REST
  API at https://api.azbar.org/. Every request carries a *static, public*
  credential that the page itself embeds in plain JavaScript
  (userid="publictools", a fixed password GUID, updatedBy="Hub"):

    GET /EthicsRules/OpinionSearch/ByYear/?Year={YYYY}
        -> {"Result": [{Id, OpinionNumber, Title, OpinionDate, Summary}, ...]}
    GET /EthicsRules/Opinion/?Id={id}
        -> {"Result": {Body (full HTML text), Title, OpinionNumber,
                       OpinionDate, Summary, Note, ...}}

  The credential trio is read live from the listing page's inline JS (with a
  hard-coded fallback) so the scraper keeps working if the Bar rotates it.

Strategy:
  Enumerate opinions year-by-year (1977..next year) via ByYear, collect Ids,
  then GET each opinion's detail and extract the full Body HTML as clean text.

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
import time
import html as _html
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
logger = logging.getLogger("legal-data-hunter.US.AZ-LegalEthics")

LISTING_URL = "https://www.azbar.org/for-legal-professionals/ethics/ethics-opinions/"
API = "https://api.azbar.org"
BYYEAR = API + "/EthicsRules/OpinionSearch/ByYear/?Year={year}"
OPINION = API + "/EthicsRules/Opinion/?Id={id}"

# Static, public credential embedded in the listing page's JavaScript (fallback).
DEFAULT_CREDS = {
    "userid": "publictools",
    "password": "12B631CC-5922-4EF8-8978-23CF2F32EA8D",
    "updatedBy": "Hub",
}

YEAR_MIN = 1977
YEAR_MAX = datetime.now(timezone.utc).year + 1


class AZLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._creds = None
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept": "application/json",
        })

    # ------------------------------------------------------------- creds
    def _api_creds(self) -> dict:
        if self._creds:
            return self._creds
        try:
            r = self._session.get(LISTING_URL, timeout=45)
            if r.status_code == 200:
                uid = re.search(r'setRequestHeader\("userid",\s*"([^"]+)"', r.text)
                pw = re.search(r'setRequestHeader\("password",\s*"([^"]+)"', r.text)
                if uid and pw:
                    self._creds = {"userid": uid.group(1),
                                   "password": pw.group(1),
                                   "updatedBy": "Hub"}
                    logger.info("  using live API credential from listing page")
                    return self._creds
        except Exception as e:
            logger.warning(f"could not read live credential: {e}")
        self._creds = dict(DEFAULT_CREDS)
        logger.info("  using fallback API credential")
        return self._creds

    # ---------------------------------------------------------------- http
    def _get_json(self, url: str) -> dict | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, headers=self._api_creds(), timeout=60)
                if r.status_code == 200 and r.text:
                    return r.json()
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_opinions(self) -> Generator[dict, None, None]:
        """Yield {Id, OpinionNumber, Title, OpinionDate} for every opinion."""
        seen: set = set()
        total = 0
        for year in range(YEAR_MAX, YEAR_MIN - 1, -1):
            data = self._get_json(BYYEAR.format(year=year))
            if not data or not data.get("IsSuccess"):
                continue
            rows = data.get("Result") or []
            for row in rows:
                oid = row.get("Id")
                if oid is None or oid in seen:
                    continue
                seen.add(oid)
                total += 1
                yield row
        logger.info(f"  listed {total} opinions across {YEAR_MIN}-{YEAR_MAX}")

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean_html(raw: str) -> str:
        if not raw or raw == "None":
            return ""
        soup = BeautifulSoup(raw, "html.parser")
        text = soup.get_text("\n", strip=True)
        text = _html.unescape(text).replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _parse_date(dt: str) -> str | None:
        if not dt:
            return None
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", dt)
        return m.group(0) if m else None

    def _fetch_one(self, row: dict) -> dict | None:
        oid = row.get("Id")
        data = self._get_json(OPINION.format(id=oid))
        if not data or not data.get("IsSuccess"):
            return None
        r = data.get("Result") or {}
        body = self._clean_html(r.get("Body", ""))
        if len(body) < 120:
            # Fall back to the summary if the body is empty for an older opinion.
            body = self._clean_html(r.get("Summary", ""))
        if len(body) < 120:
            return None
        note = self._clean_html(r.get("Note", ""))
        text = body if not note else f"{body}\n\nNOTE: {note}"
        number = (r.get("OpinionNumber") or row.get("OpinionNumber") or str(oid)).strip()
        title = (r.get("Title") or row.get("Title") or number).strip()
        return {
            "id": oid,
            "opinion_number": number,
            "title": title,
            "text": text,
            "summary": self._clean_html(r.get("Summary", "")),
            "date": self._parse_date(r.get("OpinionDate") or row.get("OpinionDate", "")),
            "url": f"{LISTING_URL}?V=Opinions&oid={oid}",
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing State Bar of Arizona Legal Ethics Opinions...")
        rows = []
        for row in self._list_opinions():
            rows.append(row)
            if len(rows) >= 5:
                break
        if not rows:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for row in rows:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  Opinion {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        slug = re.sub(r"[^A-Za-z0-9]+", "-", num).strip("-") or str(raw["id"])
        return {
            "_id": f"US/AZ-LegalEthics/{slug}",
            "_source": "US/AZ-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "State Bar of Arizona (Committee on the Rules of Professional Conduct)",
            "title": raw["title"],
            "text": raw["text"],
            "summary": raw.get("summary") or None,
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-AZ",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._list_opinions():
            rec = self._fetch_one(row)
            if not rec:
                logger.warning(f"  no text for Id={row.get('Id')}, skipping")
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
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/AZ-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AZLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
