#!/usr/bin/env python3
"""
SI/DKOM -- Slovenian National Review Commission for Public Procurement
          (Državna revizijska komisija za revizijo postopkov oddaje javnih naročil)

Fetches the full-text decisions (odločitve) of the DKOM -- Slovenia's independent
quasi-judicial body that decides legal-protection (revizija) complaints in public
procurement procedures under the ZPVPJN. Its decisions (Sklep / Odločitev) are
binding, administrative-jurisdictional case law.

Strategy:
  - The listing page https://www.dkom.si/sl/revizijske-zadeve/odlocitve-dkom/
    embeds ALL decision IDs (timestamp-style, e.g. 2019020609094588) in one page,
    newest first -- no pagination needed.
  - Each decision page /sl/revizijske-zadeve/odlocitve-dkom/{id}/ is born-digital
    HTML with:
      <h1>                        -> title (case no + purchaser)
      <span class="date">         -> "Številka: 018-188/2018-6"  (case number)
      <span class="date">         -> "Datum sprejema: 25. 1. 2019" (decision date)
      <div class="decision-content"> -> full decision text (Sklep ... izrek ... obrazložitev)

Data Coverage:
  - ~7,500 DKOM decisions
  - Document types: Sklep (decision), Odločitev, Sklep o zavrženju, etc.
  - Public procurement legal-protection case law

Usage:
  python bootstrap.py bootstrap            # Full initial pull (streams to data/records.jsonl)
  python bootstrap.py bootstrap-fast       # Alias for full bootstrap (fleet runner)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental update (recent decisions)
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from bs4 import BeautifulSoup
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SI.DKOM")

BASE_URL = "https://www.dkom.si"
LISTING_URL = f"{BASE_URL}/sl/revizijske-zadeve/odlocitve-dkom/"
DECISION_URL = f"{BASE_URL}/sl/revizijske-zadeve/odlocitve-dkom/{{id}}/"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Slovenian month names are not used in the metadata dates (they are numeric,
# e.g. "25. 1. 2019"), so a simple numeric parser suffices.
_DATE_RE = re.compile(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})")


class DKOMScraper(BaseScraper):
    """
    Scraper for SI/DKOM -- Slovenian public procurement review decisions.
    Country: SI
    URL: https://www.dkom.si/

    Data types: case_law
    Auth: none (Open Government Data -- podatki.gov.si dataset)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.7",
            "Accept-Language": "sl,en;q=0.9",
        })

    # ── discovery ────────────────────────────────────────────────────
    def _list_decision_ids(self, timeout: int = 60) -> List[str]:
        """Fetch the listing page and return all decision IDs, newest first."""
        self.rate_limiter.wait()
        resp = self.session.get(LISTING_URL, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        ids = re.findall(r"odlocitve-dkom/(\d{10,})/", resp.text)
        # De-duplicate while preserving order (listing is newest-first).
        seen = set()
        ordered = []
        for i in ids:
            if i not in seen:
                seen.add(i)
                ordered.append(i)
        if not ordered:
            raise RuntimeError(
                "DKOM listing page returned 0 decision IDs -- layout change or "
                "IP block on dkom.si"
            )
        logger.info(f"Discovered {len(ordered)} DKOM decision IDs")
        return ordered

    # ── per-document fetch ───────────────────────────────────────────
    def _fetch_document(self, decision_id: str, timeout: int = 45) -> Optional[Dict[str, Any]]:
        """Fetch one decision page and extract metadata + full text."""
        url = DECISION_URL.format(id=decision_id)
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            resp.encoding = "utf-8"
        except Exception as e:
            logger.warning(f"Failed to fetch decision {decision_id}: {e}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        art = soup.select_one("article")
        if art is None:
            return None

        title = ""
        h1 = art.find("h1")
        if h1:
            title = h1.get_text(" ", strip=True)

        # The two <span class="date"> carry "Številka: ..." and "Datum sprejema: ..."
        case_number = ""
        decision_date = ""
        for span in art.select("span.date"):
            txt = span.get_text(" ", strip=True)
            if "Številka" in txt or "tevilka" in txt:
                case_number = re.sub(r"^[^:]*:\s*", "", txt).strip()
                # Collapse stray internal whitespace in the case number
                # (source occasionally emits "018- 098/2026-4").
                case_number = re.sub(r"\s+", "", case_number)
            elif "Datum" in txt:
                decision_date = re.sub(r"^[^:]*:\s*", "", txt).strip()

        content = art.select_one("div.decision-content")
        if content is None:
            return None

        full_text = self._clean_text(content.get_text("\n"))
        # Strip the trailing print-page chrome if present.
        full_text = re.sub(r"\s*Natisni stran\s*$", "", full_text).strip()

        if not full_text or len(full_text) < 80:
            logger.warning(f"Decision {decision_id}: insufficient text "
                           f"({len(full_text)} chars)")
            return None

        # Decision type = first non-empty heading line of the body (Sklep / Odločitev ...)
        decision_type = ""
        for line in full_text.split("\n"):
            line = line.strip()
            if line:
                decision_type = line if len(line) <= 60 else line.split(".")[0][:60]
                break

        return {
            "decision_id": decision_id,
            "case_number": case_number,
            "decision_date": decision_date,
            "decision_type": decision_type,
            "title": title or case_number,
            "full_text": full_text,
            "url": url,
        }

    @staticmethod
    def _clean_text(text: str) -> str:
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ── framework hooks ──────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all DKOM decisions (newest first)."""
        ids = self._list_decision_ids()
        total = len(ids)
        for n, decision_id in enumerate(ids, 1):
            if n % 250 == 0:
                logger.info(f"Progress: {n}/{total}")
            doc = self._fetch_document(decision_id)
            if doc:
                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions with a decision date newer than `since`.

        The listing is newest-first, so we can stop after crossing the cutoff.
        """
        ids = self._list_decision_ids()
        for decision_id in ids:
            doc = self._fetch_document(decision_id)
            if not doc:
                continue
            iso = self._parse_date(doc.get("decision_date", ""))
            if iso:
                try:
                    d = datetime.strptime(iso, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if d < since:
                        # crossed the cutoff; listing is chronological, stop.
                        break
                except ValueError:
                    pass
            yield doc

    @staticmethod
    def _parse_date(raw_date: str) -> str:
        m = _DATE_RE.search(raw_date or "")
        if not m:
            return ""
        day, month, year = m.groups()
        try:
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except ValueError:
            return ""

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision into the standard schema (with full text)."""
        decision_id = raw.get("decision_id", "")
        case_number = raw.get("case_number", "")
        date_iso = self._parse_date(raw.get("decision_date", ""))

        title = raw.get("title") or case_number or f"DKOM {decision_id}"

        return {
            # Required base fields
            "_id": f"DKOM-{decision_id}",
            "_source": "SI/DKOM",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get("full_text", ""),
            "date": date_iso,
            "url": raw.get("url", ""),
            # Source-specific fields
            "case_number": case_number,
            "decision_type": raw.get("decision_type", ""),
            "language": "sl",
        }

    # ── connectivity test ────────────────────────────────────────────
    def test_api(self):
        print("Testing SI/DKOM (Državna revizijska komisija)...")
        print("\n1. Listing page...")
        ids = self._list_decision_ids()
        print(f"   Decision IDs found: {len(ids)}")
        print(f"   Newest IDs: {ids[:3]}")

        print("\n2. Fetching newest decision...")
        doc = self._fetch_document(ids[0])
        if doc:
            print(f"   Case number: {doc.get('case_number')}")
            print(f"   Date:        {doc.get('decision_date')} -> "
                  f"{self._parse_date(doc.get('decision_date',''))}")
            print(f"   Type:        {doc.get('decision_type')}")
            print(f"   Title:       {doc.get('title')[:70]}")
            print(f"   Text length: {len(doc.get('full_text',''))} chars")
            print(f"   Preview:     {doc.get('full_text','')[:180]}...")
        else:
            print("   ERROR: could not fetch decision")
        print("\nAPI test complete!")


def main():
    scraper = DKOMScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api] "
              "[--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        # bootstrap-fast is an alias for the full path (fleet runner invokes it).
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: "
                  f"{stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
