#!/usr/bin/env python3
"""
US/NJ-EthicsDecisions -- New Jersey State Ethics Commission --
Final Decisions.

Fetches the full text of the final agency actions of the New Jersey State
Ethics Commission (SEC) under the Conflicts of Interest Law (N.J.S.A.
52:13D-12 et seq.) and Executive Order 14: final orders, consent orders,
penalty-waiver determinations, casino/cannabis post-employment waivers,
Section 19 and EO-14 exceptions, advisory opinions and adopted codes of
ethics. Case-specific final agency actions on a named respondent/requester
= case_law; advisory opinions / codes of ethics / rule adoptions = doctrine.

NOTE: this is the New Jersey *State* Ethics Commission (Dept. of Law &
Public Safety), distinct from the NJ *School* Ethics Commission
(US/NJ-SchoolEthics), US/NJ-PERC, US/NJ-OAL and US/NJ-AGOpinions.

Access (no CAPTCHA, no auth, no JavaScript engine needed):
  The official NJ open-data (Socrata) dataset

      https://data.nj.gov/resource/54br-q95u.json   (id 54br-q95u,
      "State Ethics Commission Final Decisions")

  returns every decision (~695) in a single call. Each row carries
  file_type.url = a direct born-digital PDF on
  https://nj.gov/ethics/docs/final/, plus type, firstname/lastname,
  agency_department, case_number, final_agency_action, year, month.

Full text:
  Each PDF is born-digital (clean text layer; OCR fallback for any scan)
  and extracted via the shared common.pdf_extract backend. The decision
  date is parsed from the "Month DD, YYYY" line near the top of the PDF,
  falling back to the dataset year/month (portal posting date).

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
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
from typing import Generator, Optional

import requests
from requests.utils import requote_uri

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NJ-EthicsDecisions")

DATASET_URL = "https://data.nj.gov/resource/54br-q95u.json"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

# decision categories that are general doctrine rather than a case_law
# determination on a named party
DOCTRINE_TYPES = {
    "advisory opinion", "code of ethics", "rule adoption", "request-for-advice",
}

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
DATE_RE = re.compile(
    r"(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})", re.I
)


def _iso_date(text: str) -> Optional[str]:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = MONTHS[m.group(1).lower()]
    d = int(m.group(2))
    y = int(m.group(3))
    if 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def _slug_from_url(url: str) -> str:
    return Path(url.split("?")[0]).stem


class NJEthicsDecisionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    # ---------------------------------------------------------------- http
    def _get(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                return self.session.get(url, timeout=60, allow_redirects=True)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _collect_index(self) -> list[dict]:
        r = self._get(f"{DATASET_URL}?$limit=1000")
        if r is None or r.status_code != 200:
            logger.error("Could not fetch Socrata dataset")
            return []
        try:
            data = r.json()
        except Exception as e:
            logger.error(f"Bad JSON from dataset: {e}")
            return []
        rows: list[dict] = []
        seen: set[str] = set()
        for rec in data:
            ft = rec.get("file_type") or {}
            url = (ft.get("url") or "").strip()
            if not url or ".pdf" not in url.lower():
                # skip .zip archives and rows without a document
                continue
            pdf_url = requote_uri(url)
            slug = _slug_from_url(pdf_url)
            if slug in seen:
                continue
            seen.add(slug)
            first = (rec.get("firstname") or "").strip()
            last = (rec.get("lastname") or "").strip()
            party = " ".join(p for p in (first, last) if p).strip()
            rows.append({
                "slug": slug,
                "pdf_url": pdf_url,
                "decision_type": (rec.get("type") or "").strip(),
                "case_number": (rec.get("case_number") or "").strip(),
                "party": party,
                "agency_department": (rec.get("agency_department") or "").strip(),
                "final_agency_action": (rec.get("final_agency_action") or "").strip(),
                "year": (rec.get("year") or "").strip(),
                "month": (rec.get("month") or "").strip(),
            })
        logger.info(f"Dataset: {len(rows)} decision PDFs")
        return rows

    # ------------------------------------------------------------- fetch1
    def _fetch_one(self, row: dict) -> Optional[dict]:
        r = self._get(row["pdf_url"])
        if r is None or r.status_code != 200 or not r.content:
            logger.warning(f"  {row.get('slug')}: PDF download failed — skipped")
            return None
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {row.get('slug')}: not a PDF — skipped")
            return None
        text = (_pdf_extract_bytes(r.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {row.get('slug')}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        out["date"] = _iso_date(text) or self._fallback_date(row)
        out["pdf_final_url"] = r.url
        return out

    @staticmethod
    def _fallback_date(row: dict) -> Optional[str]:
        y = row.get("year")
        m = row.get("month")
        if y and y.isdigit():
            mo = int(m) if (m and m.isdigit() and 1 <= int(m) <= 12) else 1
            return f"{int(y):04d}-{mo:02d}-01"
        return None

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec.get('slug')} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NJ State Ethics Commission final decisions...")
        rows = self._collect_index()
        if len(rows) < 200:
            logger.error(f"API test FAILED: dataset too small ({len(rows)})")
            return False
        ok = 0
        for row in rows[:5]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec.get('slug')} OK ({len(rec['text'])} chars)")
                ok += 1
        if ok >= 3:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        dtype = raw.get("decision_type") or ""
        is_doctrine = dtype.strip().lower() in DOCTRINE_TYPES
        rtype = "doctrine" if is_doctrine else "case_law"

        cn = raw.get("case_number") or ""
        party = raw.get("party") or ""
        action = raw.get("final_agency_action") or ""
        title_bits = [b for b in (party, dtype, action) if b]
        title = " — ".join(dict.fromkeys(title_bits)) or raw["slug"]
        if cn and cn != "000-00":
            title = f"{title} (No. {cn})"

        return {
            "_id": f"US/NJ-EthicsDecisions/{raw['slug']}",
            "_source": "US/NJ-EthicsDecisions",
            "_type": rtype,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": cn or None,
            "decision_type": dtype or None,
            "party": party or None,
            "agency_department": raw.get("agency_department") or None,
            "final_agency_action": action or None,
            "issuer": "New Jersey State Ethics Commission",
            "title": f"NJ State Ethics Commission — {title}",
            "text": raw["text"],
            "url": raw.get("pdf_final_url") or raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NJ",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NJ-EthicsDecisions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NJEthicsDecisionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
