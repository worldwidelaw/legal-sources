#!/usr/bin/env python3
"""
TH/SEC -- Thailand Securities and Exchange Commission Enforcement Actions

Fetches enforcement actions from the Thailand SEC portal via its internal
REST API. Records include criminal complaints, administrative fines, civil
actions, cases settled, and administrative orders with full summarized facts.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TH.SEC")

BASE_URL = "https://market.sec.or.th"
ENFORCE_PAGE = f"{BASE_URL}/public/idisc/en/Enforce/"
API_URL = f"{BASE_URL}/public/idisc/api/Enforce/GetEnforces"
DELAY = 2.0

ENFORCEMENT_TYPES = [
    "CRIMINALCOMPLAINTS",
    "CASESSETTLED",
    "ADMINISTRATIVESANCTIONS",
    "ADMINISTRATIVEORDERS",
    "ADMINISTRATIVESANCTIONS2",
    "CIVILACTION",
    "DISCLOSUREOFACTIONS",
]


def _parse_date(date_str: str) -> Optional[str]:
    """Parse dd/mm/yyyy date to ISO 8601."""
    if not date_str or not date_str.strip():
        return None
    try:
        parts = date_str.strip().split("/")
        if len(parts) == 3:
            d, m, y = parts
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except (ValueError, IndexError):
        pass
    return None


def _make_id(date_str: str, name: str, enforcement_type: str, idx: int) -> str:
    """Generate a stable document ID from record fields."""
    raw = f"{date_str}|{name}|{enforcement_type}|{idx}"
    h = hashlib.md5(raw.encode()).hexdigest()[:10]
    return f"TH_SEC_{h}"


class THSECScraper(BaseScraper):
    """Scraper for Thailand SEC enforcement actions."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
            },
        )
        self._rtk = None

    def _get_rtk(self) -> str:
        """Fetch CSRF token from the enforcement page."""
        if self._rtk:
            return self._rtk
        resp = self.http.get(ENFORCE_PAGE, timeout=30)
        resp.raise_for_status()
        m = re.search(r'data-rtk="([^"]+)"', resp.text)
        if not m:
            raise RuntimeError("Could not extract rtk token from enforcement page")
        self._rtk = m.group(1)
        logger.info("Obtained rtk token: %s...", self._rtk[:20])
        return self._rtk

    def _fetch_query(self, year: int, vio_type: str = "ALL") -> list:
        """Fetch enforcement records for a given year and violation type."""
        from bs4 import BeautifulSoup

        rtk = self._get_rtk()
        payload = {
            "rtk": rtk,
            "Lang": "en",
            "QueryType": "ALL",
            "OffenderFlag": "",
            "OffenderTxt": "",
            "VioTypeTxt": vio_type,
            "DateFlag": "Y",
            "StartDateTxt": f"01/01/{year}",
            "EndDateTxt": f"31/12/{year}",
            "FreeSearchFlag": "",
            "FreeSearchTxt": "",
        }

        resp = self.http.post(API_URL, json_data=payload, timeout=60)
        resp.raise_for_status()

        html = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text
        if not html or html == '""' or html == "":
            return [], 0

        if isinstance(html, str) and html.startswith('"') and html.endswith('"'):
            html = json.loads(html)

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.find_all("tr")
        if len(rows) < 2:
            return [], 0

        # Extract reported count
        reported = 0
        heading = soup.find(class_="card-heading")
        if heading:
            m = re.search(r"(\d+)\s*record", heading.get_text())
            if m:
                reported = int(m.group(1))

        records = []
        for i, row in enumerate(rows[1:], start=1):
            tds = row.find_all("td")
            if len(tds) < 7:
                continue

            date_raw = tds[1].get_text(strip=True)
            name = tds[2].get_text(strip=True)
            relevant_law = tds[3].get_text(strip=True)
            facts = tds[4].get_text(strip=True)
            sec_news = tds[5].get_text(strip=True)
            enforcement_type = tds[6].get_text(strip=True)
            details = tds[7].get_text(strip=True) if len(tds) > 7 else ""
            remark = tds[8].get_text(strip=True) if len(tds) > 8 else ""

            if not facts or len(facts) < 20:
                continue

            iso_date = _parse_date(date_raw)
            doc_id = _make_id(date_raw, name, enforcement_type, i)

            records.append({
                "_id": doc_id,
                "date_raw": date_raw,
                "date": iso_date,
                "name": name,
                "relevant_law": relevant_law,
                "facts": facts,
                "sec_news_ref": sec_news,
                "enforcement_type": enforcement_type,
                "details": details,
                "remark": remark,
            })

        return records, reported

    def _fetch_year(self, year: int) -> list:
        """Fetch all enforcement records for a given year, splitting by type if needed."""
        records, reported = self._fetch_query(year, "ALL")
        logger.info("Year %d: %d reported, %d parsed (ALL)", year, reported, len(records))

        # If reported > parsed, the API truncated results — fetch per type
        if reported > len(records) + 5:
            logger.info("Year %d: splitting by enforcement type to get all records", year)
            seen_ids = set()
            all_records = []
            for vtype in ENFORCEMENT_TYPES:
                time.sleep(DELAY)
                type_records, type_reported = self._fetch_query(year, vtype)
                for rec in type_records:
                    if rec["_id"] not in seen_ids:
                        seen_ids.add(rec["_id"])
                        all_records.append(rec)
                logger.info("Year %d/%s: %d records", year, vtype, len(type_records))
            logger.info("Year %d: %d total after split", year, len(all_records))
            return all_records

        return records

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all enforcement records, year by year."""
        current_year = datetime.now().year
        start_year = current_year if sample else 2000
        end_year = current_year

        total = 0
        for year in range(end_year, start_year - 1, -1):
            try:
                records = self._fetch_year(year)
                for rec in records:
                    yield rec
                    total += 1
                    if sample and total >= 15:
                        return
            except Exception as e:
                logger.error("Error fetching year %d: %s", year, e)
                continue
            time.sleep(DELAY)

        logger.info("Total records fetched: %d", total)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch records updated since a given date."""
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            since_dt = datetime(2020, 1, 1)

        current_year = datetime.now().year
        for year in range(current_year, since_dt.year - 1, -1):
            try:
                records = self._fetch_year(year)
                for rec in records:
                    if rec.get("date") and rec["date"] >= since[:10]:
                        yield rec
            except Exception as e:
                logger.error("Error fetching year %d: %s", year, e)
                continue
            time.sleep(DELAY)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to the standard schema."""
        # Build full text from facts + details + remark
        text_parts = [raw.get("facts", "")]
        if raw.get("details"):
            text_parts.append(f"\nOutcome: {raw['details']}")
        if raw.get("remark"):
            text_parts.append(f"\nRemark: {raw['remark']}")
        full_text = "\n".join(text_parts).strip()

        # Title combines enforcement type and name
        title = f"{raw.get('enforcement_type', 'Enforcement')}: {raw.get('name', 'Unknown')}"

        return {
            "_id": raw["_id"],
            "_source": "TH/SEC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": raw.get("date"),
            "url": ENFORCE_PAGE,
            "enforcement_type": raw.get("enforcement_type", ""),
            "relevant_law": raw.get("relevant_law", ""),
            "sec_news_ref": raw.get("sec_news_ref", ""),
            "name": raw.get("name", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="TH/SEC bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = THSECScraper()

    if args.command == "test":
        rtk = scraper._get_rtk()
        print(f"OK -- rtk token obtained: {rtk[:20]}...")
        records = scraper._fetch_year(datetime.now().year)
        print(f"Current year records: {len(records)}")
        if records:
            print(f"  Sample: {records[0].get('name', '?')} ({records[0].get('enforcement_type', '?')})")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
