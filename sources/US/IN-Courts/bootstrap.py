#!/usr/bin/env python3
"""
US/IN-Courts -- Indiana Supreme Court & Court of Appeals Opinions

Fetches case law from the Harvard Caselaw Access Project (CAP) static bulk data.
Uses static.case.law JSON endpoint which provides full opinion text directly
(no PDF extraction needed).

Courts covered:
  - Indiana Supreme Court (ind): 275 volumes, 1848-present
  - Indiana Court of Appeals (ind-app): 182 volumes

Strategy:
  1. Fetch VolumesMetadata.json for each reporter to get volume list
  2. For each volume, fetch CasesMetadata.json for case index
  3. Download individual case JSON files containing full text in casebody.opinions
  4. Normalize into standard schema

Data source: https://static.case.law/ind/ and https://static.case.law/ind-app/
License: CC0 1.0 (Public Domain)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (all volumes)
  python bootstrap.py update --since YYYY-MM-DD  # Filter by date
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IN-Courts")

# CAP static data endpoints
REPORTERS = {
    "ind": {
        "base_url": "https://static.case.law/ind",
        "court_name": "Indiana Supreme Court",
        "court_abbr": "INSC",
    },
    "ind-app": {
        "base_url": "https://static.case.law/ind-app",
        "court_name": "Indiana Court of Appeals",
        "court_abbr": "INCA",
    },
}

USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"


class HTMLTextExtractor(HTMLParser):
    """Strip HTML tags from text."""
    def __init__(self):
        super().__init__()
        self.result = []
    def handle_data(self, data):
        self.result.append(data)
    def get_text(self):
        return "".join(self.result)


def strip_html(html_str: str) -> str:
    """Remove HTML tags from a string."""
    if not html_str or "<" not in html_str:
        return html_str or ""
    extractor = HTMLTextExtractor()
    try:
        extractor.feed(html_str)
        return extractor.get_text()
    except Exception:
        return re.sub(r"<[^>]+>", "", html_str)


class INCourtsScraper(BaseScraper):
    """
    Scraper for US/IN-Courts — Indiana Supreme Court & Court of Appeals.
    Uses Harvard CAP static.case.law bulk JSON data with full text.
    """

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def _get_volumes(self, reporter_key: str) -> List[str]:
        """Get list of volume number strings for a reporter, sorted numerically."""
        reporter = REPORTERS[reporter_key]
        url = f"{reporter['base_url']}/VolumesMetadata.json"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            volumes_data = resp.json()
            vol_nums = [str(v["volume_number"]) for v in volumes_data]
            return sorted(vol_nums, key=lambda x: int(x))
        except Exception as e:
            logger.error(f"Failed to fetch volumes for {reporter_key}: {e}")
            return []

    def _get_cases_index(self, reporter_key: str, volume: int) -> List[Dict]:
        """Get case index for a specific volume."""
        reporter = REPORTERS[reporter_key]
        url = f"{reporter['base_url']}/{volume}/CasesMetadata.json"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to fetch cases index for {reporter_key} vol {volume}: {e}")
            return []

    def _get_case(self, reporter_key: str, volume: int, file_name: str) -> Optional[Dict]:
        """Download a single case JSON file with full text."""
        reporter = REPORTERS[reporter_key]
        # file_name from CasesMetadata.json is like "0001-01", need to add .json
        if not file_name.endswith(".json"):
            file_name = f"{file_name}.json"
        url = f"{reporter['base_url']}/{volume}/cases/{file_name}"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to fetch case {file_name} from vol {volume}: {e}")
            return None

    def _extract_full_text(self, casebody: Dict) -> str:
        """Extract full opinion text from casebody."""
        if not casebody:
            return ""

        parts = []

        # Head matter (parties, attorneys, etc.)
        head_matter = casebody.get("head_matter", "")
        if head_matter:
            parts.append(strip_html(head_matter))

        # Opinion text (main content)
        opinions = casebody.get("opinions", [])
        for opinion in opinions:
            op_type = opinion.get("type", "")
            author = opinion.get("author", "")
            text = opinion.get("text", "")

            if text:
                clean_text = strip_html(text)
                if op_type and op_type != "majority":
                    header = f"[{op_type.upper()}]"
                    if author:
                        header += f" ({author})"
                    parts.append(f"\n{header}\n{clean_text}")
                else:
                    if author:
                        parts.append(f"({author})\n{clean_text}")
                    else:
                        parts.append(clean_text)

        full_text = "\n\n".join(parts)
        # Clean up excessive whitespace
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        return full_text.strip()

    def _process_case(self, case_data: Dict, reporter_key: str) -> Optional[Dict[str, Any]]:
        """Process a full case JSON into a raw record."""
        casebody = case_data.get("casebody")
        if not casebody:
            return None

        text = self._extract_full_text(casebody)
        if not text or len(text) < 50:
            return None

        reporter = REPORTERS[reporter_key]
        citations = case_data.get("citations", [])
        citation_str = ""
        if citations:
            cite = citations[0]
            citation_str = f"{cite.get('cite', '')}"

        court_info = case_data.get("court", {})

        return {
            "cap_id": case_data.get("id"),
            "reporter_key": reporter_key,
            "case_name": case_data.get("name", ""),
            "case_name_short": case_data.get("name_abbreviation", ""),
            "decision_date": case_data.get("decision_date"),
            "docket_number": case_data.get("docket_number", ""),
            "court_id": reporter_key,
            "court_name": court_info.get("name", reporter["court_name"]),
            "citation": citation_str,
            "first_page": case_data.get("first_page"),
            "last_page": case_data.get("last_page"),
            "text": text,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Indiana opinions from CAP static data."""
        for reporter_key, reporter_info in REPORTERS.items():
            logger.info(f"Fetching volumes for {reporter_info['court_name']}...")
            volumes = self._get_volumes(reporter_key)
            logger.info(f"Found {len(volumes)} volumes for {reporter_key}")

            for vol_idx, volume in enumerate(volumes):
                logger.info(f"Processing {reporter_key} volume {volume} ({vol_idx+1}/{len(volumes)})...")
                cases_index = self._get_cases_index(reporter_key, volume)
                time.sleep(0.5)

                for case_meta in cases_index:
                    file_name = case_meta.get("file_name", "")
                    if not file_name:
                        continue

                    time.sleep(0.5)
                    case_data = self._get_case(reporter_key, volume, file_name)
                    if not case_data:
                        continue

                    raw = self._process_case(case_data, reporter_key)
                    if raw:
                        yield raw

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch opinions decided after a given date."""
        if not since:
            from datetime import timedelta
            since = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

        # For updates, only check the most recent volumes
        for reporter_key, reporter_info in REPORTERS.items():
            logger.info(f"Checking recent volumes for {reporter_info['court_name']}...")
            volumes = self._get_volumes(reporter_key)
            # Check last 5 volumes for recent cases
            recent_volumes = volumes[-5:] if len(volumes) > 5 else volumes

            for volume in recent_volumes:
                cases_index = self._get_cases_index(reporter_key, volume)
                time.sleep(0.5)

                for case_meta in cases_index:
                    decision_date = case_meta.get("decision_date", "")
                    if decision_date and decision_date >= since:
                        file_name = case_meta.get("file_name", "")
                        if not file_name:
                            continue

                        time.sleep(0.5)
                        case_data = self._get_case(reporter_key, volume, file_name)
                        if not case_data:
                            continue

                        raw = self._process_case(case_data, reporter_key)
                        if raw:
                            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw case record into the standard schema."""
        reporter_key = raw.get("reporter_key", "ind")
        reporter = REPORTERS.get(reporter_key, REPORTERS["ind"])
        cap_id = raw.get("cap_id", "")
        doc_id = f"US-IN-{reporter['court_abbr']}-{cap_id}"

        return {
            "_id": doc_id,
            "_source": "US/IN-Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("case_name_short") or raw.get("case_name", ""),
            "text": raw.get("text", ""),
            "date": raw.get("decision_date"),
            "url": f"https://static.case.law/{reporter_key}/",
            "case_number": raw.get("docket_number", ""),
            "court": raw.get("court_name", reporter["court_name"]),
            "citation": raw.get("citation", ""),
            "jurisdiction": "US-IN",
            "first_page": raw.get("first_page"),
            "last_page": raw.get("last_page"),
        }

    def test_connection(self) -> bool:
        """Test connectivity to static.case.law."""
        try:
            for reporter_key in REPORTERS:
                volumes = self._get_volumes(reporter_key)
                logger.info(f"{reporter_key}: {len(volumes)} volumes available")
                if not volumes:
                    return False
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/IN-Courts data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample for validation",
    )
    parser.add_argument(
        "--since",
        help="ISO date for incremental updates (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full bootstrap (all volumes)",
    )
    args = parser.parse_args()

    scraper = INCourtsScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0
        target = 15 if args.sample else 999999

        # For sample mode, just fetch from the latest volume of each reporter
        if args.sample:
            for reporter_key, reporter_info in REPORTERS.items():
                if count >= target:
                    break
                logger.info(f"Sampling from {reporter_info['court_name']}...")
                volumes = scraper._get_volumes(reporter_key)
                if not volumes:
                    continue

                # Use the latest volume
                vol = volumes[-1]
                cases_index = scraper._get_cases_index(reporter_key, vol)
                time.sleep(0.5)

                for case_meta in cases_index:
                    if count >= target:
                        break

                    file_name = case_meta.get("file_name", "")
                    if not file_name:
                        continue

                    time.sleep(0.5)
                    case_data = scraper._get_case(reporter_key, vol, file_name)
                    if not case_data:
                        continue

                    raw = scraper._process_case(case_data, reporter_key)
                    if not raw:
                        continue

                    record = scraper.normalize(raw)
                    safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
                    out_path = sample_dir / f"{safe_id}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(record, f, ensure_ascii=False, indent=2)

                    text_len = len(record.get("text", ""))
                    logger.info(
                        f"[{count + 1}] {record['_id']}: "
                        f"{record['title'][:60]} ({text_len} chars)"
                    )
                    count += 1
        else:
            gen = scraper.fetch_all()
            for raw in gen:
                record = scraper.normalize(raw)
                safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
                out_path = sample_dir / f"{safe_id}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                text_len = len(record.get("text", ""))
                logger.info(
                    f"[{count + 1}] {record['_id']}: "
                    f"{record['title'][:60]} ({text_len} chars)"
                )
                count += 1

                if count >= target:
                    break

        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        gen = scraper.fetch_updates(since=args.since)
        for raw in gen:
            record = scraper.normalize(raw)
            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        print(f"\nUpdate complete: {count} records")


if __name__ == "__main__":
    main()
