#!/usr/bin/env python3
"""
FI/FinlexLabourCourt — Finland Labour Court Decisions (Työtuomioistuin)

Fetches case law decisions with full text from the Finlex Open Data REST API.
Returns Akoma Ntoso XML which is parsed for full text, metadata, and keywords.

~6,700 decisions from 1970 to present.

Strategy:
  - List decisions via REST API: /doc/labour-court-decision/list
  - Fetch individual Akoma Ntoso XML per decision
  - Parse XML for full text (judgmentBody), title, date, keywords

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch 12 sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.FinlexLabourCourt")

API_BASE = "https://opendata.finlex.fi/finlex/avoindata/v1"
DOC_TYPE = "labour-court-decision"
SOURCE_ID = "FI/FinlexLabourCourt"


class FinlexLabourCourtScraper(BaseScraper):
    """Scraper for FI/FinlexLabourCourt — Finnish Labour Court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json, application/xml, text/xml",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    wait = 15 * (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _list_decisions(self, page: int = 1) -> List[Dict]:
        """List decisions from the API (max 10 per page)."""
        url = f"{API_BASE}/akn/fi/doc/{DOC_TYPE}/list?format=json&page={page}&limit=10"
        resp = self._request(url)
        if resp is None:
            return []
        try:
            return resp.json()
        except Exception:
            return []

    def _fetch_decision_xml(self, year: str, number: str) -> Optional[str]:
        """Fetch Akoma Ntoso XML for a single decision."""
        url = f"{API_BASE}/akn/fi/doc/{DOC_TYPE}/{year}/{number}/fin@"
        resp = self._request(url, timeout=30)
        if resp is None:
            return None
        return resp.text

    def _parse_akn_xml(self, xml_text: str) -> Dict[str, str]:
        """Parse Akoma Ntoso XML to extract title, text, date, keywords, etc."""
        result = {"title": "", "text": "", "date": "", "number": "", "year": "",
                  "ecli": "", "keywords": [], "diary_number": ""}

        # Title from docTitle
        title_m = re.search(r"<docTitle[^>]*>(.*?)</docTitle>", xml_text, re.DOTALL)
        if title_m:
            result["title"] = re.sub(r"<[^>]+>", "", title_m.group(1)).strip()

        # Fallback title from shortTitle
        if not result["title"]:
            short_m = re.search(r"<shortTitle[^>]*>(.*?)</shortTitle>", xml_text, re.DOTALL)
            if short_m:
                result["title"] = re.sub(r"<[^>]+>", "", short_m.group(1)).strip()

        # Date issued
        date_m = re.search(r'FRBRdate date="([^"]+)" name="dateIssued"', xml_text)
        if date_m:
            result["date"] = date_m.group(1)

        # Number
        num_m = re.search(r'FRBRnumber value="([^"]+)"', xml_text)
        if num_m:
            result["number"] = num_m.group(1)

        # Year from URI
        year_m = re.search(rf"/{DOC_TYPE}/(\d{{4}})/", xml_text)
        if year_m:
            result["year"] = year_m.group(1)

        # ECLI
        ecli_m = re.search(r'value="(ECLI:[^"]+)"', xml_text)
        if ecli_m:
            result["ecli"] = ecli_m.group(1)

        # Keywords
        kw_matches = re.findall(r'<keyword[^>]*value="([^"]+)"', xml_text)
        result["keywords"] = kw_matches

        # Diary number
        diary_m = re.search(r'<proprietary[^>]*>.*?diaarinumero[^>]*>([^<]+)</.*?</proprietary>',
                            xml_text, re.DOTALL)
        if diary_m:
            result["diary_number"] = diary_m.group(1).strip()

        # Full text from judgmentBody
        body_m = re.search(r"<judgmentBody[^>]*>(.*?)</judgmentBody>", xml_text, re.DOTALL)
        if body_m:
            body_text = re.sub(r"<[^>]+>", " ", body_m.group(1))
            body_text = re.sub(r"\s+", " ", body_text).strip()
            result["text"] = body_text
        else:
            # Fallback: try <body>
            body_m = re.search(r"<body[^>]*>(.*?)</body>", xml_text, re.DOTALL)
            if body_m:
                body_text = re.sub(r"<[^>]+>", " ", body_m.group(1))
                body_text = re.sub(r"\s+", " ", body_text).strip()
                result["text"] = body_text
            else:
                # Last resort: strip all tags except meta
                text = re.sub(r"<meta>.*?</meta>", "", xml_text, flags=re.DOTALL)
                text = re.sub(r"<[^>]+>", " ", text)
                text = re.sub(r"\s+", " ", text).strip()
                result["text"] = text

        return result

    def _extract_year_number(self, akn_uri: str) -> Optional[tuple]:
        """Extract year and number from an AKN URI."""
        # URI format: /akn/fi/doc/labour-court-decision/2024/123/fin@
        # or: /akn/fi/judgment/labour-court-decision/2024/123/fin@
        m = re.search(r'/(\d{4})/(\d+)(?:/|$)', akn_uri)
        if m:
            return m.group(1), m.group(2)
        return None

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw decision data into standard schema."""
        text = raw.get("text", "")
        if len(text) < 100:
            logger.warning(f"Text too short for {raw.get('document_id', '?')}: {len(text)} chars")
            return None

        return {
            "_id": raw.get("document_id", ""),
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "court": "Työtuomioistuin",
            "court_en": "Labour Court",
            "case_number": raw.get("number", ""),
            "year": raw.get("year", ""),
            "ecli": raw.get("ecli", ""),
            "keywords": raw.get("keywords", []),
            "diary_number": raw.get("diary_number", ""),
            "language": "fi",
        }

    def fetch_all(self) -> Generator[Dict, None, None]:
        """Yield all labour court decisions via paginated API."""
        page = 1
        total = 0
        while True:
            items = self._list_decisions(page)
            if not items:
                break

            logger.info(f"Page {page}: {len(items)} items")

            for item in items:
                akn_uri = item.get("akn_uri", "")
                parsed = self._extract_year_number(akn_uri)
                if not parsed:
                    continue
                year, number = parsed

                xml = self._fetch_decision_xml(year, number)
                if not xml:
                    continue

                data = self._parse_akn_xml(xml)
                if not data["text"] or len(data["text"]) < 50:
                    continue

                raw = {
                    "document_id": f"FI-TT-{year}-{number}",
                    "title": data["title"] or f"Työtuomioistuin {number}/{year}",
                    "text": data["text"],
                    "date": data["date"],
                    "url": f"https://www.finlex.fi/fi/oikeus/tt/{year}/{number}",
                    "number": data["number"] or number,
                    "year": data["year"] or year,
                    "ecli": data["ecli"],
                    "keywords": data["keywords"],
                    "diary_number": data["diary_number"],
                }
                total += 1
                yield raw

            if len(items) < 10:
                break
            page += 1

        logger.info(f"Total decisions fetched: {total}")

    def fetch_updates(self, since: str = None) -> Generator[Dict, None, None]:
        """Fetch recent decisions (last 2 years)."""
        current_year = datetime.now().year
        page = 1
        total = 0
        while True:
            url = (f"{API_BASE}/akn/fi/doc/{DOC_TYPE}/list"
                   f"?format=json&page={page}&limit=10"
                   f"&startYear={current_year - 1}&endYear={current_year}")
            resp = self._request(url)
            if resp is None:
                break
            try:
                items = resp.json()
            except Exception:
                break
            if not items:
                break

            for item in items:
                akn_uri = item.get("akn_uri", "")
                parsed = self._extract_year_number(akn_uri)
                if not parsed:
                    continue
                year, number = parsed

                xml = self._fetch_decision_xml(year, number)
                if not xml:
                    continue

                data = self._parse_akn_xml(xml)
                if not data["text"] or len(data["text"]) < 50:
                    continue

                raw = {
                    "document_id": f"FI-TT-{year}-{number}",
                    "title": data["title"] or f"Työtuomioistuin {number}/{year}",
                    "text": data["text"],
                    "date": data["date"],
                    "url": f"https://www.finlex.fi/fi/oikeus/tt/{year}/{number}",
                    "number": data["number"] or number,
                    "year": data["year"] or year,
                    "ecli": data["ecli"],
                    "keywords": data["keywords"],
                    "diary_number": data["diary_number"],
                }
                total += 1
                yield raw

            if len(items) < 10:
                break
            page += 1

        logger.info(f"Update: {total} recent decisions fetched")

    def test_api(self):
        """Quick connectivity test."""
        print("Testing Finlex Open Data API for Labour Court decisions...")

        items = self._list_decisions(page=1)
        print(f"  List endpoint: {len(items)} items on page 1")

        if items:
            akn_uri = items[0].get("akn_uri", "")
            parsed = self._extract_year_number(akn_uri)
            if parsed:
                year, number = parsed
                print(f"  Fetching sample: {year}/{number}...")
                xml = self._fetch_decision_xml(year, number)
                if xml:
                    data = self._parse_akn_xml(xml)
                    print(f"  Title: {data['title']}")
                    print(f"  Date: {data['date']}")
                    print(f"  ECLI: {data['ecli']}")
                    print(f"  Keywords: {data['keywords'][:5]}")
                    print(f"  Text length: {len(data['text'])} chars")
                    if data["text"]:
                        print(f"  Preview: {data['text'][:200]}...")
                    print("\n  API test PASSED!")
                else:
                    print("  FAILED: Could not fetch decision XML")
            else:
                print(f"  FAILED: Could not parse URI: {akn_uri}")
        else:
            print("  FAILED: List endpoint returned no items")

    def run_sample(self, n: int = 12) -> dict:
        """Fetch a sample of decisions with full text."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        checked = 0
        errors = []
        text_lengths = []

        page = 1
        while saved < n:
            items = self._list_decisions(page)
            if not items:
                break

            for item in items:
                if saved >= n:
                    break

                checked += 1
                akn_uri = item.get("akn_uri", "")
                parsed = self._extract_year_number(akn_uri)
                if not parsed:
                    errors.append(f"Could not parse URI: {akn_uri}")
                    continue
                year, number = parsed
                case_id = f"TT/{year}/{number}"

                try:
                    xml = self._fetch_decision_xml(year, number)
                    if not xml:
                        errors.append(f"{case_id}: XML fetch returned None")
                        continue

                    data = self._parse_akn_xml(xml)
                    raw = {
                        "document_id": f"FI-TT-{year}-{number}",
                        "title": data["title"] or f"Työtuomioistuin {number}/{year}",
                        "text": data["text"],
                        "date": data["date"],
                        "url": f"https://www.finlex.fi/fi/oikeus/tt/{year}/{number}",
                        "number": data["number"] or number,
                        "year": data["year"] or year,
                        "ecli": data["ecli"],
                        "keywords": data["keywords"],
                        "diary_number": data["diary_number"],
                    }

                    normalized = self.normalize(raw)
                    if not normalized:
                        errors.append(f"{case_id}: Normalization returned None (text too short)")
                        continue

                    text_len = len(normalized.get("text", ""))
                    safe_name = re.sub(r'[^\w\-]', '_', normalized["_id"])
                    sample_path = sample_dir / f"{safe_name}.json"
                    with open(sample_path, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)

                    saved += 1
                    text_lengths.append(text_len)
                    logger.info(f"  Saved {case_id}: {text_len} chars")

                except Exception as e:
                    errors.append(f"{case_id}: {str(e)}")
                    logger.error(f"Error processing {case_id}: {e}")

            if len(items) < 10:
                break
            page += 1

        stats = {
            "sample_records_saved": saved,
            "documents_checked": checked,
            "errors": errors[:10],
            "avg_text_length": int(sum(text_lengths) / len(text_lengths)) if text_lengths else 0,
            "min_text_length": min(text_lengths) if text_lengths else 0,
            "max_text_length": max(text_lengths) if text_lengths else 0,
        }

        return stats


def main():
    scraper = FinlexLabourCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
            print(json.dumps(stats, indent=2))
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
            print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
