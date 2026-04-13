#!/usr/bin/env python3
"""
INTL/OECD-LegalInstruments -- OECD Legal Instruments

Fetches OECD legal instruments (Decisions, Recommendations, Declarations,
International Agreements, Guidelines, etc.) with full text from the official
REST API at legalinstruments.oecd.org.

Strategy:
  - GET /api/instruments?lang=en for the full catalogue (~503 instruments)
  - GET /api/instruments/{id}?lang=en for detailed metadata + body text URIs
  - GET the HTML body text from /public/doc/{id}/{uuid}.htm
  - No authentication required, no pagination needed

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recently changed instruments
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.OECD-LegalInstruments")

BASE_URL = "https://legalinstruments.oecd.org"
API_BASE = f"{BASE_URL}/api"

INSTRUMENT_TYPES = {
    1: "Decision",
    2: "Recommendation",
    3: "Substantive Outcome Document",
    4: "International Agreement",
    5: "Agreement",
    6: "Arrangement/Understanding",
    7: "DAC Recommendation",
    8: "Guidelines",
    5678: "Others",
}

STATUS_TYPES = {
    1: "In Force",
    2: "Abrogated",
    3: "Not yet in force",
}


class OECDLegalInstrumentsScraper(BaseScraper):
    SOURCE_ID = "INTL/OECD-LegalInstruments"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "application/json",
        })

    def _get_json(self, url: str, params: Optional[Dict] = None) -> Any:
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as e:
                if attempt == 2:
                    raise
                logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 * (attempt + 1))

    def _get_text(self, url: str) -> Optional[str]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                if attempt == 2:
                    logger.warning("Failed to fetch %s: %s", url, e)
                    return None
                time.sleep(2 * (attempt + 1))

    @staticmethod
    def _clean_html(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _extract_full_text(self, detail: Dict) -> str:
        """Extract full text from instrument detail, preferring English HTML."""
        body_text = detail.get("bodyText", {})
        refs = body_text.get("ref", [])
        if isinstance(refs, list):
            for ref in refs:
                lang = ref.get("lang", "")
                uri = ref.get("uri", "")
                if lang == "en" and uri.endswith(".htm"):
                    full_url = f"{BASE_URL}{uri}" if uri.startswith("/") else uri
                    html = self._get_text(full_url)
                    if html:
                        return self._clean_html(html)
            # Fall back to any .htm
            for ref in refs:
                uri = ref.get("uri", "")
                if uri.endswith(".htm"):
                    full_url = f"{BASE_URL}{uri}" if uri.startswith("/") else uri
                    html = self._get_text(full_url)
                    if html:
                        return self._clean_html(html)

        # Try blurb as fallback
        blurb = detail.get("blurb", {})
        blurb_texts = blurb.get("text", [])
        if isinstance(blurb_texts, list):
            for bt in blurb_texts:
                if bt.get("lang") == "en" and bt.get("value"):
                    return self._clean_html(bt["value"])

        # Try background text
        background = detail.get("background", {})
        bg_texts = background.get("text", [])
        if isinstance(bg_texts, list):
            for bt in bg_texts:
                if bt.get("lang") == "en" and bt.get("value"):
                    return self._clean_html(bt["value"])

        return ""

    def test_connection(self) -> bool:
        try:
            data = self._get_json(f"{API_BASE}/instruments/keys", params={"lang": "en"})
            count = len(data) if isinstance(data, list) else 0
            logger.info("Connection OK: %d instruments in catalogue", count)
            return count > 0
        except Exception as e:
            logger.error("Connection failed: %s", e)
            return False

    def fetch_all(self) -> Generator[Dict, None, None]:
        logger.info("Fetching OECD Legal Instruments catalogue...")
        data = self._get_json(f"{API_BASE}/instruments", params={"lang": "en"})
        instruments = data if isinstance(data, list) else []
        logger.info("Found %d instruments", len(instruments))

        for i, inst in enumerate(instruments):
            inst_id = inst.get("id")
            title = inst.get("title", "Unknown")
            logger.info("[%d/%d] %s (id=%s)", i + 1, len(instruments), title[:60], inst_id)

            self.rate_limiter.wait()
            try:
                detail = self._get_json(f"{API_BASE}/instruments/{inst_id}", params={"lang": "en"})
            except Exception as e:
                logger.warning("Skipping instrument %s: %s", inst_id, e)
                continue

            full_text = self._extract_full_text(detail)
            if not full_text:
                logger.warning("No full text for instrument %s: %s", inst_id, title[:60])

            inst["_detail"] = detail
            inst["_full_text"] = full_text
            yield inst

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        try:
            recent = self._get_json(f"{API_BASE}/recent-developments", params={"lang": "en"})
            if isinstance(recent, list):
                for item in recent:
                    inst_id = item.get("instrumentId") or item.get("id")
                    if inst_id:
                        self.rate_limiter.wait()
                        try:
                            detail = self._get_json(f"{API_BASE}/instruments/{inst_id}", params={"lang": "en"})
                        except Exception:
                            continue
                        raw = {
                            "id": inst_id,
                            "title": item.get("title", ""),
                            "typeId": item.get("typeId", 0),
                            "statusId": item.get("statusId", 0),
                            "key": item.get("key", ""),
                            "_detail": detail,
                            "_full_text": self._extract_full_text(detail),
                        }
                        yield raw
        except Exception as e:
            logger.error("Failed to fetch updates: %s", e)

    def normalize(self, raw: dict) -> dict:
        detail = raw.get("_detail", {})
        full_text = raw.get("_full_text", "")
        instrument_id = raw.get("id", 0)

        # Type and status come as nested objects: {"id": "1", "name": "Decision"}
        type_obj = raw.get("type", {})
        type_id = int(type_obj.get("id", 0)) if isinstance(type_obj, dict) else 0
        type_name = type_obj.get("name", "") if isinstance(type_obj, dict) else ""

        status_obj = raw.get("status", {})
        status_name = status_obj.get("name", "") if isinstance(status_obj, dict) else ""

        # Extract adoption date
        adoption_date = raw.get("adoptionDate") or raw.get("inForceDate")
        if not adoption_date:
            status_summary = detail.get("statusSummary", {}) or {}
            adoption_date = status_summary.get("adoptionDate")

        date_str = None
        if adoption_date and isinstance(adoption_date, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    dt = datetime.strptime(adoption_date.split(".")[0], fmt)
                    date_str = dt.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        instrument_type = type_name or INSTRUMENT_TYPES.get(type_id, f"Type {type_id}")
        status = status_name or STATUS_TYPES.get(int(status_obj.get("id", 0)) if isinstance(status_obj, dict) else 0, "Unknown")
        # Decisions and International Agreements are legislation; rest is doctrine
        data_type = "legislation" if type_id in (1, 4, 5) else "doctrine"

        return {
            "_id": f"OECD-LI-{instrument_id}",
            "_source": "INTL/OECD-LegalInstruments",
            "_type": data_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", "").strip(),
            "text": full_text,
            "date": date_str,
            "url": f"{BASE_URL}/en/instruments/OECD-LEGAL-{raw.get('key', instrument_id)}",
            "instrument_type": instrument_type,
            "status": status,
            "oecd_key": raw.get("key", ""),
        }

    def run_bootstrap(self, sample: bool = False):
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        label = "SAMPLE" if sample else "FULL"
        logger.info("Running %s bootstrap", label)

        count = 0
        for raw in self.fetch_all():
            normalized = self.normalize(raw)

            fname = re.sub(r'[^\w\-.]', '_', f"{normalized['_id'][:80]}.json")
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1

            text_len = len(normalized.get("text", ""))
            logger.info("  -> %d chars of text", text_len)

            if sample and count >= 15:
                break

        logger.info("%s bootstrap complete: %d records saved", label, count)
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/OECD-LegalInstruments Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    args = parser.parse_args()

    scraper = OECDLegalInstrumentsScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        count = 0
        for raw in scraper.fetch_updates(since=datetime.now(timezone.utc)):
            scraper.normalize(raw)
            count += 1
        logger.info("Update complete: %d records", count)


if __name__ == "__main__":
    main()
