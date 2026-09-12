#!/usr/bin/env python3
"""
PS/SecurityLegislation — Palestine Security Sector Legal Database (DCAF)

Fetches legislation from the DCAF-operated database at https://security-legislation.ps
using the WordPress REST API. Contains ~2200 legal texts: laws, decrees,
decisions, bylaws, and emergency legislation.
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://security-legislation.ps"
API_BASE = f"{BASE_URL}/wp-json/wp/v2"
SOURCE_ID = "PS/SecurityLegislation"
SAMPLE_DIR = Path(__file__).parent / "sample"
PER_PAGE = 100
MIN_TEXT_LENGTH = 100

# Taxonomy term IDs (from /wp-json/wp/v2/text-type-categories)
TEXT_TYPE_MAP = {
    6276: "Decision",
    6277: "Law",
    6278: "Decree",
    6279: "Law by Decree",
    6280: "Constitutional Law",
    6281: "Bylaw",
    6282: "State of Emergency Legislation",
}

STATUS_MAP = {
    6274: "Applicable",
    6275: "Repealed",
}


class SecurityLegislationFetcher:
    """Fetcher for DCAF Palestine Security Sector Legal Database."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })

    def _clean_html(self, html: str) -> str:
        """Strip HTML tags and clean whitespace."""
        text = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
        text = re.sub(r"</p>", "\n\n", text, flags=re.I)
        text = re.sub(r"</li>", "\n", text, flags=re.I)
        text = re.sub(r"<[^>]+>", "", text)
        text = unescape(text)
        text = text.replace("\xa0", " ")
        text = re.sub(r"[\u200e\u200f\u200b]", "", text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _get_text_type(self, cat_ids: List[int]) -> str:
        """Resolve text type taxonomy IDs to label."""
        for cid in cat_ids:
            if cid in TEXT_TYPE_MAP:
                return TEXT_TYPE_MAP[cid]
        return "Unknown"

    def _get_status(self, cat_ids: List[int]) -> str:
        """Resolve status taxonomy IDs to label."""
        for cid in cat_ids:
            if cid in STATUS_MAP:
                return STATUS_MAP[cid]
        return "Unknown"

    def fetch_page(self, page: int) -> List[Dict]:
        """Fetch a single page of documents from the WP REST API."""
        url = f"{API_BASE}/latest-laws"
        params = {"per_page": PER_PAGE, "page": page}
        resp = self.session.get(url, params=params, timeout=30)
        if resp.status_code == 400:
            # Past last page
            return []
        resp.raise_for_status()
        return resp.json()

    def normalize(self, raw: Dict) -> Optional[Dict]:
        """Transform a WP post into a normalized record."""
        title = self._clean_html(raw.get("title", {}).get("rendered", ""))
        content_html = raw.get("content", {}).get("rendered", "")
        text = self._clean_html(content_html)

        if len(text) < MIN_TEXT_LENGTH:
            logger.warning(f"Skipping {raw.get('id')}: text too short ({len(text)} chars)")
            return None

        date_str = raw.get("date", "")
        if date_str:
            try:
                date_str = datetime.fromisoformat(date_str).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        modified_str = raw.get("modified", "")
        if modified_str:
            try:
                modified_str = datetime.fromisoformat(modified_str).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        text_type_ids = raw.get("text-type-categories", [])
        status_ids = raw.get("status-categories", [])

        return {
            "_id": str(raw["id"]),
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str or None,
            "modified": modified_str or None,
            "url": raw.get("link", ""),
            "text_type": self._get_text_type(text_type_ids),
            "status_label": self._get_status(status_ids),
            "slug": raw.get("slug", ""),
        }

    def fetch_all(self) -> Iterator[Dict]:
        """Yield all normalized documents."""
        page = 1
        total = 0
        while True:
            logger.info(f"Fetching page {page}...")
            items = self.fetch_page(page)
            if not items:
                break
            for item in items:
                record = self.normalize(item)
                if record:
                    total += 1
                    yield record
            if len(items) < PER_PAGE:
                break
            page += 1
            time.sleep(0.5)
        logger.info(f"Total records fetched: {total}")

    def fetch_updates(self, since: str) -> Iterator[Dict]:
        """Yield documents modified since a given date."""
        page = 1
        total = 0
        while True:
            url = f"{API_BASE}/latest-laws"
            params = {
                "per_page": PER_PAGE,
                "page": page,
                "modified_after": f"{since}T00:00:00",
                "orderby": "modified",
                "order": "desc",
            }
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 400:
                break
            resp.raise_for_status()
            items = resp.json()
            if not items:
                break
            for item in items:
                record = self.normalize(item)
                if record:
                    total += 1
                    yield record
            if len(items) < PER_PAGE:
                break
            page += 1
            time.sleep(0.5)
        logger.info(f"Updated records since {since}: {total}")


def bootstrap_sample(max_records: int = 15):
    """Fetch sample records and save to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = SecurityLegislationFetcher()
    count = 0
    for record in fetcher.fetch_all():
        if count >= max_records:
            break
        out_path = SAMPLE_DIR / f"{record['_id']}.json"
        out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"Saved {out_path.name}: {record['title'][:80]}")
        count += 1
    logger.info(f"Sample complete: {count} records saved to {SAMPLE_DIR}")
    return count


def bootstrap_full():
    """Fetch all records."""
    fetcher = SecurityLegislationFetcher()
    count = 0
    for record in fetcher.fetch_all():
        count += 1
        if count % 100 == 0:
            logger.info(f"Progress: {count} records...")
    logger.info(f"Full bootstrap complete: {count} records")
    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="PS/SecurityLegislation bootstrap")
    parser.add_argument("action", choices=["bootstrap", "bootstrap-full"],
                        help="bootstrap = sample, bootstrap-full = all")
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--max", type=int, default=15, help="Max sample records")
    args = parser.parse_args()

    if args.action == "bootstrap" or args.sample:
        bootstrap_sample(args.max)
    else:
        bootstrap_full()
