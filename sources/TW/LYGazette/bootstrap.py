#!/usr/bin/env python3
"""
TW/LYGazette — Taiwan Legislative Yuan Gazette (立法院公報)

Fetches full-text parliamentary proceedings from the g0v LY Open Data API.

Strategy:
  1. Paginate through gazette issues via /gazettes
  2. For each gazette, fetch agenda items via /gazette/{id}/agendas
  3. For each agenda, download full text from the txt endpoint
  4. Each agenda item = one record with full text

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOURCE_ID = "TW/LYGazette"
SAMPLE_DIR = Path(__file__).parent / "sample"
API_BASE = "https://v2.ly.govapi.tw"
REQUEST_DELAY = 1.5
MIN_TEXT_CHARS = 100

CATEGORY_NAMES = {
    1: "報告事項",      # Report items
    2: "國是論壇",      # National affairs forum
    3: "討論事項",      # Discussion items
    4: "質詢事項",      # Interpellations
    5: "議事錄",        # Meeting minutes
    6: "書面質詢",      # Written interpellations
    7: "附錄",          # Appendix
}


def _get_json(session: requests.Session, url: str, params: Optional[dict] = None) -> Optional[dict]:
    """GET JSON from API with retry."""
    for attempt in range(3):
        try:
            time.sleep(REQUEST_DELAY)
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            logger.warning("HTTP %d from %s (attempt %d)", r.status_code, url, attempt + 1)
        except Exception as e:
            logger.warning("Request failed for %s: %s (attempt %d)", url, e, attempt + 1)
        if attempt < 2:
            time.sleep(5 * (attempt + 1))
    return None


def _get_text(session: requests.Session, url: str) -> str:
    """GET plain text content from a txt endpoint."""
    for attempt in range(2):
        try:
            time.sleep(REQUEST_DELAY)
            r = session.get(url, timeout=60)
            if r.status_code == 200:
                return r.text.strip()
            logger.debug("HTTP %d from txt endpoint %s", r.status_code, url)
        except Exception as e:
            logger.debug("Text fetch failed: %s", e)
        if attempt < 1:
            time.sleep(3)
    return ""


def _make_id(gazette_id: str, agenda_num: int) -> str:
    """Create stable ID from gazette + agenda number."""
    return f"LY-{gazette_id}-{agenda_num:03d}"


def fetch_all(sample: bool = False) -> Iterator[Dict[str, Any]]:
    """Yield gazette agenda records with full text."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/ZachLaik/LegalDataHunter)",
        "Accept": "application/json",
    })

    count = 0
    errors = 0
    sample_limit = 15
    page = 1
    page_size = 10

    while True:
        # Fetch gazette list
        data = _get_json(session, f"{API_BASE}/gazettes", {
            "limit": page_size,
            "page": page,
        })
        if not data or not data.get("gazettes"):
            break

        for gazette in data["gazettes"]:
            gazette_id = gazette.get("公報編號", "")
            pub_date = gazette.get("發布日期", "")
            volume = gazette.get("卷", "")
            issue = gazette.get("期", "")
            booklet = gazette.get("冊別", "")

            if not gazette_id:
                continue

            # Fetch agendas for this gazette
            agendas_data = _get_json(session, f"{API_BASE}/gazette/{gazette_id}/agendas", {
                "limit": 50,
            })
            if not agendas_data or not agendas_data.get("gazetteagendas"):
                continue

            for agenda in agendas_data["gazetteagendas"]:
                agenda_id = agenda.get("公報議程編號", "")
                agenda_num = agenda.get("目錄編號", 0)
                category_code = agenda.get("類別代碼", 0)
                title = agenda.get("案由", "").strip()
                term = agenda.get("屆", "")
                session_num = agenda.get("會期", "")
                meeting_dates = agenda.get("會議日期", [])
                start_page = agenda.get("起始頁碼", 0)
                end_page = agenda.get("結束頁碼", 0)

                # Find txt URL in processed URLs
                txt_url = None
                for url_info in agenda.get("處理後公報網址", []):
                    if url_info.get("type") == "txt":
                        txt_url = url_info.get("url")
                        break

                if not txt_url:
                    continue

                # Fetch full text
                text = _get_text(session, txt_url)
                if len(text) < MIN_TEXT_CHARS:
                    logger.debug("Skipping %s: insufficient text (%d chars)", agenda_id, len(text))
                    errors += 1
                    continue

                # Build title
                category_name = CATEGORY_NAMES.get(category_code, f"類別{category_code}")
                if not title:
                    title = f"第{term}屆第{session_num}會期 {category_name}"
                display_title = f"[{category_name}] {title[:200]}"

                # Gazette web URL
                gazette_web_url = agenda.get("公報網網址", f"https://ppg.ly.gov.tw/ppg/publications/official-gazettes/{volume}/{issue}/{booklet:02d}/details" if isinstance(booklet, int) else "")

                record = {
                    "_id": _make_id(gazette_id, agenda_num),
                    "_source": SOURCE_ID,
                    "_type": "legislation",
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "title": display_title,
                    "text": text,
                    "date": meeting_dates[0] if meeting_dates else pub_date,
                    "url": gazette_web_url or txt_url,
                    "gazette_id": gazette_id,
                    "volume": volume,
                    "issue": issue,
                    "agenda_number": agenda_num,
                    "category_code": category_code,
                    "category_name": category_name,
                    "term": term,
                    "session": session_num,
                    "pages": end_page - start_page + 1 if end_page and start_page else None,
                }

                count += 1
                yield record

                if sample and count >= sample_limit:
                    logger.info("Sample limit reached (%d records)", count)
                    return

        total_pages = data.get("total_page", 0)
        if page >= total_pages:
            break
        page += 1

        if count % 100 == 0 and count > 0:
            logger.info("Progress: %d records fetched, %d errors", count, errors)

    logger.info("Total records yielded: %d (errors: %d)", count, errors)


def bootstrap(sample: bool = False) -> None:
    """Run bootstrap and save records to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    saved = 0
    for record in fetch_all(sample=sample):
        out_path = SAMPLE_DIR / f"{record['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        saved += 1
        logger.info(
            "[%d] %s — %d chars",
            saved,
            record["title"][:60],
            len(record.get("text", "")),
        )

    logger.info("Bootstrap complete: %d records saved to %s", saved, SAMPLE_DIR)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="TW/LYGazette bootstrap")
    parser.add_argument("command", choices=["bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap(sample=args.sample)
