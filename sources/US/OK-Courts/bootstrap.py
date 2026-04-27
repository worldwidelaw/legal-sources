#!/usr/bin/env python3
"""
US/OK-Courts -- Oklahoma Supreme Court & Court of Civil/Criminal Appeals

Fetches Oklahoma court opinions via CourtListener search API + HTML full text
from storage.courtlistener.com.

Courts covered:
  - okla: Oklahoma Supreme Court (~43,400 opinions)
  - oklacivapp: Oklahoma Court of Civil Appeals (~5,700 opinions)
  - oklacrimapp: Oklahoma Court of Criminal Appeals (~20,700 opinions)

Data access:
  - Search API: https://www.courtlistener.com/api/rest/v4/search/
  - HTML opinions: https://storage.courtlistener.com/{local_path}

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Optional, Dict, Any

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OK-Courts")

SEARCH_URL = "https://www.courtlistener.com/api/rest/v4/search/"
STORAGE_BASE = "https://storage.courtlistener.com/"
COURTS = ["okla", "oklacivapp", "oklacrimapp"]
DELAY = 2.0
PAGE_SIZE = 20


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
        "Accept": "application/json,text/html,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def clean_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</h\d>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def fetch_opinion_text(session: requests.Session, local_path: str) -> str:
    """Fetch opinion HTML from CourtListener storage and extract text."""
    url = STORAGE_BASE + local_path
    try:
        resp = session.get(url, timeout=60)
        if resp.status_code == 404:
            return ""
        resp.raise_for_status()
        return clean_html(resp.text)
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch opinion: {e}")
        return ""


def build_citation(result: Dict[str, Any]) -> str:
    citations = result.get("citation", [])
    if citations:
        return "; ".join(citations)
    neutral = result.get("neutralCite", "")
    if neutral:
        return neutral
    return ""


def court_name(court_id: str) -> str:
    return {
        "okla": "Oklahoma Supreme Court",
        "oklacivapp": "Oklahoma Court of Civil Appeals",
        "oklacrimapp": "Oklahoma Court of Criminal Appeals",
    }.get(court_id, f"Oklahoma Court ({court_id})")


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    cluster_id = raw.get("cluster_id", "")
    court_id = raw.get("court_id", "okla")
    _id = f"US-OK-{court_id}-{cluster_id}"

    case_name = raw.get("caseName", "") or raw.get("caseNameFull", "") or "Unknown"
    citation = build_citation(raw)
    title = case_name
    if citation:
        title += f", {citation}"

    return {
        "_id": _id,
        "_source": "US/OK-Courts",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get("text", ""),
        "date": raw.get("dateFiled", ""),
        "url": "https://www.courtlistener.com" + raw.get("absolute_url", ""),
        "court": court_name(court_id),
        "court_id": court_id,
        "docket_number": raw.get("docketNumber", ""),
        "citation": citation,
        "judge": raw.get("judge", ""),
        "status": raw.get("status", ""),
        "syllabus": raw.get("syllabus", ""),
    }


def search_court(session: requests.Session, court: str, cursor_url: Optional[str] = None) -> Optional[Dict]:
    try:
        if cursor_url:
            resp = session.get(cursor_url, timeout=30)
        else:
            params = {
                "format": "json",
                "type": "o",
                "court": court,
                "page_size": PAGE_SIZE,
                "order_by": "dateFiled desc",
            }
            resp = session.get(SEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.warning(f"Search failed for {court}: {e}")
        return None


def fetch_all(sample: bool = False) -> Generator[Dict[str, Any], None, None]:
    session = get_session()
    total_yielded = 0
    max_records = 15 if sample else 999999
    max_pages_per_court = 2 if sample else 5000

    for court in COURTS:
        logger.info(f"Fetching opinions for court: {court}")
        cursor_url = None
        page = 0

        while page < max_pages_per_court:
            page += 1
            time.sleep(DELAY)
            data = search_court(session, court, cursor_url=cursor_url)
            if not data or not data.get("results"):
                logger.info(f"No more results for {court} at page {page}")
                break

            count = data.get("count", 0)
            if page == 1:
                logger.info(f"  Total available for {court}: {count}")

            for result in data["results"]:
                opinions = result.get("opinions", [])
                if not opinions:
                    continue

                # Find an opinion with a local_path for HTML download
                local_path = None
                for op in opinions:
                    lp = op.get("local_path", "")
                    if lp:
                        local_path = lp
                        break

                if not local_path:
                    logger.debug(f"  No local_path for {result.get('caseName', 'unknown')}")
                    continue

                time.sleep(DELAY)
                logger.info(f"  Fetching: {result.get('caseName', '')[:50]}")
                text = fetch_opinion_text(session, local_path)
                if not text or len(text) < 100:
                    logger.warning(f"  Insufficient text ({len(text)} chars)")
                    continue

                result["text"] = text
                record = normalize(result)
                yield record
                total_yielded += 1
                logger.info(f"  Record {total_yielded}: {record['_id']} ({len(text)} chars)")

                if total_yielded >= max_records:
                    logger.info(f"Target reached: {total_yielded} records")
                    return

            cursor_url = data.get("next")
            if not cursor_url:
                break

    logger.info(f"Total records: {total_yielded}")


def fetch_updates(since: str) -> Generator[Dict[str, Any], None, None]:
    since_date = datetime.fromisoformat(since).date()
    for record in fetch_all():
        if record.get("date"):
            try:
                rec_date = datetime.fromisoformat(record["date"]).date()
                if rec_date < since_date:
                    return
            except ValueError:
                pass
        yield record


def test_connectivity() -> bool:
    session = get_session()
    try:
        data = search_court(session, "okla")
        if not data:
            return False
        count = data.get("count", 0)
        results = data.get("results", [])
        logger.info(f"Search OK: {count} total, {len(results)} returned")

        if results and results[0].get("opinions"):
            lp = results[0]["opinions"][0].get("local_path", "")
            if lp:
                text = fetch_opinion_text(session, lp)
                logger.info(f"Opinion text: {len(text)} chars")
                logger.info(f"Preview: {text[:200]}...")
                return len(text) > 0
        return True
    except Exception as e:
        logger.error(f"Failed: {e}")
        return False


def main():
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        ok = test_connectivity()
        sys.exit(0 if ok else 1)

    if command in ("bootstrap", "bootstrap-fast"):
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_all(sample=sample):
            fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  [{count}] {record['_id']} — {record['title'][:60]}")
        print(f"\nDone: {count} records saved to {sample_dir}/")

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else "2024-01-01"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_updates(since):
            fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"Updated: {count} records since {since}")


if __name__ == "__main__":
    main()
