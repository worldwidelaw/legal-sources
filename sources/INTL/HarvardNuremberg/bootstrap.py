#!/usr/bin/env python3
"""
INTL/HarvardNuremberg -- Harvard Nuremberg Trials Project

Fetches full-text transcripts from all 13 Nuremberg trials via the Harvard
Law School Library's transcript partial-rendering endpoint.

Strategy:
  - For each of 13 transcripts (IDs 1-13), fetch pages in batches of 10
    via /transcripts/{id}?seq={seq}&partial=1 (returns JSON with HTML)
  - Group pages by date to create per-session records
  - Strip HTML to extract clean text
  - Each record = one day's proceedings for one trial

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Not applicable (static archive)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from collections import defaultdict

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.HarvardNuremberg")

BASE_URL = "https://nuremberg.law.harvard.edu"

# Transcript ID -> (case code, trial name)
TRIALS = {
    7:  ("IMT",    "International Military Tribunal"),
    1:  ("NMT-01", "Medical Case (NMT 1)"),
    2:  ("NMT-02", "Milch Case (NMT 2)"),
    3:  ("NMT-03", "Justice Case (NMT 3)"),
    5:  ("NMT-04", "Pohl Case (NMT 4)"),
    8:  ("NMT-05", "Flick Case (NMT 5)"),
    9:  ("NMT-06", "IG Farben Case (NMT 6)"),
    4:  ("NMT-07", "Hostages Case (NMT 7)"),
    10: ("NMT-08", "RuSHA Case (NMT 8)"),
    6:  ("NMT-09", "Einsatzgruppen Case (NMT 9)"),
    11: ("NMT-10", "Krupp Case (NMT 10)"),
    12: ("NMT-11", "Ministries Case (NMT 11)"),
    13: ("NMT-12", "High Command Case (NMT 12)"),
}

# Page counts per transcript (discovered empirically)
TRANSCRIPT_PAGES = {
    1: 11688, 2: 3136, 3: 11125, 4: 10553, 5: 8226,
    6: 6894, 7: 17439, 8: 11094, 9: 15634, 10: 5416,
    11: 13451, 12: 29085, 13: 9516,
}

PAGES_PER_BATCH = 10  # Server returns 10 pages per request


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities, preserving paragraph breaks."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<span class="speaker">(.*?)</span>', r'\1', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_mod.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pages_from_html(html_content: str) -> list:
    """Parse the HTML from the partial endpoint into page dicts."""
    pages = []
    # Each page is wrapped in <div class="page" data-seq="N" data-page="P" data-date="D">
    page_pattern = re.compile(
        r'<div\s+class="page"\s+data-seq="(\d+)"\s+data-page="([^"]*)"\s+data-date="([^"]*)">(.*?)</div>\s*(?=<div\s+class="page"|$)',
        re.DOTALL
    )
    for m in page_pattern.finditer(html_content):
        seq = int(m.group(1))
        page_num = m.group(2) or None
        date = m.group(3) or None
        content_html = m.group(4)
        # Remove the page-handle div
        content_html = re.sub(
            r'<div\s+class="page-handle">.*?</div>\s*',
            '', content_html, count=1, flags=re.DOTALL
        )
        text = strip_html(content_html)
        if text:
            pages.append({
                "seq": seq,
                "page_number": page_num,
                "date": date,
                "text": text,
            })
    return pages


class HarvardNurembergScraper(BaseScraper):
    """Scraper for Harvard Nuremberg Trials Project transcripts."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research; +https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json, text/html",
        })

    def _fetch_transcript_batch(self, transcript_id: int, seq: int) -> dict:
        """Fetch a batch of pages from the transcript partial endpoint."""
        url = f"{BASE_URL}/transcripts/{transcript_id}?seq={seq}&partial=1"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _get_total_pages(self, transcript_id: int) -> int:
        """Get total page count for a transcript."""
        return TRANSCRIPT_PAGES.get(transcript_id, 0)

    def _fetch_transcript_pages(self, transcript_id: int, max_pages: int = None) -> list:
        """Fetch all pages from a transcript, returning list of page dicts."""
        total = self._get_total_pages(transcript_id)
        if max_pages:
            total = min(total, max_pages)

        all_pages = []
        seq = 1
        while seq <= total:
            try:
                data = self._fetch_transcript_batch(transcript_id, seq)
                html_content = data.get("html", "")
                if not html_content or not html_content.strip():
                    break
                pages = extract_pages_from_html(html_content)
                if not pages:
                    break
                all_pages.extend(pages)
                to_seq = data.get("to_seq")
                if to_seq and to_seq >= seq:
                    seq = to_seq + 1
                else:
                    seq += PAGES_PER_BATCH
                time.sleep(1.0)  # Rate limiting
            except requests.RequestException as e:
                logger.warning(f"Error fetching transcript {transcript_id} seq {seq}: {e}")
                seq += PAGES_PER_BATCH
                time.sleep(2.0)
        return all_pages

    def _pages_to_sessions(self, transcript_id: int, pages: list) -> list:
        """Group pages by date into session records."""
        case_code, trial_name = TRIALS[transcript_id]

        # Group by date
        by_date = defaultdict(list)
        undated_pages = []
        for p in pages:
            date = p.get("date")
            if date:
                by_date[date].append(p)
            else:
                undated_pages.append(p)

        # Attach undated pages to the next dated group or create separate record
        sessions = []
        sorted_dates = sorted(by_date.keys())

        # If there are undated pages at the start, prepend to first dated group
        if undated_pages and sorted_dates:
            by_date[sorted_dates[0]] = undated_pages + by_date[sorted_dates[0]]
        elif undated_pages:
            # All pages undated (shouldn't happen, but handle it)
            sessions.append({
                "transcript_id": transcript_id,
                "case_code": case_code,
                "trial_name": trial_name,
                "date": None,
                "pages": undated_pages,
            })

        for date in sorted_dates:
            session_pages = by_date[date]
            sessions.append({
                "transcript_id": transcript_id,
                "case_code": case_code,
                "trial_name": trial_name,
                "date": date,
                "pages": session_pages,
            })

        return sessions

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all transcript sessions across all 13 trials."""
        for transcript_id in sorted(TRIALS.keys()):
            case_code, trial_name = TRIALS[transcript_id]
            logger.info(f"Fetching transcript {transcript_id}: {trial_name} "
                        f"({self._get_total_pages(transcript_id)} pages)")
            pages = self._fetch_transcript_pages(transcript_id)
            sessions = self._pages_to_sessions(transcript_id, pages)
            logger.info(f"  → {len(sessions)} sessions from {len(pages)} pages")
            for session in sessions:
                yield session

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Not applicable — this is a static historical archive."""
        logger.info("Harvard Nuremberg archive is static; no updates to fetch.")
        return
        yield  # Make this a generator

    def normalize(self, raw: dict) -> dict:
        """Transform a session dict into a standardized record."""
        transcript_id = raw["transcript_id"]
        case_code = raw["case_code"]
        trial_name = raw["trial_name"]
        date = raw.get("date")
        pages = raw.get("pages", [])

        # Combine all page texts
        text_parts = []
        for p in pages:
            page_text = p.get("text", "")
            if page_text:
                text_parts.append(page_text)
        full_text = "\n\n".join(text_parts)

        if not full_text.strip():
            return None

        # Build title
        if date:
            title = f"{trial_name} — Transcript, {date}"
        else:
            title = f"{trial_name} — Transcript (undated pages)"

        # Unique ID: case code + date
        record_id = f"nuremberg-{case_code}-{date or 'undated'}"

        page_range = ""
        if pages:
            seqs = [p["seq"] for p in pages]
            page_range = f"{min(seqs)}-{max(seqs)}"

        return {
            "_id": record_id,
            "_source": "INTL/HarvardNuremberg",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date,
            "url": f"{BASE_URL}/transcripts/{transcript_id}?seq={pages[0]['seq']}" if pages else f"{BASE_URL}/transcripts/{transcript_id}",
            "trial_name": trial_name,
            "case_code": case_code,
            "transcript_id": transcript_id,
            "page_count": len(pages),
            "page_range": page_range,
            "jurisdiction": "International",
        }


# ── CLI ───────────────────────────────────────────────────────────────

def _sample_fetch(scraper: HarvardNurembergScraper, sample_size: int = 15):
    """
    Custom sample: fetch first ~30 pages from a few different trials
    to get diverse session records quickly.
    """
    # Sample from 7 different trials for diversity
    sample_transcripts = [1, 7, 3, 6, 5, 11, 12]
    records = []

    for tid in sample_transcripts:
        if len(records) >= sample_size:
            break
        case_code, trial_name = TRIALS[tid]
        logger.info(f"Sample: fetching from transcript {tid} ({trial_name})")
        pages = scraper._fetch_transcript_pages(tid, max_pages=50)
        sessions = scraper._pages_to_sessions(tid, pages)
        for session in sessions:
            record = scraper.normalize(session)
            if record and record.get("text"):
                records.append(record)
                if len(records) >= sample_size:
                    break

    return records


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="Harvard Nuremberg Trials scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--full", action="store_true",
                        help="Full bootstrap (all pages)")
    args = parser.parse_args()

    scraper = HarvardNurembergScraper()

    if args.command == "test":
        print("Testing connectivity to nuremberg.law.harvard.edu...")
        try:
            data = scraper._fetch_transcript_batch(1, 1)
            html = data.get("html", "")
            pages = extract_pages_from_html(html)
            print(f"OK: Got {len(pages)} pages from transcript 1, seq 1")
            if pages:
                print(f"  First page text preview: {pages[0]['text'][:200]}...")
            print("Test PASSED")
        except Exception as e:
            print(f"Test FAILED: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            logger.info("Running sample bootstrap...")
            records = _sample_fetch(scraper, sample_size=15)
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            for i, rec in enumerate(records):
                path = sample_dir / f"{i+1:03d}_{rec['_id']}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(records)} sample records to {sample_dir}")
            # Print summary
            for rec in records:
                text_len = len(rec.get("text", ""))
                print(f"  {rec['_id']}: {rec['title']} ({text_len} chars, {rec['page_count']} pages)")
        else:
            logger.info("Running full bootstrap...")
            stats = scraper.bootstrap(sample_mode=False)
            print(json.dumps(stats, indent=2))

    elif args.command == "update":
        logger.info("Archive is static — nothing to update.")
