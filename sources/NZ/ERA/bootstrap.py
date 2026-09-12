#!/usr/bin/env python3
"""
NZ/ERA -- Employment Relations Authority Determinations

Fetches employment dispute determinations from the NZ ERA database.

Strategy:
  - Enumerate determination pages by sequential ID (1 to ~21207)
  - Parse metadata from HTML (title, reference, date, member, jurisdiction, parties)
  - Download PDF and extract full text via common/pdf_extract
  - PDFs available since 2005; pre-2005 records included with summary text if available

Source: https://determinations.era.govt.nz/determinations (NZ Government, open access)
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull (resumable)
  python bootstrap.py bootstrap-fast       # Same, concurrent normalize (fleet entry point)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NZ.ERA")

BASE_URL = "https://determinations.era.govt.nz"

# Approximate highest known ID (will be discovered dynamically)
MAX_KNOWN_ID = 21210

# Per-request budget for the ID walk. `timeout` is per socket operation, so it
# is the wall-clock deadline that actually bounds one determination fetch
# (urllib3 retries and Retry-After sleeps happen inside it).
REQUEST_TIMEOUT = (10, 25)
REQUEST_WALL_TIMEOUT = 60

# A host that drops our packets looks exactly like a long run of empty IDs, so
# transport failures are counted separately from honest 404s and abort the run
# loudly rather than silently grinding through 21K dead fetches (issue #1416).
MAX_CONSECUTIVE_TRANSPORT_ERRORS = 25

# How often the ID walk reports where it is, so a slow crawl is never mistaken
# for a hung one.
PROGRESS_EVERY = 100


class ERAScraper(BaseScraper):
    """
    Scraper for NZ/ERA -- Employment Relations Authority.
    Country: NZ
    URL: https://determinations.era.govt.nz/determinations
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=REQUEST_TIMEOUT,
            wall_timeout=REQUEST_WALL_TIMEOUT,
        )
        self.checkpoint_path = source_dir / "data" / "era_checkpoint.json"
        # Sample runs must see the newest determinations, not resume a crawl.
        self.use_checkpoint = True

    # ── Checkpoint ────────────────────────────────────────────────────

    def _load_checkpoint(self) -> dict:
        """Return {'high_water': int, 'cursor': int} for a resumed ID walk."""
        if not self.use_checkpoint or not self.checkpoint_path.exists():
            return {}
        try:
            data = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "cursor" in data:
                return data
        except Exception as e:
            logger.warning(f"Ignoring unreadable checkpoint: {e}")
        return {}

    def _save_checkpoint(self, high_water: int, cursor: int) -> None:
        if not self.use_checkpoint:
            return
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.checkpoint_path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps({"high_water": high_water, "cursor": cursor}),
            encoding="utf-8",
        )
        tmp.replace(self.checkpoint_path)

    # ── Fetching ──────────────────────────────────────────────────────

    def _discover_max_id(self) -> int:
        """Find the current highest determination ID from the recent page."""
        try:
            resp = self.client.get(f"{BASE_URL}/determinations/recent")
            if resp and resp.status_code == 200:
                ids = re.findall(r'/determination/view/(\d+)', resp.text)
                if ids:
                    return max(int(i) for i in ids)
        except Exception as e:
            logger.warning(f"Could not discover max ID: {e}")
        return MAX_KNOWN_ID

    def _parse_determination_page(self, det_id: int) -> Tuple[str, Optional[dict]]:
        """Fetch and parse one determination page.

        Returns ``(outcome, record)`` where outcome is ``"ok"`` (parsed),
        ``"missing"`` (the server answered, but there is no determination at
        this ID) or ``"error"`` (the request never completed — timeout, reset,
        DNS failure). Callers need the distinction: a wall of ``missing`` is a
        normal gap in the ID space, a wall of ``error`` means the host is
        refusing us and the crawl should stop instead of pretending the corpus
        ends here.
        """
        url = f"{BASE_URL}/determination/view/{det_id}"
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except requests.RequestException as e:
            logger.debug(f"Transport error on determination {det_id}: {e}")
            return "error", None
        except Exception as e:
            logger.debug(f"Transport error on determination {det_id}: {e}")
            return "error", None

        try:
            if resp is None:
                return "error", None
            if resp.status_code in (429, 500, 502, 503, 504):
                logger.debug(f"Determination {det_id} returned {resp.status_code}")
                return "error", None
            if resp.status_code != 200:
                return "missing", None

            html = resp.text

            # Title
            title_m = re.search(
                r'class="determination__title"[^>]*>(.*?)</h1>',
                html, re.DOTALL
            )
            title = re.sub(r'<[^>]+>', '', title_m.group(1)).strip() if title_m else ""
            if not title:
                return "missing", None

            # Extract table rows: <th>Label</th> ... <td>Value</td>
            metadata = {}
            rows = re.findall(
                r'<th[^>]*scope="row"[^>]*>(.*?)</th>\s*<td>(.*?)</td>',
                html, re.DOTALL
            )
            for label, value in rows:
                label = re.sub(r'<[^>]+>', '', label).strip().rstrip(':')
                value = re.sub(r'<[^>]+>', ' ', value).strip()
                value = re.sub(r'\s+', ' ', value).strip()
                metadata[label] = value

            # PDF link
            pdf_m = re.search(r'href="([^"]*elawpdf[^"]*\.pdf)"', html)
            pdf_path = pdf_m.group(1) if pdf_m else None
            pdf_url = None
            if pdf_path:
                if pdf_path.startswith('http'):
                    pdf_url = pdf_path
                else:
                    pdf_url = f"{BASE_URL}{pdf_path}"

            # Parse date
            date_str = metadata.get('Determination date', '')
            date_iso = None
            if date_str:
                for fmt in ('%d %B %Y', '%d %b %Y'):
                    try:
                        date_iso = datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                        break
                    except ValueError:
                        continue

            ref_no = metadata.get('Reference No', '')

            return "ok", {
                'det_id': det_id,
                'title': title,
                'reference_no': ref_no,
                'date': date_iso,
                'date_raw': date_str,
                'member': metadata.get('Member', ''),
                'jurisdiction': metadata.get('Jurisdiction', ''),
                'parties': metadata.get('Parties', title),
                'location': metadata.get('Location', ''),
                'hearing_date': metadata.get('Hearing date', ''),
                'representation': metadata.get('Representation', ''),
                'summary': metadata.get('Summary', ''),
                'result': metadata.get('Result', ''),
                'main_category': metadata.get('Main Category', ''),
                'restrictions': metadata.get('Restrictions', ''),
                'pdf_url': pdf_url,
                'page_url': url,
            }

        except Exception as e:
            logger.debug(f"Error parsing determination {det_id}: {e}")
            return "missing", None

    def _extract_full_text(self, raw: dict) -> str:
        """Download and extract text from PDF, fall back to summary."""
        pdf_url = raw.get('pdf_url')
        if pdf_url:
            try:
                text = extract_pdf_markdown(
                    source="NZ/ERA",
                    source_id=str(raw['det_id']),
                    pdf_url=pdf_url,
                    table="case_law",
                    force=True,
                )
                if text and len(text) > 100:
                    return text
            except Exception as e:
                logger.debug(f"PDF extraction failed for {raw['det_id']}: {e}")

        # Fallback to summary
        summary = raw.get('summary', '')
        if summary and len(summary) > 50:
            return summary
        return ""

    def normalize(self, raw: dict) -> dict:
        """Transform raw determination data into standard schema."""
        det_id = raw['det_id']
        ref_no = raw.get('reference_no', '')
        _id = ref_no if ref_no else f"ERA-{det_id}"

        return {
            '_id': _id,
            '_source': 'NZ/ERA',
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('page_url', f"{BASE_URL}/determination/view/{det_id}"),
            'reference_no': ref_no,
            'member': raw.get('member', ''),
            'jurisdiction': raw.get('jurisdiction', ''),
            'parties': raw.get('parties', ''),
            'pdf_url': raw.get('pdf_url', ''),
        }

    def _walk_ids(self, ids, high_water: int) -> Generator[dict, None, None]:
        """Walk an ID sequence, yielding raw determinations with full text."""
        transport_errors = 0
        seen = missing = errors = 0

        for det_id in ids:
            outcome, raw = self._parse_determination_page(det_id)

            if outcome == "error":
                errors += 1
                transport_errors += 1
                if transport_errors >= MAX_CONSECUTIVE_TRANSPORT_ERRORS:
                    raise RuntimeError(
                        f"{transport_errors} consecutive transport failures ending at "
                        f"determination {det_id} — {BASE_URL} is unreachable from this "
                        f"vantage (datacenter-IP block or outage). Aborting rather than "
                        f"walking the remaining IDs; retry from a residential/NZ vantage."
                    )
                # Leave the checkpoint behind this ID so a rerun retries it.
                continue

            transport_errors = 0
            if outcome == "missing":
                missing += 1
            else:
                text = self._extract_full_text(raw)
                if text:
                    raw['text'] = text
                    seen += 1
                    yield raw
                else:
                    logger.debug(f"No text for determination {det_id}, skipping")

            # Only advance past IDs the server actually answered for.
            self._save_checkpoint(high_water, det_id - 1)

            if det_id % PROGRESS_EVERY == 0:
                logger.info(
                    f"ID walk at {det_id}: {seen} with text, {missing} empty IDs, "
                    f"{errors} transport errors"
                )

        logger.info(
            f"ID walk finished: {seen} with text, {missing} empty IDs, "
            f"{errors} transport errors"
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all determinations (raw) by walking the sequential ID space.

        Descends newest-first and resumes from ``data/era_checkpoint.json`` so a
        fleet relaunch advances instead of re-walking from the top. New IDs
        published since the last run are picked up before the walk continues.
        """
        max_id = self._discover_max_id()
        ckpt = self._load_checkpoint()
        high_water = max(max_id, ckpt.get("high_water", 0))
        cursor = ckpt.get("cursor")

        if cursor is None:
            logger.info(f"Fetching determinations from ID {max_id} down to 1")
            yield from self._walk_ids(range(max_id, 0, -1), high_water)
            return

        previous_high = ckpt.get("high_water", max_id)
        if max_id > previous_high:
            logger.info(
                f"Resuming: {max_id - previous_high} new IDs "
                f"({max_id}..{previous_high + 1}) before cursor {cursor}"
            )
            yield from self._walk_ids(range(max_id, previous_high, -1), high_water)

        if cursor < 1:
            logger.info("Checkpoint says the ID space is exhausted — nothing older to fetch")
            return

        logger.info(f"Resuming ID walk from {cursor} down to 1")
        yield from self._walk_ids(range(cursor, 0, -1), high_water)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch recent determinations (raw), stopping once older than `since`."""
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        max_id = self._discover_max_id()
        for det_id in range(max_id, max(1, max_id - 500), -1):
            _outcome, raw = self._parse_determination_page(det_id)
            if raw is None:
                continue
            if raw.get('date') and raw['date'] < since:
                break

            text = self._extract_full_text(raw)
            if not text:
                continue
            raw['text'] = text
            yield raw

    def test_api(self) -> bool:
        """Test connectivity to ERA site."""
        try:
            resp = self.client.get(f"{BASE_URL}/determinations/recent", timeout=15)
            if resp and resp.status_code == 200:
                ids = re.findall(r'/determination/view/(\d+)', resp.text)
                logger.info(f"ERA site OK — found {len(ids)} recent determinations")
                return True
            logger.error(f"ERA site returned {resp.status_code if resp else 'None'}")
            return False
        except Exception as e:
            logger.error(f"ERA connectivity test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NZ/ERA bootstrap")
    # The fleet wrapper invokes `bootstrap-fast`; without it argparse exits 2
    # and the wrapper falls back to re-ingesting sample/.
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Full fetch (all records)")
    args = parser.parse_args()

    scraper = ERAScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    if args.sample:
        # Sample runs always start at the newest determination.
        scraper.use_checkpoint = False
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            count += 1
            (sample_dir / f"{count:04d}.json").write_text(
                json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            logger.info(
                f"[{count}] {record['_id']} — {record['title'][:60]} "
                f"({len(record.get('text', ''))} chars)"
            )
            if count >= 15:
                break

        logger.info(f"Done: {count} sample records fetched")
        return

    # Full corpus — BaseScraper streams normalized records to data/records.jsonl.
    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
    else:
        stats = scraper.bootstrap()

    logger.info(
        f"Done: {stats.get('records_fetched', 0)} fetched, "
        f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
    )
    if stats.get("error_message"):
        logger.error(f"Bootstrap failed: {stats['error_message']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
