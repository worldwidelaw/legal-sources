"""
US/LegiScan — all 50 US states + DC + US Congress legislation.

Uses the LegiScan v1.91 JSON API. Free tier gives 30,000 queries/month which
is more than enough: each `getDataset` call returns an entire legislative
session as a ZIP of per-bill JSON files, so ~150 queries can backfill all 50
states historically.

Ingest flow:
    1. getSessionList per state → catalog of historical sessions.
    2. getDataset per session → ZIP with every bill's full JSON + text.
    3. Parse each bill JSON; fetch full bill text via the `doc` fields when
       absent from the dataset snapshot.
    4. Normalize to the legislation schema with FULL TEXT.

Requires LEGISCAN_API_KEY in .env or environment.
See https://api.legiscan.com/dl/LegiScan_API_User_Manual.pdf for full spec.
"""

from __future__ import annotations

import base64
import io
import logging
import os
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import requests

# Make common/ importable when running this file directly.
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.base_scraper import BaseScraper  # noqa: E402

logger = logging.getLogger(__name__)

API_BASE = "https://api.legiscan.com"


class LegiScanScraper(BaseScraper):
    SOURCE_ID = "US/LegiScan"

    def __init__(self, source_dir: str | Path | None = None):
        super().__init__(str(source_dir or Path(__file__).parent))
        self.api_key = os.environ.get("LEGISCAN_API_KEY") or self._load_env_var("LEGISCAN_API_KEY")
        if not self.api_key:
            raise EnvironmentError(
                "LEGISCAN_API_KEY is not set. Register at "
                "https://legiscan.com/legiscan-register and add the key to "
                f"{self.source_dir}/.env or export it in your shell."
            )
        self.session = requests.Session()
        self.delay = float(self.config.get("fetch", {}).get("pagination", {}).get("delay_seconds", 1.0))
        self.min_year = self.config.get("fetch", {}).get("min_year")
        self.states_filter = self.config.get("fetch", {}).get("states") or []

    # ── LegiScan API helpers ─────────────────────────────────────────

    def _call(self, op: str, **params: Any) -> dict:
        """Call a LegiScan operation. Counts as one query against the quota."""
        q = {"key": self.api_key, "op": op, **params}
        resp = self.session.get(API_BASE, params=q, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "OK":
            raise RuntimeError(f"LegiScan {op} failed: {data}")
        time.sleep(self.delay)
        return data

    def _get_session_list(self, state: str) -> list[dict]:
        data = self._call("getSessionList", state=state)
        return data.get("sessions", [])

    def _get_dataset(self, session_id: int, access_key: str) -> list[dict]:
        """Return the full list of bill dicts for a session, unpacked from the dataset ZIP."""
        data = self._call("getDataset", id=session_id, access_key=access_key)
        ds = data.get("dataset") or {}
        zip_b64 = ds.get("zip")
        if not zip_b64:
            return []
        zip_bytes = base64.b64decode(zip_b64)
        bills: list[dict] = []
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in zf.namelist():
                if not name.endswith(".json") or "/bill/" not in name:
                    continue
                try:
                    import json as _json
                    with zf.open(name) as f:
                        payload = _json.load(f)
                    bill = payload.get("bill")
                    if isinstance(bill, dict):
                        bills.append(bill)
                except Exception as e:
                    logger.warning("Failed to parse %s: %s", name, e)
        return bills

    def _get_bill_text(self, doc_id: int) -> str:
        """Fetch full bill text for a specific document version."""
        data = self._call("getBillText", id=doc_id)
        doc = data.get("text") or {}
        b64 = doc.get("doc")
        if not b64:
            return ""
        raw = base64.b64decode(b64)
        mime = (doc.get("mime") or "").lower()
        if "pdf" in mime:
            # Route PDF bytes through the centralized extractor for markdown.
            from common.pdf_extract import extract_pdf_markdown
            md = extract_pdf_markdown(
                source=self.SOURCE_ID,
                source_id=str(doc_id),
                pdf_bytes=raw,
                table="legislation",
                force=True,  # per-doc call, no preload gating here
            )
            return md or ""
        # HTML / plain text
        try:
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return ""

    # ── BaseScraper interface ────────────────────────────────────────

    STATES = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        "DC", "US",
    ]

    def fetch_all(self) -> Iterator[dict]:
        from common.pdf_extract import preload_existing_ids

        existing = preload_existing_ids(self.SOURCE_ID, table="legislation")
        logger.info("[LegiScan] preloaded %d existing bill_ids from Neon", len(existing))

        states = self.states_filter or self.STATES
        for state in states:
            try:
                sessions = self._get_session_list(state)
            except Exception as e:
                logger.warning("[LegiScan] %s getSessionList failed: %s", state, e)
                continue

            for sess in sessions:
                year = sess.get("year_start") or sess.get("year_end")
                if self.min_year and year and year < self.min_year:
                    continue
                sid = sess.get("session_id")
                akey = sess.get("dataset_hash") or sess.get("access_key")
                if not sid or not akey:
                    continue
                try:
                    bills = self._get_dataset(sid, akey)
                except Exception as e:
                    logger.warning("[LegiScan] getDataset %s failed: %s", sid, e)
                    continue
                for bill in bills:
                    bill_id = str(bill.get("bill_id", ""))
                    if not bill_id or bill_id in existing:
                        continue
                    yield bill

    def fetch_updates(self, since: datetime) -> Iterator[dict]:
        """Incremental: dataset hash changes when a session has new/updated bills."""
        # LegiScan exposes `getDatasetList` with `since` param; same shape as fetch_all.
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        bill_id = str(raw.get("bill_id", ""))
        state = raw.get("state", "")
        session = raw.get("session", {}) or {}
        session_id = session.get("session_id")
        texts = raw.get("texts") or []

        # Prefer the most recent text version.
        full_text = ""
        for t in sorted(texts, key=lambda x: x.get("date") or "", reverse=True):
            doc_id = t.get("doc_id")
            if not doc_id:
                continue
            try:
                full_text = self._get_bill_text(int(doc_id))
            except Exception as e:
                logger.warning("[LegiScan] getBillText %s failed: %s", doc_id, e)
            if full_text:
                break

        return {
            "_id": f"US/LegiScan/{bill_id}",
            "_source": self.SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or raw.get("bill_number") or f"Bill {bill_id}",
            "text": full_text,
            "date": raw.get("status_date") or raw.get("last_action_date"),
            "url": raw.get("state_link") or raw.get("url"),
            "bill_id": bill_id,
            "bill_number": raw.get("bill_number"),
            "state": state,
            "session_id": session_id,
            "status": raw.get("status"),
            "sponsors": [s.get("name") for s in raw.get("sponsors") or [] if s.get("name")],
            "subjects": [s.get("subject_name") for s in raw.get("subjects") or [] if s.get("subject_name")],
            "committee": (raw.get("committee") or {}).get("name"),
            "last_action": raw.get("last_action"),
        }


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="US/LegiScan data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--sample-size", type=int, default=10)
    args = parser.parse_args()

    scraper = LegiScanScraper()
    if args.command == "bootstrap":
        result = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        print(result)
    elif args.command == "update":
        result = scraper.update()
        print(result)
