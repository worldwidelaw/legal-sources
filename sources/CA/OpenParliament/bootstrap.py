#!/usr/bin/env python3
"""
CA/OpenParliament -- Canadian Parliamentary Debates Fetcher

Fetches House of Commons debates (Hansard) from OpenParliament.ca API.
30+ years of debates, free JSON API, no auth required.

Strategy:
  - Paginate through /debates/ to list debate dates
  - For each debate, fetch all speeches via /speeches/?document=...
  - Combine speeches into a single debate document with full text

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.OpenParliament")

API_BASE = "https://api.openparliament.ca"


class OpenParliamentScraper(BaseScraper):
    """
    Scraper for CA/OpenParliament -- Canadian House of Commons Debates.
    Country: CA
    URL: https://openparliament.ca

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    # -- Helpers ------------------------------------------------------------

    def _get_debates(self, limit=20, offset=0):
        """Fetch list of debate dates."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get("/debates/", params={
                "format": "json",
                "limit": limit,
                "offset": offset,
            })
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch debates: {e}")
            return {"objects": [], "pagination": {}}

    def _get_speeches(self, debate_url, limit=100, offset=0):
        """Fetch speeches for a specific debate."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get("/speeches/", params={
                "document": debate_url,
                "format": "json",
                "limit": limit,
                "offset": offset,
            })
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch speeches for {debate_url}: {e}")
            return {"objects": [], "pagination": {}}

    def _fetch_all_speeches(self, debate_url):
        """Fetch all speeches for a debate (handles pagination)."""
        all_speeches = []
        offset = 0
        limit = 100

        while True:
            data = self._get_speeches(debate_url, limit=limit, offset=offset)
            objects = data.get("objects", [])
            if not objects:
                break

            all_speeches.extend(objects)
            pagination = data.get("pagination", {})
            next_url = pagination.get("next_url")
            if not next_url:
                break

            offset += limit

        return all_speeches

    def _clean_html(self, text):
        """Strip HTML tags from speech content."""
        if not text:
            return ""
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&#\d+;', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _compile_debate(self, debate_info, speeches):
        """Combine debate metadata and speeches into a single document."""
        date = debate_info.get("date", "")
        number = debate_info.get("number", "")
        session = debate_info.get("session", "")

        parts = []
        parts.append(f"House of Commons Debate - {date} (Sitting #{number})")
        if session:
            parts.append(f"Session: {session}")
        parts.append("")

        for speech in speeches:
            content = speech.get("content", {})
            en_text = content.get("en", "")
            if not en_text:
                continue

            clean_text = self._clean_html(en_text)
            if not clean_text:
                continue

            politician = speech.get("politician_url", "")
            heading = speech.get("h1", {}).get("en", "")
            subheading = speech.get("h2", {}).get("en", "")

            if heading:
                parts.append(f"\n--- {heading} ---")
            if subheading:
                parts.append(f"  {subheading}")
            if politician:
                # Extract name from URL like /politicians/tom-kmiec/
                name = politician.strip("/").split("/")[-1].replace("-", " ").title()
                parts.append(f"[{name}]: {clean_text}")
            else:
                parts.append(clean_text)

        return "\n".join(parts)

    # -- BaseScraper interface ----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all debate documents with full text."""
        offset = 0
        limit = 20
        total_fetched = 0

        while True:
            data = self._get_debates(limit=limit, offset=offset)
            debates = data.get("objects", [])
            if not debates:
                break

            for debate in debates:
                debate_url = debate.get("url", "")
                date = debate.get("date", "")

                speeches = self._fetch_all_speeches(debate_url)
                if not speeches:
                    continue

                text = self._compile_debate(debate, speeches)
                if len(text) < 100:
                    continue

                yield {
                    "date": date,
                    "number": debate.get("number", ""),
                    "session": debate.get("session", ""),
                    "url": debate_url,
                    "text": text,
                    "speech_count": len(speeches),
                    "source_url": debate.get("source_url", ""),
                }
                total_fetched += 1

                if total_fetched % 50 == 0:
                    logger.info(f"Fetched {total_fetched} debates")

            pagination = data.get("pagination", {})
            if not pagination.get("next_url"):
                break
            offset += limit

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch debates since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        offset = 0
        limit = 20

        while True:
            data = self._get_debates(limit=limit, offset=offset)
            debates = data.get("objects", [])
            if not debates:
                break

            for debate in debates:
                date = debate.get("date", "")
                if date < since_str:
                    return  # Debates are sorted newest first

                debate_url = debate.get("url", "")
                speeches = self._fetch_all_speeches(debate_url)
                if not speeches:
                    continue

                text = self._compile_debate(debate, speeches)
                if len(text) < 100:
                    continue

                yield {
                    "date": date,
                    "number": debate.get("number", ""),
                    "session": debate.get("session", ""),
                    "url": debate_url,
                    "text": text,
                    "speech_count": len(speeches),
                    "source_url": debate.get("source_url", ""),
                }

            pagination = data.get("pagination", {})
            if not pagination.get("next_url"):
                break
            offset += limit

    def normalize(self, raw: dict) -> dict:
        """Transform raw debate data into standard schema."""
        date = raw.get("date", "")
        return {
            "_id": f"debate-{date}-{raw.get('number', '')}",
            "_source": "CA/OpenParliament",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"House of Commons Debate - {date} (Sitting #{raw.get('number', '')})",
            "text": raw.get("text", ""),
            "date": date,
            "url": f"https://openparliament.ca{raw.get('url', '')}",
            "session": raw.get("session", ""),
            "speech_count": raw.get("speech_count", 0),
            "source_url": raw.get("source_url", ""),
        }

    # -- Sample mode --------------------------------------------------------

    def _fetch_sample(self) -> list:
        """Fetch sample records for validation."""
        samples = []
        data = self._get_debates(limit=12, offset=0)

        for debate in data.get("objects", []):
            debate_url = debate.get("url", "")
            date = debate.get("date", "")

            # Get first 20 speeches only for sample
            speech_data = self._get_speeches(debate_url, limit=20)
            speeches = speech_data.get("objects", [])

            if not speeches:
                continue

            # Get debate detail for session info
            self.rate_limiter.wait()
            try:
                resp = self.client.get(debate_url, params={"format": "json"})
                resp.raise_for_status()
                detail = resp.json()
                debate["session"] = detail.get("session", "")
            except Exception:
                pass

            text = self._compile_debate(debate, speeches)
            if len(text) < 100:
                continue

            raw = {
                "date": date,
                "number": debate.get("number", ""),
                "session": debate.get("session", ""),
                "url": debate_url,
                "text": text,
                "speech_count": len(speeches),
                "source_url": debate.get("source_url", ""),
            }
            normalized = self.normalize(raw)
            samples.append(normalized)
            logger.info(f"  {date}: {len(speeches)} speeches, {len(text)} chars")

            if len(samples) >= 12:
                break

        return samples


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CA/OpenParliament data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = OpenParliamentScraper()

    if args.command == "test-api":
        print("Testing OpenParliament API...")
        data = scraper._get_debates(limit=3)
        debates = data.get("objects", [])
        if debates:
            print(f"OK: Found debates")
            for d in debates:
                print(f"  {d['date']}: Sitting #{d.get('number', '?')}")
        else:
            print("FAIL: No debates found")
            sys.exit(1)
        return

    if args.command == "bootstrap":
        if args.sample:
            print("Running sample mode...")
            samples = scraper._fetch_sample()
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            for i, record in enumerate(samples):
                fname = sample_dir / f"sample_{i+1:03d}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to sample/")
            if samples:
                texts = [s["text"] for s in samples if s.get("text")]
                avg_len = sum(len(t) for t in texts) // max(len(texts), 1)
                print(f"Average text length: {avg_len} chars")
                for s in samples:
                    assert s.get("text"), f"Missing text: {s['_id']}"
                    assert s.get("title"), f"Missing title: {s['_id']}"
                    assert s.get("date"), f"Missing date: {s['_id']}"
                print("All validation checks passed!")
            return

        result = scraper.bootstrap()
        print(f"Bootstrap complete: {result}")

    elif args.command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")


if __name__ == "__main__":
    main()
