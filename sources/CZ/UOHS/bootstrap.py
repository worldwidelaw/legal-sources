#!/usr/bin/env python3
"""
Czech Competition Authority (ÚOHS) Decisions

JSON-LD open data at uohs.gov.cz/opendata/rozhodnuti.jsonld
with full text on HTML detail pages.

~10K+ decisions covering competition law, public procurement, state aid.
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

JSONLD_URL = "https://uohs.gov.cz/opendata/rozhodnuti.jsonld"
SOURCE_ID = "CZ/UOHS"
RATE_LIMIT = 1.5


class UOHSFetcher:
    """Fetcher for Czech Competition Authority decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data-research)",
            "Accept": "text/html,application/json",
        })
        self._last_request = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < RATE_LIMIT:
            time.sleep(RATE_LIMIT - elapsed)
        self._last_request = time.time()

    def _get_json(self, url: str) -> Any:
        self._rate_limit()
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _get_html(self, url: str) -> str:
        self._rate_limit()
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _extract_text_from_html(self, html: str) -> str:
        """Extract decision text from HTML detail page."""
        # Extract paragraphs with substantial content
        paras = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL)
        text_parts = []
        for p in paras:
            clean = re.sub(r'<[^>]+>', ' ', p)
            clean = unescape(clean)
            clean = re.sub(r'\s+', ' ', clean).strip()
            if len(clean) > 50:
                text_parts.append(clean)
        return "\n\n".join(text_parts)

    def fetch_all(self, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Yield raw decision data from JSON-LD + HTML detail pages."""
        logger.info("Downloading JSON-LD index...")
        data = self._get_json(JSONLD_URL)
        items = data.get("rozhodnutí", [])
        logger.info(f"Found {len(items)} decisions in JSON-LD")

        count = 0
        for item in items:
            if limit and count >= limit:
                return

            detail_url = item.get("iri", "")
            if not detail_url:
                continue

            try:
                html = self._get_html(detail_url)
                text = self._extract_text_from_html(html)
            except Exception as e:
                logger.warning(f"Failed to fetch {detail_url}: {e}")
                continue

            if len(text) < 100:
                continue

            item["_text"] = text
            item["_detail_url"] = detail_url
            count += 1
            yield item

            if count % 10 == 0:
                logger.info(f"Fetched {count} decisions...")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw decision into standard schema."""
        subject = raw.get("věc", {})
        if isinstance(subject, dict):
            subject = subject.get("cs", "")

        case_ref = raw.get("spisová_značka", "")
        file_ref = raw.get("číslo_jednací", "")

        # Participants
        participants = []
        for p in raw.get("účastník", []):
            if isinstance(p, dict):
                name = p.get("jméno", {})
                if isinstance(name, dict):
                    name = name.get("cs", "")
                if name:
                    participants.append(name)

        # Date
        date_obj = raw.get("datum_právní_moci", {})
        date = date_obj.get("datum", "") if isinstance(date_obj, dict) else ""

        # Document URL
        doc_urls = []
        for d in raw.get("dokument", []):
            if isinstance(d, dict) and d.get("url"):
                doc_urls.append(d["url"])

        title_parts = [case_ref or file_ref]
        if subject:
            title_parts.append(f"— {subject[:100]}")
        title = " ".join(title_parts)

        return {
            "_id": f"UOHS-{case_ref}" if case_ref else f"UOHS-{file_ref}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": date,
            "url": raw.get("_detail_url", raw.get("iri", "")),
            "case_reference": case_ref,
            "file_reference": file_ref,
            "subject": subject,
            "participants": participants,
            "instance": raw.get("instance", []),
            "department": raw.get("odbor", []),
            "proceeding_type": raw.get("typ_řízení", []),
            "decision_type": raw.get("typ_rozhodnutí", []),
            "document_urls": doc_urls,
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        fetcher = UOHSFetcher()
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap of CZ/UOHS...")

        sample_count = 0
        target = 15 if "--sample" in sys.argv else 50

        for raw_doc in fetcher.fetch_all(limit=target * 2):
            if sample_count >= target:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get("text", ""))

            if text_len < 500:
                continue

            doc_id = normalized["_id"].replace("/", "_").replace(" ", "_")
            filepath = sample_dir / f"{doc_id}.json"

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            sample_count += 1
            logger.info(
                f"Saved [{sample_count}/{target}]: {normalized.get('case_reference', '')} "
                f"({text_len:,} chars)"
            )

        logger.info(f"Bootstrap complete. {sample_count} decisions saved to {sample_dir}")

        files = list(sample_dir.glob("*.json"))
        total_chars = sum(
            len(json.load(open(f, encoding="utf-8")).get("text", ""))
            for f in files
        )
        logger.info(f"Summary: {len(files)} files, {total_chars:,} total text chars")
        if files:
            logger.info(f"Average: {total_chars // len(files):,} chars/decision")

    else:
        print("Usage: python bootstrap.py bootstrap [--sample]")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
