#!/usr/bin/env python3
"""
FI/FinlexInsuranceCourt — Finland Insurance Court Decisions (Vakuutusoikeus)

Fetches from Finlex (www.finlex.fi), Finland's official legal information service.
The Insurance Court (Vakuutusoikeus) handles social security appeal cases.
~1,500-2,000 selected decisions from 1957 to present.

Strategy:
  - Fetch year index pages to discover decision URLs
  - Fetch individual decision HTML pages (Next.js SSR)
  - Extract full text from React Server Component payload (escaped JSON in script tags)
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.finlex.fi"
VAKO_URL = f"{BASE_URL}/fi/oikeuskaytanto/vakuutusoikeus"
SOURCE_ID = "FI/FinlexInsuranceCourt"
RATE_LIMIT = 1.5


class FinlexInsuranceCourtFetcher:
    """Fetcher for Finnish Insurance Court decisions from Finlex."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data-research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self._last_request = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < RATE_LIMIT:
            time.sleep(RATE_LIMIT - elapsed)

    def _get(self, url: str, rsc: bool = False) -> str:
        """Fetch URL and return text content (UTF-8 decoded)."""
        self._rate_limit()
        self._last_request = time.time()
        headers = {"RSC": "1"} if rsc else {}
        resp = self.session.get(url, timeout=30, headers=headers)
        resp.raise_for_status()
        return resp.content.decode("utf-8")

    def _get_available_years(self) -> List[int]:
        """Get all available years from the main VAKO page."""
        data = self._get(VAKO_URL, rsc=True)
        years = set()
        for match in re.finditer(r'vakuutusoikeus/(\d{4})', data):
            year = int(match.group(1))
            if 1950 <= year <= 2030:
                years.add(year)
        return sorted(years, reverse=True)

    def _get_decisions_for_year(self, year: int) -> List[Tuple[str, str]]:
        """Get all decision refs (number-year) for a given year."""
        url = f"{VAKO_URL}/{year}/"
        try:
            data = self._get(url, rsc=True)
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Failed to fetch year {year}: {e}")
            return []

        decisions = set()
        for match in re.finditer(rf'vakuutusoikeus/{year}/(\d+-\d+)', data):
            ref = match.group(1)
            decisions.add((str(year), ref))
        return sorted(decisions)

    def _extract_text_from_rsc(self, data: str) -> str:
        """Extract full text from Next.js RSC payload.

        With RSC: 1 header, the server returns raw RSC data without script
        wrappers. Content is in spans with className "highlightable".
        """
        text_parts = []

        # Match: "highlightable","children":"TEXT"
        for match in re.finditer(
            r'"highlightable","children":"((?:[^"\\]|\\.)*)"',
            data,
        ):
            raw = match.group(1)
            try:
                text = json.loads('"' + raw + '"')
            except Exception:
                text = raw.replace('\\n', '\n').replace('\\"', '"').replace('\\t', '\t')
            text_parts.append(text.strip())

        return "\n".join(part for part in text_parts if part)

    def _extract_metadata_from_rsc(self, data: str) -> Dict[str, Any]:
        """Extract metadata from RSC payload."""
        meta = {}

        # Extract date from dateTime field
        date_match = re.search(r'"dateTime":"([^"]+)"', data)
        if date_match:
            meta["date_iso"] = date_match.group(1)[:10]

        # Extract title: "children":"VakO ..."
        title_match = re.search(
            r'"children":"(VakO[^"]*)"',
            data,
        )
        if title_match:
            raw = title_match.group(1)
            try:
                meta["title"] = json.loads('"' + raw + '"')
            except Exception:
                meta["title"] = raw
            # Clean trailing pipe content
            meta["title"] = re.sub(r'\s*\|.*$', '', meta["title"]).strip()

        # Extract date from title pattern if no dateTime found
        if "date_iso" not in meta and "title" in meta:
            date_in_title = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', meta["title"])
            if date_in_title:
                meta["date_raw"] = date_in_title.group(1)

        # Extract subject/keyword tags
        tags = []
        for match in re.finditer(r'"tagText":"((?:[^"\\]|\\.)*)"', data):
            raw = match.group(1)
            try:
                tag = json.loads('"' + raw + '"')
            except Exception:
                tag = raw
            tags.append(tag.strip())
        if tags:
            meta["subject_tags"] = tags

        return meta

    @staticmethod
    def _parse_finnish_date(date_str: str) -> Optional[str]:
        """Parse Finnish date '6.11.2024' to ISO format."""
        if not date_str:
            return None
        match = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", date_str.strip())
        if match:
            return f"{match.group(3)}-{match.group(2).zfill(2)}-{match.group(1).zfill(2)}"
        return None

    def fetch_all(self, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Yield raw document data from all insurance court decisions."""
        years = self._get_available_years()
        logger.info(f"Found {len(years)} years with decisions: {years[0]}–{years[-1]}")

        count = 0
        for year in years:
            if limit and count >= limit:
                return

            decisions = self._get_decisions_for_year(year)
            logger.info(f"Year {year}: {len(decisions)} decisions")

            for year_str, ref in decisions:
                if limit and count >= limit:
                    return

                url = f"{VAKO_URL}/{year_str}/{ref}"
                logger.info(f"  Fetching {year_str}/{ref}")

                try:
                    data = self._get(url, rsc=True)
                except Exception as e:
                    logger.warning(f"  Failed to fetch {year_str}/{ref}: {e}")
                    continue

                text = self._extract_text_from_rsc(data)
                meta = self._extract_metadata_from_rsc(data)

                if not text or len(text) < 50:
                    logger.warning(f"  Skipping {year_str}/{ref}: insufficient text ({len(text)} chars)")
                    continue

                raw = {
                    "year": year_str,
                    "ref": ref,
                    "url": url,
                    "title": meta.get("title", f"VakO {ref}"),
                    "text": text,
                    "date_iso": meta.get("date_iso"),
                    "date_raw": meta.get("date_raw", ""),
                    "subject_tags": meta.get("subject_tags", []),
                }

                count += 1
                yield raw

                if count % 10 == 0:
                    logger.info(f"  Fetched {count} decisions so far...")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw decision into standard schema."""
        year = raw.get("year", "")
        ref = raw.get("ref", "")
        safe_ref = ref.replace("/", "-")
        date = raw.get("date_iso") or self._parse_finnish_date(raw.get("date_raw", ""))

        return {
            "_id": f"VAKO-{year}-{safe_ref}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", f"VakO {ref}"),
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "case_number": ref,
            "year": year,
            "ecli": f"ECLI:FI:VAKO:{year}:{ref.split('-')[0]}" if ref else "",
            "subject_tags": raw.get("subject_tags", []),
        }


def main():
    fetcher = FinlexInsuranceCourtFetcher()

    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap of FI/FinlexInsuranceCourt...")

        sample_count = 0
        target = 15 if "--sample" in sys.argv else 50

        for raw_doc in fetcher.fetch_all(limit=target * 2):
            if sample_count >= target:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get("text", ""))

            if text_len < 100:
                logger.warning(
                    f"  Skipping {normalized['case_number']}: "
                    f"only {text_len} chars of text"
                )
                continue

            doc_id = normalized["_id"].replace("/", "_").replace(" ", "_")
            filepath = sample_dir / f"{doc_id}.json"

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            sample_count += 1
            logger.info(
                f"Saved [{sample_count}/{target}]: {normalized['case_number']} "
                f"({text_len:,} chars)"
            )

        logger.info(f"Bootstrap complete. {sample_count} documents saved to {sample_dir}")

        files = list(sample_dir.glob("*.json"))
        total_chars = sum(
            len(json.load(open(f, encoding="utf-8")).get("text", ""))
            for f in files
        )
        logger.info(f"Summary: {len(files)} files, {total_chars:,} total text chars")
        if files:
            logger.info(f"Average: {total_chars // len(files):,} chars/document")
    else:
        print("Usage: python bootstrap.py bootstrap [--sample]")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
