#!/usr/bin/env python3
"""
BD/BDLaws -- Laws of Bangladesh (Bangladesh Code)

Fetches Bangladesh legislation (Acts, Ordinances, President's Orders) from
a CC-BY 4.0 HuggingFace dataset scraped from bdlaws.minlaw.gov.bd.

Strategy:
  - Download individual act JSON files from HuggingFace
  - Each JSON contains full text sections, footnotes, metadata
  - Concatenate sections into full text body
  - 1,484 acts from 1799 to 2025

Data: sakhadib/Bangladesh-Legal-Acts-Dataset (HuggingFace, CC-BY 4.0)
Original source: http://bdlaws.minlaw.gov.bd/ (unreachable outside Bangladesh)
Rate limit: 2 req/sec (respectful to HuggingFace).

Usage:
  python bootstrap.py bootstrap            # Full pull (all 1,484 acts)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BD.BDLaws")

HF_BASE = "https://huggingface.co/datasets/sakhadib/Bangladesh-Legal-Acts-Dataset/resolve/main"
ACT_URL = f"{HF_BASE}/acts/act-print-{{id}}.json"
BDLAWS_BASE = "http://bdlaws.minlaw.gov.bd"

# Known act ID range (1 to ~1549, with gaps)
MAX_ACT_ID = 1550

# Sample IDs known to have substantial content
SAMPLE_IDS = [
    367,  # Constitution of Bangladesh
    10,   # Societies Registration Act, 1860
    75,   # Penal Code, 1860
    100,  # Contract Act, 1872
    161,  # Transfer of Property Act, 1882
    26,   # Criminal Procedure Code, 1898
    50,   # Civil Procedure Code, 1908
    395,  # Companies Act, 1994
    415,  # Labour Act, 2006
    500,  # Income Tax Act
    1,    # Districts Act, 1836
    20,   # Oaths Act, 1873
    30,   # Dramatic Performances Act, 1876
    200,  # Presidency-Towns Insolvency Act
    300,  # Bengal Vagrancy Act
]


class BDLawsScraper(BaseScraper):
    """
    Scraper for BD/BDLaws -- Laws of Bangladesh.
    Country: BD
    URL: http://bdlaws.minlaw.gov.bd/

    Data types: legislation
    Auth: none (CC-BY 4.0 dataset)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=30,
        )

    # -- Fetch individual act ------------------------------------------------

    def _fetch_act(self, act_id: int) -> Optional[dict]:
        """Fetch a single act JSON from HuggingFace."""
        url = ACT_URL.format(id=act_id)
        try:
            resp = self.client.get(url, timeout=30)
            if resp is None or resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.debug(f"Act {act_id}: HTTP {resp.status_code}")
                return None
            data = resp.json()
            if not data.get("sections"):
                return None
            data["_act_id"] = act_id
            return data
        except Exception as e:
            logger.debug(f"Failed to fetch act {act_id}: {e}")
            return None

    # -- Text extraction -----------------------------------------------------

    @staticmethod
    def _extract_full_text(act: dict) -> str:
        """Combine all sections into full text."""
        sections = act.get("sections", [])
        parts = []
        for section in sections:
            content = section.get("section_content", "")
            if content:
                # Clean up any remaining HTML or special markers
                content = re.sub(r"<[^>]+>", "", content)
                content = re.sub(r"\s+", " ", content).strip()
                parts.append(content)

        # Add footnotes if available
        footnotes = act.get("footnotes", [])
        if footnotes:
            parts.append("\n--- Footnotes ---")
            for fn in footnotes:
                fn_text = fn.get("footnote_text", "")
                if fn_text:
                    fn_text = re.sub(r"<[^>]+>", "", fn_text)
                    fn_text = re.sub(r"\s+", " ", fn_text).strip()
                    parts.append(fn_text)

        return "\n\n".join(parts)

    # -- Date parsing --------------------------------------------------------

    @staticmethod
    def _parse_year(act: dict) -> Optional[str]:
        """Extract year from act metadata."""
        year = act.get("act_year")
        if year:
            year_str = str(year).strip()
            # Handle Bengali numerals or other formats
            if re.match(r"^\d{4}$", year_str):
                return f"{year_str}-01-01"
        return None

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all acts by scanning ID range."""
        found = 0
        for act_id in range(1, MAX_ACT_ID + 1):
            self.rate_limiter.wait()
            act = self._fetch_act(act_id)
            if not act:
                continue

            text = self._extract_full_text(act)
            if not text or len(text) < 50:
                logger.debug(f"Act {act_id}: text too short ({len(text)} chars)")
                continue

            act["_full_text"] = text
            found += 1
            if found % 100 == 0:
                logger.info(f"Progress: {found} acts found (scanned up to ID {act_id})")
            yield act

        logger.info(f"Scan complete: {found} acts from {MAX_ACT_ID} IDs")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch acts - dataset is static, so return all recent IDs."""
        logger.info("Dataset is static; fetching all acts")
        yield from self.fetch_all()

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample acts for validation."""
        found = 0
        for act_id in SAMPLE_IDS:
            if found >= count:
                break

            self.rate_limiter.wait()
            act = self._fetch_act(act_id)
            if not act:
                logger.debug(f"Sample act {act_id}: not found")
                continue

            text = self._extract_full_text(act)
            if not text or len(text) < 50:
                logger.debug(f"Sample act {act_id}: text too short")
                continue

            act["_full_text"] = text
            found += 1
            title = act.get("act_title", "N/A")[:60]
            logger.info(f"Sample {found}/{count}: Act {act_id} - {title} ({len(text)} chars)")
            yield act

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw act record to standard schema."""
        act_id = raw.get("_act_id", 0)
        title = raw.get("act_title", "Unknown Act")
        # Clean title (remove leading numbers/markers)
        title = re.sub(r"^[\d\s]*\[?\*+\]?\s*", "", title).strip()
        # Remove leading "1" artifact from some titles (e.g., "1The Districts Act")
        title = re.sub(r"^(\d+)(The |THE )", r"\2", title)
        if not title:
            title = f"Bangladesh Act {act_id}"

        text = raw.get("_full_text", "")
        date = self._parse_year(raw)
        source_url = raw.get("source_url", f"{BDLAWS_BASE}/act-print-{act_id}.html")

        return {
            "_id": f"BD-BDLaws-{act_id}",
            "_source": "BD/BDLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": source_url,
            "act_id": act_id,
            "act_no": raw.get("act_no"),
            "act_year": raw.get("act_year"),
            "language": raw.get("language", "english"),
            "token_count": raw.get("token_count"),
        }

    def test_api(self) -> bool:
        """Test HuggingFace dataset connectivity."""
        logger.info("Testing HuggingFace dataset access...")

        act = self._fetch_act(10)
        if not act:
            logger.error("Failed to fetch sample act")
            return False

        title = act.get("act_title", "N/A")
        sections = act.get("sections", [])
        text = self._extract_full_text(act)

        logger.info(f"Act found: {title[:60]}")
        logger.info(f"Sections: {len(sections)}, Text: {len(text)} chars")

        if len(text) < 50:
            logger.error("Text extraction returned too little content")
            return False

        logger.info("All tests passed")
        return True


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = BDLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        logger.info("Dataset is static; running full bootstrap instead")
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_all():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
