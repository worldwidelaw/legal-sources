#!/usr/bin/env python3
"""
HR/Sabor -- Croatian Parliament Debate Transcripts Fetcher

Fetches full-text parliamentary debate transcripts (fonogrami) from
the Croatian Parliament (Hrvatski sabor) e-document system.

Strategy:
  - Grid listing at edoc.sabor.hr/Fonogrami.aspx provides debate IDs.
  - Each debate is at /Views/FonogramView.aspx?tdrid={id}.
  - HTML structure: speaker entries with <div class="contentHeader speaker">
    containing <h2>Surname, Name (PARTY)</h2> followed by speech text in
    <dd class="textColor">.

Endpoints:
  - Grid: https://edoc.sabor.hr/Fonogrami.aspx
  - Fonogram: https://edoc.sabor.hr/Views/FonogramView.aspx?tdrid={id}

Data:
  - Multiple parliamentary terms (sazivi), ~740 debates total
  - Language: Croatian (HRV)
  - Rate limit: 1 request/second (conservative)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HR.sabor")

BASE_URL = "https://edoc.sabor.hr"


class CroatianParliamentScraper(BaseScraper):
    """
    Scraper for HR/Sabor -- Croatian Parliament debate transcripts.
    Country: HR
    URL: https://edoc.sabor.hr

    Data types: doctrine (parliamentary debate transcripts)
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept-Language": "hr,en",
            },
            timeout=60,
        )

    def _extract_tdrids_from_grid(self) -> List[str]:
        """
        Fetch the Fonogrami.aspx grid page and extract tdrid values
        from FonogramView links.

        Returns list of tdrid strings from the first page (10 items).
        """
        tdrids = []
        try:
            self.rate_limiter.wait()
            resp = self.client.get("/Fonogrami.aspx")
            resp.raise_for_status()
            content = resp.text

            # Extract tdrid values from links
            pattern = re.compile(r'FonogramView\.aspx\?tdrid=(\d+)')
            seen = set()
            for match in pattern.finditer(content):
                tdrid = match.group(1)
                if tdrid not in seen:
                    seen.add(tdrid)
                    tdrids.append(tdrid)

            logger.info(f"Extracted {len(tdrids)} tdrids from grid page")
        except Exception as e:
            logger.error(f"Failed to fetch grid page: {e}")
        return tdrids

    def _extract_tdrids_from_grid_all(self) -> List[str]:
        """
        Extract tdrids from the grid, including pagination via ASP.NET postbacks.
        Falls back to ID range enumeration if postback fails.
        """
        all_tdrids = self._extract_tdrids_from_grid()

        if not all_tdrids:
            return all_tdrids

        # For full fetch, also try enumerating backward from the highest known ID.
        # The grid has ~740 items. IDs are roughly sequential but with gaps.
        max_id = max(int(t) for t in all_tdrids)
        min_id = max_id - 1500  # generous range to cover gaps

        logger.info(f"Enumerating tdrids from {min_id} to {max_id}")

        for tdrid in range(max_id, min_id, -1):
            tdrid_str = str(tdrid)
            if tdrid_str not in set(all_tdrids):
                all_tdrids.append(tdrid_str)

        return all_tdrids

    def _parse_fonogram(self, html_content: str, tdrid: str) -> Optional[Dict[str, Any]]:
        """
        Parse a FonogramView.aspx page into structured data.

        Returns dict with: tdrid, saziv, sjednica, agenda_item, title, date, speakers, text
        Or None if the page has no content.
        """
        # Check if page has real content (empty templates are ~10KB)
        if len(html_content) < 15000:
            return None

        # Extract session info: "Saziv: XI, sjednica: 9"
        saziv = ""
        sjednica = ""
        session_match = re.search(
            r'lblSazivSjednicaDatum[^>]*>([^<]+)<', html_content
        )
        if session_match:
            session_text = session_match.group(1).strip()
            saziv_match = re.search(r'Saziv:\s*([^,]+)', session_text)
            sjednica_match = re.search(r'sjednica:\s*(\d+)', session_text)
            if saziv_match:
                saziv = saziv_match.group(1).strip()
            if sjednica_match:
                sjednica = sjednica_match.group(1).strip()

        # Extract agenda item number
        agenda_item = ""
        agenda_match = re.search(
            r'lblTdrBrojevi[^>]*>(\d+)<', html_content
        )
        if agenda_match:
            agenda_item = agenda_match.group(1).strip()

        # Extract agenda item title from <li> in contentList
        title = ""
        title_match = re.search(
            r'<ul\s+class="contentList">\s*<li>([^<]+)</li>',
            html_content, re.DOTALL
        )
        if title_match:
            title = html.unescape(title_match.group(1).strip())

        # Extract date from dateString
        date_str = ""
        date_match = re.search(
            r'class="dateString">\s*([^<]+)<', html_content
        )
        if date_match:
            raw_date = date_match.group(1).strip().rstrip(".")
            # Parse Croatian date format: DD.MM.YYYY
            try:
                parts = raw_date.split(".")
                if len(parts) >= 3:
                    day, month, year = parts[0], parts[1], parts[2]
                    date_str = f"{year.strip()}-{month.strip().zfill(2)}-{day.strip().zfill(2)}"
            except Exception:
                date_str = raw_date

        # Extract speakers and their text
        speakers = []
        text_parts = []

        # Pattern: <div class="contentHeader speaker"> ... <h2>Name (PARTY)</h2> ...
        # followed by <dd class="textColor">speech text</dd>
        speaker_pattern = re.compile(
            r'<div\s+class="contentHeader\s+speaker">\s*'
            r'.*?<h2>([^<]+)</h2>.*?'
            r'<dd\s+class="textColor">\s*(.*?)\s*</dd>',
            re.DOTALL
        )

        for match in speaker_pattern.finditer(html_content):
            speaker_name = html.unescape(match.group(1).strip())
            speech_html = match.group(2).strip()

            # Clean speech text: replace <br /> with newlines, strip tags
            speech_text = re.sub(r'<br\s*/?>', '\n', speech_html)
            speech_text = re.sub(r'<[^>]+>', '', speech_text)
            speech_text = html.unescape(speech_text)
            speech_text = re.sub(r'\n\s*\n', '\n\n', speech_text).strip()
            # Clean up leading dashes/bullets
            speech_text = re.sub(r'^\s*-\s*', '', speech_text)

            if speaker_name and speaker_name not in speakers:
                speakers.append(speaker_name)

            if speech_text:
                text_parts.append(f"[{speaker_name}]\n{speech_text}")

        full_text = "\n\n".join(text_parts)

        if not full_text or len(full_text) < 50:
            return None

        return {
            "tdrid": tdrid,
            "saziv": saziv,
            "sjednica": sjednica,
            "agenda_item": agenda_item,
            "title": title,
            "date": date_str,
            "speakers": speakers,
            "full_text": full_text,
        }

    def _fetch_fonogram(self, tdrid: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single fonogram by tdrid."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/Views/FonogramView.aspx?tdrid={tdrid}")
            resp.raise_for_status()
            return self._parse_fonogram(resp.text, tdrid)
        except Exception as e:
            logger.warning(f"Failed to fetch fonogram {tdrid}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all debate transcripts from the Croatian Parliament.

        First fetches the grid to discover IDs, then enumerate a range
        of IDs to find all debates.
        """
        tdrids = self._extract_tdrids_from_grid_all()

        if not tdrids:
            logger.error("No tdrids found, cannot proceed")
            return

        logger.info(f"Processing {len(tdrids)} potential tdrids")
        yielded = 0
        empty_streak = 0

        for tdrid in tdrids:
            result = self._fetch_fonogram(tdrid)
            if result:
                empty_streak = 0
                yielded += 1
                yield result
                if yielded % 50 == 0:
                    logger.info(f"Yielded {yielded} fonograms so far")
            else:
                empty_streak += 1
                # If we've hit 100 empty IDs in a row during enumeration,
                # we've likely passed the start of the range
                if empty_streak > 100:
                    logger.info(f"100 consecutive empty IDs, stopping enumeration")
                    break

        logger.info(f"Total fonograms yielded: {yielded}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield debates from the grid's first page (most recent items).
        Filter by date >= since.
        """
        tdrids = self._extract_tdrids_from_grid()

        for tdrid in tdrids:
            result = self._fetch_fonogram(tdrid)
            if not result:
                continue

            # Filter by date
            if result.get("date"):
                try:
                    doc_date = datetime.strptime(result["date"], "%Y-%m-%d")
                    doc_date = doc_date.replace(tzinfo=timezone.utc)
                    if doc_date < since:
                        continue
                except Exception:
                    pass

            yield result

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw fonogram data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        tdrid = raw.get("tdrid", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        date_str = raw.get("date", "")
        saziv = raw.get("saziv", "")
        sjednica = raw.get("sjednica", "")
        agenda_item = raw.get("agenda_item", "")
        speakers = raw.get("speakers", [])

        url = f"{BASE_URL}/Views/FonogramView.aspx?tdrid={tdrid}"

        # Build a descriptive title if empty
        if not title:
            title = f"Saziv {saziv}, sjednica {sjednica}, točka {agenda_item}"

        return {
            "_id": f"fonogram-{tdrid}",
            "_source": "HR/Sabor",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_str,
            "url": url,
            "tdrid": tdrid,
            "saziv": saziv,
            "sjednica": sjednica,
            "agenda_item": agenda_item,
            "speakers": speakers,
            "language": "hrv",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Croatian Parliament (edoc.sabor.hr) endpoints...")

        # Test grid page
        print("\n1. Testing Fonogrami.aspx grid...")
        try:
            tdrids = self._extract_tdrids_from_grid()
            print(f"   Found {len(tdrids)} tdrids on first page")
            if tdrids:
                print(f"   IDs: {', '.join(tdrids[:5])}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test fonogram fetch
        if tdrids:
            print(f"\n2. Testing FonogramView (tdrid={tdrids[0]})...")
            try:
                result = self._fetch_fonogram(tdrids[0])
                if result:
                    print(f"   Saziv: {result['saziv']}, Sjednica: {result['sjednica']}")
                    print(f"   Title: {result['title'][:80]}...")
                    print(f"   Date: {result['date']}")
                    print(f"   Speakers: {len(result['speakers'])}")
                    print(f"   Text length: {len(result['full_text'])} chars")
                    print(f"   Text sample: {result['full_text'][:200]}...")
                else:
                    print("   ERROR: No content parsed")
            except Exception as e:
                print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = CroatianParliamentScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
