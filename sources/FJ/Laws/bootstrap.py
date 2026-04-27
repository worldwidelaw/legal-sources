#!/usr/bin/env python3
"""
FJ/Laws -- Laws of Fiji (laws.gov.fj)

Fetches all consolidated legislation from the official Laws of Fiji website,
maintained by the Office of the Attorney-General.

Strategy:
  - Fetch alphabetical act lists from /acts/actlist/{A-Z}
  - For each act, load /Acts/DisplayAct/{id} to get section IDs
  - Fetch full text of each section from /Acts/ViewSection/{section_id}
  - Concatenate all section texts to produce the full act text

URL patterns:
  - Act list by letter: /acts/actlist/A .. /acts/actlist/Z
  - Act TOC page: /Acts/DisplayAct/{act_id}
  - Section content: /Acts/ViewSection/{section_id}

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FJ.Laws")

BASE_URL = "https://www.laws.gov.fj"

# Letters used in the alphabetical index (some letters may have no acts)
ALPHABET = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

# Regex to extract act links from listing page
ACT_LINK_RE = re.compile(
    r'href=["\']?/Acts/DisplayAct/(\d+)["\']?\s*[^>]*>\s*([^<]+)',
    re.IGNORECASE,
)

# Regex to extract section IDs from act page
# Sections have data-id="12345" attribute on labels and links
SECTION_ID_RE = re.compile(r'data-id[=]"?(\d+)"?')

# Regex to extract last updated date from section pages
LAST_UPDATED_RE = re.compile(
    r'Last\s+Updated:\s*(\d{1,2}\s+\w+\s+\d{4})', re.IGNORECASE
)


def clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</tr>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</td>', '\t', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    # Normalize whitespace but preserve line breaks
    lines = text.split('\n')
    lines = [re.sub(r'[ \t]+', ' ', line).strip() for line in lines]
    text = '\n'.join(line for line in lines if line)
    return text.strip()


def extract_section_text(html_content: str) -> Tuple[str, Optional[str]]:
    """Extract section text and last-updated date from a ViewSection page.

    Returns (text, last_updated_date).
    """
    # Extract last updated date
    date_match = LAST_UPDATED_RE.search(html_content)
    last_updated = date_match.group(1) if date_match else None

    # Find the main content area - look for the act content between
    # the header/navigation and footer sections
    # The content typically appears after the breadcrumb/navigation
    # and before the footer "Contact Us" section
    content = html_content

    # Remove header/navigation (everything before the act title section)
    # The act content starts after the menu items
    menu_end = content.rfind('</style>')
    if menu_end > 0:
        content = content[menu_end:]

    # Remove footer
    footer_start = content.find('Contact Us')
    if footer_start > 0:
        content = content[:footer_start]

    text = clean_html(content)

    # Remove common noise patterns
    text = re.sub(r'I Agree\s*', '', text)
    text = re.sub(r'All\s+Principal\s+Subsidiary', '', text)
    text = re.sub(r'The Laws of Fiji', '', text, count=2)
    text = re.sub(r'Home\s+The Fijian Constitution.*?Contact Us', '', text, flags=re.DOTALL)
    text = re.sub(r'Search.*?Contact Us', '', text, flags=re.DOTALL)
    text = text.strip()

    return text, last_updated


class FijiLawsScraper(BaseScraper):
    """
    Scraper for FJ/Laws -- Laws of Fiji.
    Country: FJ
    URL: https://www.laws.gov.fj

    Data types: legislation
    Auth: none (free public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=60,
        )

    def _get_all_acts(self) -> List[Dict[str, str]]:
        """Fetch all acts from the alphabetical listing pages."""
        all_acts = []

        for letter in ALPHABET:
            url = f"/acts/actlist/{letter}"
            logger.info(f"Fetching act list: {letter}")

            try:
                self.rate_limiter.wait()
                resp = self.client.get(url)
                if resp.status_code == 404:
                    logger.debug(f"No acts for letter {letter}")
                    continue
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch list for {letter}: {e}")
                continue

            html = resp.text
            matches = ACT_LINK_RE.findall(html)

            for act_id, title in matches:
                title = title.strip()
                if title:
                    all_acts.append({
                        "act_id": act_id,
                        "title": title,
                        "url": f"{BASE_URL}/Acts/DisplayAct/{act_id}",
                    })

            logger.info(f"  Letter {letter}: {len(matches)} acts")

        logger.info(f"Total acts found: {len(all_acts)}")
        return all_acts

    def _get_section_ids(self, act_id: str) -> List[str]:
        """Get all section IDs from an act's DisplayAct page."""
        url = f"/Acts/DisplayAct/{act_id}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch act page {act_id}: {e}")
            return []

        html = resp.text
        section_ids = SECTION_ID_RE.findall(html)
        # Remove duplicates while preserving order
        seen = set()
        unique_ids = []
        for sid in section_ids:
            if sid not in seen:
                seen.add(sid)
                unique_ids.append(sid)

        return unique_ids

    def _fetch_section_text(self, section_id: str) -> Tuple[str, Optional[str]]:
        """Fetch full text of a section."""
        url = f"/Acts/ViewSection/{section_id}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.debug(f"Failed to fetch section {section_id}: {e}")
            return "", None

        return extract_section_text(resp.text)

    def _fetch_act_full_text(self, act_id: str, title: str) -> Tuple[str, Optional[str]]:
        """Fetch full text by fetching all sections and concatenating."""
        section_ids = self._get_section_ids(act_id)

        if not section_ids:
            logger.warning(f"No sections found for act {act_id}: {title}")
            return "", None

        logger.info(f"    Fetching {len(section_ids)} sections for: {title[:60]}")

        full_text_parts = []
        last_updated = None

        for i, sid in enumerate(section_ids):
            text, date = self._fetch_section_text(sid)
            if text:
                full_text_parts.append(text)
            if date and not last_updated:
                last_updated = date

            # Log progress for large acts
            if (i + 1) % 50 == 0:
                logger.info(f"      Progress: {i + 1}/{len(section_ids)} sections")

        full_text = "\n\n".join(full_text_parts)
        return full_text, last_updated

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        act_id = raw.get("act_id", "")

        return {
            "_id": f"FJ/Laws/{act_id}",
            "_source": "FJ/Laws",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", "Unknown"),
            "text": raw.get("text", ""),
            "date": raw.get("last_updated"),
            "url": raw.get("url", ""),
            "act_id": act_id,
            "section_count": raw.get("section_count", 0),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        all_acts = self._get_all_acts()
        if not all_acts:
            logger.error("No acts found")
            return

        count = 0
        errors = 0

        for i, act in enumerate(all_acts):
            act_id = act["act_id"]
            title = act["title"]

            logger.info(f"  [{i + 1}/{len(all_acts)}] Processing: {title[:60]}")

            full_text, last_updated = self._fetch_act_full_text(act_id, title)

            if not full_text or len(full_text.strip()) < 50:
                logger.warning(
                    f"  Insufficient text for {act_id}: "
                    f"{len(full_text) if full_text else 0} chars"
                )
                errors += 1
                continue

            act["text"] = full_text
            act["last_updated"] = last_updated
            act["section_count"] = len(self._get_section_ids(act_id))
            yield act
            count += 1

        logger.info(f"Fetched {count} acts ({errors} errors)")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = FijiLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing act listing...")
        acts = scraper._get_all_acts()
        if not acts:
            logger.error("FAILED — no acts found")
            sys.exit(1)
        logger.info(f"OK — {len(acts)} acts found")

        logger.info("Testing section fetch...")
        first = acts[0]
        section_ids = scraper._get_section_ids(first["act_id"])
        if not section_ids:
            logger.error(f"FAILED — no sections for {first['title']}")
            sys.exit(1)
        logger.info(f"OK — {len(section_ids)} sections for: {first['title']}")

        text, date = scraper._fetch_section_text(section_ids[0])
        logger.info(f"OK — section text: {len(text)} chars, date: {date}")

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
