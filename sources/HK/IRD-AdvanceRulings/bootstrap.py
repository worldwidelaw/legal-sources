#!/usr/bin/env python3
"""
HK/IRD-AdvanceRulings -- Hong Kong IRD Advance Rulings

Fetches anonymised advance ruling cases from the Hong Kong Inland Revenue Department.

Strategy:
  - Discovery: Index page at ird.gov.hk/eng/ppr/arc.htm lists all rulings by category
  - Full text: Each ruling is an HTML page at /eng/ppr/advance{N}.htm (N=1..78+)
  - Clean HTML content to plain text

Endpoints:
  - Index: https://www.ird.gov.hk/eng/ppr/arc.htm
  - Individual: https://www.ird.gov.hk/eng/ppr/advance{N}.htm

Data:
  - 78 advance ruling cases (as of 2025)
  - No authentication required
  - Full text in HTML

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HK.IRD-AdvanceRulings")

BASE_URL = "https://www.ird.gov.hk"
INDEX_URL = f"{BASE_URL}/eng/ppr/arc.htm"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class HKAdvanceRulingsScraper(BaseScraper):
    """
    Scraper for HK/IRD-AdvanceRulings -- Hong Kong IRD Advance Rulings.
    Country: HK
    URL: https://www.ird.gov.hk/eng/ppr/arc.htm

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, timeout: int = 60) -> requests.Response:
        """Make HTTP GET request with rate limiting."""
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp

    def _parse_index(self) -> List[Dict[str, Any]]:
        """Parse the index page to get all ruling links with categories."""
        resp = self._get(INDEX_URL)
        soup = BeautifulSoup(resp.content, "html.parser")

        rulings = []
        seen_numbers = set()
        current_category = "General"

        # Find the main content area
        content = soup.find("div", id="content_area") or soup.find("div", class_="content") or soup

        for element in content.find_all(["h2", "h3", "li", "a"]):
            # Track categories from headings
            if element.name in ("h2", "h3"):
                cat_text = element.get_text(strip=True)
                if cat_text and "Advance Ruling" not in cat_text and len(cat_text) < 100:
                    current_category = cat_text
                continue

            # Process links to advance rulings
            if element.name == "a":
                href = element.get("href", "")
                if "advance" not in href.lower():
                    continue

                # Extract ruling number
                match = re.search(r'advance(\d+)\.htm', href, re.IGNORECASE)
                if not match:
                    continue

                num = int(match.group(1))
                if num in seen_numbers:
                    continue
                seen_numbers.add(num)

                title = element.get_text(strip=True)
                # Clean title
                title = re.sub(r'\s+', ' ', title).strip()
                if not title or title.isdigit():
                    title = f"Advance Ruling Case No. {num}"

                rulings.append({
                    "number": num,
                    "title": title,
                    "category": current_category,
                    "url": f"{BASE_URL}/eng/ppr/advance{num}.htm",
                })

            # Also check li elements that contain links
            elif element.name == "li":
                link = element.find("a", href=re.compile(r'advance\d+\.htm'))
                if link:
                    href = link.get("href", "")
                    match = re.search(r'advance(\d+)\.htm', href)
                    if not match:
                        continue

                    num = int(match.group(1))
                    if num in seen_numbers:
                        continue
                    seen_numbers.add(num)

                    title = link.get_text(strip=True) or element.get_text(strip=True)
                    title = re.sub(r'\s+', ' ', title).strip()
                    if not title or title.isdigit():
                        title = f"Advance Ruling Case No. {num}"

                    rulings.append({
                        "number": num,
                        "title": title,
                        "category": current_category,
                        "url": f"{BASE_URL}/eng/ppr/advance{num}.htm",
                    })

        # Sort by number
        rulings.sort(key=lambda x: x["number"])
        logger.info(f"Found {len(rulings)} advance rulings on index page")
        return rulings

    def _fetch_ruling_text(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a single ruling page and extract text + metadata."""
        try:
            resp = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(resp.content, "html.parser")

        # Remove navigation, headers, footers
        for tag in soup.find_all(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        # Remove "back to top" links
        for link in soup.find_all("a", string=re.compile(r"back to top|Top", re.IGNORECASE)):
            link.decompose()

        # Find main content area
        content = soup.find("div", id="content_area") or soup.find("div", class_="content") or soup.find("body")
        if not content:
            return None

        # Extract text, preserving some structure
        text_parts = []
        for element in content.find_all(["h1", "h2", "h3", "h4", "p", "li", "td", "pre"]):
            text = element.get_text(separator=" ", strip=True)
            if text and text not in ("Back to Top", "top"):
                if element.name in ("h1", "h2", "h3", "h4"):
                    text_parts.append(f"\n## {text}\n")
                elif element.name == "li":
                    text_parts.append(f"- {text}")
                else:
                    text_parts.append(text)

        full_text = "\n".join(text_parts).strip()

        # If structured extraction got too little, fall back to get_text
        if len(full_text) < 200:
            full_text = content.get_text(separator="\n", strip=True)
            # Clean up excessive whitespace
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            full_text = re.sub(r'[ \t]+', ' ', full_text)

        # Extract date if present
        date = None
        date_match = re.search(
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            full_text
        )
        if date_match:
            try:
                date_str = f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}"
                dt = datetime.strptime(date_str, "%d %B %Y")
                date = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return {"text": full_text, "date": date}

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        return {
            "_id": f"HK-IRD-AR-{raw['number']}",
            "_source": "HK/IRD-AdvanceRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "doc_id": f"advance-ruling-{raw['number']}",
            "category": raw.get("category", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all advance ruling cases."""
        rulings = self._parse_index()
        if not rulings:
            logger.error("No rulings found on index page")
            return

        sample_limit = 15 if sample else None
        fetched = 0

        for ruling in rulings:
            if sample_limit and fetched >= sample_limit:
                break

            num = ruling["number"]
            url = ruling["url"]
            logger.info(f"Fetching ruling #{num}: {ruling['title'][:50]}...")

            result = self._fetch_ruling_text(url)
            if not result or not result.get("text"):
                logger.warning(f"No text extracted for ruling #{num}")
                continue

            if len(result["text"]) < 100:
                logger.warning(f"Insufficient text for ruling #{num}: {len(result['text'])} chars")
                continue

            ruling["text"] = result["text"]
            ruling["date"] = result.get("date", "")

            record = self.normalize(ruling)
            yield record
            fetched += 1

        logger.info(f"Total fetched: {fetched}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates - re-fetches all since there's no date filtering on index."""
        yield from self.fetch_all(sample=False)

    def test(self) -> bool:
        """Quick connectivity test."""
        logger.info("Testing HK/IRD-AdvanceRulings connectivity...")
        try:
            rulings = self._parse_index()
            logger.info(f"Found {len(rulings)} rulings")

            if rulings:
                result = self._fetch_ruling_text(rulings[0]["url"])
                if result and result.get("text"):
                    logger.info(f"Sample text length: {len(result['text'])} chars")
                    logger.info("Test PASSED")
                    return True

            logger.error("Test FAILED: Could not extract text")
            return False
        except Exception as e:
            logger.error(f"Test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="HK/IRD-AdvanceRulings bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = HKAdvanceRulingsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            filename = re.sub(r'[^\w\-.]', '_', f"{record['_id']}.json")
            filepath = sample_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
