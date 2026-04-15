#!/usr/bin/env python3
"""
IS/Landsrettur — Icelandic Court of Appeals (Landsréttur) Case Law

Fetches court decisions from the Icelandic Court of Appeals website.

Strategy:
  - AJAX pagination with pageitemid to discover case UUIDs
  - Fetch each decision page and extract text from sr-only div
  - ~6,000 decisions from 2018-present

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10 sample records
  python bootstrap.py update             # Incremental update
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

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
logger = logging.getLogger("legal-data-hunter.IS.Landsrettur")

# Site-specific constants
DOMAR_PATH = "/domar-og-urskurdir/"
DOMUR_PATH = "/domar-og-urskurdir/domur-urskurdur/"
PAGINATION_PATH = "/default.aspx"
PAGEITEM_ID = "landsrettur-domar-listing"
PAGE_SIZE = 50


def parse_date(date_str: str) -> Optional[str]:
    """Parse Icelandic date string to ISO format."""
    month_map = {
        'janúar': '01', 'febrúar': '02', 'mars': '03', 'apríl': '04',
        'maí': '05', 'júní': '06', 'júlí': '07', 'ágúst': '08',
        'september': '09', 'október': '10', 'nóvember': '11', 'desember': '12'
    }

    try:
        match = re.search(r'(\d{1,2})\.\s*(\w+)\s*(\d{4})', date_str)
        if match:
            day = match.group(1).zfill(2)
            month_name = match.group(2).lower()
            year = match.group(3)
            month = month_map.get(month_name)
            if month:
                return f"{year}-{month}-{day}"
    except Exception:
        pass

    try:
        match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            day = match.group(1).zfill(2)
            month = match.group(2).zfill(2)
            year = match.group(3)
            return f"{year}-{month}-{day}"
    except Exception:
        pass

    return None


def extract_text_from_html(soup: BeautifulSoup) -> str:
    """Extract clean text from the decision body."""
    body = soup.find('div', class_='sr-only')
    if not body:
        body = soup.find('div', class_='verdict__body')
    if not body:
        body = soup.find('div', class_='verdict')

    if not body:
        return ""

    for element in body(['script', 'style']):
        element.decompose()

    text = body.get_text(separator='\n', strip=True)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    return text.strip()


class LandsretturScraper(BaseScraper):
    """
    Scraper for IS/Landsrettur — Icelandic Court of Appeals decisions.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.base_url = self.config.get("api", {}).get("base_url", "https://www.landsrettur.is")
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalSourcesBot/1.0; worldwidelaw/legal-sources)",
        })

    def _extract_case_ids_from_listing(self, html: str) -> list[str]:
        """Extract case UUID IDs from the domar listing page."""
        pattern = r'[Ii]d=([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})'
        return list(set(re.findall(pattern, html)))

    def _get_all_case_ids(self, max_ids: int = None) -> list[str]:
        """Get all available case IDs using AJAX pagination."""
        all_ids = set()
        offset = 0
        consecutive_empty = 0

        domar_url = f"{self.base_url}{DOMAR_PATH}"
        pagination_url = f"{self.base_url}{PAGINATION_PATH}"

        logger.info(f"Fetching case listings from {domar_url}...")

        # First, get IDs from the main listing page
        try:
            resp = self.session.get(domar_url, timeout=30)
            resp.raise_for_status()
            ids = self._extract_case_ids_from_listing(resp.text)
            all_ids.update(ids)
            logger.info(f"Found {len(ids)} cases on main listing")
        except requests.RequestException as e:
            logger.error(f"Error fetching main listing: {e}")

        # Paginate through all decisions using AJAX endpoint
        logger.info(f"Paginating through archive (batch size: {PAGE_SIZE})...")

        while True:
            if max_ids and len(all_ids) >= max_ids:
                logger.info(f"Reached max_ids limit ({max_ids})")
                break

            try:
                params = {
                    'pageitemid': PAGEITEM_ID,
                    'offset': offset,
                    'count': PAGE_SIZE,
                }
                resp = self.session.get(pagination_url, params=params, timeout=30)
                resp.raise_for_status()

                ids = self._extract_case_ids_from_listing(resp.text)
                new_ids = [id for id in ids if id not in all_ids]

                if not new_ids:
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        logger.info(f"No new IDs for 3 consecutive pages, stopping at offset {offset}")
                        break
                else:
                    consecutive_empty = 0
                    all_ids.update(new_ids)

                if offset % 500 == 0:
                    logger.info(f"  Offset {offset}: {len(all_ids)} unique IDs collected")

                offset += PAGE_SIZE
                time.sleep(0.5)

                # Safety limit - ~6,000 decisions expected
                if offset > 8000:
                    logger.info(f"Safety limit reached at offset {offset}")
                    break

            except requests.RequestException as e:
                logger.error(f"Error at offset {offset}: {e}")
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                offset += PAGE_SIZE
                time.sleep(1)

        result = list(all_ids)
        logger.info(f"Total unique case IDs discovered: {len(result)}")
        return result

    def _parse_decision(self, html: str, case_id: str) -> Optional[dict]:
        """Parse a Court of Appeals decision HTML page."""
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Extract case number - format "Mál nr. N/YYYY"
            case_number = None
            heading = soup.find('h1') or soup.find('h2')
            if heading:
                heading_text = heading.get_text(strip=True)
                match = re.search(r'(\d+/\d{4})', heading_text)
                if match:
                    case_number = match.group(1)

            if not case_number:
                subtitle = soup.find('h2', class_='verdict-head__subtitle')
                if subtitle:
                    case_text = subtitle.get_text(strip=True)
                    match = re.search(r'(\d+/\d{4})', case_text)
                    if match:
                        case_number = match.group(1)

            # Extract date
            date = None
            time_elem = soup.find('time', class_='verdict-head__time')
            if time_elem:
                date_str = time_elem.get_text(strip=True)
                date = parse_date(date_str)
                if not date and time_elem.get('datetime'):
                    date = parse_date(time_elem.get('datetime'))

            # Extract parties
            parties = None
            parties_div = soup.find('div', class_='verdict-head__parties')
            if parties_div:
                parties = parties_div.get_text(strip=True)

            if not parties:
                for tag in soup.find_all(['h2', 'h3']):
                    tag_text = tag.get_text(strip=True)
                    if 'gegn' in tag_text and ('lögmaður' in tag_text or 'ehf' in tag_text):
                        parties = tag_text
                        break

            # Extract keywords
            keywords = []
            keyword_section = soup.find('div', class_='verdict__keywords')
            if keyword_section:
                keywords = [li.get_text(strip=True) for li in keyword_section.find_all('li')]
            if not keywords and keyword_section:
                keywords_text = keyword_section.get_text(strip=True)
                if keywords_text:
                    keywords = [k.strip().rstrip('.') for k in re.split(r'[.,]', keywords_text) if k.strip()]

            if not keywords:
                for tag in soup.find_all(['h3', 'h4', 'strong']):
                    if 'lykilorð' in tag.get_text(strip=True).lower() or 'lykilor' in tag.get_text(strip=True).lower():
                        next_elem = tag.find_next_sibling()
                        if next_elem:
                            kw_text = next_elem.get_text(strip=True)
                            keywords = [k.strip().rstrip('.') for k in re.split(r'[.,]', kw_text) if k.strip()]
                        break

            # Extract abstract/summary
            abstract = None
            abstract_section = soup.find('div', class_='verdict__reifun')
            if abstract_section:
                abstract = abstract_section.get_text(strip=True)

            if not abstract:
                for tag in soup.find_all(['h3', 'h4', 'strong']):
                    tag_text = tag.get_text(strip=True).lower()
                    if 'útdráttur' in tag_text or 'reifun' in tag_text:
                        next_div = tag.find_next_sibling('div')
                        if next_div:
                            abstract = next_div.get_text(strip=True)
                        else:
                            parts = []
                            for sibling in tag.next_siblings:
                                if sibling.name in ['h3', 'h4', 'h2']:
                                    break
                                text = sibling.get_text(strip=True) if hasattr(sibling, 'get_text') else str(sibling).strip()
                                if text:
                                    parts.append(text)
                            if parts:
                                abstract = ' '.join(parts)
                        break

            # Extract full text from sr-only div
            text = extract_text_from_html(soup)

            if not text or len(text) < 100:
                return None

            return {
                "case_id": case_id,
                "case_number": case_number,
                "date": date,
                "text": text,
                "parties": parties,
                "keywords": keywords,
                "abstract": abstract,
            }

        except Exception as e:
            logger.error(f"Error parsing decision {case_id}: {e}")
            return None

    def _fetch_decision(self, case_id: str) -> Optional[dict]:
        """Fetch and parse a single Court of Appeals decision."""
        url = f"{self.base_url}{DOMUR_PATH}?Id={case_id}"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return self._parse_decision(resp.text, case_id)
        except requests.RequestException as e:
            logger.error(f"Error fetching {case_id}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Court of Appeals decisions."""
        case_ids = self._get_all_case_ids()

        logger.info(f"Processing {len(case_ids)} cases...")

        for i, case_id in enumerate(case_ids):
            logger.info(f"[{i+1}/{len(case_ids)}] Fetching {case_id}...")

            raw = self._fetch_decision(case_id)
            if raw:
                yield raw

            time.sleep(1.5)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions modified since `since`."""
        for raw in self.fetch_all():
            if raw.get('date'):
                try:
                    doc_date = datetime.fromisoformat(raw['date'])
                    if doc_date >= since:
                        yield raw
                except (ValueError, TypeError):
                    yield raw

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standardized schema."""
        case_id = raw["case_id"]
        case_number = raw.get("case_number")
        doc_id = case_number if case_number else case_id
        title = f"Landsréttur - Mál nr. {case_number}" if case_number else f"Landsréttur {case_id[:8]}"

        record = {
            "_id": doc_id,
            "_source": "IS/Landsrettur",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "url": f"{self.base_url}{DOMUR_PATH}?Id={case_id}",
            "language": "isl",
            "court": "Landsréttur",
            "case_number": case_number,
        }

        if raw.get("parties"):
            record["parties"] = raw["parties"]
        if raw.get("keywords"):
            record["keywords"] = raw["keywords"]
        if raw.get("abstract"):
            record["abstract"] = raw["abstract"]

        return record


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    scraper = LandsretturScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
