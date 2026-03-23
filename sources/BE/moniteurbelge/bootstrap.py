#!/usr/bin/env python3
"""
BE/MoniteurBelge -- Belgian Official Journal Data Fetcher

Fetches Belgian federal legislation from the Moniteur Belge / Belgisch Staatsblad.

Strategy:
  - Uses ELI (European Legislation Identifier) URIs for discovery and full text.
  - Browse by year: /eli/{type}/{year} lists all documents for that year.
  - Full text: /eli/{type}/{yyyy}/{mm}/{dd}/{numac}/justel returns consolidated HTML.
  - Parse HTML to extract clean text content.

Endpoints:
  - Browse: https://www.ejustice.just.fgov.be/eli/loi/2024
  - Full text: https://www.ejustice.just.fgov.be/eli/loi/2024/01/07/2024000164/justel

Data:
  - Legislation types: loi, decret, ordonnance, arrete, constitution
  - Languages: French, Dutch, German
  - License: CC Zero (CC 0)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent years only)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BE.moniteurbelge")

# Base URL for Belgian ELI system
BASE_URL = "https://www.ejustice.just.fgov.be"

# Legislation types to scrape
LEGISLATION_TYPES = ["loi", "decret", "ordonnance", "arrete"]

# Years to scrape (most recent first for sample mode)
YEARS_TO_SCRAPE = list(range(2024, 1997, -1))  # 2024 down to 1998


class MoniteurBelgeScraper(BaseScraper):
    """
    Scraper for BE/MoniteurBelge -- Belgian Official Journal.
    Country: BE
    URL: https://www.ejustice.just.fgov.be

    Data types: legislation
    Auth: none (Open Government Data, CC Zero)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept-Language": "fr,nl,de",
            },
            timeout=60,
        )

    def _parse_year_listing(self, html_content: str, doc_type: str, year: int) -> List[Dict[str, Any]]:
        """
        Parse the year listing page to extract document references.

        Returns a list of dicts with: numac, title, date, eli_url
        """
        documents = []

        # The year listing has entries like:
        # <td align=left>7 JANVIER 2024. - Loi modifiant...</td>
        # <td><A href=.../eli/loi/2024/01/07/2024000164/moniteur>Moniteur</A>
        #     <A href=.../eli/loi/2024/01/07/2024000164/justel>Justel</A></td>

        # Extract document rows using regex
        # Pattern looks for lines with date and title, then extracts numac from nearby links

        # Find all document entries with ELI links
        eli_pattern = re.compile(
            r'/eli/(\w+)/(\d{4})/(\d{2})/(\d{2})/(\d+)/justel',
            re.IGNORECASE
        )

        # Find all ELI links with their surrounding context
        for match in eli_pattern.finditer(html_content):
            try:
                eli_type = match.group(1)
                eli_year = match.group(2)
                eli_month = match.group(3)
                eli_day = match.group(4)
                numac = match.group(5)

                # Construct the full ELI URL
                eli_url = f"{BASE_URL}/eli/{eli_type}/{eli_year}/{eli_month}/{eli_day}/{numac}/justel"

                # Extract title from surrounding HTML (look for text before the link)
                # Find the row containing this link
                row_start = max(0, match.start() - 2000)
                row_end = min(len(html_content), match.end() + 500)
                row_context = html_content[row_start:row_end]

                # Look for title pattern (e.g., "7 JANVIER 2024. - Loi modifiant...")
                title_match = re.search(
                    r'(\d{1,2}\s+\w+\s+\d{4})\.\s*-\s*([^<]+)',
                    row_context,
                    re.IGNORECASE
                )

                title = ""
                if title_match:
                    date_part = title_match.group(1)
                    title_text = title_match.group(2).strip()
                    title = f"{date_part}. - {title_text}"
                    # Clean up the title
                    title = html.unescape(title)
                    title = re.sub(r'\s+', ' ', title).strip()

                # Parse date
                date_str = f"{eli_year}-{eli_month}-{eli_day}"

                documents.append({
                    "numac": numac,
                    "title": title,
                    "date": date_str,
                    "year": int(eli_year),
                    "month": int(eli_month),
                    "day": int(eli_day),
                    "eli_url": eli_url,
                    "document_type": eli_type,
                })

            except Exception as e:
                logger.warning(f"Failed to parse ELI link: {e}")
                continue

        # Remove duplicates (same numac)
        seen_numacs = set()
        unique_docs = []
        for doc in documents:
            if doc["numac"] not in seen_numacs:
                seen_numacs.add(doc["numac"])
                unique_docs.append(doc)

        return unique_docs

    def _fetch_full_text(self, eli_url: str) -> str:
        """
        Fetch and extract full text from an ELI /justel page.

        The page contains the consolidated text of the legislation in HTML format.
        We extract all text content, stripping HTML tags.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(eli_url)
            resp.raise_for_status()

            # Decode with correct encoding
            content = resp.content.decode('latin-1', errors='replace')

            # Extract text from substantial text blocks (avoiding navigation)
            text_parts = []

            # Find all text content between tags
            texts = re.findall(r'>([^<]+)<', content)

            in_article = False
            for t in texts:
                t = t.strip()
                if len(t) < 5:
                    continue

                # Skip navigation and metadata elements
                skip_patterns = [
                    'menu', 'cookie', 'javascript', 'navigation',
                    'www.belgium', 'img_', 'css_', 'function',
                    'Moniteur', 'Justel', 'NL', 'FR', 'DE',
                    'Home', 'Contact', 'FAQ', 'Annexe',
                ]
                if any(x.lower() in t.lower() for x in skip_patterns if len(x) > 3):
                    continue

                # Look for article markers to start capturing
                if re.search(r'Art\.\s*\d|CHAPITRE|TITRE|Section|Dispositions', t):
                    in_article = True

                # Also capture if it looks like legal text
                if re.search(r'§|alinéa|paragraphe|article|présente loi|modifi|abroge', t, re.IGNORECASE):
                    in_article = True

                # Capture substantial text after we're in an article
                if in_article and len(t) > 20:
                    # Clean up the text
                    clean_t = html.unescape(t)
                    clean_t = re.sub(r'\s+', ' ', clean_t).strip()
                    if clean_t:
                        text_parts.append(clean_t)

            full_text = '\n'.join(text_parts)

            # If we didn't get much text, try a different approach
            if len(full_text) < 500:
                # Fallback: extract all substantial text blocks
                text_parts = []
                for match in re.findall(r'>([^<]{50,})<', content):
                    clean = html.unescape(match.strip())
                    clean = re.sub(r'\s+', ' ', clean).strip()
                    if clean and not any(x in clean.lower() for x in ['script', 'style', 'function']):
                        text_parts.append(clean)
                full_text = '\n'.join(text_parts)

            return full_text.strip()

        except Exception as e:
            logger.warning(f"Failed to fetch full text from {eli_url}: {e}")
            return ""

    def _fetch_year_listing(self, doc_type: str, year: int) -> List[Dict[str, Any]]:
        """Fetch and parse the listing of documents for a given type and year."""
        url = f"/eli/{doc_type}/{year}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            content = resp.content.decode('latin-1', errors='replace')
            documents = self._parse_year_listing(content, doc_type, year)

            logger.info(f"Found {len(documents)} {doc_type} documents from {year}")
            return documents

        except Exception as e:
            logger.error(f"Failed to fetch year listing for {doc_type}/{year}: {e}")
            return []

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Belgian Official Journal.

        Iterates through legislation types and years, fetching document
        listings and then full text for each document.
        """
        for doc_type in LEGISLATION_TYPES:
            for year in YEARS_TO_SCRAPE:
                logger.info(f"Fetching {doc_type} documents from {year}...")

                documents = self._fetch_year_listing(doc_type, year)

                for doc in documents:
                    # Fetch full text for each document
                    full_text = self._fetch_full_text(doc["eli_url"])

                    if not full_text:
                        logger.warning(f"No full text for {doc['numac']}, skipping")
                        continue

                    doc["full_text"] = full_text
                    yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Since ELI doesn't have a direct "modified since" filter, we
        fetch recent years and filter by date.
        """
        since_year = since.year
        current_year = datetime.now().year

        years_to_check = list(range(current_year, since_year - 1, -1))

        for doc_type in LEGISLATION_TYPES:
            for year in years_to_check:
                logger.info(f"Checking {doc_type}/{year} for updates...")

                documents = self._fetch_year_listing(doc_type, year)

                for doc in documents:
                    # Parse document date
                    try:
                        doc_date = datetime(doc["year"], doc["month"], doc["day"], tzinfo=timezone.utc)
                        if doc_date < since:
                            continue
                    except:
                        pass

                    full_text = self._fetch_full_text(doc["eli_url"])
                    if not full_text:
                        continue

                    doc["full_text"] = full_text
                    yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        numac = raw.get("numac", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        doc_type = raw.get("document_type", "loi")

        # Parse date
        date_str = raw.get("date", "")
        if not date_str:
            year = raw.get("year", "")
            month = raw.get("month", "")
            day = raw.get("day", "")
            if year and month and day:
                date_str = f"{year}-{month:02d}-{day:02d}" if isinstance(month, int) else f"{year}-{month}-{day}"

        return {
            # Required base fields
            "_id": numac,
            "_source": "BE/MoniteurBelge",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": raw.get("eli_url", f"{BASE_URL}/eli/{doc_type}/{raw.get('year')}/{raw.get('month'):02d}/{raw.get('day'):02d}/{numac}/justel" if isinstance(raw.get('month'), int) else ""),
            # Additional metadata
            "numac": numac,
            "document_type": doc_type,
            "year": raw.get("year", ""),
            "language": "fr",  # Default to French, can be detected from content
            "eli_uri": raw.get("eli_url", ""),
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Belgian ELI endpoints...")

        # Test year listing
        print("\n1. Testing year listing (loi/2024)...")
        try:
            resp = self.client.get("/eli/loi/2024")
            print(f"   Status: {resp.status_code}")
            content = resp.content.decode('latin-1', errors='replace')
            docs = self._parse_year_listing(content, "loi", 2024)
            print(f"   Found {len(docs)} documents in 2024")
            if docs:
                print(f"   Sample: {docs[0].get('title', 'N/A')[:60]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test full text
        print("\n2. Testing full text endpoint...")
        try:
            # Use a known good NUMAC
            test_url = "/eli/loi/2024/01/07/2024000164/justel"
            resp = self.client.get(test_url)
            print(f"   Status: {resp.status_code}")

            text = self._fetch_full_text(f"{BASE_URL}{test_url}")
            print(f"   Text length: {len(text)} characters")
            if text:
                print(f"   Sample: {text[:200]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = MoniteurBelgeScraper()

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
