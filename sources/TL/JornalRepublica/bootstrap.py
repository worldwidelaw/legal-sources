#!/usr/bin/env python3
"""
TL/JornalRepublica -- Timor-Leste Jornal da Republica

Fetches legislation from the Official Journal of Timor-Leste published by the
Ministry of Justice.

Strategy:
  - Parse HTML table pages on mj.gov.tl/jornal to discover acts with PDF links
  - Serie I category nodes contain tables with columns: NUMÉRO, DESCRIÇÃO,
    PUBLICADA EM, PDF
  - Download PDFs and extract full text via common/pdf_extract
  - Each row is one legislative act (law, decree-law, presidential decree, etc.)

Endpoints:
  - Base URL: https://www.mj.gov.tl/jornal/
  - Category pages: ?q=node/{id} (each category lists all acts across all years)
  - PDFs: public/docs/{year}/serie_1/SERIE_I_NO_{n}.pdf (and variants)

Data:
  - Serie I: laws, decree-laws, presidential decrees, government resolutions
  - Coverage: 2002-present
  - Language: Portuguese
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent acts)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TL.JornalRepublica")

BASE_URL = "https://www.mj.gov.tl/jornal/"

# Serie I category nodes with their Drupal node IDs
SERIE_I_CATEGORIES = {
    12: "Leis do Parlamento Nacional",
    13: "Decretos-Leis do Governo",
    10: "Decretos do Presidente da Republica",
    18: "Decretos do Governo",
    19: "Resolucoes do Parlamento Nacional",
    20: "Resolucoes do Governo",
    2501: "Ministerio Publico",
}

# Portuguese month names for date parsing
PT_MONTHS = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
    "abril": 4, "maio": 5, "junho": 6, "julho": 7,
    "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}


def _parse_date(date_str: str) -> str:
    """Parse date strings like '25/3/2026' or '1/04/2026' to ISO 8601."""
    if not date_str:
        return ""
    date_str = date_str.strip()
    # Try d/m/yyyy format
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year}-{month:02d}-{day:02d}"
    return ""


class JornalRepublicaScraper(BaseScraper):
    """
    Scraper for TL/JornalRepublica -- Timor-Leste Official Journal.
    Country: TL
    URL: https://www.mj.gov.tl/jornal/

    Data types: legislation
    Auth: none (Open access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf",
                "Accept-Language": "pt,en",
            },
            timeout=120,
        )
        self._seen_pdfs: set = set()

    def _parse_category_page(self, node_id: int, category_name: str) -> List[Dict[str, Any]]:
        """
        Parse a category page and extract all acts with their metadata.

        Returns list of dicts: {number, description, date, pdf_url, category, year}
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("beautifulsoup4 not installed")
            return []

        url = f"?q=node/{node_id}"
        self.rate_limiter.wait()
        resp = self.client.get(url)
        if resp.status_code != 200:
            logger.warning(f"Failed to fetch node/{node_id}: {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        acts = []

        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # First row is year header, second is column headers
            year_text = rows[0].get_text(strip=True)
            year_match = re.search(r"(\d{4})", year_text)
            year = int(year_match.group(1)) if year_match else 0

            for row in rows[2:]:  # Skip year header and column header rows
                tds = row.find_all("td")
                if len(tds) < 4:
                    continue

                act_number = tds[0].get_text(strip=True)
                description = tds[1].get_text(strip=True)
                date_str = tds[2].get_text(strip=True)
                pdf_link = tds[3].find("a")

                if not pdf_link or not pdf_link.get("href"):
                    continue

                href = pdf_link["href"]
                # Resolve relative URLs
                if not href.startswith("http"):
                    pdf_url = urljoin(BASE_URL, href)
                else:
                    # Normalize to https
                    pdf_url = href.replace("http://www.mj.gov.tl/jornal/", BASE_URL)
                    pdf_url = pdf_url.replace("http://www.jornal.gov.tl/", BASE_URL)

                # Skip duplicate PDFs (same PDF may appear under multiple acts)
                if pdf_url in self._seen_pdfs:
                    continue
                self._seen_pdfs.add(pdf_url)

                pub_date = _parse_date(date_str)
                if not pub_date and year:
                    pub_date = f"{year}-01-01"

                acts.append({
                    "act_number": act_number,
                    "description": description,
                    "date": pub_date,
                    "pdf_url": pdf_url,
                    "category": category_name,
                    "year": year or (int(pub_date[:4]) if pub_date else 0),
                    "node_id": node_id,
                })

        logger.info(f"Node {node_id} ({category_name}): {len(acts)} acts found")
        return acts

    def _fetch_act_text(self, pdf_url: str, act_id: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning(f"PDF download failed ({resp.status_code}): {pdf_url}")
                return None

            text = extract_pdf_markdown(
                source="TL/JornalRepublica",
                source_id=act_id,
                pdf_bytes=resp.content,
                table="legislation",
            )
            return text
        except Exception as e:
            logger.warning(f"Failed to extract PDF {pdf_url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislative acts from Serie I categories.

        Parses each category page, then downloads and extracts PDFs.
        """
        preload_existing_ids("TL/JornalRepublica", "legislation")

        all_acts = []
        for node_id, category_name in SERIE_I_CATEGORIES.items():
            acts = self._parse_category_page(node_id, category_name)
            all_acts.extend(acts)

        logger.info(f"Total acts discovered: {len(all_acts)}")

        # Sort by year descending (newest first)
        all_acts.sort(key=lambda a: a.get("year", 0), reverse=True)

        for act in all_acts:
            # Build a stable ID
            cat_short = re.sub(r"[^a-zA-Z]", "", act["category"])[:10]
            num_clean = re.sub(r"[^\w/]", "", act["act_number"])
            act_id = f"TL-JR-{cat_short}-{num_clean}-{act['year']}"

            text = self._fetch_act_text(act["pdf_url"], act_id)
            if not text or len(text) < 100:
                logger.warning(f"Skipping {act_id}: insufficient text ({len(text) if text else 0} chars)")
                continue

            yield {
                "act_id": act_id,
                "act_number": act["act_number"],
                "description": act["description"],
                "date": act["date"],
                "pdf_url": act["pdf_url"],
                "category": act["category"],
                "year": act["year"],
                "full_text": text,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield acts published since the given date."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw act data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        act_id = raw.get("act_id", "")
        act_number = raw.get("act_number", "")
        description = raw.get("description", "")
        category = raw.get("category", "")
        full_text = raw.get("full_text", "")
        pub_date = raw.get("date", "")
        pdf_url = raw.get("pdf_url", "")
        year = raw.get("year", 0)

        # Build title
        title_parts = []
        if act_number:
            title_parts.append(act_number)
        if description:
            title_parts.append(description)
        title = " - ".join(title_parts) if title_parts else f"Jornal da República {year}"

        return {
            "_id": act_id,
            "_source": "TL/JornalRepublica",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": pub_date,
            "url": pdf_url,
            "act_number": act_number,
            "category": category,
            "language": "pt",
            "country": "TL",
            "jurisdiction": "national",
            "document_type": "official_journal_act",
            "year": year,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Timor-Leste Jornal da Republica endpoints...")

        print("\n1. Testing homepage...")
        try:
            resp = self.client.get("")
            print(f"   Status: {resp.status_code}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\n2. Testing category page (Leis do Parlamento)...")
        try:
            resp = self.client.get("?q=node/12")
            print(f"   Status: {resp.status_code}, {len(resp.text)} chars")
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.find_all("a", href=re.compile(r"\.pdf$", re.I))
            print(f"   PDF links found: {len(links)}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\n3. Testing PDF download and text extraction...")
        try:
            pdf_url = urljoin(BASE_URL, "public/docs/2026/serie_1/SERIE_I_NO_12.pdf")
            resp = self.client.session.get(pdf_url, timeout=30)
            print(f"   PDF status: {resp.status_code}, {len(resp.content)} bytes")
            if resp.status_code == 200:
                text = extract_pdf_markdown(
                    source="TL/JornalRepublica",
                    source_id="test",
                    pdf_bytes=resp.content,
                    table="legislation",
                    force=True,
                )
                if text:
                    print(f"   Extracted: {len(text)} chars")
                    print(f"   Sample: {text[:200]}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = JornalRepublicaScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
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
