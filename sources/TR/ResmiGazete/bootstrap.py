#!/usr/bin/env python3
"""
Legal Data Hunter - Turkish Official Gazette (Resmi Gazete) Scraper

Fetches legislation from the Turkish Official Gazette archive:
  - GET /eskiler/{YYYY}/{MM}/{YYYYMMDD}.htm   (daily index page)
  - GET /eskiler/{YYYY}/{MM}/{YYYYMMDD}-{N}.htm (full text HTML documents)

Coverage: Daily gazette publications since 1920. Laws, decrees, regulations,
presidential decisions, international treaties, and official announcements.

Complements TR/Mevzuat (consolidated legislation) with as-published text.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
"""

import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional
from html import unescape

from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("TR/ResmiGazete")

# Section header to legislation_type mapping
SECTION_TYPES = {
    "KANUN": "Kanun",
    "KANUNLAR": "Kanun",
    "CUMHURBAŞKANI KARARLARI": "Cumhurbaşkanı Kararı",
    "CUMHURBAŞKANLIĞI KARARNAMELERİ": "Cumhurbaşkanlığı Kararnamesi",
    "CUMHURBAŞKANLIĞI GENELGELERİ": "Cumhurbaşkanlığı Genelgesi",
    "YÖNETMELİK": "Yönetmelik",
    "YÖNETMELİKLER": "Yönetmelik",
    "TEBLİĞ": "Tebliğ",
    "TEBLİĞLER": "Tebliğ",
    "BAKANLAR KURULU KARARLARI": "Bakanlar Kurulu Kararı",
    "BAKANLAR KURULU KARARI": "Bakanlar Kurulu Kararı",
    "KANUN HÜKMÜNDE KARARNAME": "KHK",
    "KANUN HÜKMÜNDE KARARNAMELER": "KHK",
    "TÜZÜK": "Tüzük",
    "TÜZÜKLER": "Tüzük",
    "GENELGE": "Genelge",
    "GENELGELER": "Genelge",
    "ANAYASA MAHKEMESİ KARARLARI": "Anayasa Mahkemesi Kararı",
    "UYUŞMAZLIK MAHKEMESİ KARARLARI": "Uyuşmazlık Mahkemesi Kararı",
    "MİLLETLERARASI ANDLAŞMALAR": "Milletlerarası Andlaşma",
    "MİLLETLERARASI ANDLAŞMA": "Milletlerarası Andlaşma",
}


class TurkishOfficialGazetteScraper(BaseScraper):
    """
    Scraper for: Turkish Official Gazette (Resmî Gazete)
    Country: TR
    URL: https://www.resmigazete.gov.tr

    Data types: legislation
    Auth: none

    Strategy:
    - Iterate over dates, fetching daily index pages
    - Parse index for .htm links to individual legislation items
    - Fetch each item page for full text
    - Extract section type (KANUN, YÖNETMELİK, etc.) from index page structure
    """

    BASE_URL = "https://www.resmigazete.gov.tr"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all gazette legislation entries by iterating dates backwards."""
        current = datetime.now().date()
        # Go back to 2000 for full run
        end_date = datetime(2000, 1, 1).date()

        while current >= end_date:
            try:
                yield from self._fetch_date(current)
            except Exception as e:
                logger.warning(f"Failed to process {current}: {e}")
            current -= timedelta(days=1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given datetime."""
        current = datetime.now().date()
        end_date = since.date() if hasattr(since, 'date') else since

        while current >= end_date:
            try:
                yield from self._fetch_date(current)
            except Exception as e:
                logger.warning(f"Failed to process {current}: {e}")
            current -= timedelta(days=1)

    def _fetch_date(self, date) -> Generator[dict, None, None]:
        """Fetch all gazette items for a single date."""
        date_str = date.strftime("%Y%m%d")
        y = date.strftime("%Y")
        m = date.strftime("%m")
        index_url = f"/eskiler/{y}/{m}/{date_str}.htm"

        self.rate_limiter.wait()
        try:
            resp = self.client.get(index_url)
            if resp.status_code != 200:
                return
        except Exception as e:
            logger.debug(f"No gazette for {date}: {e}")
            return

        # Parse index page - encoding is windows-1254
        html = resp.text
        if resp.encoding and resp.encoding.lower() != 'utf-8':
            try:
                html = resp.content.decode('windows-1254', errors='replace')
            except Exception:
                pass

        soup = BeautifulSoup(html, "html.parser")

        # Extract gazette number from header
        gazette_number = ""
        header_text = soup.get_text()
        gn_match = re.search(r'(\d+)\s*Sayılı\s*Resmî\s*Gazete', header_text)
        if not gn_match:
            gn_match = re.search(r'Sayı\s*:\s*(\d+)', header_text)
        if gn_match:
            gazette_number = gn_match.group(1)

        # Build a map of link -> section type by walking the HTML structure
        link_types = self._map_links_to_types(soup)

        # Find all .htm document links (pattern: YYYYMMDD-N.htm)
        doc_links = soup.find_all("a", href=re.compile(r"\d{8}-\d+\.htm"))
        if not doc_links:
            return

        seen = set()
        for link in doc_links:
            href = link.get("href", "")
            if href in seen:
                continue
            seen.add(href)

            # Skip announcement/ilan links
            if "ilan" in href.lower():
                continue

            link_text = link.get_text(strip=True)
            if not link_text:
                continue

            # Get legislation type from section mapping
            leg_type = link_types.get(href, "")

            # Extract law/decree number from link text
            law_number = ""
            num_match = re.match(r'^(\d+)\s', link_text)
            if num_match:
                law_number = num_match.group(1)

            # Build document URL
            doc_url = f"{self.BASE_URL}/eskiler/{y}/{m}/{href}"

            # Fetch full text
            full_text = self._fetch_document_text(doc_url)
            if not full_text or len(full_text) < 50:
                logger.debug(f"Skipping {href}: no full text")
                continue

            # Build a clean title from the link text
            title = re.sub(r'\s+', ' ', link_text).strip()
            # Remove leading dashes
            title = re.sub(r'^[–—-]+\s*', '', title)

            # Extract sequence number from href for unique ID
            seq_match = re.search(r'\d{8}-(\d+)\.htm', href)
            seq = seq_match.group(1) if seq_match else "1"

            doc_id = f"RG-{date_str}-{seq}"

            yield {
                "document_id": doc_id,
                "title": title,
                "full_text": full_text,
                "law_number": law_number,
                "legislation_type": leg_type,
                "gazette_number": gazette_number,
                "gazette_date": date.strftime("%Y-%m-%d"),
                "url": doc_url,
            }

        logger.info(f"{date}: {len(seen)} items processed (gazette #{gazette_number})")

    def _map_links_to_types(self, soup: BeautifulSoup) -> dict:
        """Map document links to their section types from the index page."""
        link_types = {}
        current_type = ""

        # Walk through all elements looking for section headers and links
        for elem in soup.find_all(["p", "a", "b", "u", "span", "font"]):
            text = elem.get_text(strip=True).upper()

            # Check if this is a section header
            if elem.name in ("b", "u", "span", "font"):
                # Section headers are bold+underlined text
                parent_b = elem.find_parent("b") or (elem.name == "b" and elem)
                parent_u = elem.find_parent("u") or (elem.name == "u" and elem)
                if parent_b or parent_u or (elem.name == "b"):
                    clean = re.sub(r'\s+', ' ', text).strip()
                    for key, val in SECTION_TYPES.items():
                        if key in clean:
                            current_type = val
                            break

            # If this is a link, map it to current section type
            if elem.name == "a":
                href = elem.get("href", "")
                if re.match(r'\d{8}-\d+\.htm', href):
                    link_types[href] = current_type

        return link_types

    def _fetch_document_text(self, url: str) -> str:
        """Fetch and extract text from a gazette document page."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                return ""

            html = resp.text
            if resp.encoding and resp.encoding.lower() != 'utf-8':
                try:
                    html = resp.content.decode('windows-1254', errors='replace')
                except Exception:
                    pass

            return self._extract_gazette_text(html)
        except Exception as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return ""

    def _extract_gazette_text(self, html: str) -> str:
        """Extract clean text from gazette HTML page."""
        if not html:
            return ""

        soup = BeautifulSoup(html, "html.parser")

        # Remove scripts, styles, navigation
        for tag in soup.find_all(["script", "style", "nav", "header", "footer", "meta"]):
            tag.decompose()

        # Try WordSection1 (common in gazette docs exported from Word)
        word_section = soup.find("div", class_="WordSection1")
        if word_section:
            return self._clean_text(word_section.get_text(separator="\n", strip=True))

        # Try the main body content
        body = soup.find("body")
        if body:
            text = body.get_text(separator="\n", strip=True)
            if len(text) > 100:
                return self._clean_text(text)

        return ""

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text."""
        if not text:
            return ""
        text = unescape(text)
        text = text.replace("\xa0", " ")
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)
        return text.strip()

    def normalize(self, raw: dict) -> dict:
        """Transform a raw document into the standard schema."""
        doc_id = raw.get("document_id", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        gazette_date = raw.get("gazette_date", "")

        if not title:
            title = f"Resmi Gazete {raw.get('gazette_number', '')}"

        return {
            "_id": f"TR/ResmiGazete/{doc_id}",
            "_source": "TR/ResmiGazete",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            "title": title,
            "text": full_text,
            "date": gazette_date if gazette_date else None,
            "url": raw.get("url"),

            "document_id": doc_id,
            "law_number": raw.get("law_number"),
            "legislation_type": raw.get("legislation_type"),
            "gazette_number": raw.get("gazette_number"),
            "gazette_date": gazette_date,
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = TurkishOfficialGazetteScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
