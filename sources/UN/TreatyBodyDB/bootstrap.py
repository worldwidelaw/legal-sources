#!/usr/bin/env python3
"""
UN/TreatyBodyDB -- UN Treaty Body Database (General Comments)

Fetches General Comments and General Recommendations from all 9 UN human
rights treaty bodies with full text content.

Strategy:
  - For each treaty body, query TBSearch.aspx with TreatyID + DocTypeID=11
  - Parse search results to extract document symbols and metadata
  - For each document, fetch Download.aspx to find HTML download link
  - Fetch HTML version from docstore.ohchr.org and extract clean text

Data: ~200+ General Comments/Recommendations from 9 treaty bodies.
License: Open data (UN documents are public domain).
Rate limit: 1 req/sec (self-imposed, respectful).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, quote, unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UN.TreatyBodyDB")

BASE_URL = "https://tbinternet.ohchr.org"
SEARCH_URL = BASE_URL + "/_layouts/15/treatybodyexternal/TBSearch.aspx"
DOWNLOAD_URL = BASE_URL + "/_layouts/15/treatybodyexternal/Download.aspx"

# Treaty body ID mappings: TreatyID -> (short_name, full_treaty_name)
TREATY_BODIES = {
    1: ("CAT", "Convention against Torture"),
    2: ("CED", "Convention on Enforced Disappearances"),
    3: ("CEDAW", "Convention on the Elimination of Discrimination against Women"),
    4: ("CRPD", "Convention on the Rights of Persons with Disabilities"),
    5: ("CRC", "Convention on the Rights of the Child"),
    6: ("CERD", "Convention on the Elimination of Racial Discrimination"),
    7: ("CMW", "Convention on the Rights of Migrant Workers"),
    8: ("CCPR", "International Covenant on Civil and Political Rights"),
    9: ("CESCR", "International Covenant on Economic, Social and Cultural Rights"),
}

# DocTypeID 11 = General Comments / General Recommendations
DOC_TYPE_ID = 11


class TreatyBodyDBScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
            max_retries=3,
            backoff_factor=2.0,
            timeout=60,
        )

    def _parse_search_results(self, html_content: str) -> list:
        """Parse TBSearch results page to extract document info."""
        docs = []
        # Look for document rows - each has a symbol number and title
        # Pattern: <a ...>SYMBOL</a> in the results table
        # The results contain links to Download.aspx?symbolno=XXX

        # Extract symbol numbers from Download.aspx links (URL-encoded)
        download_pattern = re.compile(
            r'Download\.aspx\?symbolno=([^&"]+?)&(?:amp;)?Lang=en',
            re.IGNORECASE
        )
        # Extract titles - they appear in specific spans/links
        title_pattern = re.compile(
            r'<span[^>]*class="[^"]*symbol[^"]*"[^>]*>([^<]+)</span>',
            re.IGNORECASE
        )

        # Find all document entries - each row has symbol and title
        # The page uses a repeating pattern with document blocks
        row_pattern = re.compile(
            r'<tr[^>]*>.*?</tr>',
            re.DOTALL | re.IGNORECASE
        )

        # Simpler approach: find all Download.aspx links with symbols
        symbols_found = download_pattern.findall(html_content)
        # Decode URL-encoding and HTML entities
        symbols_found = [unquote(html_module.unescape(s)) for s in symbols_found]
        # Deduplicate while preserving order
        seen = set()
        unique_symbols = []
        for s in symbols_found:
            if s not in seen:
                seen.add(s)
                unique_symbols.append(s)

        # Extract titles - look for text near each symbol
        # Titles are in the format: <a ...title="TITLE"...> or as text content
        for symbol in unique_symbols:
            # Try to find the title near the symbol reference
            escaped_symbol = re.escape(symbol)
            title_near = re.search(
                rf'{escaped_symbol}.*?<td[^>]*>\s*([^<]+?)\s*</td>',
                html_content,
                re.DOTALL | re.IGNORECASE
            )
            title = ""
            if title_near:
                title = title_near.group(1).strip()
                title = html_module.unescape(title)

            # Also try to find the title from a broader pattern
            if not title or len(title) < 5:
                title_alt = re.search(
                    rf'title="([^"]*{escaped_symbol}[^"]*)"',
                    html_content,
                    re.IGNORECASE
                )
                if title_alt:
                    title = html_module.unescape(title_alt.group(1).strip())

            # Find date if available
            date_match = re.search(
                rf'{escaped_symbol}.*?(\d{{2}}\s+\w{{3}}\s+\d{{4}})',
                html_content,
                re.DOTALL | re.IGNORECASE
            )
            date_str = ""
            if date_match:
                date_str = date_match.group(1).strip()

            docs.append({
                "symbol": symbol,
                "title": title,
                "date_str": date_str,
            })

        return docs

    def _get_search_page(self, treaty_id: int, page: int = 1) -> str:
        """Fetch a search results page."""
        params = {
            "Lang": "en",
            "TreatyID": str(treaty_id),
            "DocTypeID": str(DOC_TYPE_ID),
        }
        if page > 1:
            # The pagination uses __EVENTTARGET and __EVENTARGUMENT via postback
            # For now, we rely on getting all results from page 1 (most treaty bodies
            # have <10 general comments, fitting one page)
            pass

        url = SEARCH_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        resp = self.http.get(url)
        if resp and resp.status_code == 200:
            return resp.text
        return ""

    def _get_total_pages(self, html_content: str) -> int:
        """Extract total number of pages from search results."""
        match = re.search(r'(\d+)\s+items?\s+in\s+(\d+)\s+pages?', html_content, re.IGNORECASE)
        if match:
            return int(match.group(2))
        return 1

    def _get_total_items(self, html_content: str) -> int:
        """Extract total items from search results."""
        match = re.search(r'(\d+)\s+items?\s+in\s+(\d+)\s+pages?', html_content, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return 0

    def _fetch_download_page(self, symbol: str) -> str:
        """Fetch the Download.aspx page for a document symbol."""
        url = DOWNLOAD_URL + f"?symbolno={quote(symbol, safe='/')}&Lang=en"
        resp = self.http.get(url)
        if resp and resp.status_code == 200:
            return resp.text
        return ""

    def _extract_html_url(self, download_html: str) -> Optional[str]:
        """Extract the English HTML file download URL from the Download.aspx page."""
        # Links have title="English html" (or other language + format)
        # Pattern: title="English html" href="https://docstore.ohchr.org/..."
        link_pattern = re.compile(
            r'title="([^"]+)"\s+href="(https://docstore\.ohchr\.org/SelfServices/FilesHandler\.ashx\?enc=[^"]+)"',
            re.IGNORECASE
        )
        for title, url in link_pattern.findall(download_html):
            title_decoded = html_module.unescape(title).strip().lower()
            if title_decoded == "english html":
                return html_module.unescape(url)

        # Fallback: any English format (prefer docx over doc)
        for preferred in ["english docx", "english doc"]:
            for title, url in link_pattern.findall(download_html):
                title_decoded = html_module.unescape(title).strip().lower()
                if title_decoded == preferred:
                    return html_module.unescape(url)

        # Last resort: first link
        all_urls = link_pattern.findall(download_html)
        if all_urls:
            return html_module.unescape(all_urls[0][1])
        return None

    def _extract_docx_url(self, download_html: str) -> Optional[str]:
        """Extract DOCX download URL as fallback."""
        docx_pattern = re.compile(
            r'<a[^>]*href="(https://docstore\.ohchr\.org/SelfServices/FilesHandler\.ashx\?enc=[^"]+)"[^>]*>\s*\.?docx\s*</a>',
            re.IGNORECASE
        )
        matches = docx_pattern.findall(download_html)
        if matches:
            return html_module.unescape(matches[0])
        return None

    def _fetch_full_text(self, symbol: str) -> Optional[str]:
        """Fetch full text of a document by its symbol."""
        time.sleep(1)  # Rate limit
        download_html = self._fetch_download_page(symbol)
        if not download_html:
            logger.warning(f"Could not fetch download page for {symbol}")
            return None

        html_url = self._extract_html_url(download_html)
        if not html_url:
            logger.warning(f"No HTML download URL found for {symbol}")
            return None

        time.sleep(1)  # Rate limit
        resp = self.http.get(html_url)
        if not resp or resp.status_code != 200:
            logger.warning(f"Could not fetch HTML content for {symbol}")
            return None

        # Handle UTF-16 encoding (common for OHCHR HTML documents)
        content = resp.content
        try:
            if content[:2] in (b'\xff\xfe', b'\xfe\xff'):
                text = content.decode('utf-16')
            else:
                text = resp.text
        except (UnicodeDecodeError, LookupError):
            text = resp.text

        return self._clean_html(text)

    def _clean_html(self, html_content: str) -> str:
        """Strip HTML tags and clean up text content."""
        # Remove script and style elements
        text = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode HTML entities
        text = html_module.unescape(text)
        # Normalize whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = text.strip()
        return text

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse a date string into ISO 8601 format."""
        if not date_str:
            return None
        for fmt in ["%d %B %Y", "%d %b %Y", "%Y-%m-%d", "%B %d, %Y"]:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _list_all_documents(self) -> Generator[dict, None, None]:
        """List all general comments from all treaty bodies."""
        for treaty_id, (short_name, treaty_name) in TREATY_BODIES.items():
            logger.info(f"Fetching document list for {short_name} (TreatyID={treaty_id})...")
            time.sleep(1)

            html_content = self._get_search_page(treaty_id)
            if not html_content:
                logger.warning(f"No search results for {short_name}")
                continue

            total_items = self._get_total_items(html_content)
            total_pages = self._get_total_pages(html_content)
            logger.info(f"  {short_name}: {total_items} items in {total_pages} pages")

            docs = self._parse_search_results(html_content)
            for doc in docs:
                doc["committee"] = short_name
                doc["treaty"] = treaty_name
                yield doc

            # Handle pagination - for treaty bodies with multiple pages
            # Since the site uses ASP.NET postbacks, we can't easily paginate
            # But most treaty bodies have <10 general comments per page (10/page)
            # Only CEDAW (43), CERD (43), CCPR (37), CESCR (29), CRC (27) have >10
            if total_pages > 1:
                logger.info(f"  {short_name} has {total_pages} pages - page 1 retrieved {len(docs)} docs")
                logger.info(f"  Note: Multi-page results may be incomplete due to ASP.NET postback pagination")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all general comments with full text."""
        for doc_info in self._list_all_documents():
            symbol = doc_info["symbol"]
            logger.info(f"Fetching full text for {symbol}...")

            full_text = self._fetch_full_text(symbol)
            if not full_text:
                logger.warning(f"Could not get full text for {symbol}, skipping")
                continue

            if len(full_text) < 100:
                logger.warning(f"Full text too short for {symbol} ({len(full_text)} chars), skipping")
                continue

            doc_info["text"] = full_text
            yield doc_info

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """General comments rarely change; just re-fetch all."""
        yield from self.fetch_all()

    def _extract_date_from_text(self, text: str) -> Optional[str]:
        """Extract document date from the full text header."""
        # UN docs typically have "DD Month YYYY" in the header
        date_pattern = re.compile(
            r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
            re.IGNORECASE
        )
        # Search in the first 1000 chars (header area)
        match = date_pattern.search(text[:1000])
        if match:
            return self._parse_date(match.group(1))
        return None

    def normalize(self, raw: dict) -> dict:
        """Normalize a document into standard schema."""
        symbol = raw.get("symbol", "")
        title = raw.get("title", "") or f"General Comment {symbol}"
        text = raw.get("text", "")
        committee = raw.get("committee", "")
        treaty = raw.get("treaty", "")

        # Prefer date from document text header over search page
        date = self._extract_date_from_text(text) or self._parse_date(raw.get("date_str", ""))

        # Try to extract title from the text if we don't have one
        if not title or len(title) < 5:
            # First meaningful line of text is often the title
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            for line in lines[:10]:
                if "general comment" in line.lower() or "general recommendation" in line.lower():
                    title = line[:200]
                    break
            if not title or len(title) < 5:
                title = f"{committee} - {symbol}"

        return {
            "_id": f"UN/TreatyBodyDB/{symbol}",
            "_source": "UN/TreatyBodyDB",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "title": title,
            "text": text,
            "date": date,
            "url": f"{DOWNLOAD_URL}?symbolno={quote(symbol, safe='/')}&Lang=en",
            "committee": committee,
            "treaty": treaty,
        }


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    scraper = TreatyBodyDBScraper()

    if not args or args[0] == "bootstrap":
        sample = "--sample" in args
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(stats, indent=2))

    elif args[0] == "test-api":
        print("Testing TBSearch.aspx connectivity...")
        html = scraper._get_search_page(8)  # CCPR
        if html:
            total = scraper._get_total_items(html)
            docs = scraper._parse_search_results(html)
            print(f"OK: CCPR search returned {total} items, parsed {len(docs)} symbols")
            if docs:
                print(f"First symbol: {docs[0]['symbol']}")
                print("\nTesting Download.aspx + full text fetch...")
                text = scraper._fetch_full_text(docs[0]["symbol"])
                if text:
                    print(f"OK: Full text retrieved ({len(text)} chars)")
                    print(f"Preview: {text[:300]}...")
                else:
                    print("FAIL: Could not fetch full text")
        else:
            print("FAIL: Could not reach TBSearch.aspx")

    elif args[0] == "list":
        for doc in scraper._list_all_documents():
            print(f"  {doc['committee']:6s} | {doc['symbol']:30s} | {doc.get('title', '')[:60]}")

    else:
        print(f"Usage: {sys.argv[0]} [bootstrap [--sample] | test-api | list]")
        sys.exit(1)


if __name__ == "__main__":
    main()
