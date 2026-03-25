#!/usr/bin/env python3
"""
BN/AGCLaws -- Brunei Attorney General's Chambers Laws

Fetches legislation from the Brunei AGC Laws of Brunei page.

Strategy:
  - Parse the index page table to extract chapter numbers, titles, PDF URLs
  - Download PDFs and extract text with pdfplumber
  - Each row = one chapter of Brunei law

Data: ~220 chapters of consolidated legislation
License: Open access (government legislation portal)
Rate limit: 0.5 req/sec.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from urllib.parse import unquote, quote

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber not installed. Run: pip3 install pdfplumber")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BN.AGCLaws")

BASE_URL = "https://www.agc.gov.bn"
INDEX_URL = BASE_URL + "/AGC%20Site%20Pages/Laws%20of%20Brunei.aspx"


def _extract_blro_year(blro: str) -> Optional[str]:
    """Extract the most recent year from a B.L.R.O. string like 'B.L.R.O 3/2013'."""
    # Match 4-digit years first
    years = re.findall(r'(\d{4})', blro)
    if years:
        return max(years)
    # Match 2-digit years (e.g., '1/84' -> 1984)
    short_years = re.findall(r'/(\d{2})\b', blro)
    if short_years:
        full_years = [f"19{y}" if int(y) > 25 else f"20{y}" for y in short_years]
        return max(full_years)
    return None


class BNAGCLawsScraper(BaseScraper):
    """
    Scraper for BN/AGCLaws -- Brunei Attorney General's Chambers.
    Country: BN
    URL: https://www.agc.gov.bn

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _get_with_retry(self, url: str, max_retries: int = 3, timeout: int = 90) -> Optional[requests.Response]:
        """GET with retry logic."""
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))
        return None

    def _parse_index(self) -> List[dict]:
        """Parse the Laws of Brunei index page to extract chapters."""
        resp = self._get_with_retry(INDEX_URL)
        if not resp:
            logger.error("Failed to fetch index page")
            return []

        html = resp.text
        items = []

        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')

            seen_chapters = {}
            for link in soup.find_all('a', href=re.compile(r'/AGC%20Images/LAWS/ACT_PDF/', re.I)):
                row = link.find_parent('tr')
                if not row:
                    continue
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue

                chapter = cells[0].get_text(strip=True)
                if not re.match(r'^\d+$', chapter):
                    continue
                if chapter in seen_chapters:
                    continue

                # Title: get first <span> in first <p> to avoid concatenation
                title_cell = cells[1]
                first_p = title_cell.find('p')
                if first_p:
                    first_span = first_p.find('span')
                    title = first_span.get_text(strip=True) if first_span else first_p.get_text(strip=True)
                else:
                    title = title_cell.get_text(strip=True)
                title = re.sub(r'\s+', ' ', title).strip()
                title = re.sub(r'\s*\[$', '', title)  # Remove trailing '['
                if len(title) < 3:
                    title = link.get_text(strip=True)

                # BLRO: take only first entry to avoid concatenation
                blro_raw = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                blro_parts = re.split(r'(?=B\.L\.R\.O)', blro_raw)
                blro = blro_parts[0].strip() if blro_parts[0].strip() else (blro_parts[1].strip() if len(blro_parts) > 1 else "")

                pdf_href = link.get('href', '')
                pdf_url = BASE_URL + pdf_href if pdf_href.startswith('/') else pdf_href

                seen_chapters[chapter] = True
                items.append({
                    "chapter": chapter,
                    "title": title,
                    "blro": blro,
                    "pdf_url": pdf_url,
                    "pdf_path": unquote(pdf_href),
                })

        except ImportError:
            # Fallback: regex-based parsing
            logger.info("BeautifulSoup not available, using regex parsing")
            # Match rows with pattern: chapter_num | title | blro + pdf link
            row_pattern = re.compile(
                r'<tr[^>]*>.*?<td[^>]*>.*?(\d+).*?</td>'
                r'.*?<td[^>]*>(.*?)</td>'
                r'.*?<td[^>]*>(.*?)</td>.*?</tr>',
                re.S
            )
            for m in row_pattern.finditer(html):
                chapter = m.group(1).strip()
                title_html = m.group(2)
                blro_html = m.group(3)

                title = re.sub(r'<[^>]+>', ' ', title_html).strip()
                title = re.sub(r'\s+', ' ', title)
                blro = re.sub(r'<[^>]+>', ' ', blro_html).strip()
                blro = re.sub(r'\s+', ' ', blro)

                # Find first PDF in this row
                pdf_match = re.search(r'href="(/AGC%20Images/[^"]*\.pdf)"', m.group(0), re.I)
                if not pdf_match:
                    continue

                pdf_href = pdf_match.group(1)
                pdf_url = BASE_URL + pdf_href

                items.append({
                    "chapter": chapter,
                    "title": title,
                    "blro": blro,
                    "pdf_url": pdf_url,
                    "pdf_path": unquote(pdf_href),
                })

        logger.info(f"Parsed {len(items)} chapters from index")
        return items

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text."""
        resp = self._get_with_retry(pdf_url, timeout=120)
        if not resp:
            return None

        if resp.content[:4] != b'%PDF':
            logger.debug(f"Not a PDF: {pdf_url[-50:]}")
            return None

        try:
            pdf = pdfplumber.open(io.BytesIO(resp.content))
            text_parts = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
            pdf.close()

            text = "\n\n".join(text_parts)
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = re.sub(r' {2,}', ' ', text)
            return text.strip() if len(text) > 50 else None
        except Exception as e:
            logger.debug(f"PDF extraction failed for {pdf_url[-50:]}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all chapters with extractable full text."""
        items = self._parse_index()
        total = 0
        skipped = 0

        for item in items:
            time.sleep(2)
            text = self._extract_text_from_pdf(item["pdf_url"])
            if not text:
                skipped += 1
                logger.debug(f"No text: Ch.{item['chapter']} {item['title'][:50]}")
                continue

            item["text"] = text
            total += 1
            yield item

            if total % 20 == 0:
                logger.info(f"Progress: {total} docs ({skipped} skipped)")

        logger.info(f"Scan complete: {total} docs with text, {skipped} skipped")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch chapters updated since a date (based on B.L.R.O. year)."""
        since_year = str(since.year)
        items = self._parse_index()

        for item in items:
            blro_year = _extract_blro_year(item.get("blro", ""))
            if not blro_year or blro_year < since_year:
                continue

            time.sleep(2)
            text = self._extract_text_from_pdf(item["pdf_url"])
            if text:
                item["text"] = text
                yield item

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample chapters."""
        items = self._parse_index()
        found = 0

        for item in items:
            if found >= count:
                break

            time.sleep(2)
            text = self._extract_text_from_pdf(item["pdf_url"])
            if not text:
                logger.debug(f"No text: Ch.{item['chapter']} {item['title'][:50]}")
                continue

            item["text"] = text
            found += 1
            logger.info(
                f"Sample {found}/{count}: Ch.{item['chapter']} "
                f"({len(text)} chars) {item['title'][:60]}"
            )
            yield item

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw chapter record to standard schema."""
        chapter = raw["chapter"]
        title = raw.get("title", f"Chapter {chapter}")
        title = re.sub(r'\s+', ' ', title).strip()[:500]

        blro_year = _extract_blro_year(raw.get("blro", ""))
        date = f"{blro_year}-01-01" if blro_year else None

        return {
            "_id": f"BN-AGCLaws-Ch{chapter}",
            "_source": "BN/AGCLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": raw["pdf_url"],
            "chapter": chapter,
            "blro": raw.get("blro", ""),
            "pdf_path": raw.get("pdf_path", ""),
        }

    def test_api(self) -> bool:
        """Test connectivity and PDF extraction."""
        logger.info("Testing Brunei AGC Laws...")

        items = self._parse_index()
        if not items:
            logger.error("Failed to parse index")
            return False
        logger.info(f"Index OK: {len(items)} chapters")

        # Test PDF extraction
        for item in items[:5]:
            time.sleep(2)
            text = self._extract_text_from_pdf(item["pdf_url"])
            if text:
                logger.info(f"PDF extraction OK: {len(text)} chars from Ch.{item['chapter']}")
                logger.info("All tests passed")
                return True

        logger.error("No PDFs with extractable text found")
        return False


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = BNAGCLawsScraper()

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
        since_str = None
        for i, arg in enumerate(sys.argv):
            if arg == "--since" and i + 1 < len(sys.argv):
                since_str = sys.argv[i + 1]

        if not since_str:
            print("Usage: python bootstrap.py update --since YYYY-MM-DD")
            sys.exit(1)

        since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
