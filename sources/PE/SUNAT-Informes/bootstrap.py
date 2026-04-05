#!/usr/bin/env python3
"""
PE/SUNAT-Informes -- Peru SUNAT Tax Authority Guidance Fetcher

Fetches tax doctrine (informes, oficios, cartas) from SUNAT's legislation
section at sunat.gob.pe/legislacion/oficios/.

Strategy:
  - Bootstrap: Iterate years 1996-current, parse HTML index pages,
    download PDF/HTM full text for each document.
  - Update: Fetch only recent years.
  - Sample: Fetch a few documents from recent and older years.

Website: https://www.sunat.gob.pe/legislacion/oficios/

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py update               # Incremental update
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PE.SUNAT-Informes")

BASE_URL = "https://www.sunat.gob.pe/legislacion/oficios"
CURRENT_YEAR = datetime.now().year
START_YEAR = 1996
HEADERS = {"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"}


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_text:
        return ""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html_text, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", html_text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"\xa0", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using PyPDF2."""
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(content))
        pages = []
        for page in reader.pages:
            t = page.extract_text()
            if t:
                pages.append(t)
        return "\n\n".join(pages).strip()
    except Exception as e:
        logger.warning(f"PyPDF2 extraction failed: {e}")
        return ""


def parse_index_page(html: str, year: int) -> list:
    """Parse index page for any year. Works with all HTML variants."""
    docs = []
    seen = set()

    # Split HTML into table rows for row-level parsing
    rows = re.split(r'<tr[^>]*>', html, flags=re.IGNORECASE)

    for row in rows:
        # Find document links (PDF or HTM)
        link_match = re.search(
            r'<a\s+href="([^"]+\.(?:pdf|htm))"[^>]*>(.*?)</a>',
            row, re.DOTALL | re.IGNORECASE
        )
        if not link_match:
            continue

        href = link_match.group(1)
        title_html = link_match.group(2)

        # Skip navigation/index links
        if href.startswith("/") or href.startswith("#") or href.startswith("http"):
            continue
        if "index" in href.lower() or "indcor" in href.lower():
            continue

        title = strip_html(title_html).strip()
        if not title or len(title) < 5:
            continue

        doc_id = Path(href).stem
        if doc_id in seen:
            continue
        seen.add(doc_id)

        # Extract summary from the next <td> after the link's <td>
        summary = ""
        # Find all <td> blocks in this row
        tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        if len(tds) >= 2:
            # Summary is typically in the second <td>
            summary = strip_html(tds[1]).strip()

        # Extract date - look for DD.MM.YYYY, DD/MM/YYYY, or DD.MM.YY patterns
        date_iso = None
        date_match = re.search(r'(\d{2})[./](\d{2})[./](\d{2,4})', row)
        if date_match:
            day, month, yr = date_match.group(1), date_match.group(2), date_match.group(3)
            if len(yr) == 2:
                yr = f"20{yr}" if int(yr) < 50 else f"19{yr}"
            date_iso = f"{yr}-{month}-{day}"

        # Determine doc type from title
        doc_type = "informe"
        title_lower = title.lower()
        if "oficio" in title_lower:
            doc_type = "oficio"
        elif "carta" in title_lower:
            doc_type = "carta"

        docs.append({
            "doc_id": doc_id,
            "title": title,
            "summary": summary,
            "date": date_iso,
            "href": href,
            "doc_type": doc_type,
            "year": year,
        })

    # Fallback: if no rows found, try finding links outside tables (legacy pages)
    if not docs:
        links = re.findall(
            r'<a\s+href="([^"]+\.(?:pdf|htm))"[^>]*>(.*?)</a>',
            html, re.DOTALL | re.IGNORECASE
        )
        for href, title_html in links:
            if href.startswith("/") or href.startswith("#") or href.startswith("http"):
                continue
            if "index" in href.lower() or "indcor" in href.lower():
                continue
            title = strip_html(title_html).strip()
            if not title or len(title) < 5:
                continue

            doc_id = Path(href).stem
            if doc_id in seen:
                continue
            seen.add(doc_id)

            doc_type = "informe"
            if "oficio" in title.lower():
                doc_type = "oficio"
            elif "carta" in title.lower():
                doc_type = "carta"

            docs.append({
                "doc_id": doc_id,
                "title": title,
                "summary": "",
                "date": None,
                "href": href,
                "doc_type": doc_type,
                "year": year,
            })

    return docs


class SUNATInformesScraper(BaseScraper):
    """
    Scraper for PE/SUNAT-Informes -- Peru SUNAT Tax Doctrine.
    Country: PE
    URL: https://www.sunat.gob.pe/legislacion/oficios/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_url(self, url: str, binary: bool = False):
        """Fetch a URL with retries."""
        import requests
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=30)
                if resp.status_code == 200:
                    return resp.content if binary else resp.text
                if resp.status_code == 404:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url} (attempt {attempt + 1})")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 * (attempt + 1))
        return None

    def _fetch_year_index(self, year: int) -> list:
        """Fetch and parse the index page for a given year."""
        url = f"{BASE_URL}/{year}/indcor.htm"
        html = self._fetch_url(url)
        if not html:
            logger.warning(f"Could not fetch index for year {year}")
            return []

        docs = parse_index_page(html, year)

        logger.info(f"Year {year}: found {len(docs)} documents")
        return docs

    def _fetch_full_text(self, doc: dict) -> str:
        """Download and extract full text for a document."""
        year = doc["year"]
        href = doc["href"]
        url = f"{BASE_URL}/{year}/{href}"

        content = self._fetch_url(url, binary=True)
        if not content:
            return ""

        if href.endswith(".pdf"):
            text = extract_pdf_text(content)
        else:
            # HTML document
            try:
                html = content.decode("utf-8", errors="replace")
            except Exception:
                html = content.decode("latin-1", errors="replace")
            text = strip_html(html)

        return text

    def normalize(self, raw: dict) -> dict:
        """Transform raw document record into standard schema."""
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", "")
        text = raw.get("full_text", "") or raw.get("summary", "")
        date = raw.get("date")
        year = raw.get("year", "")
        href = raw.get("href", "")
        doc_type = raw.get("doc_type", "informe")

        url = f"{BASE_URL}/{year}/{href}" if href else f"{BASE_URL}/{year}/indcor.htm"

        return {
            "_id": f"PE-SUNAT-{doc_id}",
            "_source": "PE/SUNAT-Informes",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "doc_type": doc_type,
            "year": year,
            "summary": raw.get("summary", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all documents with full text."""
        total = 0
        for year in range(CURRENT_YEAR, START_YEAR - 1, -1):
            docs = self._fetch_year_index(year)
            for doc in docs:
                full_text = self._fetch_full_text(doc)
                if full_text:
                    doc["full_text"] = full_text
                elif doc.get("summary"):
                    doc["full_text"] = doc["summary"]
                else:
                    logger.warning(f"No text for {doc['doc_id']}")
                    continue

                normalized = self.normalize(doc)
                if normalized.get("text"):
                    yield normalized
                    total += 1

                time.sleep(1)

            time.sleep(1)

        logger.info(f"Total fetched: {total} documents")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch documents from recent years."""
        try:
            since_year = int(since[:4])
        except (ValueError, IndexError):
            since_year = CURRENT_YEAR - 1

        for year in range(CURRENT_YEAR, since_year - 1, -1):
            docs = self._fetch_year_index(year)
            for doc in docs:
                if doc.get("date") and doc["date"] < since:
                    continue

                full_text = self._fetch_full_text(doc)
                if full_text:
                    doc["full_text"] = full_text
                elif doc.get("summary"):
                    doc["full_text"] = doc["summary"]
                else:
                    continue

                normalized = self.normalize(doc)
                if normalized.get("text"):
                    yield normalized

                time.sleep(1)
            time.sleep(1)

    def fetch_sample(self) -> list:
        """Fetch sample records from recent and older years."""
        samples = []
        sample_years = [CURRENT_YEAR, CURRENT_YEAR - 1, 2020, 2015, 2010, 2005, 2000]

        for year in sample_years:
            if len(samples) >= 15:
                break

            logger.info(f"Sampling year {year}...")
            docs = self._fetch_year_index(year)
            if not docs:
                continue

            # Take up to 3 docs per year
            for doc in docs[:3]:
                if len(samples) >= 15:
                    break

                logger.info(f"  Fetching full text for {doc['doc_id']}...")
                full_text = self._fetch_full_text(doc)
                if full_text:
                    doc["full_text"] = full_text
                    logger.info(f"  Got {len(full_text)} chars")
                elif doc.get("summary"):
                    doc["full_text"] = doc["summary"]
                    logger.info(f"  Using summary ({len(doc['summary'])} chars)")
                else:
                    logger.warning(f"  No text for {doc['doc_id']}")
                    continue

                normalized = self.normalize(doc)
                samples.append(normalized)
                time.sleep(1.5)

            time.sleep(2)

        return samples

    def test_api(self):
        """Quick API connectivity test."""
        print("Testing PE/SUNAT-Informes endpoints...")

        for year in [CURRENT_YEAR, 2020, 2010, 2000]:
            docs = self._fetch_year_index(year)
            print(f"  Year {year}: {len(docs)} documents")

            if docs:
                doc = docs[0]
                print(f"    First: {doc['title'][:70]}")
                full_text = self._fetch_full_text(doc)
                if full_text:
                    print(f"    Full text: {len(full_text)} chars")
                    print(f"    Preview: {full_text[:120]}...")
                else:
                    print("    Full text: FAILED")

            time.sleep(1)

        print("\nAPI test complete.")


def main():
    scraper = SUNATInformesScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample:
            logger.info("Fetching sample records...")
            samples = scraper.fetch_sample()

            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            for i, record in enumerate(samples):
                path = sample_dir / f"sample_{i:03d}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved {len(samples)} sample records to {sample_dir}")

            texts = [r for r in samples if r.get("text") and len(r["text"]) > 100]
            print(f"\nValidation: {len(texts)}/{len(samples)} records have full text")
            for r in samples:
                text_len = len(r.get("text", ""))
                print(f"  {r['_id']}: {r['title'][:60]} | text: {text_len} chars")
        else:
            logger.info("Starting full bootstrap...")
            count = 0
            output_dir = Path(__file__).parent / "data"
            output_dir.mkdir(exist_ok=True)

            for record in scraper.fetch_all():
                path = output_dir / f"{record['_id']}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 50 == 0:
                    logger.info(f"  Saved {count} records...")

            logger.info(f"Bootstrap complete: {count} records saved")

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else "2026-01-01"
        logger.info(f"Fetching updates since {since}...")
        count = 0
        output_dir = Path(__file__).parent / "data"
        output_dir.mkdir(exist_ok=True)

        for record in scraper.fetch_updates(since):
            path = output_dir / f"{record['_id']}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Update complete: {count} new/updated records")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
