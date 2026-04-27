#!/usr/bin/env python3
"""
INTL/WIPO-LexDB -- WIPO Lex Database (IP Laws Worldwide)

Fetches intellectual property legislation from WIPO Lex covering 200+ jurisdictions.

Strategy:
  - Enumerate country codes from /legislation/members (JS data)
  - For each country, get legislation list from /legislation/results?countryOrgs={CC}
  - For each legislation, fetch detail page and extract:
    1. Inline HTML full text from <div class="htmlView"> (preferred)
    2. Signed PDF URL as fallback, download and extract via pdfminer
  - Metadata extracted from detail page spans (Type of Text, Subject Matter, dates)

Data:
  - 50K+ IP legislation documents from 200+ jurisdictions
  - Full text available inline (HTML) or via signed PDF download
  - Multi-language (EN/FR/ES/RU/ZH/AR)
  - Open access, reproduction permitted with attribution

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import gc
import sys
import json
import logging
import re
import time
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WIPO-LexDB")

BASE_URL = "https://www.wipo.int"
WIPOLEX = f"{BASE_URL}/wipolex/en"


class WipoLexScraper(BaseScraper):
    """
    Scraper for INTL/WIPO-LexDB -- WIPO Lex IP Legislation Database.
    Country: INTL
    URL: https://www.wipo.int/wipolex/en/
    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en",
            },
            timeout=60,
        )

    def _get(self, url: str, max_retries: int = 3) -> Optional[Any]:
        """GET with retry logic."""
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait()
                resp = self.client.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 3
                    logger.debug(f"Retry {attempt+1} for {url}: {e}")
                    time.sleep(wait)
                else:
                    logger.warning(f"Failed after {max_retries} attempts: {url}: {e}")
                    return None

    def _get_country_codes(self) -> List[Dict[str, str]]:
        """Extract country codes and names from the members page JS data."""
        resp = self._get(f"{WIPOLEX}/legislation/members")
        if not resp:
            return []

        match = re.search(r'window\.membersPageData\s*=\s*\{[^;]*members:\s*(\[.*?\])', resp.text, re.DOTALL)
        if not match:
            logger.error("Could not find membersPageData in members page")
            return []

        try:
            members = json.loads(match.group(1))
            return [{"code": m["code"], "name": m.get("cntryOrgTitle", "")} for m in members]
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse members data: {e}")
            return []

    def _get_legislation_ids(self, country_code: str) -> List[Dict[str, str]]:
        """Get legislation IDs and basic metadata for a country."""
        resp = self._get(f"{WIPOLEX}/legislation/results?countryOrgs={country_code}")
        if not resp:
            return []

        from bs4 import BeautifulSoup
        html_text = resp.text
        del resp
        soup = BeautifulSoup(html_text, "html.parser")
        del html_text

        results = []
        tables = soup.find_all("table", id="members_profile_laws_table")
        for table in tables:
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue

                type_of_text = cells[0].get_text(strip=True)
                year = cells[1].get_text(strip=True)

                # Extract detail link and title from col 3
                link = cells[3].find("a", href=re.compile(r"/legislation/details/"))
                if not link:
                    continue

                href = link.get("href", "")
                detail_id_m = re.search(r"/details/(\d+)", href)
                if not detail_id_m:
                    continue

                detail_id = detail_id_m.group(1)
                title = link.get_text(strip=True)

                # Subject matter from col 4
                subject = cells[4].get_text(strip=True) if len(cells) > 4 else ""

                # Dates from col 2
                date_text = cells[2].get_text(strip=True)

                results.append({
                    "detail_id": detail_id,
                    "title": title,
                    "type_of_text": type_of_text,
                    "year": year,
                    "subject_matter": subject,
                    "date_text": date_text,
                    "country_code": country_code,
                })

        # Free soup memory
        soup.decompose()
        del soup

        return results

    def _extract_full_text_html(self, soup) -> str:
        """Extract full text from inline HTML view on detail page."""
        html_view = soup.find("div", class_=re.compile(r"htmlView"))
        if not html_view:
            return ""

        # Get text content, preserving paragraph breaks
        for br in html_view.find_all("br"):
            br.replace_with("\n")
        for p in html_view.find_all("p"):
            p.insert_before("\n")
            p.insert_after("\n")

        text = html_view.get_text()
        # Clean whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()
        return text

    def _extract_signed_pdf_url(self, soup) -> Optional[str]:
        """Extract signed PDF download URL from detail page."""
        for a in soup.find_all("a", href=re.compile(r"wipolex-res\.wipo\.int.*\.pdf")):
            href = a.get("href", "")
            if href:
                return unescape(href)
        return None

    def _download_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF using centralized extractor.

        Skips PDFs larger than 20MB to avoid OOM on constrained VPS.
        """
        # Check PDF size before downloading (HEAD request)
        try:
            head = self.client.session.head(pdf_url, timeout=15, allow_redirects=True)
            content_length = int(head.headers.get("Content-Length", 0))
            if content_length > 20 * 1024 * 1024:  # 20MB
                logger.warning(f"Skipping oversized PDF ({content_length // 1024 // 1024}MB): {pdf_url}")
                return ""
        except Exception:
            pass  # If HEAD fails, proceed with download anyway

        result = extract_pdf_markdown(
            source="INTL/WIPO-LexDB",
            source_id="",
            pdf_url=pdf_url,
            table="legislation",
        ) or ""
        return result

    def _extract_detail_metadata(self, soup) -> Dict[str, str]:
        """Extract metadata from detail page spans."""
        meta = {}
        field_map = {
            "Type of Text": "type_of_text",
            "Subject Matter": "subject_matter",
            "Adopted": "adopted_date",
            "Year of Version": "year_of_version",
            "Entry into force": "entry_into_force",
            "Published": "published_date",
            "Repealed": "repealed_date",
            "ISN": "isn",
        }

        for label, key in field_map.items():
            elem = soup.find(string=re.compile(re.escape(label)))
            if elem:
                parent = elem.parent
                nxt = parent.find_next_sibling() if parent else None
                if nxt:
                    val = nxt.get_text(strip=True)
                    if val:
                        meta[key] = val

        return meta

    def _fetch_detail(self, detail_id: str, listing_meta: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Fetch a legislation detail page and extract full text + metadata."""
        url = f"{WIPOLEX}/legislation/details/{detail_id}"
        resp = self._get(url)
        if not resp:
            return None

        from bs4 import BeautifulSoup
        html_text = resp.text
        del resp  # Free response memory
        soup = BeautifulSoup(html_text, "html.parser")
        del html_text

        # Get metadata from page
        page_meta = self._extract_detail_metadata(soup)

        # Get full text - prefer inline HTML
        full_text = self._extract_full_text_html(soup)

        pdf_url = None
        if not full_text:
            pdf_url = self._extract_signed_pdf_url(soup)

        # Free soup memory before potentially downloading PDF
        soup.decompose()
        del soup

        if not full_text and pdf_url:
            logger.debug(f"Falling back to PDF for {detail_id}")
            full_text = self._download_pdf_text(pdf_url)

        if not full_text:
            logger.debug(f"No full text for detail {detail_id}")
            return None

        # Merge listing metadata with page metadata
        record = {**listing_meta, **page_meta}
        record["detail_id"] = detail_id
        record["full_text"] = full_text
        record["url"] = f"{BASE_URL}/wipolex/en/legislation/details/{detail_id}"
        return record

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation with full text."""
        countries = self._get_country_codes()
        logger.info(f"Found {len(countries)} countries/organizations")

        record_count = 0
        for country in countries:
            code = country["code"]
            name = country["name"]
            logger.info(f"Processing {code} ({name})")

            laws = self._get_legislation_ids(code)
            logger.info(f"  {code}: {len(laws)} legislation entries")

            for law in laws:
                record = self._fetch_detail(law["detail_id"], law)
                if record:
                    record_count += 1
                    yield record
                    # Periodic garbage collection to avoid OOM on constrained VPS
                    if record_count % 100 == 0:
                        gc.collect()
                        logger.info(f"  Progress: {record_count} records yielded")

    def fetch_sample(self, n: int = 15) -> Generator[dict, None, None]:
        """Yield a sample of legislation from a few countries."""
        # Use a diverse set of countries for sample
        sample_countries = ["FR", "US", "DE", "JP", "BR", "KE"]
        count = 0

        for code in sample_countries:
            if count >= n:
                break

            laws = self._get_legislation_ids(code)
            if not laws:
                continue

            # Take first 3 from each country
            for law in laws[:3]:
                if count >= n:
                    break
                record = self._fetch_detail(law["detail_id"], law)
                if record:
                    yield record
                    count += 1

    def run_sample(self, n: int = 15) -> dict:
        """Fetch sample records."""
        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "sample_records_saved": 0,
            "errors": 0,
        }
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        for raw in self.fetch_sample(n=n):
            record = self.normalize(raw)
            if not record:
                stats["errors"] += 1
                continue
            fname = re.sub(r"[^\w\-]", "_", record["_id"])[:80]
            out = sample_dir / f"{fname}.json"
            out.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            stats["sample_records_saved"] += 1
            logger.info(f"Sample {stats['sample_records_saved']}: {record.get('title', '')[:60]}")
            if stats["sample_records_saved"] >= n:
                break

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        return stats

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recently updated legislation."""
        countries = self._get_country_codes()
        current_year = datetime.now().year

        for country in countries:
            code = country["code"]
            laws = self._get_legislation_ids(code)
            for law in laws:
                year = law.get("year", "")
                try:
                    if int(year) >= since.year:
                        record = self._fetch_detail(law["detail_id"], law)
                        if record:
                            yield record
                except (ValueError, TypeError):
                    continue

    def _parse_date(self, date_str: str) -> str:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return ""

        # Try common formats: "September 4, 1979", "January 1, 2024"
        import locale
        for fmt in ["%B %d, %Y", "%B, %Y", "%Y"]:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d") if "%d" in fmt else dt.strftime("%Y-%m")
            except ValueError:
                continue

        # Extract year if nothing else works
        m = re.search(r"(\d{4})", date_str)
        if m:
            return m.group(1)

        return date_str

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        detail_id = raw.get("detail_id", "")
        country_code = raw.get("country_code", "INTL")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")

        # Parse date - prefer adopted, then entry into force
        date_str = raw.get("adopted_date", "")
        if not date_str:
            date_str = raw.get("entry_into_force", "")
        if not date_str:
            date_str = raw.get("date_text", "")
        date_iso = self._parse_date(date_str)

        return {
            "_id": f"WIPO-{country_code}-{detail_id}",
            "_source": "INTL/WIPO-LexDB",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": raw.get("url", f"{BASE_URL}/wipolex/en/legislation/details/{detail_id}"),
            "country_code": country_code,
            "type_of_text": raw.get("type_of_text", ""),
            "subject_matter": raw.get("subject_matter", ""),
            "year_of_version": raw.get("year_of_version", raw.get("year", "")),
            "adopted_date": raw.get("adopted_date", ""),
            "entry_into_force": raw.get("entry_into_force", ""),
            "isn": raw.get("isn", ""),
            "language": "mul",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing WIPO Lex Database...")

        print("\n1. Testing members page...")
        countries = self._get_country_codes()
        print(f"   Found {len(countries)} countries/organizations")
        if countries:
            print(f"   Sample: {', '.join(c['code'] for c in countries[:10])}")

        print("\n2. Testing legislation listing (FR)...")
        laws = self._get_legislation_ids("FR")
        print(f"   Found {len(laws)} French legislation entries")
        if laws:
            print(f"   First: {laws[0]['title'][:80]}")

        if laws:
            print("\n3. Testing detail page with full text...")
            detail = self._fetch_detail(laws[0]["detail_id"], laws[0])
            if detail:
                text = detail.get("full_text", "")
                print(f"   Title: {detail.get('title', '')[:80]}")
                print(f"   Text length: {len(text)} chars")
                if text:
                    print(f"   Preview: {text[:200]}...")
            else:
                print("   FAILED to fetch detail")

        print("\nTest complete!")


def main():
    scraper = WipoLexScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
