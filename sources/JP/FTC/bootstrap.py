#!/usr/bin/env python3
"""
JP/FTC -- Japan Fair Trade Commission (JFTC) Decisions & Guidelines Fetcher

Fetches JFTC enforcement cases and competition policy guidelines.

IMPORTANT: www.jftc.go.jp is behind Akamai WAF (returns 403).
Use cms03.jftc.go.jp subdomain instead (same content, no WAF).

Content:
  1. Press releases / enforcement cases: ~350+ HTML pages (2002-2026)
     Yearly indexes: /en/pressreleases/yearly-{YEAR}/index.html
     Individual pages: /en/pressreleases/yearly-{YEAR}/{Month}/{YYMMDD}.html
  2. Guidelines: ~40 PDFs from /en/legislation_gls/imonopoly_guidelines.html

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List

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
logger = logging.getLogger("legal-data-hunter.JP.FTC")

# Use CMS subdomain to bypass Akamai WAF
BASE_URL = "https://cms03.jftc.go.jp"

# Year range for press releases
YEARS = list(range(2013, 2027))  # 2013-2026, older years use different URL pattern

# Key guideline PDFs (path relative to base, title)
GUIDELINE_PDFS = [
    ("/en/legislation_gls/AMA.pdf", "Antimonopoly Act (AMA)"),
    ("/en/legislation_gls/imonopoly_guidelines_files/administrative.pdf", "Administrative Guidance Guidelines"),
    ("/en/legislation_gls/imonopoly_guidelines_files/20122501.pdf", "Exclusionary Private Monopolization Guidelines"),
    ("/en/legislation_gls/210122.pdf", "Distribution Systems & Business Practices Guidelines"),
    ("/en/legislation_gls/imonopoly_guidelines_files/20122502.pdf", "Trade Associations Activities Guidelines"),
    ("/en/legislation_gls/imonopoly_guidelines_files/20122503.pdf", "Public Bids Activities Guidelines"),
    ("/en/legislation_gls/imonopoly_guidelines_files/191217GL.pdf", "Business Combination Review Guidelines"),
    ("/en/legislation_gls/imonopoly_guidelines_files/101130GL.pdf", "Abuse of Superior Bargaining Position Guidelines"),
    ("/en/legislation_gls/imonopoly_guidelines_files/191217DPconsumerGL.pdf", "Digital Platform Consumer Transactions Guidelines"),
    ("/en/legislation_gls/imonopoly_guidelines_files/IPGL_Frand.pdf", "Intellectual Property Guidelines"),
    ("/en/legislation_gls/imonopoly_guidelines_files/Patent_Pool.pdf", "Standardization & Patent Pool Guidelines"),
    ("/en/legislation_gls/imonopoly_guidelines_files/electric.pdf", "Electric Power Trade Guidelines"),
    ("/en/legislation_gls/imonopoly_guidelines_files/telecom.pdf", "Telecommunications Competition Guidelines"),
    ("/en/legislation_gls/20210915.pdf", "Freelance Work Environment Guidelines"),
    ("/en/legislation_gls/201225002.pdf", "Leniency/Cooperation in Investigation Guidelines"),
    ("/en/legislation_gls/240424EN.pdf", "Green Society Activities Guidelines"),
]


def clean_html_text(raw_html: str) -> str:
    """Strip HTML tags and clean up text."""
    text = re.sub(r'<(script|style|noscript)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


class JapanFTCScraper(BaseScraper):
    """
    Scraper for JP/FTC -- Japan Fair Trade Commission.
    Country: JP
    URL: https://www.jftc.go.jp/en/

    Data types: case_law, doctrine
    Auth: none (Public Access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=60,
        )

    def _get_yearly_press_releases(self, year: int) -> List[Dict]:
        """Fetch press release links from a yearly index page."""
        items = []
        # Determine URL pattern
        if year >= 2013:
            path = f"/en/pressreleases/yearly-{year}/index.html"
        else:
            path = f"/en/pressreleases/yearly_{year}/index.html"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            content = resp.text

            # Find links to individual press release pages
            # Pattern: href="/en/pressreleases/yearly-YYYY/Month/YYMMDD.html"
            # or relative: href="Month/YYMMDD.html"
            link_pattern = re.compile(
                r'href="(/en/pressreleases/yearly[-_]\d{4}/[A-Za-z]+/\d+\.html)"',
                re.IGNORECASE
            )
            # Also try relative links
            rel_pattern = re.compile(
                r'href="([A-Za-z]+/(\d{6})\.html)"',
                re.IGNORECASE
            )

            found_paths = set()

            for match in link_pattern.finditer(content):
                found_paths.add(match.group(1))

            for match in rel_pattern.finditer(content):
                rel_path = match.group(1)
                base_dir = path.rsplit('/', 1)[0]
                full_path = f"{base_dir}/{rel_path}"
                found_paths.add(full_path)

            # Extract titles from nearby <a> tags
            for fpath in sorted(found_paths):
                # Try to find title text in the link
                escaped = re.escape(fpath.split('/')[-1])
                title_match = re.search(
                    r'href="[^"]*' + escaped + r'"[^>]*>([^<]+)</a>',
                    content
                )
                title = ""
                if title_match:
                    title = html.unescape(title_match.group(1).strip())

                # Extract date from filename (YYMMDD)
                date_match = re.search(r'/(\d{6})\.html', fpath)
                iso_date = ""
                if date_match:
                    yymmdd = date_match.group(1)
                    try:
                        # Handle 2-digit year
                        yy = int(yymmdd[:2])
                        full_year = 2000 + yy if yy < 50 else 1900 + yy
                        mm = yymmdd[2:4]
                        dd = yymmdd[4:6]
                        iso_date = f"{full_year}-{mm}-{dd}"
                    except (ValueError, IndexError):
                        pass

                items.append({
                    "path": fpath,
                    "title": title,
                    "date": iso_date,
                    "year": str(year),
                    "doc_type": "enforcement_case",
                })

            logger.info(f"Year {year}: Found {len(items)} press releases")
            return items

        except Exception as e:
            logger.warning(f"Failed to get year {year}: {e}")
            return []

    def _fetch_press_release(self, item: Dict) -> Dict:
        """Fetch full text of a single press release page."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(item["path"])
            resp.raise_for_status()
            raw_html = resp.text

            # Extract main content - look for main/article/content div
            # Try to find the main content area
            main_match = re.search(
                r'<(?:main|article|div\s+(?:id|class)="(?:main|content|article)[^"]*")[^>]*>(.*?)</(?:main|article|div)>',
                raw_html,
                re.DOTALL | re.IGNORECASE
            )
            if main_match:
                text = clean_html_text(main_match.group(1))
            else:
                text = clean_html_text(raw_html)

            # Extract title from <title> tag if we don't have one
            title = item.get("title", "")
            if not title:
                title_match = re.search(r'<title>([^<]+)</title>', raw_html, re.IGNORECASE)
                if title_match:
                    title = html.unescape(title_match.group(1).strip())

            item["full_text"] = text
            if title:
                item["title"] = title

            return item

        except Exception as e:
            logger.warning(f"Failed to fetch {item['path']}: {e}")
            return item

    def _fetch_guideline_pdf(self, path: str, title: str) -> Dict:
        """Download and extract text from a guideline PDF."""
        if not HAS_PDFPLUMBER:
            logger.warning("pdfplumber not available, skipping PDF")
            return {}

        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()

            ct = resp.headers.get("Content-Type", "")
            if "html" in ct.lower() and len(resp.content) < 5000:
                logger.warning(f"PDF URL returned HTML: {path}")
                return {}

            if len(resp.content) < 500:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {path}")
                return {}

            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp.write(resp.content)
                tmp_path = tmp.name

            try:
                text_parts = []
                with pdfplumber.open(tmp_path) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)

                full_text = "\n\n".join(text_parts)
                logger.info(f"Extracted {len(full_text)} chars from {path}")

                return {
                    "path": path,
                    "title": title,
                    "full_text": full_text,
                    "doc_type": "guideline",
                    "date": "",
                }
            finally:
                os.unlink(tmp_path)

        except Exception as e:
            logger.warning(f"Failed to extract PDF {path}: {e}")
            return {}

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all press releases and guidelines."""
        seen = set()

        # 1. Press releases by year
        for year in YEARS:
            items = self._get_yearly_press_releases(year)
            for item in items:
                path = item["path"]
                if path in seen:
                    continue
                seen.add(path)

                result = self._fetch_press_release(item)
                if result.get("full_text") and len(result["full_text"]) > 100:
                    yield result

        # 2. Guideline PDFs
        if HAS_PDFPLUMBER:
            for path, title in GUIDELINE_PDFS:
                result = self._fetch_guideline_pdf(path, title)
                if result.get("full_text"):
                    yield result

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent press releases."""
        current_year = datetime.now().year
        for year in [current_year - 1, current_year]:
            items = self._get_yearly_press_releases(year)
            for item in items:
                if item.get("date") and item["date"] >= since.strftime("%Y-%m-%d"):
                    result = self._fetch_press_release(item)
                    if result.get("full_text") and len(result["full_text"]) > 100:
                        yield result

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        doc_type = raw.get("doc_type", "enforcement_case")
        path = raw.get("path", "")
        title = raw.get("title", "")
        text = raw.get("full_text", "")
        date = raw.get("date", "")

        # Build document ID
        if doc_type == "guideline":
            slug = path.split("/")[-1].replace(".pdf", "").replace(" ", "_")
            doc_id = f"JP-FTC-GL-{slug}"
            _type = "doctrine"
        else:
            filename = path.split("/")[-1].replace(".html", "")
            doc_id = f"JP-FTC-{filename}"
            _type = "case_law"

        # Build canonical URL (use www domain for public-facing URLs)
        canonical_path = path
        full_url = f"https://www.jftc.go.jp{canonical_path}" if canonical_path.startswith("/") else canonical_path

        return {
            "_id": doc_id,
            "_source": "JP/FTC",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": full_url,
            "doc_type": doc_type,
            "jurisdiction": "JP",
            "language": "en",
            "authority": "Japan Fair Trade Commission (JFTC)",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Japan FTC (JFTC) endpoints...")
        print(f"Base URL: {BASE_URL}")
        print(f"pdfplumber available: {HAS_PDFPLUMBER}")

        print("\n1. Testing yearly index (2025)...")
        try:
            items = self._get_yearly_press_releases(2025)
            print(f"   Found {len(items)} press releases for 2025")
            if items:
                print(f"   Sample: {items[0].get('title', 'N/A')[:70]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        if items:
            print("\n2. Testing press release fetch...")
            try:
                result = self._fetch_press_release(items[0])
                text = result.get("full_text", "")
                print(f"   Extracted {len(text)} characters")
                if text:
                    print(f"   Preview: {text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")

        if HAS_PDFPLUMBER:
            print("\n3. Testing guideline PDF...")
            try:
                path, title = GUIDELINE_PDFS[0]
                result = self._fetch_guideline_pdf(path, title)
                text = result.get("full_text", "")
                print(f"   Extracted {len(text)} chars from {title}")
                if text:
                    print(f"   Preview: {text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = JapanFTCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
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
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated")
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
