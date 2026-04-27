#!/usr/bin/env python3
"""
CZ/MFCR -- Czech Ministry of Finance Tax Guidance (Finanční zpravodaj)

Fetches the Finanční zpravodaj (Financial Gazette) — the official publication
for Czech tax doctrine including instructions (pokyny), communications (sdělení),
and guidance from the General Financial Directorate (GFŘ).

Strategy:
  - Iterate year listing pages from 2003–current year
  - Parse issue links from each year page
  - Fetch each issue page to find the PDF download URL
  - Download PDF and extract full text via common/pdf_extract

Endpoints:
  - Year listing: https://mf.gov.cz/cs/dane-a-ucetnictvi/financni-zpravodaj/{YEAR}
  - Issue page:   https://mf.gov.cz/cs/dane-a-ucetnictvi/financni-zpravodaj/{YEAR}/{slug}
  - PDF pattern:  /assets/attachments/{date}_Financni-zpravodaj-cislo-{N}-{YEAR}.pdf

Data:
  - ~19-21 issues per year, 2003–2026
  - ~400+ total gazette issues with tax doctrine
  - Language: Czech (CS)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

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
logger = logging.getLogger("legal-data-hunter.CZ.mfcr")

BASE_URL = "https://mf.gov.cz"
YEAR_LISTING_PATH = "/cs/dane-a-ucetnictvi/financni-zpravodaj/{year}"
START_YEAR = 2003
# Issue link pattern on year listing page — captures link and following date paragraph
ISSUE_LINK_RE = re.compile(
    r'href="(/cs/dane-a-ucetnictvi/financni-zpravodaj/\d{4}/[^"]+)"'
)
# Pattern to extract link + date pairs from listing page
# Structure: <a href="..." class="b-article__link ...">Title</a> ... <span class="b-article__date"> DD. month YYYY</span>
ISSUE_WITH_DATE_RE = re.compile(
    r'<a\s+href="(/cs/dane-a-ucetnictvi/financni-zpravodaj/\d{4}/[^"]+)"[^>]*>'
    r'\s*([^<]+?)\s*</a>.*?<span[^>]*class="b-article__date"[^>]*>\s*(\d{1,2})\.\s*(\w+)\s+(\d{4})\s*</span>',
    re.DOTALL,
)
# Czech month name to number mapping
CZ_MONTHS = {
    "ledna": 1, "února": 2, "března": 3, "dubna": 4,
    "května": 5, "června": 6, "července": 7, "srpna": 8,
    "září": 9, "října": 10, "listopadu": 11, "prosince": 12,
}
# Numeric date fallback (DD.MM.YYYY)
DATE_NUMERIC_RE = re.compile(r'(\d{1,2})\.(\d{1,2})\.(\d{4})')
# PDF link on issue page
PDF_LINK_RE = re.compile(r'href="(/assets/[^"]+\.pdf)"', re.IGNORECASE)


class CzechMFCRScraper(BaseScraper):
    """
    Scraper for CZ/MFCR -- Czech Ministry of Finance Financial Gazette.
    Country: CZ
    URL: https://mf.gov.cz

    Data types: doctrine
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "cs,en;q=0.5",
            },
            timeout=60,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="CZ/MFCR",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="doctrine",
        ) or ""

    def _get_year_issues(self, year: int) -> List[Dict[str, Any]]:
        """Scrape year listing page for issue links and dates."""
        results = []
        url = YEAR_LISTING_PATH.format(year=year)
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            content = resp.text

            # Try to extract links with Czech dates
            matches = ISSUE_WITH_DATE_RE.findall(content)
            seen = set()
            for href, title, day, month_name, yr in matches:
                if href in seen:
                    continue
                seen.add(href)
                slug = href.split("/")[-1]
                issue_num = None
                num_match = re.search(r'cislo-(\d+)-', slug)
                if num_match:
                    issue_num = int(num_match.group(1))
                # Parse Czech date
                month_num = CZ_MONTHS.get(month_name.lower())
                date_str = None
                if month_num:
                    date_str = f"{int(yr)}-{month_num:02d}-{int(day):02d}"
                results.append({
                    "href": href,
                    "slug": slug,
                    "year": year,
                    "issue_number": issue_num,
                    "title": html.unescape(title).strip(),
                    "date": date_str,
                })

            # Fallback: if regex didn't match, find links without dates
            if not results:
                links = ISSUE_LINK_RE.findall(content)
                for link in links:
                    if link in seen:
                        continue
                    seen.add(link)
                    slug = link.split("/")[-1]
                    issue_num = None
                    num_match = re.search(r'cislo-(\d+)-', slug)
                    if num_match:
                        issue_num = int(num_match.group(1))
                    results.append({
                        "href": link,
                        "slug": slug,
                        "year": year,
                        "issue_number": issue_num,
                    })

            logger.info(f"Year {year}: found {len(results)} issues")
        except Exception as e:
            logger.warning(f"Failed to fetch year {year} listing: {e}")
        return results

    def _get_issue_details(self, issue: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch an individual issue page to get title and PDF URL."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(issue["href"])
            resp.raise_for_status()
            content = resp.text

            # Extract title from <h1> or <title>
            title = issue.get("title")
            if not title:
                h1_match = re.search(r'<h1[^>]*>([^<]+)</h1>', content)
                if h1_match:
                    title = html.unescape(h1_match.group(1)).strip()
                if not title:
                    title_match = re.search(r'<title>([^<]+)</title>', content)
                    if title_match:
                        title = html.unescape(title_match.group(1)).strip()
                        title = re.sub(r'\s*[|–-]\s*Ministerstvo financí.*$', '', title)

            # Use date from listing page, fall back to numeric date on issue page
            date_str = issue.get("date")
            if not date_str:
                # Look for DD.MM.YYYY pattern near the top of the page content
                date_match = DATE_NUMERIC_RE.search(content[:3000])
                if date_match:
                    day, month, yr = date_match.groups()
                    try:
                        date_str = f"{yr}-{int(month):02d}-{int(day):02d}"
                    except ValueError:
                        pass

            # Find PDF download link
            pdf_url = None
            pdf_matches = PDF_LINK_RE.findall(content)
            for pm in pdf_matches:
                if 'zpravodaj' in pm.lower() or 'financni' in pm.lower():
                    pdf_url = pm
                    break
            if not pdf_url and pdf_matches:
                pdf_url = pdf_matches[0]

            return {
                "href": issue["href"],
                "slug": issue["slug"],
                "year": issue["year"],
                "issue_number": issue.get("issue_number"),
                "title": title or f"Finanční zpravodaj č. {issue.get('issue_number', '?')}/{issue['year']}",
                "date": date_str,
                "pdf_url": pdf_url,
                "page_url": BASE_URL + issue["href"],
            }
        except Exception as e:
            logger.warning(f"Failed to fetch issue page {issue['href']}: {e}")
            return None

    def _download_pdf(self, pdf_path: str) -> str:
        """Download a PDF and extract text."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(pdf_path)
            resp.raise_for_status()
            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_path}")
                return ""
            return self._extract_pdf_text(resp.content)
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_path}: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Finanční zpravodaj issues from 2003 to current year."""
        current_year = datetime.now().year
        for year in range(current_year, START_YEAR - 1, -1):
            issues = self._get_year_issues(year)
            for issue in issues:
                details = self._get_issue_details(issue)
                if details:
                    yield details

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield issues from current year (incremental)."""
        current_year = datetime.now().year
        issues = self._get_year_issues(current_year)
        for issue in issues:
            details = self._get_issue_details(issue)
            if details and details.get("date"):
                try:
                    issue_date = datetime.fromisoformat(details["date"])
                    if issue_date.date() >= since.date():
                        yield details
                except (ValueError, TypeError):
                    yield details

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw issue dict into standard schema."""
        pdf_url = raw.get("pdf_url")
        text = ""
        if pdf_url:
            text = self._download_pdf(pdf_url)

        if not text:
            logger.warning(f"No text extracted for {raw.get('title', '?')}")
            return None

        doc_id = f"CZ-MFCR-FZ-{raw['year']}-{raw.get('issue_number', raw['slug'][-6:])}"

        return {
            "_id": doc_id,
            "_source": "CZ/MFCR",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("page_url", BASE_URL + raw.get("href", "")),
            "doc_id": doc_id,
            "issue_number": raw.get("issue_number"),
            "year": raw.get("year"),
            "pdf_url": BASE_URL + pdf_url if pdf_url else None,
            "language": "cs",
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="CZ/MFCR bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full initial fetch")
    boot.add_argument("--sample", action="store_true", help="Sample mode (10 records)")
    boot.add_argument("--full", action="store_true", help="Full fetch (all records)")

    sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = CzechMFCRScraper()

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        logger.info(f"Bootstrap complete: {stats}")
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    elif args.command == "test":
        logger.info("Testing connectivity...")
        try:
            resp = scraper.client.get(YEAR_LISTING_PATH.format(year=2025))
            resp.raise_for_status()
            links = ISSUE_LINK_RE.findall(resp.text)
            logger.info(f"OK — found {len(links)} issue links on 2025 page")
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
