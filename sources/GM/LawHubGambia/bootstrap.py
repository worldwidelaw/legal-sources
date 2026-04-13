#!/usr/bin/env python3
"""
GM/LawHubGambia -- Law Hub Gambia

Fetches case law, constitution, and legislation from lawhubgambia.com.

Strategy:
  - Curated list of content pages (case law, constitution, legislation)
  - HTML full text extraction for case law and constitution pages
  - PDF download + text extraction for legislation documents
  - BeautifulSoup for HTML parsing, pdfplumber for PDF text

Data: ~50 documents (case law, constitution, legislation)
License: Open access (non-profit legal resource)
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
from html import unescape

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GM.LawHubGambia")

BASE_URL = "https://www.lawhubgambia.com"

# ── Content catalog ──────────────────────────────────────────────────
# Case law pages with full text in HTML
CASE_LAW_PAGES = [
    # Standalone judgment pages
    {"slug": "sc-1-2002", "title": "Jammeh v Attorney-General (2001)", "date": "2001-11-29"},
    {"slug": "sc-1-2014", "title": "Gambia Press Union v The Attorney General (2018)", "date": "2018-05-09"},
    {"slug": "sc-1-2017", "title": "Bai Emil Touray v The Attorney General (2017)", "date": "2017"},
    {"slug": "jabbi-v-coma", "title": "Jabbi v Coma and Others", "date": None},
    # Pages with inline full-text judgments
    {"slug": "assembly", "title": "Ousainou Darboe & 19 Others v IGP et al (2017)", "date": "2017-11-23"},
    {"slug": "arbitrary-detention", "title": "Denton v Director-General NIA (2006)", "date": "2006-07-24"},
    {"slug": "fair-trial", "title": "Sabally v Inspector General of Police (2001)", "date": "2001"},
    {"slug": "commission-of-inquiry", "title": "M.A Kharafi & Sons v The Attorney General (2020)", "date": "2020-06-01"},
]

# Constitution pages with full text in HTML
CONSTITUTION_PAGES = [
    {"slug": "1997-constitution", "title": "The Constitution of The Republic of The Gambia, 1997", "date": "1997-01-16"},
]

# Legislation index pages that contain links to PDFs
LEGISLATION_INDEX_PAGES = [
    "criminal-law-database",
    "electoral-laws",
    "womens-rights",
    "freedom-expression-and-media",
    "persons-with-disabilities-bill-2020",
]

# Direct PDF legislation links (discovered from index pages)
LEGISLATION_PDFS = [
    # Criminal law
    {"slug": "criminal-code-1933", "pdf": "/s/Criminal-Code-Act-No-25-of-1933.pdf",
     "title": "Criminal Code Act No. 25 of 1933", "date": "1933"},
    {"slug": "criminal-procedure-code-1933", "pdf": "/s/Criminal-Procedure-Code-Act-No-26-of-1933.pdf",
     "title": "Criminal Procedure Code Act No. 26 of 1933", "date": "1933"},
    {"slug": "criminal-code-ordinance-1934", "pdf": "/s/1934_An-Ordinance-to-Establish-a-Code-of-Criminal-Law-An-Ordinance-to-Make-Provision-for-the-Procedu.pdf",
     "title": "Criminal Law Ordinance 1934", "date": "1934"},
    {"slug": "criminal-offences-bill-2020", "pdf": "/s/Criminal-Offences-Bill_2020.pdf",
     "title": "Criminal Offences Bill 2020", "date": "2020"},
    # Electoral laws
    {"slug": "election-act-1963", "pdf": "/s/GM1963ElectionAct.pdf",
     "title": "Election Act 1963", "date": "1963"},
    {"slug": "elections-decree-1996", "pdf": "/s/Elections-Decree-78-of-1996.pdf",
     "title": "Elections Decree No. 78 of 1996", "date": "1996"},
    {"slug": "elections-act-chapter-3-01", "pdf": "/s/Elections-Act_Decree-No-78-of-1996.pdf",
     "title": "Elections Act (Chapter 3:01)", "date": "1996"},
    {"slug": "code-of-election-campaign-1996", "pdf": "/s/Code-of-Election-Campaign-1996.pdf",
     "title": "Code of Election Campaign Ethics 1996", "date": "1996"},
    {"slug": "election-petition-rules", "pdf": "/s/Election-Petition-Rules.pdf",
     "title": "Election Petition Rules", "date": None},
    {"slug": "election-amendment-act-2017", "pdf": "/s/Election-Amendment-Act.pdf",
     "title": "Election (Amendment) Act 2017", "date": "2017"},
    # Women's rights / human rights
    {"slug": "womens-act-2010", "pdf": "/s/Womens-Act-2010.pdf",
     "title": "Women's Act 2010", "date": "2010"},
    {"slug": "womens-act-amendment-2015", "pdf": "/s/Womens-Act-Amendment-Act-2015.pdf",
     "title": "Women's (Amendment) Act 2015", "date": "2015"},
    {"slug": "sexual-offences-act-2013", "pdf": "/s/Sexual-Offences-Act-2013.pdf",
     "title": "Sexual Offences Act 2013", "date": "2013"},
    {"slug": "domestic-violence-act-2013", "pdf": "/s/Domestic-Violence-Act-2013.pdf",
     "title": "Domestic Violence Act 2013", "date": "2013"},
    {"slug": "access-to-information-bill-2020", "pdf": "/s/Access-to-Information-Bill_2020.pdf",
     "title": "Access to Information Bill 2020", "date": "2020"},
    {"slug": "persons-with-disabilities-bill-2020", "pdf": "/s/Persons-with-Disabilities-Bill_2020.pdf",
     "title": "Persons with Disabilities Bill 2020", "date": "2020"},
]

# Gambia Law Reports (bulk PDFs with compiled case law)
LAW_REPORT_PDFS = [
    {"slug": "gambia-law-reports-1960-1993", "pdf": "/s/The-Gambia-Law-Reports-1960-1993.pdf",
     "title": "The Gambia Law Reports 1960-1993", "date": "1993"},
    {"slug": "gambia-law-reports-1997-2001", "pdf": "/s/The-Gambia-Law-Reports-1997-2001.pdf",
     "title": "The Gambia Law Reports 1997-2001", "date": "2001"},
    {"slug": "gambia-law-reports-2002-2008-vol1", "pdf": "/s/The-Gambia-Law-Reports-2002-2008-Volume-1.pdf",
     "title": "The Gambia Law Reports 2002-2008 Volume 1", "date": "2008"},
    {"slug": "gambia-law-reports-2002-2008-vol2", "pdf": "/s/The-Gambia-Law-Reports-2002-2008-Volume-2.pdf",
     "title": "The Gambia Law Reports 2002-2008 Volume 2", "date": "2008"},
]


def clean_html_text(html_content: str) -> str:
    """Extract clean text from HTML, removing tags and normalizing whitespace."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove script and style elements
    for tag in soup(["script", "style", "nav", "header", "footer", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator="\n")

    # Normalize whitespace
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            lines.append(stripped)

    text = "\n".join(lines)
    # Collapse multiple blank lines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_judgment_text(html_content: str) -> str:
    """Extract the main judgment/legal text from a Squarespace page."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove navigation, header, footer
    for tag in soup(["script", "style", "nav", "noscript"]):
        tag.decompose()

    # Look for the main content area
    # Squarespace uses various content block classes
    main_content = None
    for selector in [
        "div.sqs-block-content",
        "article",
        "div.entry-content",
        "div.sqs-layout",
        "main",
    ]:
        blocks = soup.select(selector)
        if blocks:
            # Concatenate all content blocks
            texts = []
            for block in blocks:
                text = block.get_text(separator="\n")
                if len(text.strip()) > 100:
                    texts.append(text)
            if texts:
                main_content = "\n\n".join(texts)
                break

    if not main_content:
        # Fallback: get all text from body
        body = soup.find("body")
        if body:
            main_content = body.get_text(separator="\n")
        else:
            main_content = soup.get_text(separator="\n")

    # Clean up
    lines = []
    for line in main_content.splitlines():
        stripped = line.strip()
        if stripped:
            lines.append(stripped)

    text = "\n".join(lines)
    text = re.sub(r'\n{3,}', '\n\n', text)

    # Remove common Squarespace boilerplate
    boilerplate_patterns = [
        r'Law Hub Gambia.*?All Rights Reserved\.?',
        r'Powered by Squarespace',
        r'lawhubgambia@gmail\.com',
        r'Share\s*Facebook\s*Twitter\s*LinkedIn',
        r'Cookie Policy',
    ]
    for pattern in boilerplate_patterns:
        text = re.sub(pattern, '', text, flags=re.I | re.S)

    return text.strip()


class GMLawHubGambiaScraper(BaseScraper):
    """
    Scraper for GM/LawHubGambia.
    Country: GM
    URL: https://www.lawhubgambia.com

    Data types: case_law, legislation
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

    def _get_with_retry(self, url: str, max_retries: int = 3, timeout: int = 60) -> Optional[requests.Response]:
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

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="GM/LawHubGambia",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def _discover_pdfs_from_index(self, slug: str) -> List[dict]:
        """Discover PDF links from a legislation index page."""
        url = f"{BASE_URL}/{slug}"
        resp = self._get_with_retry(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        pdfs = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/s/" in href and href.endswith(".pdf"):
                # Normalize to relative path
                if href.startswith("http"):
                    from urllib.parse import urlparse
                    parsed = urlparse(href)
                    href = parsed.path
                title = link.get_text(strip=True) or Path(href).stem.replace("-", " ")
                pdf_slug = Path(href).stem.lower().replace(" ", "-")
                pdfs.append({
                    "slug": pdf_slug,
                    "pdf": href,
                    "title": title,
                })
        return pdfs

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from Law Hub Gambia."""

        # 1. Case law pages (HTML full text)
        for case in CASE_LAW_PAGES:
            url = f"{BASE_URL}/{case['slug']}"
            logger.info(f"Fetching case law: {case['slug']}")
            resp = self._get_with_retry(url)
            if not resp:
                logger.warning(f"Failed to fetch {url}")
                continue

            text = extract_judgment_text(resp.text)
            if len(text) < 200:
                logger.warning(f"Insufficient text for {case['slug']} ({len(text)} chars)")
                continue

            yield {
                "slug": case["slug"],
                "title": case["title"],
                "text": text,
                "date": case["date"],
                "url": url,
                "doc_type": "case_law",
            }
            time.sleep(2)

        # 2. Constitution pages (HTML full text)
        for const in CONSTITUTION_PAGES:
            url = f"{BASE_URL}/{const['slug']}"
            logger.info(f"Fetching constitution: {const['slug']}")
            resp = self._get_with_retry(url)
            if not resp:
                continue

            text = extract_judgment_text(resp.text)
            if len(text) < 500:
                logger.warning(f"Insufficient text for {const['slug']}")
                continue

            yield {
                "slug": const["slug"],
                "title": const["title"],
                "text": text,
                "date": const["date"],
                "url": url,
                "doc_type": "legislation",
            }
            time.sleep(2)

        # 3. Legislation PDFs
        for leg in LEGISLATION_PDFS:
            pdf_url = f"{BASE_URL}{leg['pdf']}"
            logger.info(f"Fetching legislation PDF: {leg['slug']}")
            text = self._extract_pdf_text(pdf_url)
            if not text:
                logger.warning(f"No text extracted from {leg['pdf']}")
                continue

            yield {
                "slug": leg["slug"],
                "title": leg["title"],
                "text": text,
                "date": leg.get("date"),
                "url": pdf_url,
                "doc_type": "legislation",
            }
            time.sleep(2)

        # 4. Gambia Law Reports (bulk PDFs — case law compilations)
        for report in LAW_REPORT_PDFS:
            pdf_url = f"{BASE_URL}{report['pdf']}"
            logger.info(f"Fetching law report PDF: {report['slug']}")
            text = self._extract_pdf_text(pdf_url)
            if not text:
                logger.warning(f"No text extracted from {report['pdf']}")
                continue

            yield {
                "slug": report["slug"],
                "title": report["title"],
                "text": text,
                "date": report.get("date"),
                "url": pdf_url,
                "doc_type": "case_law",
            }
            time.sleep(2)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Law Hub Gambia is a static site — updates are infrequent."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw document to standard schema."""
        doc_type = raw.get("doc_type", "legislation")

        # Map doc_type to standard _type
        type_map = {
            "case_law": "case_law",
            "legislation": "legislation",
            "constitution": "legislation",
        }

        return {
            "_id": f"GM/LawHubGambia/{raw['slug']}",
            "_source": "GM/LawHubGambia",
            "_type": type_map.get(doc_type, "legislation"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "doc_type": doc_type,
        }

    def test_api(self):
        """Test connectivity to lawhubgambia.com."""
        print("Testing connection to lawhubgambia.com...")

        # Test homepage
        resp = self._get_with_retry(f"{BASE_URL}/")
        if resp:
            print(f"  Homepage: OK ({resp.status_code})")
        else:
            print("  Homepage: FAILED")
            return

        # Test a case law page
        resp = self._get_with_retry(f"{BASE_URL}/sc-1-2002")
        if resp:
            text = extract_judgment_text(resp.text)
            print(f"  Case law page (sc-1-2002): OK ({len(text)} chars)")
        else:
            print("  Case law page: FAILED")

        # Test a PDF
        resp = self._get_with_retry(f"{BASE_URL}/s/Criminal-Code-Act-No-25-of-1933.pdf")
        if resp:
            print(f"  PDF download: OK ({len(resp.content)} bytes)")
        else:
            print("  PDF download: FAILED")

        # Test constitution page
        resp = self._get_with_retry(f"{BASE_URL}/1997-constitution")
        if resp:
            text = extract_judgment_text(resp.text)
            print(f"  Constitution: OK ({len(text)} chars)")
        else:
            print("  Constitution: FAILED")

        print("Done.")


def main():
    scraper = GMLawHubGambiaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap --sample|test-api]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        scraper.test_api()
    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\n{'='*60}")
        print(f"Bootstrap complete ({'sample' if sample_mode else 'full'}):")
        print(f"  Records fetched: {stats['records_fetched']}")
        if sample_mode:
            print(f"  Sample records saved: {stats.get('sample_records_saved', 0)}")
        else:
            print(f"  New: {stats['records_new']}")
            print(f"  Updated: {stats['records_updated']}")
            print(f"  Skipped: {stats['records_skipped']}")
        print(f"  Errors: {stats['errors']}")
        print(f"{'='*60}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
