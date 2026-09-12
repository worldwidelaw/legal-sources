#!/usr/bin/env python3
"""
TL/MJ-LawsTL -- Timor-Leste Laws (English Translations)

Fetches English translations of Timor-Leste legislation from the UNMIT
Office of Legal Affairs archive hosted at mj.gov.tl.

Strategy:
  - Parse HTML listing pages under RDTL-Law/ and UNTAET-Law/ sections
  - Each page is a table with: document number, subject/description, dates
  - Links point to individual PDFs (English translations)
  - Download each PDF and extract full text via pdfplumber

Sections:
  RDTL (post-independence, 2002+):
    - Parliamentary Laws, Decree-Laws, Government Decrees, Ministerial Orders,
      Ministerial Instructions, Government Resolutions, Parliamentary Resolutions,
      Presidential Decrees, Public Instructions/Regulations, Constitution
  UNTAET (transitional admin, 1999-2002):
    - Regulations, Directives, Executive Orders, Notifications

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TL.MJ-LawsTL")

BASE_URL = "https://mj.gov.tl/jornal/lawsTL/"

# All English category pages to scrape
# Format: (section, category_name, relative_url_to_listing)
CATEGORIES = [
    # RDTL Legislation (post-independence)
    ("RDTL", "Constitution", "RDTL-Law/RDTL-Constitution.pdf"),  # Single PDF, special case
    ("RDTL", "Parliamentary Laws", "RDTL-Law/RDTL-Laws/RDTL-Laws.htm"),
    ("RDTL", "Decree-Laws", "RDTL-Law/RDTL-Decree-Laws/RDTL-Decree-Laws.htm"),
    ("RDTL", "Government Decrees", "RDTL-Law/RDTL-Gov-Decrees/RDTL-Decrees.htm"),
    ("RDTL", "Ministerial Orders", "RDTL-Law/RDTL-Minist-Orders/RDTL-Oreders.htm"),
    ("RDTL", "Ministerial Instructions", "RDTL-Law/RDTL-Instructions/RDTL-Instr.htm"),
    ("RDTL", "Government Resolutions", "RDTL-Law/RDTL-Gov-Resolutions/RDTL-Gov-Resolutions.htm"),
    ("RDTL", "Parliamentary Resolutions", "RDTL-Law/RDTL-Resolutions/RDTL-Resolutions.htm"),
    ("RDTL", "Presidential Decrees", "RDTL-Law/Presidential-Decree-Laws/Presidential-Decree-Laws.htm"),
    ("RDTL", "Public Instructions and Regulations", "RDTL-Law/Public%20Inst-Regs/Public%20Inst-Regs.htm"),
    # UNTAET Legislation (transitional administration)
    ("UNTAET", "Regulations", "UNTAET-Law/Regulations%20English/regenglish.htm"),
    ("UNTAET", "Directives", "UNTAET-Law/Directives%20English/DirEnglish.htm"),
    ("UNTAET", "Executive Orders", "UNTAET-Law/Executive%20Orders/ExEnglish.htm"),
    ("UNTAET", "Notifications", "UNTAET-Law/Notifications%20English/NoteEnglish.htm"),
]


def _parse_date_ddmmyy(date_str: str) -> str:
    """Parse dates like '29/06/02', '07/08/2002', '16 July 2002'."""
    if not date_str:
        return ""
    date_str = date_str.strip()

    # dd/mm/yy or dd/mm/yyyy
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", date_str)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 2000 if year < 50 else 1900
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year}-{month:02d}-{day:02d}"

    # "16 July 2002" etc
    m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_str)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        months = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12,
        }
        month = months.get(month_name)
        if month and 1 <= day <= 31:
            return f"{year}-{month:02d}-{day:02d}"

    return ""


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
        pages_text = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
        return "\n\n".join(pages_text)
    except Exception as e:
        logger.warning(f"pdfplumber extraction failed: {e}")
        return ""


class MJLawsTLScraper(BaseScraper):
    """
    Scraper for TL/MJ-LawsTL -- English translations of Timor-Leste laws.
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
            },
            timeout=120,
        )
        self._seen_urls: set = set()

    def _parse_listing_page(self, section: str, category: str, rel_url: str) -> List[Dict[str, Any]]:
        """Parse an HTML listing page and extract document metadata + PDF links."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("beautifulsoup4 not installed")
            return []

        full_url = urljoin(BASE_URL, rel_url)
        # Derive base for resolving relative PDF links
        page_base = full_url.rsplit("/", 1)[0] + "/"

        self.rate_limiter.wait()
        try:
            resp = self.client.session.get(full_url, timeout=60)
        except Exception as e:
            logger.warning(f"Failed to fetch {full_url}: {e}")
            return []

        if resp.status_code != 200:
            logger.warning(f"HTTP {resp.status_code} for {full_url}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        docs = []

        # Find all PDF links in the page
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if not href.lower().endswith(".pdf"):
                continue

            # Resolve to absolute URL
            if href.startswith("http"):
                pdf_url = href
            else:
                pdf_url = urljoin(page_base, href)

            if pdf_url in self._seen_urls:
                continue
            self._seen_urls.add(pdf_url)

            # Get the link text (document number)
            link_text = a_tag.get_text(strip=True)

            # Try to get the parent row for more metadata
            row = a_tag.find_parent("tr")
            subject = ""
            date_str = ""
            pub_date_str = ""

            if row:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    # Typical patterns:
                    # RDTL: [number, subject, promulgation_date, publication_date]
                    # UNTAET: [number, description]
                    texts = [c.get_text(strip=True) for c in cells]

                    if len(cells) >= 4:
                        subject = texts[1]
                        date_str = texts[2]  # promulgation date
                        pub_date_str = texts[3]  # publication date
                    elif len(cells) >= 2:
                        # The PDF link cell might be first or the description second
                        for i, t in enumerate(texts):
                            if t and t != link_text and not t.endswith(".pdf"):
                                subject = t
                                break

            # Clean subject text (remove amendment notes in red)
            subject = re.sub(r'\s+', ' ', subject).strip()

            # Parse dates - prefer promulgation, fallback to publication
            date = _parse_date_ddmmyy(date_str) or _parse_date_ddmmyy(pub_date_str)

            # Extract year from the document number if no date
            if not date:
                year_match = re.search(r'(19\d{2}|20\d{2})', link_text)
                if year_match:
                    date = f"{year_match.group(1)}-01-01"

            docs.append({
                "doc_number": link_text,
                "subject": subject,
                "date": date,
                "pdf_url": pdf_url,
                "category": category,
                "section": section,
            })

        logger.info(f"{section}/{category}: {len(docs)} documents found")
        return docs

    def _fetch_pdf_text(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning(f"PDF download failed ({resp.status_code}): {pdf_url}")
                return None
            if len(resp.content) < 500:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return None

            text = _extract_pdf_text(resp.content)
            return text if text else None
        except Exception as e:
            logger.warning(f"Failed to fetch PDF {pdf_url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all English-translated legislative documents."""
        all_docs = []

        for section, category, rel_url in CATEGORIES:
            # Special case: Constitution is a single PDF, not a listing page
            if rel_url.endswith(".pdf"):
                pdf_url = urljoin(BASE_URL, rel_url)
                if pdf_url not in self._seen_urls:
                    self._seen_urls.add(pdf_url)
                    all_docs.append({
                        "doc_number": "Constitution",
                        "subject": "Constitution of the Democratic Republic of Timor-Leste",
                        "date": "2002-03-22",
                        "pdf_url": pdf_url,
                        "category": category,
                        "section": section,
                    })
                continue

            docs = self._parse_listing_page(section, category, rel_url)
            all_docs.extend(docs)

        logger.info(f"Total documents discovered: {len(all_docs)}")

        for doc in all_docs:
            # Build stable ID
            section_short = doc["section"][:5]
            cat_short = re.sub(r"[^a-zA-Z]", "", doc["category"])[:12]
            num_clean = re.sub(r"[^\w/.-]", "", doc["doc_number"]).replace("/", "-")
            doc_id = f"TL-MJL-{section_short}-{cat_short}-{num_clean}"

            text = self._fetch_pdf_text(doc["pdf_url"], doc_id)
            if not text or len(text) < 50:
                logger.warning(f"Skipping {doc_id}: insufficient text ({len(text) if text else 0} chars)")
                continue

            yield {
                "doc_id": doc_id,
                "doc_number": doc["doc_number"],
                "subject": doc["subject"],
                "date": doc["date"],
                "pdf_url": doc["pdf_url"],
                "category": doc["category"],
                "section": doc["section"],
                "full_text": text,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Static archive — fetch_all is the only strategy."""
        yield from self.fetch_all()

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing TL/MJ-LawsTL endpoints...")
        print("\n1. Testing index page...")
        try:
            resp = self.client.session.get(BASE_URL + "sidehome-e.htm", timeout=30)
            print(f"   Status: {resp.status_code}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\n2. Testing RDTL Laws listing...")
        try:
            resp = self.client.session.get(
                BASE_URL + "RDTL-Law/RDTL-Laws/RDTL-Laws.htm", timeout=30
            )
            print(f"   Status: {resp.status_code}, {len(resp.text)} chars")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\n3. Testing PDF download + text extraction...")
        try:
            pdf_url = BASE_URL + "RDTL-Law/RDTL-Laws/Law-2002-01.pdf"
            resp = self.client.session.get(pdf_url, timeout=30)
            print(f"   PDF status: {resp.status_code}, {len(resp.content)} bytes")
            if resp.status_code == 200:
                text = _extract_pdf_text(resp.content)
                print(f"   Extracted: {len(text)} chars")
                if text:
                    print(f"   Sample: {text[:200]}")
        except Exception as e:
            print(f"   ERROR: {e}")
        print("\nTest complete!")

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        doc_id = raw.get("doc_id", "")
        doc_number = raw.get("doc_number", "")
        subject = raw.get("subject", "")
        category = raw.get("category", "")
        section = raw.get("section", "")
        full_text = raw.get("full_text", "")
        pub_date = raw.get("date", "")
        pdf_url = raw.get("pdf_url", "")

        # Build title
        title_parts = []
        if section:
            title_parts.append(f"[{section}]")
        if category:
            title_parts.append(f"{category}")
        if doc_number:
            title_parts.append(f"No. {doc_number}")
        if subject:
            title_parts.append(f"— {subject}")
        title = " ".join(title_parts) or doc_id

        return {
            "_id": doc_id,
            "_source": "TL/MJ-LawsTL",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": pub_date,
            "url": pdf_url,
            "doc_number": doc_number,
            "category": category,
            "section": section,
            "language": "en",
        }


def main():
    scraper = MJLawsTLScraper()

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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
