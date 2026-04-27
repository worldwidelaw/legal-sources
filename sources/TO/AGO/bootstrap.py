#!/usr/bin/env python3
"""
TO/AGO -- Tonga Attorney General Legislation & Judgments

Fetches legislation and court judgments from the official Tonga AG portal.

Legislation strategy (same directory crawl as KY/Legislation):
  1. Crawl /cms/images/LEGISLATION/PRINCIPAL/ for year directories
  2. For each year, list legislation item directories
  3. For each item, find the latest version PDF
  4. Download PDF and extract full text

Judgments strategy:
  1. For each court type, scrape the year category pages
  2. For each year, scrape to get individual judgment download links
  3. Download PDF and extract full text

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
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
from html.parser import HTMLParser
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TO.AGO")

BASE_URL = "https://ago.gov.to"
PRINCIPAL_DIR = "/cms/images/LEGISLATION/PRINCIPAL/"
SUBORDINATE_DIR = "/cms/images/LEGISLATION/SUBORDINATE/"

# Court types with their URL paths for judgment scraping
COURT_TYPES = [
    ("court-of-appeal", "Court of Appeal"),
    ("supreme-court-criminal", "Supreme Court Criminal"),
    ("supreme-court-civil", "Supreme Court Civil"),
    ("supreme-court-family", "Supreme Court Family"),
    ("supreme-court-probate", "Supreme Court Probate"),
    ("supreme-court-admiralty", "Supreme Court Admiralty"),
    ("supreme-court-appellate", "Supreme Court Appellate"),
    ("land-court", "Land Court"),
    ("magistrates-court", "Magistrates Court"),
    ("privy-council-decisions", "Privy Council"),
]


class _LinkParser(HTMLParser):
    """Extract href links from HTML."""

    def __init__(self):
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.links.append(value)


class _DownloadLinkParser(HTMLParser):
    """Extract download links and their text from judgment pages."""

    def __init__(self):
        super().__init__()
        self.links: List[Tuple[str, str]] = []
        self._current_href: Optional[str] = None
        self._current_text: List[str] = []
        self._in_a = False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href", "")
            if "download=" in href:
                self._in_a = True
                self._current_href = href
                self._current_text = []

    def handle_data(self, data):
        if self._in_a:
            self._current_text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._in_a:
            self._in_a = False
            if self._current_href:
                text = " ".join(self._current_text).strip()
                if text:
                    self.links.append((self._current_href, text))
            self._current_href = None
            self._current_text = []


class _CategoryLinkParser(HTMLParser):
    """Extract year category links from court type pages."""

    def __init__(self, court_path: str):
        super().__init__()
        self.links: List[Tuple[str, str]] = []
        self.court_path = court_path
        self._current_href: Optional[str] = None
        self._current_text: List[str] = []
        self._in_a = False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href", "")
            if "category/" in href and self.court_path in href:
                self._in_a = True
                self._current_href = href
                self._current_text = []

    def handle_data(self, data):
        if self._in_a:
            self._current_text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._in_a:
            self._in_a = False
            if self._current_href:
                text = " ".join(self._current_text).strip()
                self.links.append((self._current_href, text))
            self._current_href = None
            self._current_text = []


def _parse_links(html: str) -> List[str]:
    parser = _LinkParser()
    parser.feed(html)
    return parser.links


def _extract_year_dirs(links: List[str]) -> List[str]:
    year_dirs = []
    for link in links:
        clean = link.strip("/").split("/")[-1]
        if re.match(r"^\d{4}$", clean):
            year_dirs.append(clean)
    return sorted(year_dirs)


def _extract_item_dirs(links: List[str]) -> List[str]:
    items = []
    for link in links:
        clean = link.strip("/").split("/")[-1]
        if re.match(r"^\d{4}-\d+$", clean):
            items.append(clean)
    return items


def _pick_best_pdf(links: List[str]) -> Optional[str]:
    """Pick the best (highest version) PDF from directory listing."""
    pdfs = [l for l in links if l.lower().endswith(".pdf")]
    if not pdfs:
        return None

    # Sort by version number (suffix _N before .pdf)
    def version_key(name):
        m = re.search(r"_(\d+)\.pdf$", name, re.IGNORECASE)
        return int(m.group(1)) if m else 0

    pdfs.sort(key=version_key, reverse=True)
    return pdfs[0]


def _title_from_filename(pdf_name: str) -> str:
    """Derive title from PDF filename."""
    name = re.sub(r"_\d+\.pdf$", "", pdf_name, flags=re.IGNORECASE)
    # CamelCase to spaces
    name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", name)
    name = name.replace("-", " ").replace("_", " ")
    return name.strip()


def _extract_download_id(href: str) -> str:
    """Extract download ID from ?download=ID:slug URL."""
    m = re.search(r"download=(\d+):", href)
    return m.group(1) if m else ""


def _extract_case_date(text: str) -> str:
    """Extract date from judgment text like '(27 Sep 2024)'."""
    m = re.search(r"\((\d{1,2}\s+\w+\s+\d{4})\)", text)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%d %b %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Fall back to year
    m = re.search(r"\b(20\d{2}|19\d{2})\b", text)
    return m.group(1) if m else ""


class TongaAGOScraper(BaseScraper):
    """Scraper for TO/AGO -- Tonga AG Legislation & Judgments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
            },
            timeout=120,
        )

    def _fetch_page(self, path: str) -> str:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {path}: {e}")
            return ""

    def _download_pdf(self, path: str) -> Optional[bytes]:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            content = resp.content
            if content and (content[:5] == b"%PDF-" or len(content) > 500):
                return content
            logger.warning(f"Empty or invalid response for {path}")
            return None
        except Exception as e:
            logger.warning(f"Failed to download PDF {path}: {e}")
            return None

    # ── Legislation crawl ──

    def _crawl_legislation(self, base_dir: str, leg_type: str) -> Generator[Dict[str, Any], None, None]:
        logger.info(f"Crawling {leg_type} legislation: {base_dir}")
        html = self._fetch_page(base_dir)
        if not html:
            return
        years = _extract_year_dirs(_parse_links(html))
        logger.info(f"Found {len(years)} year directories for {leg_type}")

        for year in years:
            year_html = self._fetch_page(f"{base_dir}{year}/")
            if not year_html:
                continue
            items = _extract_item_dirs(_parse_links(year_html))
            if not items:
                continue

            for item_id in items:
                item_path = f"{base_dir}{year}/{item_id}/"
                file_html = self._fetch_page(item_path)
                if not file_html:
                    continue

                pdf_file = _pick_best_pdf(_parse_links(file_html))
                if not pdf_file:
                    continue

                if pdf_file.startswith("/"):
                    pdf_path = pdf_file
                elif pdf_file.startswith("http"):
                    pdf_path = pdf_file.replace(BASE_URL, "")
                else:
                    pdf_path = f"{item_path}{pdf_file}"

                yield {
                    "doc_id": f"leg-{item_id}",
                    "title": _title_from_filename(pdf_file),
                    "year": year,
                    "doc_type": "legislation",
                    "leg_type": leg_type,
                    "pdf_path": pdf_path,
                    "pdf_url": f"{BASE_URL}{pdf_path}",
                }

    # ── Judgments crawl ──

    def _crawl_judgments(self) -> Generator[Dict[str, Any], None, None]:
        for court_path, court_name in COURT_TYPES:
            logger.info(f"Crawling judgments: {court_name}")
            page_url = f"/cms/judgements/{court_path}.html"
            html = self._fetch_page(page_url)
            if not html:
                continue

            # Parse year category links
            cat_parser = _CategoryLinkParser(court_path)
            cat_parser.feed(html)
            categories = cat_parser.links

            if not categories:
                logger.info(f"  No year categories found for {court_name}")
                continue

            logger.info(f"  Found {len(categories)} year categories for {court_name}")

            for cat_href, cat_text in categories:
                # Fetch the year category page
                if cat_href.startswith("http"):
                    cat_path = cat_href.replace(BASE_URL, "")
                elif cat_href.startswith("/"):
                    cat_path = cat_href
                else:
                    cat_path = f"/cms/judgements/{court_path}/{cat_href}"

                cat_html = self._fetch_page(cat_path)
                if not cat_html:
                    continue

                # Parse download links
                dl_parser = _DownloadLinkParser()
                dl_parser.feed(cat_html)

                for dl_href, dl_text in dl_parser.links:
                    download_id = _extract_download_id(dl_href)
                    if not download_id:
                        continue

                    case_date = _extract_case_date(dl_text)
                    year = case_date[:4] if case_date and len(case_date) >= 4 else ""

                    # Build download URL
                    if dl_href.startswith("http"):
                        dl_path = dl_href.replace(BASE_URL, "")
                    elif dl_href.startswith("/"):
                        dl_path = dl_href
                    else:
                        dl_path = f"/cms/judgements/{court_path}/{dl_href}"

                    yield {
                        "doc_id": f"jdg-{download_id}",
                        "title": dl_text.strip(),
                        "year": year,
                        "doc_type": "case_law",
                        "court": court_name,
                        "pdf_path": dl_path,
                        "pdf_url": f"{BASE_URL}{dl_path}",
                        "case_date": case_date,
                    }

    # ── BaseScraper interface ──

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_type = raw.get("doc_type", "legislation")

        result = {
            "_id": f"TO/AGO/{raw.get('doc_id', '')}",
            "_source": "TO/AGO",
            "_type": doc_type,
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("case_date", "") or raw.get("year", ""),
            "url": raw.get("pdf_url", ""),
            "doc_id": raw.get("doc_id", ""),
        }

        if doc_type == "case_law":
            result["court"] = raw.get("court", "")
        else:
            result["legislation_type"] = raw.get("leg_type", "principal")

        return result

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        count = 0
        errors = 0

        # Legislation: principal acts
        for item in self._crawl_legislation(PRINCIPAL_DIR, "principal"):
            pdf_bytes = self._download_pdf(item["pdf_path"])
            if not pdf_bytes:
                errors += 1
                continue

            text = extract_pdf_markdown(
                source="TO/AGO",
                source_id=item["doc_id"],
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text.strip()) < 50:
                logger.warning(f"Insufficient text for {item['doc_id']}: {len(text)} chars")
                errors += 1
                continue

            item["text"] = text
            yield item
            count += 1

            if count % 50 == 0:
                logger.info(f"Progress: {count} legislation records, {errors} errors")

        leg_count = count
        logger.info(f"Legislation complete: {leg_count} records, {errors} errors")

        # Legislation: subordinate
        for item in self._crawl_legislation(SUBORDINATE_DIR, "subordinate"):
            pdf_bytes = self._download_pdf(item["pdf_path"])
            if not pdf_bytes:
                errors += 1
                continue

            text = extract_pdf_markdown(
                source="TO/AGO",
                source_id=item["doc_id"],
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text.strip()) < 50:
                logger.warning(f"Insufficient text for {item['doc_id']}: {len(text)} chars")
                errors += 1
                continue

            item["text"] = text
            yield item
            count += 1

        sub_count = count - leg_count
        logger.info(f"Subordinate legislation: {sub_count} records")

        # Judgments
        jdg_start = count
        for item in self._crawl_judgments():
            pdf_bytes = self._download_pdf(item["pdf_path"])
            if not pdf_bytes:
                errors += 1
                continue

            text = extract_pdf_markdown(
                source="TO/AGO",
                source_id=item["doc_id"],
                pdf_bytes=pdf_bytes,
                table="case_law",
            ) or ""

            if not text or len(text.strip()) < 50:
                logger.warning(f"Insufficient text for judgment {item['doc_id']}: {len(text)} chars")
                errors += 1
                continue

            item["text"] = text
            yield item
            count += 1

            if count % 50 == 0:
                logger.info(f"Progress: {count} total records, {errors} errors")

        jdg_count = count - jdg_start
        logger.info(f"Judgments: {jdg_count} records")
        logger.info(f"Grand total: {count} records, {errors} errors")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = TongaAGOScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing legislation directory listing...")
        html = scraper._fetch_page(PRINCIPAL_DIR)
        years = _extract_year_dirs(_parse_links(html))
        if not years:
            logger.error("FAILED — no year directories found")
            sys.exit(1)
        logger.info(f"OK — {len(years)} year directories found (legislation)")

        # Test one PDF
        year = years[-1]
        year_html = scraper._fetch_page(f"{PRINCIPAL_DIR}{year}/")
        items = _extract_item_dirs(_parse_links(year_html))
        if items:
            item_id = items[0]
            file_html = scraper._fetch_page(f"{PRINCIPAL_DIR}{year}/{item_id}/")
            pdf_file = _pick_best_pdf(_parse_links(file_html))
            if pdf_file:
                pdf_path = f"{PRINCIPAL_DIR}{year}/{item_id}/{pdf_file}"
                pdf_bytes = scraper._download_pdf(pdf_path)
                if pdf_bytes:
                    text = extract_pdf_markdown(
                        source="TO/AGO", source_id=item_id,
                        pdf_bytes=pdf_bytes, table="legislation",
                    ) or ""
                    logger.info(f"OK — legislation PDF: {len(text)} chars from {pdf_file}")
                else:
                    logger.warning("Legislation PDF download failed")

        # Test judgments
        logger.info("Testing judgment pages...")
        html = scraper._fetch_page("/cms/judgements/supreme-court-civil.html")
        if html:
            cat_parser = _CategoryLinkParser("supreme-court-civil")
            cat_parser.feed(html)
            logger.info(f"OK — {len(cat_parser.links)} year categories found (civil judgments)")
        else:
            logger.warning("Could not fetch judgments page")

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
