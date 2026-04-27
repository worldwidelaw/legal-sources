#!/usr/bin/env python3
"""
US/WA-Legislation -- Washington State Legislative Web Services

Fetches full text of Washington state legislation:
  - Revised Code of Washington (RCW): codified statutes from lawfilesext.leg.wa.gov
  - Bills: enrolled legislation via SOAP web services + HTML full text

Strategy:
  1. RCW: Crawl directory listing at lawfilesext.leg.wa.gov/law/RCW/ →
     title dirs → chapter dirs → section .htm files → extract text
  2. Bills: Use LegislativeDocumentService SOAP/REST to enumerate bill
     document URLs, then fetch HTML full text from lawfilesext.leg.wa.gov

Data: Public domain. No authentication required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all collections)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import re
import time
import json
import logging
import html as html_module
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import unquote, urljoin, quote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WA-Legislation")

RCW_BASE = "https://lawfilesext.leg.wa.gov/law/RCW"
SOAP_BASE = "https://wslwebservices.leg.wa.gov"
FILE_BASE = "https://lawfilesext.leg.wa.gov"
DELAY = 1.0  # seconds between requests


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'</tr>', '\n', text)
    text = re.sub(r'</li>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    # Remove BOM (both UTF-8 BOM character and raw bytes) and stray "PDF" link text
    text = text.replace('\ufeff', '').replace('\xef\xbb\xbf', '')
    text = re.sub(r'^ï»¿', '', text)
    text = re.sub(r'^PDF', '', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_directory_links(html: str, base_url: str) -> list:
    """Parse Apache/IIS directory listing HTML to extract links."""
    links = []
    # Match href attributes in anchor tags
    for match in re.finditer(r'<a\s+[^>]*href="([^"]+)"', html, re.IGNORECASE):
        href = match.group(1)
        # Skip parent directory and sorting links
        if href.startswith("?") or href.startswith(".."):
            continue
        # Build full URL — handle absolute paths from IIS directory listings
        if href.startswith("http"):
            full_url = href
        elif href.startswith("/"):
            from urllib.parse import urlparse
            parsed = urlparse(base_url)
            full_url = f"{parsed.scheme}://{parsed.netloc}{href}"
        else:
            full_url = urljoin(base_url.rstrip("/") + "/", href)
        # Use link text if available, otherwise decode the href
        link_text = unquote(href.rstrip("/").split("/")[-1]) if "/" in href else unquote(href)
        links.append((link_text, full_url))
    return links


class WALegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html, application/xml, text/xml, */*",
            },
            timeout=60,
        )

    def _get(self, url: str) -> str:
        """Fetch URL and return text, with rate limiting."""
        time.sleep(DELAY)
        resp = self.http.get(url)
        return resp.text

    def _get_xml(self, url: str) -> ET.Element:
        """Fetch URL and parse XML."""
        text = self._get(url)
        return ET.fromstring(text)

    # ── RCW (Revised Code of Washington) ──────────────────────────────

    def fetch_rcw_titles(self) -> list:
        """Get list of RCW title directories from the file server."""
        html = self._get(f"{RCW_BASE}/")
        links = parse_directory_links(html, f"{RCW_BASE}/")
        titles = []
        for name, url in links:
            # Title dirs contain "TITLE" in URL or name
            if "TITLE" in name.upper() or "TITLE" in url.upper():
                titles.append((name, url))
        return titles

    def fetch_rcw_chapters(self, title_url: str) -> list:
        """Get list of chapter directories within an RCW title."""
        html = self._get(title_url)
        links = parse_directory_links(html, title_url)
        chapters = []
        for name, url in links:
            if "CHAPTER" in name.upper() or "CHAPTER" in url.upper():
                chapters.append((name, url))
        return chapters

    def fetch_rcw_sections(self, chapter_url: str) -> list:
        """Get list of section .htm files within an RCW chapter."""
        html = self._get(chapter_url)
        links = parse_directory_links(html, chapter_url)
        sections = []
        for name, url in links:
            lower_name = name.lower()
            lower_url = url.lower()
            if not (lower_url.endswith(".htm") or lower_url.endswith(".html")):
                continue
            # Skip chapter index pages (end with "CHAPTER.htm")
            if re.search(r'CHAPTER\.htm', url, re.IGNORECASE):
                continue
            sections.append((name, url))
        return sections

    def parse_rcw_section(self, html: str, section_name: str) -> dict:
        """Parse an RCW section HTML file to extract metadata and text."""
        # Extract section number from filename (e.g., "RCW  1 .04 .010.htm")
        # Clean the filename to get section number
        sec_num = section_name.replace(".htm", "").replace(".html", "")
        sec_num = re.sub(r'\s+', ' ', sec_num).strip()
        # Normalize: "RCW  1 .04 .010" -> "1.04.010"
        clean_num = sec_num.replace("RCW", "").strip()
        clean_num = re.sub(r'\s*\.\s*', '.', clean_num)
        clean_num = re.sub(r'\s+', '', clean_num)

        # Extract title from <title> or <h1> tags
        title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL | re.IGNORECASE)
        title = strip_html(title_match.group(1)) if title_match else sec_num

        # Extract the main body text
        text = strip_html(html)

        return {
            "section_number": clean_num,
            "title": title,
            "text": text,
        }

    def iter_rcw(self, max_sections: int = 0) -> Generator[dict, None, None]:
        """Iterate all RCW sections with full text."""
        titles = self.fetch_rcw_titles()
        logger.info(f"RCW: {len(titles)} titles found")
        count = 0

        for title_name, title_url in titles:
            # Extract title number from dir name
            t_match = re.search(r'(\d+[A-Z]?)', title_name)
            t_num = t_match.group(1) if t_match else title_name

            chapters = self.fetch_rcw_chapters(title_url)
            logger.info(f"  Title {t_num}: {len(chapters)} chapters")

            for ch_name, ch_url in chapters:
                # Extract chapter number
                ch_match = re.search(r'(\d+[A-Z]?\s*\.\s*\d+[A-Z]?)', ch_name)
                ch_num = re.sub(r'\s+', '', ch_match.group(1)) if ch_match else ch_name

                sections = self.fetch_rcw_sections(ch_url)

                for sec_name, sec_url in sections:
                    try:
                        html = self._get(sec_url)
                        parsed = self.parse_rcw_section(html, sec_name)

                        if not parsed["text"] or len(parsed["text"]) < 20:
                            continue

                        yield {
                            "collection": "RCW",
                            "section_id": f"RCW-{parsed['section_number']}",
                            "title_num": t_num,
                            "chapter_num": ch_num,
                            "section_number": parsed["section_number"],
                            "title": parsed["title"],
                            "text": parsed["text"],
                            "url": sec_url,
                        }
                        count += 1
                        if count % 100 == 0:
                            logger.info(f"    RCW progress: {count} sections")
                        if max_sections and count >= max_sections:
                            return
                    except Exception as e:
                        logger.warning(f"Failed to fetch {sec_url}: {e}")
                        continue

    # ── Bills (via SOAP/REST web services) ────────────────────────────

    def fetch_bill_documents(self, biennium: str, doc_class: str = "Bills") -> list:
        """Get bill document list via LegislativeDocumentService."""
        url = (
            f"{SOAP_BASE}/LegislativeDocumentService.asmx"
            f"/GetAllDocumentsByClass?biennium={biennium}&documentClass={doc_class}"
        )
        root = self._get_xml(url)
        ns = {"d": "http://WSLWebServices.leg.wa.gov/"}
        docs = []
        for doc in root.findall(".//d:LegislativeDocument", ns):
            name_el = doc.find("d:Name", ns)
            htm_el = doc.find("d:HtmUrl", ns)
            pdf_el = doc.find("d:PdfUrl", ns)
            if name_el is not None and htm_el is not None:
                docs.append({
                    "name": name_el.text or "",
                    "htm_url": htm_el.text or "",
                    "pdf_url": pdf_el.text if pdf_el is not None else "",
                })
        return docs

    def fetch_bill_text(self, htm_url: str) -> str:
        """Fetch and clean full text from a bill HTML file."""
        try:
            html = self._get(htm_url)
            return strip_html(html)
        except Exception as e:
            logger.warning(f"Failed to fetch bill text {htm_url}: {e}")
            return ""

    def iter_bills(self, biennium: str = "2025-26", max_bills: int = 0) -> Generator[dict, None, None]:
        """Iterate bills with full text for a given biennium."""
        docs = self.fetch_bill_documents(biennium)
        logger.info(f"Bills ({biennium}): {len(docs)} documents found")
        count = 0

        for doc in docs:
            htm_url = doc["htm_url"]
            if not htm_url:
                continue

            text = self.fetch_bill_text(htm_url)
            if not text or len(text) < 50:
                continue

            bill_name = doc["name"]
            yield {
                "collection": "Bill",
                "section_id": f"BILL-{biennium}-{bill_name}",
                "biennium": biennium,
                "bill_name": bill_name,
                "title": f"Washington State Bill {bill_name} ({biennium})",
                "text": text,
                "url": htm_url,
            }
            count += 1
            if count % 50 == 0:
                logger.info(f"    Bills progress: {count}")
            if max_bills and count >= max_bills:
                return

    # ── Standard interface ────────────────────────────────────────────

    def normalize(self, raw: dict) -> dict:
        """Transform raw record into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        section_id = raw.get("section_id", "")
        collection = raw.get("collection", "")

        return {
            "_id": section_id,
            "_source": "US/WA-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": now,
            "url": raw.get("url", ""),
            "collection": collection,
            "section_number": raw.get("section_number", ""),
            "title_num": raw.get("title_num", ""),
            "chapter_num": raw.get("chapter_num", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents (RCW + Bills)."""
        logger.info("Starting full fetch: RCW + Bills")
        for raw in self.iter_rcw():
            yield raw
        for raw in self.iter_bills():
            yield raw

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Yield documents updated since a date (full refresh for now)."""
        yield from self.fetch_all()

    def run_sample(self) -> list:
        """Fetch sample records for testing."""
        records = []
        # 10 RCW sections
        for raw in self.iter_rcw(max_sections=10):
            records.append(self.normalize(raw))
        # 5 bills
        for raw in self.iter_bills(max_bills=5):
            records.append(self.normalize(raw))
        return records

    def test_api(self):
        """Test connectivity to the web services and file server."""
        logger.info("Testing RCW file server...")
        try:
            titles = self.fetch_rcw_titles()
            logger.info(f"  RCW: {len(titles)} titles found")
        except Exception as e:
            logger.error(f"  RCW file server failed: {e}")

        logger.info("Testing SOAP web service...")
        try:
            docs = self.fetch_bill_documents("2025-26")
            logger.info(f"  Bills (2025-26): {len(docs)} documents")
        except Exception as e:
            logger.error(f"  SOAP service failed: {e}")


def main():
    scraper = WALegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test-api] [--sample] [--full]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test-api":
        scraper.test_api()

    elif cmd == "bootstrap":
        sample_mode = "--sample" in sys.argv
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample_mode:
            records = scraper.run_sample()
            for i, rec in enumerate(records):
                out_path = sample_dir / f"sample_{i:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(records)} sample records to {sample_dir}")

            # Validate
            total = len(records)
            with_text = sum(1 for r in records if r.get("text") and len(r["text"]) > 50)
            logger.info(f"Validation: {with_text}/{total} records have substantial text")
        else:
            count = 0
            sample_dir.mkdir(exist_ok=True)
            for rec in scraper.fetch_all():
                if count < 20:
                    out_path = sample_dir / f"sample_{count:04d}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(rec, f, indent=2, ensure_ascii=False)
                count += 1
                if count % 500 == 0:
                    logger.info(f"Progress: {count} records")
            logger.info(f"Total: {count} records fetched")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
