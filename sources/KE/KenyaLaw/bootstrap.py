#!/usr/bin/env python3
"""
KE/KenyaLaw -- Kenya Law (National Council for Law Reporting) Data Fetcher

Fetches Kenyan legislation and case law from new.kenyalaw.org (PeachJam platform).

Strategy:
  - Legislation: Paginate /legislation/ listing, fetch each act page for inline
    AKN-structured HTML full text. ~500 acts + subsidiary legislation.
  - Case law: Paginate /judgments/ listing by court, download PDF source files,
    extract text with PyPDF2. 315K+ judgments available.

robots.txt specifies 5s crawl delay, which we respect.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html.parser import HTMLParser

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KE.KenyaLaw")

BASE_URL = "https://new.kenyalaw.org"


class TextExtractor(HTMLParser):
    """Extract text from HTML, stripping tags but preserving structure."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self._skip = False
        self._block_tags = {"script", "style", "nav", "header", "footer", "noscript"}
        self._newline_tags = {
            "p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6",
            "li", "tr", "section", "article", "blockquote",
        }

    def handle_starttag(self, tag, attrs):
        if tag in self._block_tags:
            self._skip = True
        if tag in self._newline_tags:
            self.text_parts.append("\n")

    def handle_endtag(self, tag):
        if tag in self._block_tags:
            self._skip = False
        if tag in self._newline_tags:
            self.text_parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self.text_parts.append(data)

    def get_text(self) -> str:
        text = "".join(self.text_parts)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()


class KenyaLawScraper(BaseScraper):
    """
    Scraper for KE/KenyaLaw -- Kenya Law (PeachJam platform).
    Country: KE
    URL: https://new.kenyalaw.org/
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=120,
        )

    def _get_page(self, url: str) -> str:
        """Fetch a page respecting crawl delay."""
        self.rate_limiter.wait()
        resp = self.client.get(url, allow_redirects=True)
        resp.raise_for_status()
        return resp.text

    def _extract_legislation_links(self, html: str) -> list:
        """Extract legislation links from a listing page."""
        links = []
        # Pattern: href="/akn/ke/act/..." in listing
        for match in re.finditer(r'href="(/akn/ke/act/[^"]+)"', html):
            path = match.group(1).strip()
            if path not in [l["path"] for l in links]:
                links.append({"path": path, "url": f"{BASE_URL}{path}"})
        return links

    def _extract_judgment_links(self, html: str) -> list:
        """Extract judgment links from a listing page."""
        links = []
        for match in re.finditer(r'href="(/akn/ke/judgment/[^"]+)"', html):
            path = match.group(1)
            if path not in [l["path"] for l in links]:
                links.append({"path": path, "url": f"{BASE_URL}{path}"})
        return links

    def _extract_title(self, html: str) -> str:
        """Extract document title from page."""
        m = re.search(r'<title>([^<]+)</title>', html)
        if m:
            title = m.group(1).strip()
            # Remove suffixes like " | Kenya Law" or " - Kenya Law"
            title = re.sub(r'\s*[-|]\s*Kenya Law.*$', '', title)
            title = re.sub(r'\s*\n.*', '', title)  # Remove anything after newline
            return title
        return ""

    def _extract_legislation_text(self, html: str) -> str:
        """Extract full text from legislation page (inline AKN HTML)."""
        # Find the main content area - look for la-akoma-ntoso or content-body
        content = ""

        # Try to find AKN content block
        akn_match = re.search(
            r'<la-akoma-ntoso[^>]*>(.*?)</la-akoma-ntoso>',
            html, re.DOTALL
        )
        if akn_match:
            content = akn_match.group(1)
        else:
            # Try class="content-body" or similar
            body_match = re.search(
                r'class="content-body"[^>]*>(.*?)</div>\s*</div>\s*</div>',
                html, re.DOTALL
            )
            if body_match:
                content = body_match.group(1)
            else:
                # Broader fallback: find largest content block
                main_match = re.search(
                    r'<article[^>]*>(.*?)</article>',
                    html, re.DOTALL
                )
                if main_match:
                    content = main_match.group(1)

        if not content:
            return ""

        extractor = TextExtractor()
        extractor.feed(content)
        text = extractor.get_text()

        # Clean excessive whitespace: collapse runs of spaces on each line
        lines = []
        for line in text.split('\n'):
            cleaned = ' '.join(line.split())
            lines.append(cleaned)
        text = '\n'.join(lines)
        # Collapse 3+ blank lines to 2
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using PyPDF2."""
        try:
            import PyPDF2
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            pages = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n\n".join(pages).strip()
        except Exception as e:
            logger.warning(f"  PDF extraction failed: {e}")
            return ""

    def _fetch_judgment_pdf(self, path: str) -> bytes:
        """Download judgment PDF source file."""
        url = f"{BASE_URL}{path}/source.pdf"
        self.rate_limiter.wait()
        resp = self.client.get(url, allow_redirects=True)
        resp.raise_for_status()
        return resp.content

    def _parse_akn_path(self, path: str) -> dict:
        """Parse AKN URI path into components."""
        result = {"doc_type": "", "year": "", "number": "", "date": "", "court": ""}

        # Legislation: /akn/ke/act/2016/31/eng@2022-12-31
        act_match = re.match(
            r'/akn/ke/act(?:/ln)?/(\d{4})/([^/]+)/eng@(\d{4}-\d{2}-\d{2})',
            path
        )
        if act_match:
            result["doc_type"] = "legislation"
            result["year"] = act_match.group(1)
            result["number"] = act_match.group(2)
            result["date"] = act_match.group(3)
            return result

        # Judgment: /akn/ke/judgment/kesc/2026/1234/eng@2026-03-20
        jdg_match = re.match(
            r'/akn/ke/judgment/([^/]+)/(\d{4})/([^/]+)/eng@(\d{4}-\d{2}-\d{2})',
            path
        )
        if jdg_match:
            result["doc_type"] = "case_law"
            result["court"] = jdg_match.group(1)
            result["year"] = jdg_match.group(2)
            result["number"] = jdg_match.group(3)
            result["date"] = jdg_match.group(4)
            return result

        return result

    def _fetch_legislation(self) -> Generator[dict, None, None]:
        """Yield all legislation with full text."""
        seen = set()
        for page_num in range(1, 20):  # Up to 20 pages
            url = f"{BASE_URL}/legislation/?page={page_num}"
            try:
                html = self._get_page(url)
            except Exception as e:
                logger.warning(f"  Legislation page {page_num} failed: {e}")
                break

            links = self._extract_legislation_links(html)
            if not links:
                logger.info(f"  No more legislation links on page {page_num}")
                break

            new_count = 0
            for link in links:
                if link["path"] in seen:
                    continue
                seen.add(link["path"])
                new_count += 1

                try:
                    act_html = self._get_page(link["url"])
                except Exception as e:
                    logger.warning(f"  Failed to fetch {link['path']}: {e}")
                    continue

                title = self._extract_title(act_html)
                text = self._extract_legislation_text(act_html)

                if not text:
                    logger.warning(f"  No text extracted from {link['path']}")
                    continue

                parsed = self._parse_akn_path(link["path"])

                yield {
                    "path": link["path"],
                    "url": link["url"],
                    "title": title,
                    "full_text": text,
                    "date": parsed.get("date"),
                    "doc_type": "legislation",
                    "year": parsed.get("year"),
                    "number": parsed.get("number"),
                }

            logger.info(f"  Legislation page {page_num}: {new_count} new acts")

            if new_count == 0:
                break

        logger.info(f"  Total legislation items: {len(seen)}")

    def _fetch_judgments(self, max_pages: int = 50) -> Generator[dict, None, None]:
        """Yield judgments with full text from PDF."""
        courts = ["KESC", "KECA", "KEHC", "KEELRC", "KEELC"]
        seen = set()

        for court in courts:
            pages_per_court = max(1, max_pages // len(courts))
            for page_num in range(1, pages_per_court + 1):
                url = f"{BASE_URL}/judgments/{court}/?page={page_num}"
                try:
                    html = self._get_page(url)
                except Exception as e:
                    logger.warning(f"  Judgments {court} page {page_num} failed: {e}")
                    break

                links = self._extract_judgment_links(html)
                if not links:
                    break

                new_count = 0
                for link in links:
                    if link["path"] in seen:
                        continue
                    seen.add(link["path"])
                    new_count += 1

                    # Download PDF and extract text
                    try:
                        pdf_bytes = self._fetch_judgment_pdf(link["path"])
                    except Exception as e:
                        logger.warning(f"  PDF download failed for {link['path']}: {e}")
                        continue

                    text = self._extract_pdf_text(pdf_bytes)
                    if not text:
                        logger.warning(f"  No text from PDF {link['path']}")
                        continue

                    # Get title from listing (fetch page would be too slow)
                    parsed = self._parse_akn_path(link["path"])
                    title = f"Kenya {court.upper()} Judgment {parsed.get('number', '')} ({parsed.get('year', '')})"

                    # Try to get better title from first line of text
                    first_lines = text[:500].split('\n')
                    for line in first_lines:
                        line = line.strip()
                        if len(line) > 10 and not line.startswith('http'):
                            title = line[:200]
                            break

                    yield {
                        "path": link["path"],
                        "url": link["url"],
                        "title": title,
                        "full_text": text,
                        "date": parsed.get("date"),
                        "doc_type": "case_law",
                        "court": parsed.get("court"),
                        "year": parsed.get("year"),
                        "number": parsed.get("number"),
                    }

                logger.info(f"  Judgments {court} page {page_num}: {new_count} new")
                if new_count == 0:
                    break

        logger.info(f"  Total judgments: {len(seen)}")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation and case law."""
        logger.info("Fetching legislation...")
        yield from self._fetch_legislation()

        logger.info("Fetching judgments...")
        yield from self._fetch_judgments()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw Kenya Law data into standard schema."""
        path = raw.get("path", "")
        doc_type = raw.get("doc_type", "legislation")

        # Build unique ID from path
        clean_path = path.replace("/akn/ke/", "").replace("/", "-").replace("@", "-")
        _id = f"KE-KLAW-{clean_path}"

        _type = "legislation" if doc_type == "legislation" else "case_law"

        return {
            "_id": _id,
            "_source": "KE/KenyaLaw",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("full_text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "document_type": doc_type,
        }


# ── CLI entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = KenyaLawScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    elif command == "test-api":
        print("Testing Kenya Law connectivity...")
        try:
            html = scraper._get_page(f"{BASE_URL}/legislation/?page=1")
            links = scraper._extract_legislation_links(html)
            print(f"Legislation page 1: {len(links)} links found")

            if links:
                act_html = scraper._get_page(links[0]["url"])
                title = scraper._extract_title(act_html)
                text = scraper._extract_legislation_text(act_html)
                print(f"First act: {title}")
                print(f"Text length: {len(text)} chars")
                print(f"First 200 chars: {text[:200]}")

            # Test judgment PDF
            jdg_html = scraper._get_page(f"{BASE_URL}/judgments/KESC/?page=1")
            jdg_links = scraper._extract_judgment_links(jdg_html)
            print(f"\nSupreme Court page 1: {len(jdg_links)} judgment links")

            if jdg_links:
                pdf_bytes = scraper._fetch_judgment_pdf(jdg_links[0]["path"])
                pdf_text = scraper._extract_pdf_text(pdf_bytes)
                print(f"First judgment PDF: {len(pdf_bytes)} bytes, {len(pdf_text)} chars text")
                print(f"First 200 chars: {pdf_text[:200]}")

            print("\nTest passed!")
        except Exception as e:
            print(f"Test failed: {e}")
            import traceback
            traceback.print_exc()

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
