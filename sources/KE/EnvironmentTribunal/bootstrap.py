#!/usr/bin/env python3
"""
KE/EnvironmentTribunal -- Kenya National Environment Tribunal (KENET)

Fetches tribunal decisions from new.kenyalaw.org/judgments/KENET/.
Full text is inline HTML (AKN-structured) — no PDF extraction needed.
~155 decisions covering environmental licensing appeals under EMCA.

robots.txt specifies 5s crawl delay, which we respect.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import html as html_mod
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KE.EnvironmentTribunal")

BASE_URL = "https://new.kenyalaw.org"
COURT_CODE = "KENET"


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


class EnvironmentTribunalScraper(BaseScraper):
    """
    Scraper for KE/EnvironmentTribunal — KENET decisions on Kenya Law.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/125.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=120,
        )

    def _get_page(self, url: str) -> str:
        self.rate_limiter.wait()
        resp = self.client.get(url, allow_redirects=True)
        resp.raise_for_status()
        return resp.text

    def _extract_judgment_links(self, html: str) -> list:
        links = []
        for match in re.finditer(r'href="(/akn/ke/judgment/[^"]+)"', html):
            path = match.group(1)
            if path not in [l["path"] for l in links]:
                links.append({"path": path, "url": f"{BASE_URL}{path}"})
        return links

    def _extract_title(self, html: str) -> str:
        m = re.search(r'<title>([^<]+)</title>', html)
        if m:
            title = m.group(1).strip()
            title = re.sub(r'\s*[-|]\s*Kenya Law.*$', '', title)
            title = re.sub(r'\s*\n.*', '', title)
            return title
        return ""

    def _extract_judgment_text(self, html: str) -> str:
        """Extract full text from judgment HTML page."""
        content = ""

        # Try AKN content block first
        akn_match = re.search(
            r'<la-akoma-ntoso[^>]*>(.*?)</la-akoma-ntoso>',
            html, re.DOTALL
        )
        if akn_match:
            content = akn_match.group(1)
        else:
            # Try article content
            art_match = re.search(
                r'<article[^>]*>(.*?)</article>',
                html, re.DOTALL
            )
            if art_match:
                content = art_match.group(1)

        if not content:
            return ""

        extractor = TextExtractor()
        extractor.feed(content)
        text = extractor.get_text()

        # Clean whitespace
        lines = []
        for line in text.split('\n'):
            cleaned = ' '.join(line.split())
            lines.append(cleaned)
        text = '\n'.join(lines)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _parse_akn_path(self, path: str) -> dict:
        result = {"court": "", "year": "", "number": "", "date": ""}
        m = re.match(
            r'/akn/ke/judgment/([^/]+)/(\d{4})/([^/]+)/eng@(\d{4}-\d{2}-\d{2})',
            path
        )
        if m:
            result["court"] = m.group(1)
            result["year"] = m.group(2)
            result["number"] = m.group(3)
            result["date"] = m.group(4)
        return result

    def fetch_all(self) -> Generator[dict, None, None]:
        seen = set()
        for page_num in range(1, 20):
            url = f"{BASE_URL}/judgments/{COURT_CODE}/?page={page_num}"
            try:
                html = self._get_page(url)
            except Exception as e:
                logger.warning(f"  Page {page_num} failed: {e}")
                break

            links = self._extract_judgment_links(html)
            if not links:
                logger.info(f"  No more links on page {page_num}")
                break

            new_count = 0
            for link in links:
                if link["path"] in seen:
                    continue
                seen.add(link["path"])
                new_count += 1

                try:
                    jdg_html = self._get_page(link["url"])
                except Exception as e:
                    logger.warning(f"  Failed to fetch {link['path']}: {e}")
                    continue

                title = self._extract_title(jdg_html)
                text = self._extract_judgment_text(jdg_html)

                if not text:
                    logger.warning(f"  No text from {link['path']}")
                    continue

                parsed = self._parse_akn_path(link["path"])

                yield {
                    "path": link["path"],
                    "url": link["url"],
                    "title": title,
                    "full_text": text,
                    "date": parsed.get("date"),
                    "court": parsed.get("court", COURT_CODE).upper(),
                    "year": parsed.get("year"),
                    "number": parsed.get("number"),
                }

            logger.info(f"  Page {page_num}: {new_count} new decisions")
            if new_count == 0:
                break

        logger.info(f"  Total decisions: {len(seen)}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        path = raw.get("path", "")
        clean_path = path.replace("/akn/ke/", "").replace("/", "-").replace("@", "-")
        _id = f"KE-KENET-{clean_path}"

        return {
            "_id": _id,
            "_source": "KE/EnvironmentTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": html_mod.unescape(raw.get("title", "")),
            "text": raw.get("full_text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "court": raw.get("court", COURT_CODE),
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = EnvironmentTribunalScraper()

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
        print("Testing KENET connectivity...")
        try:
            html = scraper._get_page(f"{BASE_URL}/judgments/{COURT_CODE}/?page=1")
            links = scraper._extract_judgment_links(html)
            print(f"Page 1: {len(links)} judgment links")

            if links:
                jdg_html = scraper._get_page(links[0]["url"])
                title = scraper._extract_title(jdg_html)
                text = scraper._extract_judgment_text(jdg_html)
                print(f"First decision: {title}")
                print(f"Text length: {len(text)} chars")
                print(f"First 300 chars:\n{text[:300]}")

            print("\nTest passed!")
        except Exception as e:
            print(f"Test failed: {e}")
            import traceback
            traceback.print_exc()

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
