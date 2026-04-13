#!/usr/bin/env python3
"""
JP/FSA -- Japan Financial Services Agency Doctrine Fetcher

Fetches SESC enforcement actions and FSA supervisory guidelines.

Content:
  1. SESC enforcement actions: ~108 HTML pages (1998-2026)
     Index: https://www.fsa.go.jp/sesc/english/news/reco.html
     Pages: /sesc/english/news/reco/YYYYMMDD-N.html (or .htm)
  2. FSA supervisory guidelines: ~11 PDF documents
     Index: https://www.fsa.go.jp/en/laws_regulations/index.html
     PDFs: /common/law/guide/*.pdf and others

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
logger = logging.getLogger("legal-data-hunter.JP.FSA")

BASE_URL = "https://www.fsa.go.jp"

# SESC enforcement action index
SESC_INDEX_URL = "/sesc/english/news/reco.html"

# Supervisory guideline PDFs (path, title)
GUIDELINE_PDFS = [
    ("/common/law/guide/en_city.pdf", "Comprehensive Guidelines for Supervision of Major Banks"),
    ("/common/law/guide/en_ins.pdf", "Comprehensive Guidelines for Supervision of Insurance Companies"),
    ("/common/law/guide/kinyushohin_eng.pdf", "Comprehensive Guidelines for Supervision of Financial Instruments Business Operators"),
    ("/common/law/guide/kakuduke/rating.pdf", "Guidelines for Supervision of Credit Rating Agencies"),
    ("/common/law/guide/hft/hft_eng_202510.pdf", "Guidelines for Supervision of High Speed Traders"),
    ("/common/law/guide/im-rs/im-rs_eng.pdf", "Comprehensive Guidelines for Investment Management-Related Service Entrusted Business Operators"),
    ("/en/laws_regulations/eng_kinsa.pdf", "Comprehensive Guidelines for Supervision of Financial Service Intermediaries"),
    ("/common/law/guide/seisan/seisan_english.pdf", "Comprehensive Guidelines for Supervision of Financial Market Infrastructures"),
    ("/common/law/guide/kaisya/e05.pdf", "Guideline for Supervision of Issuers of Prepaid Payment Instruments"),
    ("/common/law/guide/kaisya/e014.pdf", "Guideline for Supervision of Funds Transfer Service Providers"),
    ("/common/law/guide/kaisya/e016.pdf", "Guideline for Supervision of Crypto-Asset Exchange Service Providers"),
]


def clean_html_text(raw_html: str) -> str:
    """Strip HTML tags and clean up text."""
    # Remove script/style blocks
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode HTML entities
    text = html.unescape(text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


class JapanFSAScraper(BaseScraper):
    """
    Scraper for JP/FSA -- Japan Financial Services Agency.
    Country: JP
    URL: https://www.fsa.go.jp/sesc/english/news/reco.html

    Data types: doctrine
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

    def _get_sesc_action_urls(self) -> List[Dict]:
        """Parse the SESC index page for all enforcement action links."""
        actions = []

        try:
            self.rate_limiter.wait()
            resp = self.client.get(SESC_INDEX_URL)
            resp.raise_for_status()
            content = resp.text

            # Links: href="/sesc/english/news/reco/YYYYMMDD-N.html" or .htm
            link_pattern = re.compile(
                r'href="(/sesc/english/news/reco/(\d{8})(?:[_-]\d+)?\.html?)"',
                re.IGNORECASE
            )

            for match in link_pattern.finditer(content):
                full_path = match.group(1)
                date_str = match.group(2)

                # Extract title: text inside the <a> tag
                title = ""
                # Find the closing > after the href, then text until </a>
                after_match = content[match.end():]
                # Skip to the > that closes the <a> tag
                close_bracket = after_match.find('>')
                if close_bracket >= 0:
                    text_start = close_bracket + 1
                    end_a = after_match.find('</a>', text_start)
                    if end_a > text_start:
                        raw_title = after_match[text_start:end_a]
                        title = html.unescape(re.sub(r'<[^>]+>', '', raw_title).strip())

                # Parse date
                try:
                    date_obj = datetime.strptime(date_str, "%Y%m%d")
                    iso_date = date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    iso_date = ""

                actions.append({
                    "path": full_path,
                    "date": iso_date,
                    "title": title,
                    "doc_type": "enforcement_action",
                })

            logger.info(f"Found {len(actions)} SESC enforcement actions")
            return actions

        except Exception as e:
            logger.error(f"Failed to get SESC index: {e}")
            return []

    def _fetch_sesc_action(self, action: Dict) -> Dict:
        """Fetch full text of a single SESC enforcement action."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(action["path"])
            resp.raise_for_status()
            raw_html = resp.text

            text = clean_html_text(raw_html)

            # Extract title from page if we don't have one
            title = action.get("title", "")
            if not title:
                title_match = re.search(r'<title>([^<]+)</title>', raw_html, re.IGNORECASE)
                if title_match:
                    title = html.unescape(title_match.group(1).strip())

            action["full_text"] = text
            if title:
                action["title"] = title

            return action

        except Exception as e:
            logger.warning(f"Failed to fetch {action['path']}: {e}")
            return action

    def _fetch_guideline_pdf(self, path: str, title: str) -> Dict:
        """Download and extract text from a supervisory guideline PDF."""
        if not HAS_PDFPLUMBER:
            logger.warning("pdfplumber not available, skipping PDF")
            return {}

        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()

            ct = resp.headers.get("Content-Type", "")
            if "html" in ct.lower():
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
                    "doc_type": "supervisory_guideline",
                    "date": "",
                }
            finally:
                os.unlink(tmp_path)

        except Exception as e:
            logger.warning(f"Failed to extract PDF {path}: {e}")
            return {}

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all SESC enforcement actions and supervisory guidelines."""
        # 1. SESC enforcement actions
        actions = self._get_sesc_action_urls()
        for action in actions:
            result = self._fetch_sesc_action(action)
            if result.get("full_text"):
                yield result

        # 2. Supervisory guideline PDFs
        if HAS_PDFPLUMBER:
            for path, title in GUIDELINE_PDFS:
                result = self._fetch_guideline_pdf(path, title)
                if result.get("full_text"):
                    yield result
        else:
            logger.warning("Skipping guideline PDFs (pdfplumber not installed)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch only enforcement actions since a given date."""
        actions = self._get_sesc_action_urls()
        for action in actions:
            if action.get("date") and action["date"] >= since.strftime("%Y-%m-%d"):
                result = self._fetch_sesc_action(action)
                if result.get("full_text"):
                    yield result

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        doc_type = raw.get("doc_type", "enforcement_action")
        path = raw.get("path", "")
        title = raw.get("title", "")
        text = raw.get("full_text", "")
        date = raw.get("date", "")

        # Build document ID
        if doc_type == "enforcement_action":
            # Use filename as ID: reco/20260331-1.html -> SESC-20260331-1
            filename = path.split("/")[-1].replace(".html", "").replace(".htm", "")
            doc_id = f"JP-FSA-SESC-{filename}"
        else:
            # Guideline: use sanitized path
            slug = path.split("/")[-1].replace(".pdf", "").replace(" ", "_")
            doc_id = f"JP-FSA-GL-{slug}"

        full_url = f"{BASE_URL}{path}" if path.startswith("/") else path

        return {
            "_id": doc_id,
            "_source": "JP/FSA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": full_url,
            "doc_type": doc_type,
            "jurisdiction": "JP",
            "language": "en",
            "authority": "Financial Services Agency / SESC",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Japan FSA endpoints...")
        print(f"pdfplumber available: {HAS_PDFPLUMBER}")

        print("\n1. Testing SESC index...")
        try:
            actions = self._get_sesc_action_urls()
            print(f"   Found {len(actions)} enforcement actions")
            if actions:
                print(f"   Latest: {actions[0].get('title', 'N/A')} ({actions[0].get('date', 'N/A')})")
        except Exception as e:
            print(f"   ERROR: {e}")

        if actions:
            print("\n2. Testing SESC action fetch...")
            try:
                result = self._fetch_sesc_action(actions[0])
                text = result.get("full_text", "")
                print(f"   Extracted {len(text)} characters")
                if text:
                    print(f"   Preview: {text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")

        if HAS_PDFPLUMBER:
            print("\n3. Testing guideline PDF...")
            try:
                path, title = GUIDELINE_PDFS[4]  # Use smallest PDF (163KB)
                result = self._fetch_guideline_pdf(path, title)
                text = result.get("full_text", "")
                print(f"   Extracted {len(text)} chars from {title}")
                if text:
                    print(f"   Preview: {text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = JapanFSAScraper()

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
