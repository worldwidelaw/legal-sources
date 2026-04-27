#!/usr/bin/env python3
"""
VU/VFSC-Enforcement -- Vanuatu Financial Services Commission Enforcement

Fetches enforcement actions (license revocations, fraud warnings, public
notices, liquidation notices) from the VFSC WordPress site.

Strategy:
  1. Fetch the public-notices page (ID 6609) via WP REST API
  2. Extract all PDF links from the HTML content
  3. Also query WP Media API for additional PDFs
  4. Download each PDF and extract text via common/pdf_extract

Endpoints:
  - Page: https://www.vfsc.vu/wp-json/wp/v2/pages/6609
  - Media: https://www.vfsc.vu/wp-json/wp/v2/media?media_type=application&per_page=100

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import json
import time
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Any
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VU.VFSC-Enforcement")

BASE_URL = "https://www.vfsc.vu"
PUBLIC_NOTICES_PAGE_ID = 6609

# Skip form/application PDFs and job-related content
SKIP_KEYWORDS = [
    "application form", "vacancy", "job ", "employment",
    "annual report", "strategic plan", "template",
]


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _classify_notice(title: str, url: str) -> Optional[str]:
    """Classify a notice by title/URL. Returns category or None to skip."""
    t = title.lower()
    u = url.lower()

    # Skip non-enforcement content
    if any(kw in t for kw in SKIP_KEYWORDS):
        return None

    if "revocation" in t or "revoke" in t:
        return "revocation"
    if "intention to revoke" in t:
        return "intention_to_revoke"
    if "warning" in t or "advisory" in t:
        return "warning"
    if "fraud" in t:
        return "fraud"
    if "liquidation" in t:
        return "liquidation"
    if "notice" in t or "public" in t:
        return "public_notice"
    if "removal" in t or "striking" in t or "struck" in t:
        return "removal"

    # Check URL for clues
    if "revoc" in u:
        return "revocation"
    if "warning" in u or "advisory" in u:
        return "warning"
    if "fraud" in u:
        return "fraud"
    if "liquidat" in u:
        return "liquidation"
    if "notice" in u:
        return "public_notice"

    # If it's a PDF on the public notices page, include it
    if u.endswith(".pdf"):
        return "public_notice"

    return None


class VUVFSCEnforcementScraper(BaseScraper):
    """Scraper for VU/VFSC-Enforcement -- VFSC Enforcement Actions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
                "Accept": "application/json, text/html",
            },
            timeout=60,
        )

    def _get_json(self, url: str, params: dict = None) -> Optional[Any]:
        """GET JSON with retry."""
        for attempt in range(3):
            try:
                self.rate_limiter.wait()
                resp = self.http.session.get(url, params=params, timeout=60)
                if resp.status_code == 400:
                    return None
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _extract_pdf_text(self, url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            text = extract_pdf_markdown(
                "VU/VFSC-Enforcement",
                doc_id,
                pdf_url=url,
                table="doctrine",
                force=True,
            )
            if text and len(text.strip()) > 50:
                return text.strip()
            return None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def _discover_pdfs_from_page(self) -> list[dict]:
        """Fetch public notices page and extract all PDF links."""
        url = f"{BASE_URL}/wp-json/wp/v2/pages/{PUBLIC_NOTICES_PAGE_ID}"
        data = self._get_json(url)
        if not data:
            logger.error("Failed to fetch public notices page")
            return []

        content_html = data.get("content", {}).get("rendered", "")
        if not content_html:
            logger.error("Public notices page has no content")
            return []

        # Extract all PDF links with anchor text
        pdf_links = re.findall(
            r'<a\s+[^>]*href="([^"]+\.pdf[^"]*)"[^>]*>(.*?)</a>',
            content_html, re.DOTALL | re.IGNORECASE,
        )

        results = []
        seen = set()
        for href, anchor_html in pdf_links:
            title = _strip_html(anchor_html).strip()
            if not title or len(title) < 3:
                continue

            full_url = urljoin(BASE_URL + "/", href) if not href.startswith("http") else href

            if full_url in seen:
                continue
            seen.add(full_url)

            category = _classify_notice(title, full_url)
            if category is None:
                continue

            # Extract slug from URL for ID
            slug = full_url.split("/")[-1].replace(".pdf", "")

            results.append({
                "id": f"page-{slug}",
                "title": title,
                "url": full_url,
                "category": category,
                "date": "",
            })

        logger.info(f"Found {len(results)} PDF links on public notices page")
        return results

    def _discover_pdfs_from_media_api(self) -> list[dict]:
        """Query WP Media API for PDFs."""
        results = []
        seen_urls = set()
        page = 1

        while True:
            url = f"{BASE_URL}/wp-json/wp/v2/media"
            data = self._get_json(url, params={
                "media_type": "application",
                "per_page": 100,
                "page": page,
            })
            if not data:
                break

            for item in data:
                source_url = item.get("source_url", "")
                if not source_url or not source_url.lower().endswith(".pdf"):
                    continue
                if source_url in seen_urls:
                    continue
                seen_urls.add(source_url)

                title = _strip_html(item.get("title", {}).get("rendered", ""))
                if not title:
                    title = source_url.split("/")[-1].replace(".pdf", "").replace("-", " ")

                category = _classify_notice(title, source_url)
                if category is None:
                    continue

                date_str = item.get("date", "")
                media_id = item.get("id", "")
                slug = source_url.split("/")[-1].replace(".pdf", "")

                results.append({
                    "id": f"media-{media_id}",
                    "title": title,
                    "url": source_url,
                    "category": category,
                    "date": date_str,
                })

            if len(data) < 100:
                break
            page += 1
            time.sleep(1)

        logger.info(f"Found {len(results)} PDFs from media API")
        return results

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all enforcement documents with full text."""
        limit = 15 if sample else None
        count = 0
        seen_urls = set()

        # Discover PDFs from both sources
        page_pdfs = self._discover_pdfs_from_page()
        media_pdfs = self._discover_pdfs_from_media_api()

        # Merge, preferring page items (they have better titles)
        all_items = []
        for item in page_pdfs:
            if item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                all_items.append(item)
        for item in media_pdfs:
            if item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                all_items.append(item)

        logger.info(f"Total unique PDFs to process: {len(all_items)}")

        for item in all_items:
            if limit and count >= limit:
                break

            title = item["title"]
            url = item["url"]
            logger.info(f"[{count+1}] Downloading: {title[:70]}")

            text = self._extract_pdf_text(url, item["id"])
            if not text:
                logger.warning(f"  No text extracted, skipping")
                continue

            count += 1
            yield {
                "id": item["id"],
                "title": title,
                "text": text,
                "date": item.get("date", ""),
                "url": url,
                "category": item.get("category", "public_notice"),
            }
            time.sleep(1.5)

        logger.info(f"Total: {count} documents with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents uploaded since the given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        page = 1
        while True:
            url = f"{BASE_URL}/wp-json/wp/v2/media"
            data = self._get_json(url, params={
                "media_type": "application",
                "per_page": 100,
                "after": since_iso,
                "orderby": "date",
                "order": "desc",
                "page": page,
            })
            if not data:
                break
            for item in data:
                source_url = item.get("source_url", "")
                if not source_url.lower().endswith(".pdf"):
                    continue
                title = _strip_html(item.get("title", {}).get("rendered", ""))
                category = _classify_notice(title, source_url)
                if category is None:
                    continue

                text = self._extract_pdf_text(source_url, str(item["id"]))
                if not text:
                    continue

                yield {
                    "id": f"media-{item['id']}",
                    "title": title,
                    "text": text,
                    "date": item.get("date", ""),
                    "url": source_url,
                    "category": category,
                }
                time.sleep(1.5)
            if len(data) < 100:
                break
            page += 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        date_str = raw.get("date", "")
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        return {
            "_id": raw.get("id", ""),
            "_source": "VU/VFSC-Enforcement",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = VUVFSCEnforcementScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to VFSC WP API...")
        url = f"{BASE_URL}/wp-json/wp/v2/pages/{PUBLIC_NOTICES_PAGE_ID}"
        data = scraper._get_json(url, params={"_fields": "id,title"})
        if data:
            title = data.get("title", {}).get("rendered", "")
            logger.info(f"OK — page: {title}")
            print("Test passed: WP REST API accessible")
        else:
            logger.error("Failed to reach WP REST API")
            sys.exit(1)

    elif command == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
