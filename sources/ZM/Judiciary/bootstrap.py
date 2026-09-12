#!/usr/bin/env python3
"""
ZM/Judiciary -- Zambia Judiciary Court Decisions Fetcher

Fetches court decisions from judiciaryzambia.com via WordPress REST API.

Strategy:
  - Use WP REST API to list all posts in decision categories
  - Extract PDF URLs from post content
  - Download PDFs and extract full text using pdfplumber
  - Normalize into standard schema

Categories:
  - 197: Constitutional Court Decisions (~224)
  - 198: Court of Appeal Decisions (~1,048)
  - 199: High Court Decisions (~1,596)
  - 189: General Decisions (~282)

Data:
  - ~3,150 court decisions with full text
  - Rate limit: 1 request/second
  - PDFs hosted at judiciaryzambia.com/wp-content/uploads/

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_mod
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZM.Judiciary")

API_BASE = "https://judiciaryzambia.com/wp-json/wp/v2"
SITE_BASE = "https://judiciaryzambia.com"

# Decision category IDs and their court mappings
DECISION_CATEGORIES = {
    197: "Constitutional Court",
    198: "Court of Appeal",
    199: "High Court",
    189: "Supreme Court",  # General "Decisions" category — mostly Supreme Court
}

# Posts per API page (WP max is 100)
PER_PAGE = 100


class ZambiaJudiciaryScraper(BaseScraper):
    """
    Scraper for ZM/Judiciary -- Zambia Judiciary Court Decisions.
    Country: ZM
    URL: https://judiciaryzambia.com

    Data types: case_law
    Auth: none (Public Access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=SITE_BASE,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/json",
            },
            timeout=120,
        )

    def _fetch_posts_for_category(self, cat_id: int, max_posts: int = 0) -> List[Dict]:
        """Fetch all posts for a given category via WP REST API."""
        posts = []
        page = 1

        while True:
            try:
                self.rate_limiter.wait()
                url = f"{API_BASE}/posts?categories={cat_id}&per_page={PER_PAGE}&page={page}&_fields=id,title,content,date,link,categories"
                resp = self.client.get(url)

                if resp.status_code == 400:
                    # No more pages
                    break

                resp.raise_for_status()
                data = resp.json()

                if not data:
                    break

                posts.extend(data)
                logger.info(f"Category {cat_id} page {page}: {len(data)} posts (total: {len(posts)})")

                if max_posts and len(posts) >= max_posts:
                    posts = posts[:max_posts]
                    break

                # Check if there are more pages
                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                if page >= total_pages:
                    break

                page += 1

            except Exception as e:
                logger.warning(f"Failed to fetch category {cat_id} page {page}: {e}")
                break

        return posts

    def _extract_pdf_url(self, content_html: str) -> Optional[str]:
        """Extract PDF URL from post content HTML."""
        # Look for direct PDF links
        pdf_match = re.search(
            r'href="(https?://[^"]*\.pdf[^"]*)"',
            content_html, re.IGNORECASE
        )
        if pdf_match:
            url = pdf_match.group(1)
            # Clean up HTML entities
            url = html_mod.unescape(url)
            return url

        # Look for wp-content/uploads links
        upload_match = re.search(
            r'href="(/wp-content/uploads/[^"]*\.pdf[^"]*)"',
            content_html, re.IGNORECASE
        )
        if upload_match:
            return f"{SITE_BASE}{upload_match.group(1)}"

        return None

    def _determine_court(self, categories: List[int]) -> str:
        """Determine the court name from category IDs."""
        for cat_id in categories:
            if cat_id in DECISION_CATEGORIES:
                return DECISION_CATEGORIES[cat_id]
        return "Unknown"

    def _parse_case_info(self, title: str) -> Dict[str, str]:
        """Parse case number and parties from the post title."""
        title = html_mod.unescape(title)

        # Try to extract case number (patterns like: APP-03-2025, 2021-HP-1149, etc.)
        case_num_match = re.search(
            r'((?:APP|HPA|HP|CCZ|SCZ|CAZ)\s*[-/]?\s*\d+[-/]\d+|'
            r'\d{4}[-/](?:HP|HPA|HPF|CCZ|SCZ|CAZ)[-/]\d+)',
            title, re.IGNORECASE
        )
        case_number = case_num_match.group(1).strip() if case_num_match else ""

        # Try to extract parties (pattern: X Vs/v Y)
        parties_match = re.search(r'(.+?)\s+(?:Vs?\.?|vs\.?|versus)\s+(.+?)(?:\s*[-–]\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))', title, re.IGNORECASE)
        if not parties_match:
            parties_match = re.search(r'(.+?)\s+(?:Vs?\.?|vs\.?|versus)\s+(.+?)(?:\s*[-–]\s*\d)', title, re.IGNORECASE)

        plaintiff = parties_match.group(1).strip() if parties_match else ""
        defendant = parties_match.group(2).strip() if parties_match else ""

        # Clean case number from plaintiff
        if case_number and plaintiff:
            plaintiff = plaintiff.replace(case_number, "").strip(" -–")

        return {
            "case_number": case_number,
            "plaintiff": plaintiff,
            "defendant": defendant,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw WP post into the standard schema."""
        wp_id = raw.get("wp_id", "")
        title = html_mod.unescape(raw.get("title", ""))
        court = raw.get("court", "Unknown")
        case_info = self._parse_case_info(title)

        # Parse date
        date_str = raw.get("date", "")
        if date_str:
            try:
                date_str = date_str[:10]  # YYYY-MM-DD
            except (IndexError, TypeError):
                date_str = ""

        return {
            "_id": f"ZM-JUD-{wp_id}",
            "_source": "ZM/Judiciary",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("link", ""),
            "court": court,
            "case_number": case_info["case_number"],
            "plaintiff": case_info["plaintiff"],
            "defendant": case_info["defendant"],
            "jurisdiction": "ZM",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all court decisions across all categories."""
        seen_ids = set()

        for cat_id, court_name in DECISION_CATEGORIES.items():
            logger.info(f"Fetching {court_name} decisions (category {cat_id})...")
            posts = self._fetch_posts_for_category(cat_id)

            for post in posts:
                wp_id = post["id"]
                if wp_id in seen_ids:
                    continue
                seen_ids.add(wp_id)

                title = post["title"]["rendered"]
                content = post["content"]["rendered"]
                pdf_url = self._extract_pdf_url(content)

                if not pdf_url:
                    logger.debug(f"No PDF found for post {wp_id}: {title[:60]}")
                    continue

                # Download and extract PDF text
                text = extract_pdf_markdown(
                    source="ZM/Judiciary",
                    source_id=f"ZM-JUD-{wp_id}",
                    pdf_url=pdf_url,
                    table="case_law",
                )

                if not text or len(text.strip()) < 50:
                    logger.warning(f"Insufficient text from PDF for post {wp_id}")
                    continue

                yield self.normalize({
                    "wp_id": wp_id,
                    "title": title,
                    "text": text.strip(),
                    "date": post.get("date", ""),
                    "link": post.get("link", ""),
                    "court": court_name,
                })

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions modified since a given date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        seen_ids = set()

        for cat_id, court_name in DECISION_CATEGORIES.items():
            page = 1
            while True:
                try:
                    self.rate_limiter.wait()
                    url = (
                        f"{API_BASE}/posts?categories={cat_id}&per_page={PER_PAGE}"
                        f"&page={page}&after={since}T00:00:00"
                        f"&_fields=id,title,content,date,link,categories"
                    )
                    resp = self.client.get(url)
                    if resp.status_code == 400:
                        break
                    resp.raise_for_status()
                    data = resp.json()
                    if not data:
                        break

                    for post in data:
                        wp_id = post["id"]
                        if wp_id in seen_ids:
                            continue
                        seen_ids.add(wp_id)

                        content = post["content"]["rendered"]
                        pdf_url = self._extract_pdf_url(content)
                        if not pdf_url:
                            continue

                        text = extract_pdf_markdown(
                            source="ZM/Judiciary",
                            source_id=f"ZM-JUD-{wp_id}",
                            pdf_url=pdf_url,
                            table="case_law",
                        )
                        if not text or len(text.strip()) < 50:
                            continue

                        yield self.normalize({
                            "wp_id": wp_id,
                            "title": post["title"]["rendered"],
                            "text": text.strip(),
                            "date": post.get("date", ""),
                            "link": post.get("link", ""),
                            "court": court_name,
                        })

                    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                    if page >= total_pages:
                        break
                    page += 1

                except Exception as e:
                    logger.warning(f"Update fetch failed for cat {cat_id} page {page}: {e}")
                    break

    def sample(self, n: int = 15) -> List[Dict[str, Any]]:
        """Fetch a small sample of records for testing."""
        samples = []
        seen_ids = set()

        # Get ~5 from each major category
        per_cat = max(4, n // len(DECISION_CATEGORIES) + 1)

        for cat_id, court_name in DECISION_CATEGORIES.items():
            posts = self._fetch_posts_for_category(cat_id, max_posts=per_cat)

            for post in posts:
                if len(samples) >= n:
                    return samples

                wp_id = post["id"]
                if wp_id in seen_ids:
                    continue
                seen_ids.add(wp_id)

                title = post["title"]["rendered"]
                content = post["content"]["rendered"]
                pdf_url = self._extract_pdf_url(content)

                if not pdf_url:
                    logger.debug(f"No PDF for post {wp_id}: {title[:60]}")
                    continue

                logger.info(f"Downloading PDF for: {html_mod.unescape(title)[:70]}...")

                text = extract_pdf_markdown(
                    source="ZM/Judiciary",
                    source_id=f"ZM-JUD-{wp_id}",
                    pdf_url=pdf_url,
                    table="case_law",
                )

                if not text or len(text.strip()) < 50:
                    logger.warning(f"Insufficient text for post {wp_id}")
                    continue

                record = self.normalize({
                    "wp_id": wp_id,
                    "title": title,
                    "text": text.strip(),
                    "date": post.get("date", ""),
                    "link": post.get("link", ""),
                    "court": court_name,
                })
                samples.append(record)
                logger.info(f"  Sample {len(samples)}/{n}: {len(text)} chars from {court_name}")

        return samples


# ── CLI entry point ──────────────────────────────────────────────────
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ZambiaJudiciaryScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to judiciaryzambia.com WP REST API...")
        try:
            resp = scraper.client.get(f"{API_BASE}/posts?per_page=1")
            resp.raise_for_status()
            data = resp.json()
            total = resp.headers.get("X-WP-Total", "?")
            logger.info(f"OK — {total} total posts, first: {data[0]['title']['rendered'][:60]}")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample_mode:
            records = scraper.sample(n=15)
            for rec in records:
                fname = sample_dir / f"{rec['_id']}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(rec, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(records)} sample records to {sample_dir}")
        else:
            count = 0
            for record in scraper.fetch_all():
                count += 1
                if count % 50 == 0:
                    logger.info(f"Processed {count} records...")
            logger.info(f"Bootstrap complete: {count} records")

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else "2024-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
        logger.info(f"Update complete: {count} new/modified records since {since}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
