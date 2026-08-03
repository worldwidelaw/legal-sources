#!/usr/bin/env python3
"""
IE/PressOmbudsman -- Office of the Press Ombudsman & Press Council of Ireland —
Published Decisions.

The Office of the Press Ombudsman and the Press Council of Ireland operate the
independent press-regulation system in Ireland. Members of the public may
complain that a member publication (newspaper, magazine or online news outlet)
has breached the industry Code of Practice. The Press Ombudsman decides each
complaint; a party dissatisfied with an Ombudsman decision may appeal to the
Press Council of Ireland, and some complaints raising significant issues are
referred directly to the Council. Each decision — the Ombudsman's determination,
a Press Council appeal decision, or a decision on referral — is published in full
and sets out the complaint, the publication's response, the Principle(s) of the
Code of Practice engaged, the reasoning and the outcome (upheld / not upheld /
sufficient remedial action / resolved). These quasi-judicial adjudications on the
application of the Code of Practice = case_law.

Strategy:
  - pressombudsman.ie is a WordPress site whose decisions are ordinary posts
    filed under three categories:
        37  "decisions"                         (Press Ombudsman decisions)
        32  "appeals-to-the-press-council"      (Press Council appeal decisions)
        33  "decisions-on-referral-to-the-press-council"
  - The public WP REST API (/wp-json/wp/v2/posts) returns each decision's full
    body in content.rendered (born-digital HTML — no OCR/PDF needed). We page
    through each category (per_page=100) and clean the HTML to plain text.

Data:
  - ~825 published decisions (577 Ombudsman decisions + 231 Council appeals +
    17 referral decisions), 2008-present.
  - Language: English
  - Auth: None (free public access via WP REST API)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (WP `after` filter)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.PressOmbudsman")

BASE_URL = "https://pressombudsman.ie"
POSTS_PATH = "/wp-json/wp/v2/posts"

# Decision categories on pressombudsman.ie (verified via /wp/v2/categories).
CATEGORIES = {
    37: "Press Ombudsman decision",
    32: "Press Council appeal decision",
    33: "Press Council decision on referral",
}
PER_PAGE = 100
MAX_PAGES = 60  # safety ceiling; real corpus is ~6 pages/category


def _strip_html(fragment: str) -> str:
    """Strip tags and decode entities from an HTML fragment -> clean text."""
    # Turn block-level boundaries into newlines so paragraphs are preserved.
    text = re.sub(r"(?i)</(p|div|h[1-6]|li|br)\s*>", "\n", fragment)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class PressOmbudsmanScraper(BaseScraper):
    """Scraper for Office of the Press Ombudsman / Press Council of Ireland decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-IE,en;q=0.9",
            },
            timeout=90,
        )

    def _get_page(self, category: int, page: int,
                  after: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        params = {
            "categories": str(category),
            "per_page": str(PER_PAGE),
            "page": str(page),
            "orderby": "date",
            "order": "desc",
            "_fields": "id,date,modified,slug,link,title,content,categories",
        }
        if after:
            params["after"] = after
        query = "&".join(f"{k}={v}" for k, v in params.items())
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"{POSTS_PATH}?{query}")
            if resp.status_code == 400:
                # WP returns 400 ("rest_post_invalid_page_number") past last page.
                return []
            if resp.status_code != 200:
                logger.warning(f"cat {category} page {page}: HTTP {resp.status_code}")
                return None
            return resp.json()
        except Exception as e:
            logger.warning(f"Error cat {category} page {page}: {e}")
            return None

    def _iter_category(self, category: int,
                       after: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        for page in range(1, MAX_PAGES + 1):
            batch = self._get_page(category, page, after=after)
            if batch is None:
                break
            if not batch:
                break
            for post in batch:
                post["_category_label"] = CATEGORIES.get(category, "decision")
                yield post
            logger.info(f"cat {category} ({CATEGORIES[category]}) page {page}: {len(batch)} posts")
            if len(batch) < PER_PAGE:
                break

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        any_post = False
        seen = set()
        for category in CATEGORIES:
            for post in self._iter_category(category):
                pid = post.get("id")
                if pid in seen:
                    continue
                seen.add(pid)
                any_post = True
                yield post
        if not any_post:
            raise RuntimeError(
                "Press Ombudsman WP REST API returned 0 decisions — "
                "site blocked or API changed"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        after = since.strftime("%Y-%m-%dT%H:%M:%S")
        seen = set()
        for category in CATEGORIES:
            for post in self._iter_category(category, after=after):
                pid = post.get("id")
                if pid in seen:
                    continue
                seen.add(pid)
                yield post

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pid = raw.get("id")
        if not pid:
            return None

        title = _strip_html(raw.get("title", {}).get("rendered", ""))
        content_html = raw.get("content", {}).get("rendered", "")
        text = _strip_html(content_html)
        if len(text) < 120:
            # Too short to be a real decision body.
            return None

        # WP date is 'YYYY-MM-DDTHH:MM:SS' local time -> ISO date.
        raw_date = raw.get("date") or ""
        iso_date = raw_date[:10] if re.match(r"\d{4}-\d{2}-\d{2}", raw_date) else None

        url = raw.get("link") or f"{BASE_URL}/?p={pid}"
        label = raw.get("_category_label", "decision")

        # Reference like "OMB 2532/2026" often appears in the title.
        m = re.search(r"OMB\.?[\s\-]*(\d+)[/\-](\d{4})", title, re.IGNORECASE)
        case_reference = f"OMB {m.group(1)}/{m.group(2)}" if m else None

        record = {
            "_id": f"IE-PressOmbudsman-{pid}",
            "_source": "IE/PressOmbudsman",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date,
            "url": url,
            "case_reference": case_reference,
            "decision_type": label,
            "court": "Office of the Press Ombudsman / Press Council of Ireland",
            "jurisdiction": "IE",
            "language": "en",
        }
        return record

    def test_connection(self):
        print("Testing Press Ombudsman WP REST API...")
        batch = self._get_page(37, 1)
        if not batch:
            print("  cat 37 page 1 FETCH FAILED / empty")
            return
        print(f"  cat 37 page 1: {len(batch)} posts")
        post = batch[0]
        post["_category_label"] = CATEGORIES[37]
        rec = self.normalize(post)
        if rec:
            print(f"    title: {rec['title']}")
            print(f"    date:  {rec['date']}")
            print(f"    ref:   {rec['case_reference']}")
            print(f"    text:  {len(rec['text'])} chars")
            print(f"    head:  {rec['text'][:160]!r}")


def main():
    scraper = PressOmbudsmanScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
