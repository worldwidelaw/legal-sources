#!/usr/bin/env python3
"""
MN/ConstitutionalCourt -- Mongolia Constitutional Court (Tsets) — Decisions

Fetches decisions from the Constitutional Court of Mongolia via its WordPress
REST API. Two complementary strategies:

  1. WP Posts (categories 6+7): court session reports with decision summaries
     in HTML content (тойм = digest/summary of each decision)
  2. WP Pages with PDF embeds: full decision texts extracted via pdfminer

Decision types:
  - Магадлал (conclusions)
  - Дүгнэлт (opinions)
  - Тогтоол (resolutions)

License: Public Domain (Government decisions)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import html as html_mod
import tempfile
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

try:
    from pdfminer.high_level import extract_text as pdfminer_extract
except ImportError:
    pdfminer_extract = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MN.ConstitutionalCourt")

BASE_URL = "https://constitutionalcourt.mn"
SOURCE_ID = "MN/ConstitutionalCourt"

# Decision categories on the Mongolian WP site
DECISION_CATEGORIES = [6, 7]  # 6=plenary sessions, 7=standing committee sessions

# Keywords identifying decision content in post titles
DECISION_TITLE_KEYWORDS = [
    "дүгнэлт",     # opinion
    "тогтоол",     # resolution
    "магадлал",    # conclusion/determination
    "тойм",        # digest/summary
    "шийдвэр",    # decision
]


class MNConstitutionalCourtScraper(BaseScraper):
    SOURCE_ID = SOURCE_ID

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            verify=True,
        )

    def _api_get(self, url: str, max_retries: int = 3) -> Optional[Any]:
        """GET with retry and backoff for flaky WP server."""
        for attempt in range(max_retries):
            try:
                resp = self.http.get(url, timeout=30)
                if resp.status_code == 200:
                    return resp
                if resp.status_code == 400:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}, retry {attempt+1}")
            except Exception as e:
                logger.warning(f"Request failed ({e}), retry {attempt+1}/{max_retries}")
            time.sleep(2 * (attempt + 1))
        return None

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._api_get(f"{BASE_URL}/wp-json/wp/v2/posts?per_page=1")
        if resp and resp.status_code == 200:
            logger.info("Test OK")
            return True
        logger.error("Test failed")
        return False

    # ── WP Posts approach (HTML content) ──────────────────────────────

    def _fetch_decision_posts(self) -> List[Dict[str, Any]]:
        """Fetch all posts from decision categories via WP REST API."""
        all_posts = []
        page_num = 1
        cats = ",".join(str(c) for c in DECISION_CATEGORIES)
        while True:
            url = (
                f"{BASE_URL}/wp-json/wp/v2/posts"
                f"?categories={cats}&per_page=100&page={page_num}"
            )
            resp = self._api_get(url)
            if not resp:
                break
            data = resp.json()
            if not data:
                break
            all_posts.extend(data)
            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(1)
        logger.info(f"Fetched {len(all_posts)} posts from decision categories")
        return all_posts

    def _has_decision_content(self, post: Dict[str, Any]) -> bool:
        """Check if a post contains actual decision content (not just a schedule notice)."""
        title = html_mod.unescape(
            re.sub("<[^>]+>", "", post.get("title", {}).get("rendered", ""))
        ).lower()

        # Must contain a decision keyword
        if not any(kw in title for kw in DECISION_TITLE_KEYWORDS):
            return False

        # Check minimum content length (strip HTML)
        content = self._clean_html(post.get("content", {}).get("rendered", ""))
        if len(content) < 500:
            return False

        return True

    def _clean_html(self, html_content: str) -> str:
        """Strip HTML tags, Elementor CSS, and clean whitespace."""
        # Remove Elementor inline CSS blocks
        text = re.sub(r"/\*!.*?\*/", "", html_content, flags=re.DOTALL)
        # Remove style tags
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
        # Remove HTML tags
        text = re.sub(r"<[^>]+>", " ", text)
        # Decode entities
        text = html_mod.unescape(text)
        # Collapse whitespace but keep paragraph breaks
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s*\n", "\n\n", text)
        text = text.strip()
        return text

    def _extract_year_and_number(self, title: str) -> tuple:
        """Extract decision year and number from title text."""
        # Pattern: "2025 оны 08 дугаар дүгнэлт" or "2025, №08" or "№08"
        year_match = re.search(r"(\d{4})\s*он", title)
        year = year_match.group(1) if year_match else None

        num_match = re.search(r"№\s*(\d+)", title)
        if not num_match:
            num_match = re.search(r"(\d+)\s*д[уү]г[аэ]+р", title)
        number = num_match.group(1) if num_match else None

        return year, number

    def _classify_decision(self, title: str) -> str:
        """Classify decision type from title."""
        tl = title.lower()
        if "магадлал" in tl:
            return "магадлал"
        if "дүгнэлт" in tl:
            return "дүгнэлт"
        if "тогтоол" in tl:
            return "тогтоол"
        if "тойм" in tl:
            return "тойм"
        return "шийдвэр"

    # ── WP Pages approach (PDF extraction) ────────────────────────────

    def _fetch_decision_pages(self) -> List[Dict[str, Any]]:
        """Fetch WordPress pages that contain embedded PDF decisions."""
        all_pages = []
        page_num = 1
        while True:
            url = f"{BASE_URL}/wp-json/wp/v2/pages?per_page=100&page={page_num}"
            resp = self._api_get(url)
            if not resp:
                break
            data = resp.json()
            if not data:
                break
            all_pages.extend(data)
            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(1.5)
        logger.info(f"Fetched {len(all_pages)} total WordPress pages")

        # Filter for pages with PDF embeds + decision keywords in title
        decision_pages = []
        for pg in all_pages:
            content = pg.get("content", {}).get("rendered", "")
            title = html_mod.unescape(
                re.sub("<[^>]+>", "", pg.get("title", {}).get("rendered", ""))
            ).lower()
            if ".pdf" in content and any(kw in title for kw in DECISION_TITLE_KEYWORDS):
                decision_pages.append(pg)

        logger.info(f"Found {len(decision_pages)} decision pages with PDF embeds")
        return decision_pages

    def _extract_pdf_url(self, content: str) -> Optional[str]:
        """Extract PDF URL from DFLIP viewer embed or direct link."""
        # DFLIP pattern
        match = re.search(r'"source"\s*:\s*"([^"]+\.pdf)"', content)
        if match:
            url = match.group(1).replace("\\/", "/")
            try:
                url = url.encode().decode("unicode_escape")
            except (UnicodeDecodeError, UnicodeError):
                pass
            return url

        # Direct href
        match = re.search(r'href="([^"]+\.pdf)"', content)
        if match:
            return match.group(1)

        return None

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text via pdfminer."""
        if pdfminer_extract is None:
            return None
        try:
            resp = self._api_get(pdf_url)
            if not resp or resp.status_code != 200:
                return None
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
                tmp.write(resp.content)
                tmp.flush()
                text = pdfminer_extract(tmp.name)
            if text:
                text = re.sub(r"\n{3,}", "\n\n", text).strip()
            return text if text and len(text) > 100 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    # ── Normalization ─────────────────────────────────────────────────

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        title = html_mod.unescape(re.sub("<[^>]+>", "", raw.get("title", "")))
        return {
            "_id": raw["_id"],
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "decision_type": raw.get("decision_type", ""),
            "decision_number": raw.get("decision_number"),
            "decision_year": raw.get("decision_year"),
            "source_method": raw.get("source_method", ""),
            "language": "mn",
        }

    # ── Main fetch logic ──────────────────────────────────────────────

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions from both posts and pages."""
        seen_ids = set()
        count = 0
        limit = 15 if sample else 9999

        # ── Phase 1: WP Posts (HTML content from decision categories) ──
        logger.info("Phase 1: Fetching decision posts (HTML content)...")
        posts = self._fetch_decision_posts()
        decision_posts = [p for p in posts if self._has_decision_content(p)]
        logger.info(f"Found {len(decision_posts)} posts with decision content")

        for post in decision_posts:
            if count >= limit:
                break

            post_id = post["id"]
            title_raw = post["title"]["rendered"]
            title = html_mod.unescape(re.sub("<[^>]+>", "", title_raw)).strip()
            text = self._clean_html(post["content"]["rendered"])
            wp_date = post.get("date", "")[:10]
            dec_type = self._classify_decision(title)
            dec_year, dec_num = self._extract_year_and_number(title)

            record_id = f"mn-cc-post-{post_id}"
            if record_id in seen_ids:
                continue
            seen_ids.add(record_id)

            raw = {
                "_id": record_id,
                "title": title_raw,
                "text": text,
                "date": wp_date,
                "url": post.get("link", ""),
                "decision_type": dec_type,
                "decision_number": dec_num,
                "decision_year": dec_year,
                "source_method": "wp_post_html",
            }
            yield self.normalize(raw)
            count += 1

        logger.info(f"Phase 1 complete: {count} records from posts")

        # ── Phase 2: WP Pages (PDF extraction) ──
        if count < limit and pdfminer_extract is not None:
            logger.info("Phase 2: Fetching decision pages (PDF extraction)...")
            pages = self._fetch_decision_pages()

            for pg in pages:
                if count >= limit:
                    break

                pg_id = pg["id"]
                title_raw = pg["title"]["rendered"]
                title = html_mod.unescape(re.sub("<[^>]+>", "", title_raw)).strip()
                content = pg["content"]["rendered"]

                pdf_url = self._extract_pdf_url(content)
                if not pdf_url:
                    continue

                logger.info(f"Extracting PDF: {title[:60]}...")
                text = self._extract_text_from_pdf(pdf_url)
                if not text:
                    continue

                wp_date = pg.get("date", "")[:10]
                dec_type = self._classify_decision(title)
                dec_year, dec_num = self._extract_year_and_number(title)

                record_id = f"mn-cc-page-{pg_id}"
                if record_id in seen_ids:
                    continue
                seen_ids.add(record_id)

                raw = {
                    "_id": record_id,
                    "title": title_raw,
                    "text": text,
                    "date": wp_date,
                    "url": pg.get("link", ""),
                    "decision_type": dec_type,
                    "decision_number": dec_num,
                    "decision_year": dec_year,
                    "source_method": "wp_page_pdf",
                    "pdf_url": pdf_url,
                }
                yield self.normalize(raw)
                count += 1

            logger.info(f"Phase 2 complete: total {count} records")
        elif pdfminer_extract is None:
            logger.info("Phase 2 skipped: pdfminer not available")

        logger.info(f"Fetch complete: {count} total decisions")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions modified since a given date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        since_dt = datetime.fromisoformat(since)
        for record in self.fetch_all(sample=False):
            rec_date = record.get("date", "")
            if rec_date:
                try:
                    if datetime.fromisoformat(rec_date) >= since_dt:
                        yield record
                except ValueError:
                    yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MN/ConstitutionalCourt scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (10-15 records)")
    parser.add_argument("--since", type=str, help="ISO date for incremental update")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    scraper = MNConstitutionalCourtScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            if record.get("text"):
                count += 1
                out_file = sample_dir / f"{count:03d}.json"
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(
                    f"[{count}] {record['title'][:60]} — "
                    f"{len(record['text'])} chars"
                )
        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        since = args.since or "2025-01-01T00:00:00"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info(f"[{count}] Updated: {record['title'][:60]}")
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
