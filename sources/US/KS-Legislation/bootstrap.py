#!/usr/bin/env python3
"""
US/KS-Legislation -- Kansas Statutes Annotated

Fetches Kansas Statutes with full text from kslegislature.gov.

Strategy:
  1. Fetch chapter list from /li/b2025_26/statute/
  2. For each chapter, fetch article list
  3. For each article, fetch section links
  4. For each section, fetch full text from the section page
  5. Normalize into standard schema

Data: Public domain (Kansas government works). No auth required.
Rate limit: 1 req / 2 sec.

Usage:
  python bootstrap.py bootstrap            # Full pull (all chapters)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.KS-Legislation")

BASE_URL = "https://www.kslegislature.gov/li/b2025_26/statute"

# Sample sections for quick testing (chapter_slug, article_slug, section_slug, section_k, display_name)
SAMPLE_SECTIONS = [
    ("021_000_0000_chapter", "021_054_0000_article", "021_054_0002_section", "021_054_0002_k", "21-5402"),
    ("021_000_0000_chapter", "021_054_0000_article", "021_054_0001_section", "021_054_0001_k", "21-5401"),
    ("021_000_0000_chapter", "021_055_0000_article", "021_055_0001_section", "021_055_0001_k", "21-5501"),
    ("060_000_0000_chapter", "060_001_0000_article", "060_001_0001_section", "060_001_0001_k", "60-101"),
    ("060_000_0000_chapter", "060_002_0000_article", "060_002_0001_section", "060_002_0001_k", "60-201"),
    ("079_000_0000_chapter", "079_032_0000_article", "079_032_0099_section", "079_032_0099_k", "79-3299"),
    ("008_000_0000_chapter", "008_015_0000_article", "008_015_0001_section", "008_015_0001_k", "8-1501"),
    ("022_000_0000_chapter", "022_023_0000_article", "022_023_0001_section", "022_023_0001_k", "22-2301"),
    ("044_000_0000_chapter", "044_005_0000_article", "044_005_0001_section", "044_005_0001_k", "44-501"),
    ("038_000_0000_chapter", "038_022_0000_article", "038_022_0001_section", "038_022_0001_k", "38-2201"),
    ("012_000_0000_chapter", "012_001_0000_article", "012_001_0001_section", "012_001_0001_k", "12-101"),
    ("065_000_0000_chapter", "065_001_0000_article", "065_001_0001_section", "065_001_0001_k", "65-101"),
    ("059_000_0000_chapter", "059_001_0000_article", "059_001_0001_section", "059_001_0001_k", "59-101"),
    ("077_000_0000_chapter", "077_002_0000_article", "077_002_0001_section", "077_002_0001_k", "77-201"),
    ("023_000_0000_chapter", "023_001_0000_article", "023_001_0001_section", "023_001_0001_k", "23-101"),
]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'<h[1-6][^>]*>', '\n## ', text)
    text = re.sub(r'</h[1-6]>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    # Remove navigation artifacts (Prev/Next links)
    text = re.sub(r'^Prev\s+.*?Next\s*\n?', '', text)
    text = re.sub(r'\bPrev\b.*?\bNext\b', '', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class KSLegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html",
            },
            timeout=60,
        )
        self.delay = 2.0

    def _get(self, url: str) -> str:
        """Fetch URL with rate limiting, return HTML string."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.text

    def test_api(self):
        """Test connectivity to kslegislature.gov."""
        logger.info("Testing Kansas Legislature website...")
        try:
            url = f"{BASE_URL}/021_000_0000_chapter/021_054_0000_article/021_054_0002_section/021_054_0002_k/"
            html = self._get(url)
            if "21-5402" in html and ("Murder" in html or "murder" in html):
                logger.info("  Connectivity: OK")
                logger.info("  Section content found: Yes")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("API test FAILED: expected content not found")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def get_chapters(self) -> list:
        """Get all chapter slugs from the main statute page."""
        url = f"{BASE_URL}/"
        html = self._get(url)
        # Match chapter links like 001_000_0000_chapter/
        slugs = re.findall(r'href="([0-9a-z]+_000_0000_chapter/)"', html)
        if not slugs:
            # Try relative path pattern
            slugs = re.findall(r'href="\.?/?([0-9a-z]+_000_0000_chapter)/?"', html)
        chapters = list(dict.fromkeys(slugs))  # deduplicate preserving order
        # Normalize: ensure trailing slash stripped for consistency
        chapters = [c.rstrip('/') for c in chapters]
        logger.info(f"Found {len(chapters)} chapters")
        return chapters

    def get_articles(self, chapter_slug: str) -> list:
        """Get all article slugs for a chapter."""
        url = f"{BASE_URL}/{chapter_slug}/"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch chapter {chapter_slug}: {e}")
            return []
        # Match article links like 021_054_0000_article/
        pattern = r'href="([0-9a-z]+_[0-9a-z]+_0000_article)/?\"'
        slugs = re.findall(pattern, html)
        if not slugs:
            # Try with full path
            chapter_num = chapter_slug.split('_')[0]
            pattern = rf'({chapter_num}_[0-9a-z]+_0000_article)/?'
            slugs = re.findall(pattern, html)
        articles = list(dict.fromkeys(slugs))
        logger.info(f"  Chapter {chapter_slug}: {len(articles)} articles")
        return articles

    def get_sections(self, chapter_slug: str, article_slug: str) -> list:
        """Get all section links from an article page. Returns list of (section_slug, section_k)."""
        url = f"{BASE_URL}/{chapter_slug}/{article_slug}/"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch article {article_slug}: {e}")
            return []
        # Match section links - they contain section_slug/section_k pattern
        # e.g. 021_054_0002_section/021_054_0002_k
        pattern = r'([0-9a-z]+_[0-9a-z]+_[0-9a-z]+_section)/([0-9a-z]+_[0-9a-z]+_[0-9a-z]+_k)'
        matches = re.findall(pattern, html)
        sections = list(dict.fromkeys(matches))
        return sections

    def fetch_section_text(self, chapter_slug: str, article_slug: str,
                           section_slug: str, section_k: str) -> Optional[dict]:
        """Fetch full text of a single statute section."""
        url = f"{BASE_URL}/{chapter_slug}/{article_slug}/{section_slug}/{section_k}/"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch section {section_k}: {e}")
            return None

        # Extract statute paragraphs directly (most reliable approach)
        # Statute text is in <p class="lm_5f_stat"> or <p class="P1"> elements
        paragraphs = re.findall(
            r'<p[^>]*class="(?:lm_5f_stat|P1|p_pt|lm_5f_stats_5f_hist)"[^>]*>(.*?)</p>',
            html,
            re.DOTALL,
        )
        if paragraphs:
            raw_html = '\n'.join(paragraphs)
        else:
            # Fallback: extract from statutefull div
            match = re.search(
                r'id="statutefull"[^>]*>(.*?)</div>',
                html,
                re.DOTALL,
            )
            if match:
                raw_html = match.group(1)
                # Remove navigation tables (contain Prev/Next links)
                raw_html = re.sub(
                    r'<table[^>]*>.*?(?:Prev|Next).*?</table>',
                    '', raw_html, flags=re.DOTALL
                )
            else:
                logger.warning(f"No content found for {section_k}")
                return None

        # Extract section number and caption
        num_match = re.search(r'class="stat_5f_number"[^>]*>(.*?)</span>', html, re.DOTALL)
        caption_match = re.search(r'class="stat_5f_caption"[^>]*>(.*?)</span>', html, re.DOTALL)

        section_number = strip_html(num_match.group(1)).strip().rstrip('.') if num_match else ""
        caption = strip_html(caption_match.group(1)).strip() if caption_match else ""

        # If we couldn't find a section number from spans, try to extract from URL
        if not section_number:
            # Parse from section_k: e.g. 021_054_0002_k -> 21-5402
            parts = section_k.replace('_k', '').split('_')
            if len(parts) >= 3:
                ch = parts[0].lstrip('0') or '0'
                art = parts[1].lstrip('0') or '0'
                sec = parts[2].lstrip('0') or '0'
                section_number = f"{ch}-{art}{sec}"

        text = strip_html(raw_html)

        if not text or len(text) < 10:
            logger.warning(f"Section text too short for {section_k}: {len(text) if text else 0} chars")
            return None

        # Extract chapter and article numbers from slugs
        ch_parts = chapter_slug.split('_')
        art_parts = article_slug.split('_')
        chapter_num = ch_parts[0].lstrip('0') or '0'
        article_num = art_parts[1].lstrip('0') or '0'

        return {
            "section_number": section_number,
            "caption": caption,
            "chapter": chapter_num,
            "article": article_num,
            "text": text,
            "url": url,
            "chapter_slug": chapter_slug,
            "article_slug": article_slug,
            "section_slug": section_slug,
            "section_k": section_k,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        section_num = raw["section_number"]
        section_id = f"KS-{section_num}" if section_num else f"KS-{raw['section_k']}"
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        title = f"Kansas Statutes § {section_num}"
        if raw.get("caption"):
            title += f" {raw['caption']}"

        return {
            "_id": section_id,
            "_source": "US/KS-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": raw["text"],
            "date": today,
            "url": raw["url"],
            "chapter": raw["chapter"],
            "article": raw["article"],
            "section_num": section_num,
            "caption": raw.get("caption", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all statute sections across all chapters."""
        total = 0
        chapters = self.get_chapters()
        for chapter_slug in chapters:
            articles = self.get_articles(chapter_slug)
            for article_slug in articles:
                sections = self.get_sections(chapter_slug, article_slug)
                for section_slug, section_k in sections:
                    raw = self.fetch_section_text(chapter_slug, article_slug,
                                                  section_slug, section_k)
                    if raw:
                        yield self.normalize(raw)
                        total += 1
                        if total % 50 == 0:
                            logger.info(f"  Progress: {total} sections fetched")
        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample of well-known statute sections."""
        logger.info(f"Fetching {len(SAMPLE_SECTIONS)} sample sections...")
        count = 0
        for chapter_slug, article_slug, section_slug, section_k, display in SAMPLE_SECTIONS:
            raw = self.fetch_section_text(chapter_slug, article_slug,
                                          section_slug, section_k)
            if raw:
                yield self.normalize(raw)
                count += 1
            else:
                logger.warning(f"Failed to fetch sample section {display}")
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/KS-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = KSLegislationScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            gen = scraper.fetch_sample()
        else:
            gen = scraper.fetch_all()

        count = 0
        for record in gen:
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
