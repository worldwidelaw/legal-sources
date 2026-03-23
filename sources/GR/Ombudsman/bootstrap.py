#!/usr/bin/env python3
"""
GR/Ombudsman -- Greek Ombudsman (Synigoros tou Politi)

Fetches Ombudsman investigation findings, mediation summaries, and reports.

Strategy:
  - Nuxt.js site with JSON API at /api/posts (returns all ~2154 posts)
  - Individual post: /api/posts/{id}
  - PDF files: /api/files/download/{fileId} (IDs extracted from post HTML pages)
  - Post descriptions contain HTML summaries
  - Full text from PDF attachments where available, otherwise from description HTML

Endpoints:
  - All posts: https://www.synigoros.gr/api/posts
  - Single post: https://www.synigoros.gr/api/posts/{id}
  - PDF download: https://www.synigoros.gr/api/files/download/{fileId}
  - Sitemap: https://www.synigoros.gr/sitemap-posts.xml

Data:
  - Investigation findings (porismata) ~253
  - Mediation summaries ~873
  - Special reports ~113
  - Annual reports ~36 (1998-2024)
  - Press releases ~135

License: Public

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    print("WARNING: pdfplumber not available. Install with: pip install pdfplumber")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.Ombudsman")

BASE_URL = "https://www.synigoros.gr"

# Relevant slug patterns for substantive content
RELEVANT_SLUGS = [
    "porisma",           # Investigation findings
    "synopsh-diamesolav", # Mediation summaries
    "eidikh-ek8esh",     # Special reports
    "ethsia-ek8esh",     # Annual reports
]


class OmbudsmanScraper(BaseScraper):
    """
    Scraper for GR/Ombudsman -- Greek Ombudsman (Synigoros tou Politi).
    Country: GR
    URL: https://www.synigoros.gr

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=120,
        )

    def _strip_html(self, text: str) -> str:
        """Strip HTML tags and decode entities."""
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
        text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<li[^>]*>', '\n- ', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = html_mod.unescape(text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    def _is_relevant(self, slug: str) -> bool:
        """Check if a post slug indicates substantive content."""
        return any(pattern in slug for pattern in RELEVANT_SLUGS)

    def _fetch_all_posts(self) -> List[Dict[str, Any]]:
        """Fetch all posts from the API."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get("/api/posts")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch posts: {e}")
            return []

    def _build_sitemap_index(self) -> Dict[str, str]:
        """Build slug -> full URL mapping from sitemap."""
        if hasattr(self, '_sitemap_index'):
            return self._sitemap_index
        try:
            self.rate_limiter.wait()
            resp = self.client.get("/sitemap-posts.xml", headers={"Accept": "application/xml"})
            urls = re.findall(r'<loc>(.*?)</loc>', resp.text)
            index = {}
            for u in urls:
                m = re.search(r'/post/(.+)$', u)
                if m:
                    # Store the path portion after base URL
                    index[m.group(1)] = u.replace(BASE_URL, "")
            self._sitemap_index = index
            logger.info(f"Sitemap index: {len(index)} post URLs")
            return index
        except Exception as e:
            logger.warning(f"Failed to load sitemap: {e}")
            self._sitemap_index = {}
            return {}

    # Global file IDs that appear on every page (site-wide elements)
    _GLOBAL_FILE_IDS = frozenset({
        '436102', '121', '422283', '422282', '477316', '477096',
        '476892', '477315', '477311', '476894', '436108',
    })

    def _extract_file_ids_from_page(self, post_slug: str, category_slug: str = "synopseis-porismata") -> List[str]:
        """Fetch post HTML page and extract post-specific file download IDs."""
        try:
            # Use sitemap URL (returns 200) instead of guessed URL (returns 404)
            sitemap = self._build_sitemap_index()
            url = sitemap.get(post_slug)
            if not url:
                url = f"/el/category/{category_slug}/post/{post_slug}"

            self.rate_limiter.wait()
            resp = self.client.get(url, headers={"Accept": "text/html"})
            page_html = resp.text

            # Extract file IDs from rendered HTML and NUXT state
            file_ids = re.findall(r'files(?:\\u002F|/)download(?:\\u002F|/)(\d+)', page_html)
            # Deduplicate while preserving order, exclude global IDs
            seen = set()
            unique_ids = []
            for fid in file_ids:
                if fid not in seen and fid not in self._GLOBAL_FILE_IDS:
                    seen.add(fid)
                    unique_ids.append(fid)
            return unique_ids
        except Exception as e:
            logger.debug(f"Could not fetch page for {post_slug}: {e}")
            return []

    def _download_pdf(self, file_id: str, max_pdf_size_mb: int = 10) -> str:
        """Download PDF from the files API and extract text."""
        if not PDF_SUPPORT:
            return ""

        import gc
        import requests

        try:
            self.rate_limiter.wait()
            url = f"{BASE_URL}/api/files/download/{file_id}"
            resp = requests.get(url, timeout=60, stream=True, headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
            })

            if resp.status_code != 200:
                logger.warning(f"Failed to download file {file_id}: HTTP {resp.status_code}")
                return ""

            content_length = resp.headers.get('content-length')
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                if size_mb > max_pdf_size_mb:
                    logger.warning(f"File too large {file_id}: {size_mb:.1f}MB")
                    return ""

            content = resp.content
            content_type = resp.headers.get('content-type', '')
            if 'pdf' not in content_type.lower() and not content.startswith(b'%PDF'):
                logger.debug(f"File {file_id} is not a PDF ({content_type})")
                return ""

            pdf_bytes = io.BytesIO(content)
            text_parts = []
            total_chars = 0
            max_chars = 500_000

            try:
                with pdfplumber.open(pdf_bytes) as pdf:
                    for page in pdf.pages:
                        if total_chars >= max_chars:
                            break
                        try:
                            text = page.extract_text()
                            if text:
                                text_parts.append(text)
                                total_chars += len(text)
                        except Exception:
                            pass
                        finally:
                            page.flush_cache()
            finally:
                pdf_bytes.close()
                del pdf_bytes, content

            full_text = "\n".join(text_parts)
            del text_parts
            full_text = re.sub(r'\n{3,}', '\n\n', full_text).strip()

            if total_chars > 100_000:
                gc.collect()

            return full_text

        except Exception as e:
            logger.warning(f"Failed to extract PDF {file_id}: {e}")
            return ""

    def _process_post(self, post: Dict[str, Any], fetch_pdf: bool = True) -> Optional[Dict[str, Any]]:
        """Process a single post: extract text from description and optionally PDF."""
        slug = post.get("slug", "")
        description = post.get("description", "") or ""
        short_desc = post.get("shortDescription", "") or ""

        # Start with description text
        full_text = self._strip_html(description)
        if not full_text:
            full_text = self._strip_html(short_desc)

        # Always try PDF — descriptions are just short summaries
        pdf_text = ""
        if fetch_pdf:
            file_ids = self._extract_file_ids_from_page(slug)
            if file_ids:
                # Try first PDF (usually the main document)
                pdf_text = self._download_pdf(file_ids[0])
                if pdf_text and len(pdf_text) > len(full_text):
                    full_text = pdf_text

        if not full_text or len(full_text) < 50:
            logger.warning(f"Insufficient text for post {post.get('id')}: {len(full_text)} chars")
            return None

        date_str = post.get("date")
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_str = dt.isoformat()
            except (ValueError, TypeError):
                pass

        return {
            "post_id": post.get("id"),
            "name": post.get("name", ""),
            "slug": slug,
            "full_text": full_text,
            "date": date_str,
            "url": f"{BASE_URL}/el/post/{slug}",
            "has_pdf": bool(pdf_text),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all relevant Ombudsman posts."""
        posts = self._fetch_all_posts()
        relevant = [p for p in posts if self._is_relevant(p.get("slug", ""))]
        logger.info(f"Found {len(relevant)} relevant posts out of {len(posts)} total")

        for i, post in enumerate(relevant):
            logger.info(f"  [{i+1}/{len(relevant)}] Post {post.get('id')}...")
            result = self._process_post(post, fetch_pdf=True)
            if result:
                yield result

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield posts created/updated since the given date."""
        posts = self._fetch_all_posts()
        cutoff = since.isoformat()

        for post in posts:
            if not self._is_relevant(post.get("slug", "")):
                continue
            updated = post.get("updatedAt", "") or post.get("createdAt", "")
            if updated >= cutoff:
                result = self._process_post(post, fetch_pdf=True)
                if result:
                    yield result

    def normalize(self, raw: dict) -> dict:
        """Transform raw post to standard schema."""
        title = raw.get("name", "") or f"Ombudsman Post {raw.get('post_id')}"

        return {
            "_id": f"OMBUDSMAN-{raw.get('post_id')}",
            "_source": "GR/Ombudsman",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": raw.get("date"),
            "url": raw.get("url"),
            "slug": raw.get("slug"),
            "has_pdf": raw.get("has_pdf", False),
        }

    def _fetch_sample(self, sample_size: int = 12) -> list:
        """Fetch sample records for validation."""
        posts = self._fetch_all_posts()
        relevant = [p for p in posts if self._is_relevant(p.get("slug", ""))]
        logger.info(f"Found {len(relevant)} relevant posts, sampling {sample_size}...")

        samples = []
        for post in relevant[:sample_size * 2]:
            if len(samples) >= sample_size:
                break

            result = self._process_post(post, fetch_pdf=True)
            if result:
                normalized = self.normalize(result)
                samples.append(normalized)
                logger.info(
                    f"Sample {len(samples)}/{sample_size}: {normalized['_id']} "
                    f"({len(normalized.get('text', ''))} chars, pdf={result.get('has_pdf')})"
                )

        return samples


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GR/Ombudsman Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for validation")
    args = parser.parse_args()

    scraper = OmbudsmanScraper()

    if args.command == "test":
        print("Testing Greek Ombudsman API connection...")
        posts = scraper._fetch_all_posts()
        if posts:
            relevant = [p for p in posts if scraper._is_relevant(p.get("slug", ""))]
            print(f"SUCCESS: API returned {len(posts)} total posts")
            print(f"Relevant posts (findings/reports/mediations): {len(relevant)}")
            if relevant:
                p = relevant[0]
                print(f"Sample: {p.get('name', '')[:80]}")
                print(f"Date: {p.get('date')}")
        else:
            print("FAILED: Could not retrieve posts")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records...")
            samples = scraper._fetch_sample(sample_size=12)

            sample_dir = scraper.source_dir / "sample"
            sample_dir.mkdir(exist_ok=True)

            for record in samples:
                safe_id = record['_id'].replace('/', '_')
                filepath = sample_dir / f"{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to {sample_dir}/")

            if samples:
                text_lengths = [len(s.get("text", "")) for s in samples]
                avg_len = sum(text_lengths) / len(text_lengths)
                print(f"Average text length: {avg_len:.0f} characters")
                print(f"Min text length: {min(text_lengths)} chars")
                print(f"Max text length: {max(text_lengths)} chars")
        else:
            print("Running full bootstrap...")
            count = 0
            for record in scraper.fetch_all():
                normalized = scraper.normalize(record)
                print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
                count += 1
            print(f"\nFetched {count} Ombudsman documents")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        print(f"Fetching updates since {since.isoformat()}...")
        count = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
            count += 1
        print(f"\nFetched {count} new Ombudsman documents")


if __name__ == "__main__":
    main()
