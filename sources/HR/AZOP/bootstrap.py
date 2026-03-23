#!/usr/bin/env python3
"""
HR/AZOP -- Croatian Data Protection Authority (AZOP) Data Fetcher

Fetches GDPR decisions, opinions, and recommendations from AZOP
(Agencija za zaštitu osobnih podataka).

Strategy:
  1. Decisions (Rješenja): Scrape /rjesenja/ and sub-pages for PDF links,
     download PDFs, extract full text with pypdf.
  2. Opinions (Mišljenja): Scrape /misljenja/ category pages for post links,
     fetch each post and extract HTML body text.
  3. Recommendations (Preporuke): Scrape /preporuke/ for post links + PDFs.

Endpoints:
  - Decisions: https://azop.hr/rjesenja/
  - Opinions: https://azop.hr/misljenja/
  - Recommendations: https://azop.hr/preporuke/
  - Sitemap: https://azop.hr/post-sitemap.xml

Data:
  - ~20 decision PDFs (2022-2026)
  - ~50-60 opinion posts (HTML)
  - ~12 recommendation posts
  - Language: Croatian (HR)
  - Rate limit: 1 request/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import io
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, urlparse, unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

# PDF extraction
try:
    import pypdf
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HR.azop")

BASE_URL = "https://azop.hr"

# Pages that list decision PDFs
DECISION_PAGES = [
    "/rjesenja/",
    "/upravne-novcane-kazne-2/",
    "/obrada-osobnih-podataka-putem-kolacica-upravne-novcane-kazne/",
]

# Category slugs for opinions
OPINION_CATEGORY_SLUGS = [
    "misljenja-azop",
    "misljenja-obrada-osobih-podataka-od-strane-poslodavaca",
    "misljenja-obrada-op-u-financijskom-sektoru-i-telekomunikacijskom-sektoru",
    "misljenja-drustvene-mreze",
    "misljenja-mediji",
    "misljenja-obrada-osobnih-podataka-od-strane-sudova",
    "misljenja-podaci-o-zdravlju",
    "misljenja-transferi-osobnih-podataka-u-trece-zemlje",
]


class CroatianDPAScraper(BaseScraper):
    """
    Scraper for HR/AZOP -- Croatian Data Protection Authority.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml",
                "Accept-Language": "hr,en",
            },
            timeout=60,
        )

    # ── Decision PDFs ────────────────────────────────────────────

    def _scrape_decision_pdf_links(self) -> List[Dict[str, str]]:
        """Scrape all decision listing pages for PDF links."""
        pdfs = []
        seen_urls = set()

        for page_path in DECISION_PAGES:
            try:
                self.rate_limiter.wait()
                resp = self.client.get(page_path)
                resp.raise_for_status()
                content = resp.text

                # Find all PDF links in wp-content/uploads/
                for match in re.finditer(
                    r'href="(https?://azop\.hr/wp-content/uploads/[^"]+\.pdf)"',
                    content, re.IGNORECASE
                ):
                    pdf_url = match.group(1)
                    if pdf_url not in seen_urls:
                        seen_urls.add(pdf_url)
                        # Derive a title from the filename
                        filename = unquote(urlparse(pdf_url).path.split("/")[-1])
                        title = filename.replace(".pdf", "").replace("-", " ").replace("_", " ")
                        pdfs.append({"url": pdf_url, "title": title, "source_page": page_path})

                logger.info(f"Found {len(pdfs)} PDFs so far from {page_path}")
            except Exception as e:
                logger.error(f"Failed to scrape {page_path}: {e}")

        return pdfs

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text."""
        if not HAS_PYPDF:
            logger.warning("pypdf not installed, skipping PDF extraction")
            return None

        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(pdf_url, timeout=60, headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
            })
            resp.raise_for_status()

            if len(resp.content) > 20 * 1024 * 1024:
                logger.warning(f"PDF too large ({len(resp.content)} bytes): {pdf_url}")
                return None

            pdf_file = io.BytesIO(resp.content)
            reader = pypdf.PdfReader(pdf_file)
            pages_text = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)

            full_text = "\n\n".join(pages_text).strip()
            return full_text if len(full_text) > 50 else None

        except Exception as e:
            logger.warning(f"Failed to extract PDF text from {pdf_url}: {e}")
            return None

    # ── Opinion HTML Posts ───────────────────────────────────────

    def _scrape_opinion_links(self) -> List[Dict[str, str]]:
        """Scrape opinion category pages for post links."""
        posts = []
        seen_urls = set()

        for slug in OPINION_CATEGORY_SLUGS:
            page_num = 1
            while True:
                try:
                    if page_num == 1:
                        url = f"/category/{slug}/"
                    else:
                        url = f"/category/{slug}/page/{page_num}/"

                    self.rate_limiter.wait()
                    resp = self.client.get(url)

                    if resp.status_code == 404:
                        break
                    resp.raise_for_status()
                    content = resp.text

                    # Find post links in entry-title elements
                    found = 0
                    for match in re.finditer(
                        r'class="entry-title"[^>]*>\s*<a\s+href="(https?://azop\.hr/[^"]+)"[^>]*>([^<]+)</a>',
                        content, re.DOTALL
                    ):
                        post_url = match.group(1).rstrip("/")
                        title = html.unescape(match.group(2).strip())
                        if post_url not in seen_urls:
                            seen_urls.add(post_url)
                            posts.append({"url": post_url, "title": title, "category": slug})
                            found += 1

                    if found == 0:
                        break
                    page_num += 1

                except Exception as e:
                    logger.error(f"Failed to scrape category {slug} page {page_num}: {e}")
                    break

        logger.info(f"Found {len(posts)} opinion posts total")
        return posts

    def _extract_post_text(self, post_url: str) -> Optional[Dict[str, Any]]:
        """Fetch a WordPress post and extract text + date."""
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(post_url, timeout=60, headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept-Language": "hr,en",
            })
            resp.raise_for_status()
            content = resp.text

            result = {}

            # Extract date from meta or time element
            date_match = re.search(
                r'<meta\s+property="article:published_time"\s+content="(\d{4}-\d{2}-\d{2})',
                content
            )
            if date_match:
                result["date"] = date_match.group(1)
            else:
                time_match = re.search(r'<time[^>]*datetime="(\d{4}-\d{2}-\d{2})', content)
                if time_match:
                    result["date"] = time_match.group(1)

            # Extract main content from et-boc div (Divi theme)
            body_match = re.search(
                r'<div\s+id="et-boc"[^>]*>(.*?)</div>\s*<!--\s*#et-boc',
                content, re.DOTALL
            )
            if not body_match:
                # Fallback: look for entry-content
                body_match = re.search(
                    r'<div\s+class="[^"]*entry-content[^"]*"[^>]*>(.*?)</div>\s*<!--',
                    content, re.DOTALL
                )

            if body_match:
                body_html = body_match.group(1)
                # Remove script/style tags
                body_html = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', body_html, flags=re.DOTALL)
                # Remove nav/menu sections
                body_html = re.sub(r'<nav[^>]*>.*?</nav>', '', body_html, flags=re.DOTALL)
                # Strip HTML tags
                text = re.sub(r'<[^>]+>', ' ', body_html)
                text = html.unescape(text)
                text = re.sub(r'\s+', ' ', text).strip()
                # Remove common boilerplate
                text = re.sub(r'^.*?(Izbornik|Meni|Menu)\s*', '', text, count=1)

                if len(text) > 200:
                    result["text"] = text

            # Check for embedded PDFs
            pdf_match = re.search(
                r'href="(https?://azop\.hr/wp-content/uploads/[^"]+\.pdf)"',
                content, re.IGNORECASE
            )
            if pdf_match:
                result["pdf_url"] = pdf_match.group(1)

            return result if result.get("text") or result.get("pdf_url") else None

        except Exception as e:
            logger.warning(f"Failed to fetch post {post_url}: {e}")
            return None

    # ── Recommendations ──────────────────────────────────────────

    def _scrape_recommendation_links(self) -> List[Dict[str, str]]:
        """Scrape the recommendations page for post/PDF links."""
        items = []
        try:
            self.rate_limiter.wait()
            resp = self.client.get("/preporuke/")
            resp.raise_for_status()
            content = resp.text

            # Find post links
            for match in re.finditer(
                r'<a\s+href="(https?://azop\.hr/[^"]+)"[^>]*>([^<]{10,})</a>',
                content
            ):
                url = match.group(1).rstrip("/")
                title = html.unescape(match.group(2).strip())
                # Skip navigation and generic links
                if any(skip in url for skip in ["/category/", "/tag/", "/page/", "#", "/feed"]):
                    continue
                if url not in [i["url"] for i in items]:
                    items.append({"url": url, "title": title, "category": "preporuke"})

            logger.info(f"Found {len(items)} recommendation items")
        except Exception as e:
            logger.error(f"Failed to scrape recommendations: {e}")

        return items

    # ── Normalize ────────────────────────────────────────────────

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        doc_id = raw.get("doc_id", "")
        url = raw.get("url", "")
        title = raw.get("title", "Untitled")
        text = raw.get("text", "")
        date = raw.get("date", "")
        category = raw.get("category", "unknown")

        return {
            "_id": f"HR-AZOP-{doc_id}",
            "_source": "HR/AZOP",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "doc_id": doc_id,
            "category": category,
            "language": "hr",
        }

    # ── Main fetch ───────────────────────────────────────────────

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all AZOP documents."""
        count = 0
        limit = 15 if sample else 99999

        # 1. Decision PDFs
        logger.info("=== Fetching decision PDFs ===")
        pdf_links = self._scrape_decision_pdf_links()
        for pdf_info in pdf_links:
            if count >= limit:
                return

            text = self._extract_pdf_text(pdf_info["url"])
            if not text:
                continue

            # Generate ID from URL
            url_hash = hashlib.md5(pdf_info["url"].encode()).hexdigest()[:12]
            doc_id = f"rjesenje-{url_hash}"

            # Try to extract date from URL path (uploads/YYYY/MM/)
            date = ""
            date_match = re.search(r'/uploads/(\d{4})/(\d{2})/', pdf_info["url"])
            if date_match:
                date = f"{date_match.group(1)}-{date_match.group(2)}-01"

            raw = {
                "doc_id": doc_id,
                "url": pdf_info["url"],
                "title": f"Rješenje: {pdf_info['title']}",
                "text": text,
                "date": date,
                "category": "rjesenje",
            }
            yield self.normalize(raw)
            count += 1
            logger.info(f"[{count}] Decision PDF: {pdf_info['title'][:60]}... ({len(text)} chars)")

        # 2. Opinion posts
        logger.info("=== Fetching opinion posts ===")
        opinion_links = self._scrape_opinion_links()
        for post_info in opinion_links:
            if count >= limit:
                return

            post_data = self._extract_post_text(post_info["url"])
            if not post_data:
                continue

            text = post_data.get("text", "")

            # If post has a PDF but no inline text, extract PDF
            if not text and post_data.get("pdf_url"):
                text = self._extract_pdf_text(post_data["pdf_url"]) or ""

            if len(text) < 200:
                continue

            url_hash = hashlib.md5(post_info["url"].encode()).hexdigest()[:12]
            doc_id = f"misljenje-{url_hash}"

            raw = {
                "doc_id": doc_id,
                "url": post_info["url"],
                "title": post_info["title"],
                "text": text,
                "date": post_data.get("date", ""),
                "category": post_info.get("category", "misljenje"),
            }
            yield self.normalize(raw)
            count += 1
            logger.info(f"[{count}] Opinion: {post_info['title'][:60]}... ({len(text)} chars)")

        # 3. Recommendations
        logger.info("=== Fetching recommendations ===")
        rec_links = self._scrape_recommendation_links()
        for rec_info in rec_links:
            if count >= limit:
                return

            # Check if it's a PDF link
            if rec_info["url"].lower().endswith(".pdf"):
                text = self._extract_pdf_text(rec_info["url"])
                date = ""
                date_match = re.search(r'/uploads/(\d{4})/(\d{2})/', rec_info["url"])
                if date_match:
                    date = f"{date_match.group(1)}-{date_match.group(2)}-01"
            else:
                post_data = self._extract_post_text(rec_info["url"])
                if not post_data:
                    continue
                text = post_data.get("text", "")
                if not text and post_data.get("pdf_url"):
                    text = self._extract_pdf_text(post_data["pdf_url"]) or ""
                date = post_data.get("date", "")

            if not text or len(text) < 200:
                continue

            url_hash = hashlib.md5(rec_info["url"].encode()).hexdigest()[:12]
            doc_id = f"preporuka-{url_hash}"

            raw = {
                "doc_id": doc_id,
                "url": rec_info["url"],
                "title": rec_info["title"],
                "text": text,
                "date": date,
                "category": "preporuka",
            }
            yield self.normalize(raw)
            count += 1
            logger.info(f"[{count}] Recommendation: {rec_info['title'][:60]}... ({len(text)} chars)")

        logger.info(f"=== Total: {count} documents fetched ===")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents updated since a date."""
        # For a small corpus, just re-fetch all and filter by date
        for doc in self.fetch_all():
            if doc.get("date", "") >= since:
                yield doc


def main():
    scraper = CroatianDPAScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
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
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
