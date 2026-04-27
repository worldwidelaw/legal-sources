#!/usr/bin/env python3
"""
UY/DGI-ConsultasTributarias -- Uruguay DGI Tax Publications & Written Consultations

Fetches tax doctrine from Uruguay's Dirección General Impositiva portal.

Strategy:
  - Enumerate publications via paginated HTML listing at /comunicacion/publicaciones
  - For each publication, fetch individual page and extract full text HTML content
  - Clean HTML to plain text

Data: Public (gub.uy open access, Drupal CMS).
Rate limit: 2 sec between requests.
Coverage: ~1,452 publications (guides, instructions, written consultations).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample docs
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
logger = logging.getLogger("legal-data-hunter.UY.DGI-ConsultasTributarias")

BASE_URL = "https://www.gub.uy"
LISTING_PATH = "/direccion-general-impositiva/comunicacion/publicaciones"
LISTING_URL = f"{BASE_URL}{LISTING_PATH}"


def html_to_text(html_content: str) -> str:
    """Extract clean text from HTML content."""
    if not html_content:
        return ""
    content = html_content
    content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<h[1-6][^>]*>(.*?)</h[1-6]>', r'\n\n## \1\n', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<p[^>]*>', '\n\n', content, flags=re.IGNORECASE)
    content = re.sub(r'</p>', '', content, flags=re.IGNORECASE)
    content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
    content = re.sub(r'<div[^>]*>', '\n', content, flags=re.IGNORECASE)
    content = re.sub(r'</div>', '', content, flags=re.IGNORECASE)
    content = re.sub(r'<li[^>]*>', '\n  - ', content, flags=re.IGNORECASE)
    content = re.sub(r'</?(?:ul|ol|table|thead|tbody|tr|td|th|span|strong|em|b|i|a|img|figure|figcaption|nav|header|footer|section|aside)[^>]*>', '', content, flags=re.IGNORECASE)
    content = re.sub(r'<[^>]+>', '', content)
    content = html_module.unescape(content)
    content = re.sub(r'[ \t]+', ' ', content)
    content = re.sub(r'\n[ \t]+', '\n', content)
    content = re.sub(r'\n{3,}', '\n\n', content)
    return content.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


class DGIConsultasScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html",
                "Accept-Language": "es-UY,es;q=0.9,en;q=0.5",
            },
            timeout=60,
        )

    def test_api(self):
        """Test connectivity to DGI publications listing."""
        logger.info("Testing DGI publications page connectivity...")
        try:
            resp = self.http.get(LISTING_URL, params={"page": "0"})
            logger.info(f"  Status: {resp.status_code}")
            if resp.status_code == 200:
                articles = re.findall(r'<article[^>]*>', resp.text)
                logger.info(f"  Articles on page: {len(articles)}")
                total_match = re.search(r'(\d[\d.,]*)\s*resultado', resp.text)
                if total_match:
                    logger.info(f"  Total results: {total_match.group(1)}")
                if len(articles) > 0:
                    logger.info("Connectivity test PASSED")
                    return True
            logger.error("Connectivity test FAILED")
            return False
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            return False

    def enumerate_publications(self, sample: bool = False) -> list:
        """Enumerate publication links from paginated listing pages."""
        publications = []
        page = 0
        max_pages = 2 if sample else 200

        while page < max_pages:
            logger.info(f"  Fetching listing page {page}...")
            time.sleep(2)
            try:
                resp = self.http.get(LISTING_URL, params={"page": str(page)})
                if resp.status_code != 200:
                    logger.warning(f"  HTTP {resp.status_code} for page {page}")
                    break

                articles = re.findall(
                    r'<article[^>]*>(.*?)</article>',
                    resp.text,
                    re.DOTALL,
                )
                if not articles:
                    logger.info(f"  No articles on page {page}, stopping.")
                    break

                for art_html in articles:
                    link_match = re.search(
                        r'<h3[^>]*>\s*<a\s+href="([^"]+)"[^>]*>\s*(.*?)\s*</a>',
                        art_html,
                        re.DOTALL,
                    )
                    date_match = re.search(
                        r'<span\s+class="Box-info">(\d{2}/\d{2}/\d{4})</span>',
                        art_html,
                    )
                    type_match = re.search(
                        r'<span\s+class="Box-info u-h5[^"]*">\s*(.*?)\s*</span>',
                        art_html,
                        re.DOTALL,
                    )

                    if link_match:
                        path = link_match.group(1).strip()
                        title = re.sub(r'<[^>]+>', '', link_match.group(2)).strip()
                        title = html_module.unescape(title)
                        url = f"{BASE_URL}{path}" if path.startswith("/") else path

                        pub = {
                            "url": url,
                            "title": title,
                            "date_raw": date_match.group(1) if date_match else None,
                            "doc_type": type_match.group(1).strip() if type_match else None,
                        }
                        publications.append(pub)

                logger.info(f"  Page {page}: {len(articles)} articles, total: {len(publications)}")
                page += 1

                if sample and len(publications) >= 20:
                    break

            except Exception as e:
                logger.warning(f"  Failed to fetch page {page}: {e}")
                break

        return publications

    def fetch_document(self, pub: dict) -> Optional[dict]:
        """Fetch a single publication page and extract full text."""
        url = pub["url"]
        logger.info(f"  Fetching: {pub['title'][:60]}...")
        time.sleep(2)

        try:
            resp = self.http.get(url)
            if resp.status_code != 200:
                logger.warning(f"  HTTP {resp.status_code} for {url}")
                return None

            html = resp.text

            # Extract content from the article element
            article_match = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL)
            if not article_match:
                logger.warning(f"  No article element found for {url}")
                return None

            article_html = article_match.group(1)

            # Extract title from Page-title
            title_match = re.search(
                r'<h2\s+class="Page-title[^"]*">(.*?)</h2>',
                article_html,
                re.DOTALL,
            )
            title = html_module.unescape(re.sub(r'<[^>]+>', '', title_match.group(1)).strip()) if title_match else pub["title"]

            # Extract date from Page-date
            date_match = re.search(
                r'class="Page-date[^"]*"[^>]*>(\d{2}/\d{2}/\d{4})',
                article_html,
            )
            date_raw = date_match.group(1) if date_match else pub.get("date_raw")

            # Extract description
            desc_match = re.search(
                r'class="Page-description"[^>]*>(.*?)</div>',
                article_html,
                re.DOTALL,
            )
            description = ""
            if desc_match:
                description = re.sub(r'<[^>]+>', '', desc_match.group(1)).strip()
                description = html_module.unescape(description)

            # Extract body content: everything after Page-description or Page-info
            # The body is the rest of the article content after the metadata section
            body_html = article_html

            # Remove the header/metadata section (Page-document wrapper, title, date, social buttons)
            # Keep content after Page-description
            desc_pos = body_html.find('Page-description')
            if desc_pos > 0:
                # Find the closing div of the description
                after_desc = body_html[desc_pos:]
                # Skip the description div itself
                close_div = after_desc.find('</div>')
                if close_div > 0:
                    body_html = after_desc[close_div + 6:]
            else:
                # Fallback: remove everything before the first h3 or first substantial p
                h3_pos = body_html.find('<h3')
                if h3_pos > 0:
                    body_html = body_html[h3_pos:]

            text = html_to_text(body_html)

            # Prepend description if it's not already in the body
            if description and description not in text[:200]:
                text = description + "\n\n" + text

            if len(text) < 50:
                logger.warning(f"  Insufficient text ({len(text)} chars) for {url}")
                return None

            # Generate slug-based ID from URL
            slug = url.rstrip("/").split("/")[-1]

            return {
                "slug": slug,
                "title": title,
                "date_raw": date_raw,
                "doc_type": pub.get("doc_type") or "",
                "description": description,
                "text": text,
                "url": url,
            }

        except Exception as e:
            logger.warning(f"  Failed to fetch {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all publications with full text."""
        logger.info("Starting full bootstrap of DGI publications...")
        publications = self.enumerate_publications(sample=False)
        logger.info(f"Found {len(publications)} publications to fetch")

        for i, pub in enumerate(publications):
            doc = self.fetch_document(pub)
            if doc:
                yield doc
            if (i + 1) % 50 == 0:
                logger.info(f"  Progress: {i + 1}/{len(publications)}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield publications modified since the given date."""
        logger.info(f"Fetching updates since {since}...")
        publications = self.enumerate_publications(sample=False)
        for pub in publications:
            date_str = parse_date(pub.get("date_raw", ""))
            if date_str:
                pub_date = datetime.strptime(date_str, "%Y-%m-%d")
                if pub_date < since.replace(tzinfo=None):
                    continue
            doc = self.fetch_document(pub)
            if doc:
                yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        date_iso = parse_date(raw.get("date_raw", ""))
        slug = raw.get("slug", "")

        return {
            "_id": f"UY/DGI-ConsultasTributarias/{slug}",
            "_source": "UY/DGI-ConsultasTributarias",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_iso,
            "url": raw.get("url", ""),
            "doc_type": raw.get("doc_type", ""),
            "description": raw.get("description", ""),
            "language": "es",
            "jurisdiction": "UY",
        }

    def bootstrap(self, sample: bool = False):
        """Run bootstrap: fetch documents and save to sample/data directory."""
        output_dir = self.source_dir / ("sample" if sample else "data")
        output_dir.mkdir(parents=True, exist_ok=True)

        if sample:
            logger.info("Running in SAMPLE mode (max 15 documents)")
            publications = self.enumerate_publications(sample=True)
            logger.info(f"Found {len(publications)} publications, fetching up to 15...")
            count = 0
            for pub in publications[:15]:
                doc = self.fetch_document(pub)
                if doc:
                    normalized = self.normalize(doc)
                    slug = doc["slug"]
                    out_path = output_dir / f"{slug}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)
                    count += 1
                    logger.info(f"  Saved {count}: {normalized['title'][:60]} ({len(normalized['text'])} chars)")
            logger.info(f"Sample complete: {count} documents saved to {output_dir}")
        else:
            logger.info("Running FULL bootstrap")
            count = 0
            for doc in self.fetch_all():
                normalized = self.normalize(doc)
                slug = doc["slug"]
                out_path = output_dir / f"{slug}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)
                count += 1
            logger.info(f"Bootstrap complete: {count} documents saved to {output_dir}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    scraper = DGIConsultasScraper()

    if command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        sample = "--sample" in sys.argv
        scraper.bootstrap(sample=sample)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
