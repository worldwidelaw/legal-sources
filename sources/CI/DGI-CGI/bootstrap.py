#!/usr/bin/env python3
"""
CI/DGI-CGI — Code Général des Impôts de la Côte d'Ivoire (2025 edition)

Fetches the full text of the Côte d'Ivoire General Tax Code from:
  https://dgi.cgici.com/

The tax code is served as static HTML pages in /V2025/.
ArticleLink.js maps article numbers to page filenames.
Each page contains multiple articles with full text in French.

License: Public domain (official government legislation)
"""

import argparse
import hashlib
import html
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://dgi.cgici.com"
SOURCE_ID = "CI/DGI-CGI"
SAMPLE_DIR = Path(__file__).parent / "sample"
EDITION_YEAR = "2025"
CONTENT_FOLDER = f"V{EDITION_YEAR}"


def clean_text(raw_html: str) -> str:
    """Strip HTML tags and decode entities, normalize whitespace."""
    text = re.sub(r"<[^>]+>", " ", raw_html)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(lines).strip()


def extract_section_context(page_html: str) -> str:
    """Extract structural headings from page HTML."""
    headings = []
    for cls in ["Livre", "Partie", "TitreSub", "ChapitreNo", "Chapitre", "Section"]:
        for m in re.finditer(
            rf'class="{cls}"[^>]*>(.*?)</(?:p|h[1-6])>',
            page_html, re.DOTALL | re.IGNORECASE
        ):
            heading_text = clean_text(m.group(1)).strip()
            heading_text = re.sub(r'\s*>\s*$', '', heading_text)
            if heading_text and len(heading_text) > 2 and heading_text not in headings:
                headings.append(heading_text)
    return " > ".join(headings[:5]) if headings else ""


def parse_articles_from_page(page_html: str, page_num: int) -> List[Dict[str, Any]]:
    """Parse individual articles from a page's HTML content."""
    articles = []
    section_context = extract_section_context(page_html)

    # Find all span-based article markers (most reliable delimiter)
    span_pattern = re.compile(
        r'<span\s+id="#A(\w+)"\s*>\s*</span>',
        re.IGNORECASE
    )
    spans = list(span_pattern.finditer(page_html))

    if not spans:
        return articles

    for i, span in enumerate(spans):
        start = span.end()
        end = spans[i + 1].start() if i + 1 < len(spans) else len(page_html)
        block = page_html[start:end]

        # Extract article number from the h6 tag that follows the span
        art_match = re.search(
            r'<h6\s+class="NoArticle"[^>]*>\s*Art\.\s*'
            r'([\d]+(?:\s+(?:bis|ter|quater|quinquies|sexies|septies|octies|nonies|decies))?)',
            block, re.IGNORECASE | re.DOTALL
        )
        if not art_match:
            continue
        art_num = re.sub(r'\s+', ' ', art_match.group(1)).strip()

        # Remove the h6 NoArticle tag itself from the body
        body = re.sub(
            r'<h6\s+class="NoArticle"[^>]*>.*?</h6>',
            "", block, count=1, flags=re.DOTALL | re.IGNORECASE
        )

        # Extract modification history
        mod_match = re.search(
            r'<h6\s+class="Modification"[^>]*>(.*?)</h6>',
            body, re.DOTALL | re.IGNORECASE
        )
        modification = clean_text(mod_match.group(1)) if mod_match else ""

        # Remove modification and historique blocks
        body = re.sub(
            r'<h6\s+class="Modification"[^>]*>.*?</h6>',
            "", body, flags=re.DOTALL | re.IGNORECASE
        )
        body = re.sub(
            r'<div\s+class="historique"[^>]*>.*?</div>',
            "", body, flags=re.DOTALL | re.IGNORECASE
        )

        text = clean_text(body)
        if text and len(text) > 5:
            articles.append({
                "article_number": art_num,
                "text": text,
                "modification_history": modification,
                "section": section_context,
                "page": page_num,
            })

    return articles


class DGICGIFetcher:
    """Fetcher for CI/DGI-CGI tax code articles."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html, */*",
        })

    def get_page_list(self) -> List[int]:
        """Get list of unique page numbers from ArticleLink.js."""
        url = f"{BASE_URL}/js/ArticleLink.js"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()

        pages = set()
        for m in re.finditer(r'page-(\d+)\.html', resp.text):
            pages.add(int(m.group(1)))

        return sorted(pages)

    def fetch_page(self, page_num: int) -> Optional[str]:
        """Fetch a single content page HTML."""
        url = f"{BASE_URL}/{CONTENT_FOLDER}/page-{page_num}.html"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch page {page_num}: {e}")
            return None

    def normalize(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize an article record into standard schema."""
        art_num = article["article_number"]
        art_id = f"CGI-{EDITION_YEAR}-art-{art_num}".replace(" ", "-")
        doc_id = hashlib.sha256(art_id.encode()).hexdigest()[:16]

        title = f"Code Général des Impôts - Article {art_num}"
        if article.get("section"):
            title += f" ({article['section'][:100]})"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": article["text"],
            "date": f"{EDITION_YEAR}-01-04",
            "url": f"{BASE_URL}/{CONTENT_FOLDER}/page-{article['page']}.html",
            "article_number": art_num,
            "section": article.get("section", ""),
            "modification_history": article.get("modification_history", ""),
            "language": "fr",
            "jurisdiction": "CI",
        }

    def fetch_all(self, sample: bool = False) -> Iterator[Dict[str, Any]]:
        """Fetch all tax code articles."""
        pages = self.get_page_list()
        logger.info(f"Found {len(pages)} content pages to fetch")

        if sample:
            pages = pages[:15]
            logger.info(f"Sample mode: fetching first {len(pages)} pages")

        total_articles = 0
        for i, page_num in enumerate(pages):
            page_html = self.fetch_page(page_num)
            if not page_html:
                continue

            articles = parse_articles_from_page(page_html, page_num)
            for article in articles:
                record = self.normalize(article)
                if record["text"] and len(record["text"]) > 20:
                    total_articles += 1
                    yield record

            if (i + 1) % 20 == 0:
                logger.info(f"Processed {i + 1}/{len(pages)} pages, {total_articles} articles so far")

            time.sleep(2)

        logger.info(f"Total articles fetched: {total_articles}")

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch updates since a date (re-fetches all for this static source)."""
        yield from self.fetch_all()


def main():
    parser = argparse.ArgumentParser(description="CI/DGI-CGI Tax Code fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--full", action="store_true", help="Fetch all data")
    args = parser.parse_args()

    if args.command == "bootstrap":
        fetcher = DGICGIFetcher()
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        count = 0
        for record in fetcher.fetch_all(sample=args.sample or not args.full):
            count += 1
            if args.sample or not args.full:
                out_path = SAMPLE_DIR / f"record_{count:03d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(f"Saved {out_path.name}: {record['title'][:80]}...")
                if count >= 15:
                    break
            else:
                print(json.dumps(record, ensure_ascii=False))

        logger.info(f"Bootstrap complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
