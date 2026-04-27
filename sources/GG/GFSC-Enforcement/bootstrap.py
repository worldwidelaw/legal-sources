#!/usr/bin/env python3
"""
Guernsey Financial Services Commission (GFSC) Enforcement Fetcher

Scrapes enforcement actions from gfsc.gg including:
- Public statements (detailed enforcement decisions)
- Prohibition orders
- Enforcement news (court judgments, actions)

Coverage: 2009-present, ~100-150 actions.
Full text extracted from HTML pages.

Data source: https://www.gfsc.gg
License: Public government data
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

SOURCE_ID = "GG/GFSC-Enforcement"
BASE_URL = "https://www.gfsc.gg"

# News listing categories to scrape
CATEGORIES = [
    {"path": "/news/public-statements", "type": "public_statement", "max_pages": 10},
    {"path": "/news/prohibitions", "type": "prohibition", "max_pages": 10},
    {"path": "/news/enforcement", "type": "enforcement", "max_pages": 10},
]

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter; Legal Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}


def parse_date(date_str: str) -> Optional[str]:
    """Parse GFSC date formats to ISO 8601."""
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()
    # Remove ordinal suffixes: "12th March 2026" -> "12 March 2026"
    date_str = re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", date_str)
    for fmt in ("%d %B %Y", "%d/%m/%Y", "%Y-%m-%d", "%B %d, %Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def slugify(text: str) -> str:
    """Create a filesystem-safe slug from text."""
    slug = re.sub(r"[^\w\s-]", "", text.lower())
    slug = re.sub(r"[\s_]+", "-", slug)
    return slug[:80].strip("-")


class GFSCEnforcementFetcher:
    """Fetcher for GFSC enforcement actions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse an HTML page."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            print(f"  [WARN] Failed to fetch {url}: {e}")
            return None

    def _scrape_listing(self, category: dict, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        """Scrape a news listing category for article links."""
        items = []
        path = category["path"]
        action_type = category["type"]
        page_limit = max_pages if max_pages is not None else category["max_pages"]
        page = 0

        while page < page_limit:
            url = f"{BASE_URL}{path}?page={page}"
            print(f"  Listing {action_type} page {page}: {url}")
            soup = self._get_page(url)
            if not soup:
                break

            # Find article links - news items link to /news/...
            links = soup.find_all("a", href=re.compile(r"^/news/[a-z0-9]"))
            if not links:
                break

            # Known category slugs to skip (these are tag pages, not articles)
            SKIP_SLUGS = {
                "public-statements", "prohibitions", "enforcement",
                "insurance", "investment", "financial-crime", "fiduciary",
                "banking", "telecoms", "general", "pensions",
                "news", "latest-news", "consultations", "guidance",
            }

            found = 0
            for link in links:
                href = link.get("href", "")
                # Skip category/pagination links
                if href in (path, f"{path}/") or "?page=" in href:
                    continue
                if not href.startswith("/news/"):
                    continue

                slug = href.replace("/news/", "").strip("/")
                if slug in SKIP_SLUGS:
                    continue

                title = link.get_text(strip=True)
                if not title or len(title) < 5:
                    continue
                # Skip category/count links like "Authorisations (79)"
                if re.match(r"^[\w\s]+ \(\d+\)$", title):
                    continue

                # Try to find date near the link
                parent = link.find_parent()
                date_str = None
                categories = []

                # Walk up to find context
                container = parent
                for _ in range(5):
                    if container and container.parent:
                        container = container.parent
                    else:
                        break

                if container:
                    text = container.get_text(" ", strip=True)
                    # Remove DOB strings to avoid parsing birth dates
                    text_no_dob = re.sub(r"Date of Birth[^)]*\)", "", text)
                    text_no_dob = re.sub(r"DOB[^)]*\)", "", text_no_dob)
                    # Date patterns: "12th March 2026"
                    date_match = re.search(
                        r"(\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|"
                        r"July|August|September|October|November|December)\s+\d{4})",
                        text_no_dob,
                    )
                    if date_match:
                        date_str = parse_date(date_match.group(1))

                    # Category tags
                    for cat in ("Public Statements", "Prohibitions", "Enforcement",
                                "Insurance", "Investment", "Financial Crime",
                                "Fiduciary", "Banking", "Telecoms"):
                        if cat in text:
                            categories.append(cat)

                items.append({
                    "slug": href.replace("/news/", "").strip("/"),
                    "title": title,
                    "date": date_str,
                    "action_type": action_type,
                    "categories": categories,
                    "url": f"{BASE_URL}{href}",
                })
                found += 1

            if found == 0:
                break

            page += 1
            time.sleep(1.5)

        return items

    def _scrape_all_listings(self, sample: bool = False) -> List[Dict[str, Any]]:
        """Scrape all category listings and deduplicate."""
        all_items = []
        for cat in CATEGORIES:
            max_pages = 1 if sample else None
            items = self._scrape_listing(cat, max_pages=max_pages)
            all_items.extend(items)
            time.sleep(1.0)

        # Deduplicate by slug
        seen = set()
        unique = []
        for item in all_items:
            if item["slug"] not in seen:
                seen.add(item["slug"])
                unique.append(item)

        print(f"  Total unique items: {len(unique)}")
        return unique

    def _extract_page_text(self, soup: BeautifulSoup) -> str:
        """Extract main text content from a detail page."""
        # Remove nav, header, footer, sidebar elements
        for tag in soup.find_all(["nav", "header", "footer", "aside", "script",
                                   "style", "noscript", "iframe"]):
            tag.decompose()

        # Try to find the main content area
        main = soup.find("main") or soup.find("article")
        if not main:
            # Drupal typical content area
            main = soup.find("div", class_=re.compile(r"field--name-body|node__content|content"))
        if not main:
            main = soup.find("div", {"role": "main"})
        if not main:
            # Fallback: largest text block
            main = soup.find("body") or soup

        # Convert to clean text preserving structure
        lines = []
        for elem in main.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "td", "th", "blockquote"]):
            text = elem.get_text(" ", strip=True)
            if not text:
                continue
            tag = elem.name
            if tag.startswith("h"):
                level = int(tag[1])
                lines.append(f"\n{'#' * level} {text}\n")
            elif tag == "li":
                lines.append(f"- {text}")
            else:
                lines.append(text)

        result = "\n".join(lines).strip()

        # Remove duplicate lines from nested elements
        seen_lines = set()
        deduped = []
        for line in result.split("\n"):
            stripped = line.strip()
            if stripped and stripped not in seen_lines:
                seen_lines.add(stripped)
                deduped.append(line)
            elif not stripped:
                deduped.append(line)

        return "\n".join(deduped).strip()

    def _fetch_detail(self, item: Dict[str, Any]) -> Optional[str]:
        """Fetch full text from an individual enforcement page."""
        soup = self._get_page(item["url"])
        if not soup:
            return None

        # Check for PDF links as well
        text_parts = []

        # Get HTML text
        html_text = self._extract_page_text(soup)
        if html_text and len(html_text) > 100:
            text_parts.append(html_text)

        # Check for PDF attachments
        pdf_links = soup.find_all("a", href=re.compile(r"\.pdf", re.IGNORECASE))
        for pdf_link in pdf_links:
            href = pdf_link.get("href", "")
            if href.startswith("/"):
                href = BASE_URL + href
            if not href.startswith("http"):
                continue
            try:
                resp = self.session.get(href, timeout=60)
                resp.raise_for_status()
                if len(resp.content) > 100:
                    md = extract_pdf_markdown(
                        source=SOURCE_ID,
                        source_id=item["slug"],
                        pdf_bytes=resp.content,
                        table="doctrine",
                    )
                    if md and md.strip():
                        pdf_name = pdf_link.get_text(strip=True) or "PDF Document"
                        text_parts.append(f"\n---\n## {pdf_name}\n\n{md}")
            except requests.RequestException:
                pass
            time.sleep(1.0)

        return "\n\n".join(text_parts) if text_parts else None

    def normalize(self, item: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Normalize an enforcement record."""
        return {
            "_id": f"GG/GFSC-Enforcement/{item['slug'][:100]}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": item.get("title", ""),
            "text": text,
            "date": item.get("date"),
            "action_type": item.get("action_type"),
            "categories": item.get("categories", []),
            "url": item.get("url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Iterator[Dict[str, Any]]:
        """Fetch all GFSC enforcement actions with full text."""
        items = self._scrape_all_listings(sample=sample)

        if sample:
            items = items[:15]

        for i, item in enumerate(items):
            print(f"  [{i+1}/{len(items)}] {item['title'][:70]}")
            text = self._fetch_detail(item)
            time.sleep(1.5)

            if not text or len(text) < 100:
                print(f"    [SKIP] Insufficient text ({len(text) if text else 0} chars)")
                continue

            record = self.normalize(item, text)
            print(f"    OK: {len(text)} chars")
            yield record


def main():
    parser = argparse.ArgumentParser(description="GFSC Enforcement Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--output", default=None, help="Output directory")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    source_dir = Path(__file__).parent
    output_dir = Path(args.output) if args.output else source_dir / "sample"
    output_dir.mkdir(parents=True, exist_ok=True)

    fetcher = GFSCEnforcementFetcher()
    count = 0

    for record in fetcher.fetch_all(sample=args.sample):
        safe_name = slugify(record["title"])[:60] or f"record_{count}"
        out_path = output_dir / f"{safe_name}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        print(f"  Saved: {out_path.name}")

    print(f"\nDone. {count} records saved to {output_dir}")


if __name__ == "__main__":
    main()
