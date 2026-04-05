#!/usr/bin/env python3
"""
INTL/OHADA-CCJA -- OHADA Common Court of Justice and Arbitration

Fetches jurisprudence from the OHADA database at ohada.com.

Strategy:
  - Paginate listing pages at /documentation/jurisprudence.html?page=N
  - Extract decision URLs (ohadata IDs) from listing cards
  - Fetch individual decision pages for summary/abstract + metadata
  - ~4,126 decisions across 413 pages (10 per page)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
import urllib.request
import urllib.error
import ssl
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.OHADA-CCJA")

BASE_URL = "https://www.ohada.com"
LISTING_URL = f"{BASE_URL}/documentation/jurisprudence.html"
DETAIL_URL = f"{BASE_URL}/documentation/jurisprudence/ohadata/{{id}}.html"

# Rate limit: 2 seconds between requests
RATE_LIMIT_SECONDS = 2.0


def _fetch_url(url: str, timeout: int = 30) -> str:
    """Fetch a URL and return decoded text. Uses stdlib only."""
    req = urllib.request.Request(url)
    try:
        ctx = ssl.create_default_context()
        resp = urllib.request.urlopen(req, timeout=timeout, context=ctx)
        return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        logger.error(f"HTTP {e.code} fetching {url}")
        raise
    except urllib.error.URLError as e:
        logger.error(f"URL error fetching {url}: {e.reason}")
        raise


class OHADACCJAScraper(BaseScraper):
    """
    Scraper for INTL/OHADA-CCJA -- OHADA jurisprudence database.
    Country: INTL
    URL: https://www.ohada.com/documentation/jurisprudence.html

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    # ── Listing page parsing ──────────────────────────────────────────

    def _parse_listing_page(self, page: int = 1) -> list[dict]:
        """Parse a listing page and return decision stubs.

        Each stub has: ohadata_id, url, city, country, court, date_text, title_text
        """
        if page == 1:
            url = LISTING_URL
        else:
            url = f"{LISTING_URL}?page={page}"

        logger.info(f"Fetching listing page {page}: {url}")
        html = _fetch_url(url, timeout=60)
        soup = BeautifulSoup(html, "html.parser")

        decisions = []

        # Each decision is in a card with class "card"
        cards = soup.select("div.card.border-primary")
        for card in cards:
            try:
                stub = self._parse_card(card)
                if stub:
                    decisions.append(stub)
            except Exception as e:
                logger.debug(f"Error parsing card: {e}")

        return decisions

    def _parse_card(self, card) -> Optional[dict]:
        """Parse a single decision card from the listing page."""
        # OHADATA ID from the title link
        title_link = card.select_one("h4.card-title a")
        if not title_link:
            title_link = card.select_one("a h4.card-title")
        if not title_link:
            # Try parent
            a_tag = card.select_one("a[href*='ohadata']")
            if not a_tag:
                return None
            href = a_tag.get("href", "")
            h4 = card.select_one("h4.card-title")
            ohadata_text = h4.get_text(strip=True) if h4 else ""
        else:
            parent_a = title_link.find_parent("a")
            if parent_a:
                href = parent_a.get("href", "")
            else:
                href = title_link.get("href", "") if title_link.name == "a" else ""
                if not href:
                    parent_a = title_link.find_parent("a")
                    href = parent_a.get("href", "") if parent_a else ""
            ohadata_text = title_link.get_text(strip=True)

        # If we still have no href, try the card's first ohadata link
        if not href:
            a_tags = card.select("a[href*='ohadata']")
            if a_tags:
                href = a_tags[0].get("href", "")

        if not href or "ohadata" not in href:
            return None

        # Extract OHADATA ID from URL
        m = re.search(r"ohadata/(J-[\w-]+)\.html", href)
        if not m:
            return None
        ohadata_id = m.group(1)

        # Extract metadata from list items
        items = card.select("ul li")
        city = ""
        country = ""
        court = ""
        date_text = ""

        for li in items:
            text = li.get_text(strip=True)
            # City and country: has map-marker icon
            if li.select_one("i.fa-map-marker-alt"):
                parts = text.split("-", 1)
                city = parts[0].strip()
                if len(parts) > 1:
                    country = parts[1].strip()
                    # Remove flag emojis
                    country = re.sub(r"[\U0001F1E0-\U0001F1FF]+", "", country).strip()
            # Court: has gavel icon
            elif li.select_one("i.fa-gavel"):
                court = text.strip()
            # Date: has calendar icon
            elif li.select_one("i.fa-calendar-day"):
                date_text = text.strip()

        # Extract case reference text
        card_text_el = card.select_one("p.card-text")
        card_text = card_text_el.get_text(strip=True) if card_text_el else ""

        return {
            "ohadata_id": ohadata_id,
            "url": f"{BASE_URL}{href}" if href.startswith("/") else href,
            "ohadata_label": ohadata_text,
            "city": city,
            "country": country,
            "court": court,
            "date_text": date_text,
            "card_text": card_text,
        }

    def _get_total_pages(self) -> int:
        """Get the total number of listing pages."""
        html = _fetch_url(LISTING_URL, timeout=60)
        soup = BeautifulSoup(html, "html.parser")

        # Find the last page link
        page_links = soup.select("a.page-link[href*='page=']")
        max_page = 1
        for link in page_links:
            href = link.get("href", "")
            m = re.search(r"page=(\d+)", href)
            if m:
                p = int(m.group(1))
                if p > max_page:
                    max_page = p
        return max_page

    # ── Detail page parsing ───────────────────────────────────────────

    def _fetch_decision_detail(self, ohadata_id: str) -> Optional[dict]:
        """Fetch and parse an individual decision page.

        Returns dict with: title, summary, keywords, articles, court, date, country
        """
        url = DETAIL_URL.format(id=ohadata_id)
        try:
            html = _fetch_url(url, timeout=30)
        except Exception as e:
            logger.warning(f"Failed to fetch detail for {ohadata_id}: {e}")
            return None

        soup = BeautifulSoup(html, "html.parser")

        # The decision content is inside a section after the Ohadata marker
        # Find the main content area
        result = {
            "ohadata_id": ohadata_id,
            "url": url,
        }

        # Title/heading: span with class h4
        heading = soup.select_one("span.h4")
        if heading:
            result["title"] = heading.get_text(strip=True)
        else:
            # Fallback: try page title
            title_tag = soup.find("title")
            if title_tag:
                t = title_tag.get_text(strip=True)
                t = re.sub(r"^OHADA\.com\s*-\s*", "", t)
                result["title"] = t
            else:
                result["title"] = f"OHADATA {ohadata_id}"

        # Court and date from the em tag
        court_em = soup.select_one("span.d-block em")
        if court_em:
            em_text = court_em.get_text(strip=True)
            result["court_detail"] = em_text
            # Extract court name
            m = re.match(r"(.+?)\s*(?:Arrêt|Avis|Jugement|Ordonnance|Sentence|Décision|Déclaration)", em_text)
            if m:
                result["court"] = m.group(1).strip()
            # Extract date
            date_m = re.search(r"du\s+(\d{2}/\d{2}/\d{4})", em_text)
            if date_m:
                result["date_raw"] = date_m.group(1)

        # Keywords section: p with class small-caps (first one after the heading)
        keywords_sections = soup.select("p.mb-3.small-caps")
        kw_parts = []
        articles_parts = []
        for kw in keywords_sections:
            text = kw.get_text(strip=True)
            if not text:
                continue
            # Check if it looks like article references
            if re.match(r"Article\s", text, re.IGNORECASE):
                articles_parts.append(text)
            else:
                kw_parts.append(text)

        result["keywords"] = "\n".join(kw_parts)
        result["articles"] = "\n".join(articles_parts)

        # Summary: p with class font-italic
        summary_paras = soup.select("p.mb-3.font-italic")
        summary_texts = []
        for p in summary_paras:
            t = p.get_text(strip=True)
            if t and len(t) > 20:
                summary_texts.append(t)
        result["summary"] = "\n\n".join(summary_texts)

        # Country from the badge span on the detail page
        country_badge = soup.select_one("span.badge-notification.badge-light")
        if country_badge:
            country_text = country_badge.get_text(strip=True)
            # Remove flag emojis
            country_text = re.sub(r"[\U0001F1E0-\U0001F1FF]+", "", country_text).strip()
            if country_text:
                result["country"] = country_text

        return result

    # ── Date parsing ──────────────────────────────────────────────────

    @staticmethod
    def _parse_date(date_text: str) -> Optional[str]:
        """Parse various French date formats to ISO 8601."""
        if not date_text:
            return None

        # Try DD/MM/YYYY
        m = re.search(r"(\d{2})/(\d{2})/(\d{4})", date_text)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

        # Try "du DD/MM/YYYY"
        m = re.search(r"du\s+(\d{2})/(\d{2})/(\d{4})", date_text)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

        return None

    # ── Abstract methods implementation ───────────────────────────────

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw decision record into standard schema."""
        ohadata_id = raw.get("ohadata_id", "")
        if not ohadata_id:
            return None

        # Build the text from available components
        text_parts = []

        title = raw.get("title", "").strip()
        if title:
            text_parts.append(title)

        keywords = raw.get("keywords", "").strip()
        if keywords:
            text_parts.append(f"Mots-cles: {keywords}")

        summary = raw.get("summary", "").strip()
        if summary:
            text_parts.append(summary)

        articles = raw.get("articles", "").strip()
        if articles:
            text_parts.append(f"Articles: {articles}")

        text = "\n\n".join(text_parts)
        if not text or len(text) < 50:
            return None

        # Parse date
        date = self._parse_date(raw.get("date_raw", ""))
        if not date:
            date = self._parse_date(raw.get("date_text", ""))

        # Court
        court = raw.get("court", raw.get("court_detail", "")).strip()

        # URL
        url = raw.get("url", DETAIL_URL.format(id=ohadata_id))

        # Country
        country = raw.get("country", "").strip()

        return {
            "_id": f"OHADA-{ohadata_id}",
            "_source": "INTL/OHADA-CCJA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title or f"OHADATA {ohadata_id}",
            "text": text,
            "date": date,
            "url": url,
            "court": court,
            "ohadata_id": ohadata_id,
            "country": country,
            "city": raw.get("city", ""),
            "keywords": keywords,
            "articles": articles,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all OHADA decisions by paginating listings + detail pages."""
        # Get total pages
        total_pages = self._get_total_pages()
        logger.info(f"Total listing pages: {total_pages}")

        time.sleep(RATE_LIMIT_SECONDS)

        for page in range(1, total_pages + 1):
            logger.info(f"Listing page {page}/{total_pages}")

            try:
                stubs = self._parse_listing_page(page)
            except Exception as e:
                logger.error(f"Failed to parse listing page {page}: {e}")
                time.sleep(RATE_LIMIT_SECONDS)
                continue

            time.sleep(RATE_LIMIT_SECONDS)

            for stub in stubs:
                ohadata_id = stub["ohadata_id"]

                # Fetch the detail page for full content
                try:
                    detail = self._fetch_decision_detail(ohadata_id)
                except Exception as e:
                    logger.warning(f"Failed detail for {ohadata_id}: {e}")
                    time.sleep(RATE_LIMIT_SECONDS)
                    continue

                if detail:
                    # Merge: detail overrides stub, but preserve non-empty stub values
                    merged = {**stub}
                    for k, v in detail.items():
                        if v or k not in merged or not merged[k]:
                            merged[k] = v
                    yield merged
                else:
                    # Fall back to listing-only data
                    stub["title"] = stub.get("card_text", "")
                    stub["summary"] = ""
                    stub["keywords"] = ""
                    stub["articles"] = ""
                    yield stub

                time.sleep(RATE_LIMIT_SECONDS)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions from the first few listing pages (most recent)."""
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching decisions since {since_str}")

        # Check first 5 pages for recent decisions
        for page in range(1, 6):
            stubs = self._parse_listing_page(page)
            time.sleep(RATE_LIMIT_SECONDS)

            found_old = False
            for stub in stubs:
                date = self._parse_date(stub.get("date_text", ""))
                if date and date < since_str:
                    found_old = True
                    break

                detail = self._fetch_decision_detail(stub["ohadata_id"])
                if detail:
                    merged = {**stub, **detail}
                    yield merged
                time.sleep(RATE_LIMIT_SECONDS)

            if found_old:
                break


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/OHADA-CCJA data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = OHADACCJAScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            stubs = scraper._parse_listing_page(1)
            logger.info(f"OK: {len(stubs)} decisions on page 1")
            if stubs:
                s = stubs[0]
                logger.info(f"First: {s['ohadata_id']} - {s['card_text'][:80]}")

                detail = scraper._fetch_decision_detail(s["ohadata_id"])
                if detail:
                    logger.info(f"Detail fetched: title={len(detail.get('title',''))}c, "
                                f"summary={len(detail.get('summary',''))}c, "
                                f"keywords={len(detail.get('keywords',''))}c")
                    if detail.get("summary"):
                        logger.info(f"Summary preview: {detail['summary'][:200]}")
                else:
                    logger.error("Failed to fetch detail page")
                    sys.exit(1)

            total = scraper._get_total_pages()
            logger.info(f"Total pages: {total}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
