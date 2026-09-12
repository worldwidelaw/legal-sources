#!/usr/bin/env python3
"""
LV/ConstitutionalCourt -- Latvian Constitutional Court (Satversmes tiesa) Fetcher

Fetches Constitutional Court collegium decisions from the official website.

Strategy:
  - Walk the Drupal listing pager for decision node URLs
  - Follow each node's attached PDF and extract its full text
  - Normalize into standard schema

Endpoints:
  - Listing: https://www.satversmestiesa.lv/lv/lemumi-par-atteiksanos-ierosinat-lietu?page=N
  - Decision nodes: https://www.satversmestiesa.lv/lv/kolegijas-{slug}
  - Decision PDF:   https://www.satversmestiesa.lv/lv/media/{id}/download?attachment

Data:
  - Collegium decisions refusing to initiate a case (lemumi par atteiksanos)
  - Full text in Latvian, extracted from the attached PDF
  - ~880 decisions from 2003 to present

Note: the court migrated off WordPress (www.satv.tiesa.gov.lv/decisions/{slug}/,
wp-sitemap-posts-decision-1.xml) to Drupal in 2026; the old host 301s and every
wp-sitemap path 404s, which silently zeroed this scraper (issue #1221).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LV.constitutionalcourt")

# Base URLs
#
# The court migrated from the old WordPress site (www.satv.tiesa.gov.lv,
# /decisions/{slug}/ + wp-sitemap) to a Drupal site in 2026 (issue #1221).
# The old host now 301s to satversmestiesa.lv and every wp-sitemap path 404s,
# so discovery runs off the Drupal listing view instead.
BASE_URL = "https://www.satversmestiesa.lv"
LISTING_PATH = "/lv/lemumi-par-atteiksanos-ierosinat-lietu"
DECISION_URL_PREFIX = f"{BASE_URL}/lv/"

# Listing view is a 20-per-page Drupal pager (?page=0..N).
DECISION_LINK_RE = re.compile(r'href="(/lv/[a-z0-9\-]*lemums[a-z0-9\-]*)"', re.I)
# The decision body is a PDF attached to the node.
MEDIA_HREF_RE = re.compile(r'href="(/lv/media/\d+/download[^"]*)"', re.I)
TITLE_RE = re.compile(r"<title>\s*(.*?)\s*</title>", re.S | re.I)
PUBLISHED_RE = re.compile(r"Public[eē]ts:\s*(\d{2})\.(\d{2})\.(\d{4})")

# Stop after this many consecutive empty listing pages.
MAX_EMPTY_PAGES = 2
MAX_LISTING_PAGES = 200

# Headers for requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5,lv;q=0.3",
}


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract decision text from PDF bytes via the shared extractor chain."""
    try:
        from common.pdf_extract import _extract as _shared_extract
        text = _shared_extract(pdf_bytes) or ""
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""
    # Collapse the runaway blank lines the PDF layout leaves behind.
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return "\n".join(line.strip() for line in text.split("\n")).strip()


class ConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for LV/ConstitutionalCourt -- Latvian Constitutional Court (Satversmes tiesa).
    Country: LV
    URL: https://www.satv.tiesa.gov.lv

    Data types: case_law
    Auth: none (public)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_sitemap(self) -> List[Dict]:
        """
        Walk the Drupal listing pager and collect every decision URL.

        The listing at /lv/lemumi-par-atteiksanos-ierosinat-lietu is a
        20-per-page view covering collegium refusal decisions back to 2003.
        Returns list of dicts with url and lastmod (lastmod is unavailable on
        the listing, so it stays None and fetch_updates falls back to the
        decision's own publication date).
        """
        urls: List[Dict] = []
        seen = set()
        empty_streak = 0

        for page in range(MAX_LISTING_PAGES):
            listing_url = f"{BASE_URL}{LISTING_PATH}?page={page}"
            try:
                self.rate_limiter.wait()
                resp = self.session.get(listing_url, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to fetch listing page {page}: {e}")
                break

            found = [m.group(1) for m in DECISION_LINK_RE.finditer(resp.text)]
            new = 0
            for path in found:
                url = BASE_URL + path
                if url not in seen:
                    seen.add(url)
                    urls.append({'url': url, 'lastmod': None})
                    new += 1

            if new == 0:
                empty_streak += 1
                if empty_streak >= MAX_EMPTY_PAGES:
                    break
            else:
                empty_streak = 0
                logger.info(f"Listing page {page}: +{new} decisions (total {len(urls)})")

        if not urls:
            raise RuntimeError(
                f"LV/ConstitutionalCourt listing {BASE_URL}{LISTING_PATH} returned "
                "0 decision links — site blocked or layout changed"
            )

        logger.info(f"Found {len(urls)} decision URLs in listing")
        return urls

    def _extract_slug(self, url: str) -> str:
        """Extract the slug/ID from a decision URL."""
        # Remove trailing slash and base URL
        path = url.rstrip('/').replace(DECISION_URL_PREFIX, '')
        return path

    def _parse_date_from_slug(self, slug: str) -> Optional[str]:
        """
        Try to extract date from slug.

        Examples:
        - kolegijas-2026-gada-19-februara-lemums-pieteikums-nr-18-2026
        - 2015-gada-20-janvara-kolegijas-lemums-pieteikums-nr-22015
        """
        # Month name mapping
        months = {
            'janvara': '01', 'janvari': '01',
            'februara': '02', 'februari': '02',
            'marta': '03', 'marti': '03',
            'aprila': '04', 'aprili': '04',
            'maija': '05', 'maiji': '05',
            'junija': '06', 'juniji': '06',
            'julija': '07', 'juliji': '07',
            'augusta': '08', 'augusti': '08',
            'septembra': '09', 'septembri': '09',
            'oktobra': '10', 'oktobri': '10',
            'novembra': '11', 'novembri': '11',
            'decembra': '12', 'decembri': '12',
        }

        # Pattern: YYYY-gada-DD-monthname
        pattern = r'(\d{4})-gada-(\d{1,2})-(\w+)'
        match = re.search(pattern, slug)

        if match:
            year = match.group(1)
            day = match.group(2).zfill(2)
            month_name = match.group(3).lower()
            month = months.get(month_name)

            if month:
                return f"{year}-{month}-{day}"

        return None

    def _parse_petition_number(self, slug: str) -> Optional[str]:
        """
        Extract the first petition number from a slug.

        Examples:
        - pieteikums-nr-18-2026 -> 18/2026
        - pieteikums-nr-822026  -> 82/2026
        """
        numbers = self._parse_petition_numbers(slug)
        return numbers[0] if numbers else None

    def _parse_petition_numbers(self, slug: str) -> List[str]:
        """
        Extract every petition number carried by a slug.

        A single decision can dispose of several petitions, in which case the
        slug repeats the "nr-" segment:
            ...lemums-pieteikums-nr-492025-nr-532025 -> ["49/2025", "53/2025"]

        The number/year pair runs together on Drupal slugs (822026 = 82/2026)
        but is dash-separated on older ones (18-2026). The stem is also misspelt
        upstream ("pietiekums", "pietikums") and sometimes loses the dash before
        "nr", so match it loosely.
        """
        anchor = re.search(r'piet\w*ums', slug)
        if not anchor:
            return []

        return [
            f"{m.group(1)}/{m.group(2)}"
            for m in re.finditer(r'(?:nr-?)?(\d+)-?(\d{4})', slug[anchor.end():])
        ]

    def _fetch_decision_page(self, url: str) -> Optional[Dict]:
        """
        Fetch a decision node and extract its full text.

        On the Drupal site the node itself carries only title + publication
        date; the decision body lives in an attached PDF exposed as
        /lv/media/{id}/download?attachment. Text comes from that PDF via the
        shared extractor (pdfplumber/pypdf/OCR chain).
        """
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            page = resp.text

            tm = TITLE_RE.search(page)
            title = ""
            if tm:
                title = html.unescape(re.sub(r'<[^>]+>', '', tm.group(1)))
                # Drop the trailing " | Latvijas Republikas Satversmes tiesa".
                title = re.split(r'\s*[|»–]\s*Latvijas Republikas Satversmes', title)[0].strip()

            pm = MEDIA_HREF_RE.search(page)
            if not pm:
                logger.warning(f"No attached PDF on decision page {url}")
                return None
            pdf_url = BASE_URL + html.unescape(pm.group(1))

            self.rate_limiter.wait()
            pdf_resp = self.session.get(pdf_url, timeout=60)
            pdf_resp.raise_for_status()
            if not pdf_resp.content[:5].startswith(b"%PDF"):
                logger.warning(f"Attachment is not a PDF: {pdf_url}")
                return None

            full_text = _extract_pdf_text(pdf_resp.content)
            if not full_text:
                logger.warning(f"No text extracted from {pdf_url} (scanned?)")
                return None

            # Publication date shown on the node ("Publicēts: DD.MM.YYYY").
            published = None
            pubm = PUBLISHED_RE.search(page)
            if pubm:
                published = f"{pubm.group(3)}-{pubm.group(2)}-{pubm.group(1)}"

            return {
                'url': url,
                'title': title,
                'full_text': full_text,
                'pdf_url': pdf_url,
                'published': published,
            }

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch decision {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing decision {url}: {e}")
            return None

    def fetch_all(self) -> Generator[Dict, None, None]:
        """
        Yield all Constitutional Court decisions.

        Fetches URLs from WordPress sitemap and extracts full text from each page.
        """
        doc_count = 0

        # Fetch sitemap
        sitemap_entries = self._fetch_sitemap()
        if not sitemap_entries:
            logger.error("No decisions found in sitemap")
            return

        logger.info(f"Processing {len(sitemap_entries)} decision URLs")

        for entry in sitemap_entries:
            url = entry['url']
            lastmod = entry.get('lastmod')

            result = self._fetch_decision_page(url)
            if result and result.get('full_text'):
                # Add lastmod from sitemap
                result['lastmod'] = lastmod

                doc_count += 1
                yield result

                if doc_count % 50 == 0:
                    logger.info(f"Fetched {doc_count} decisions with full text")

        logger.info(f"Total decisions fetched: {doc_count}")

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        """
        Yield decisions published on/after the given date.

        The Drupal listing carries no lastmod, but it is ordered newest-first,
        so walk it and stop once the decisions predate `since`. Dates come from
        the slug (or the node's "Publicēts:" line), which means the filter runs
        after the node fetch; the early stop keeps that bounded.
        """
        entries = self._fetch_sitemap()
        if not entries:
            return

        since_date = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since_date}")

        stale_streak = 0
        for entry in entries:
            url = entry['url']
            slug = self._extract_slug(url)

            # Cheap pre-filter: most slugs carry the decision date.
            slug_date = self._parse_date_from_slug(slug)
            if slug_date and slug_date < since_date:
                stale_streak += 1
                # Listing is newest-first; a solid run of old items means done.
                if stale_streak >= 40:
                    logger.info("Reached decisions older than `since` — stopping")
                    return
                continue

            stale_streak = 0
            result = self._fetch_decision_page(url)
            if not result or not result.get('full_text'):
                continue
            effective = slug_date or result.get('published')
            if effective and effective < since_date:
                continue
            yield result

    def normalize(self, raw: Dict) -> Dict:
        """
        Transform raw decision data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        url = raw.get('url', '')
        slug = self._extract_slug(url)
        title = raw.get('title', '')
        full_text = raw.get('full_text', '')

        # Extract date from slug, falling back to the node's publication date
        date = self._parse_date_from_slug(slug) or raw.get('published')

        # Extract petition number(s) — one decision can dispose of several.
        petition_numbers = self._parse_petition_numbers(slug)
        petition_number = petition_numbers[0] if petition_numbers else None

        # Prefer a date + petition ID: it survives site migrations, whereas the
        # URL slug changed wholesale in the 2026 WordPress -> Drupal move. The
        # date qualifier matters — petition 187/2017 was disposed of twice, in
        # 2017 and again in 2019, so the number alone is not unique.
        if petition_number and date:
            doc_id = f"{date}-pieteikums-{petition_number.replace('/', '-')}"
        elif petition_number:
            doc_id = "pieteikums-" + petition_number.replace('/', '-')
        else:
            doc_id = slug if slug else url.rstrip('/').split('/')[-1]

        # Determine decision type from title or text
        decision_type = "Lēmums"  # Default: Decision
        if 'spriedums' in title.lower() or 'spriedums' in full_text[:200].lower():
            decision_type = "Spriedums"  # Judgment
        elif 'kolegijas' in slug.lower() or 'kolēģijas' in title.lower():
            decision_type = "Kolēģijas lēmums"  # Collegium decision
        elif 'ricibas' in slug.lower():
            decision_type = "Rīcības sēdes lēmums"  # Procedural session decision

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "LV/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date or "",
            "url": url,
            # Additional metadata
            "petition_number": petition_number or "",
            "petition_numbers": petition_numbers,
            "decision_type": decision_type,
            "court": "Satversmes tiesa",
            "lastmod": raw.get('lastmod') or raw.get('published') or '',
            "slug": slug,
            "pdf_url": raw.get('pdf_url', ''),
            "language": "lv",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing LV/ConstitutionalCourt endpoints...")

        # Test sitemap
        print("\n1. Testing sitemap fetch...")
        try:
            entries = self._fetch_sitemap()
            print(f"   Found {len(entries)} decision URLs")
            if entries:
                print(f"   Sample URL: {entries[0]['url'][:80]}...")
                print(f"   Lastmod: {entries[0].get('lastmod', 'N/A')}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test decision page fetch
        print("\n2. Testing decision page fetch...")
        try:
            if entries:
                url = entries[-1]['url']  # Get a recent one
                result = self._fetch_decision_page(url)
                if result:
                    print(f"   Title: {result['title'][:60]}...")
                    print(f"   Text length: {len(result['full_text'])} characters")
                    print(f"   Keywords: {result.get('keywords', [])}")
                    if result['full_text']:
                        print(f"   Preview: {result['full_text'][:200]}...")
                else:
                    print("   ERROR: Could not fetch decision page")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test normalization
        print("\n3. Testing normalization...")
        try:
            if entries:
                url = entries[-1]['url']
                raw = self._fetch_decision_page(url)
                if raw:
                    normalized = self.normalize(raw)
                    print(f"   ID: {normalized['_id']}")
                    print(f"   Date: {normalized.get('date', 'N/A')}")
                    print(f"   Type: {normalized.get('decision_type', 'N/A')}")
                    print(f"   Petition: {normalized.get('petition_number', 'N/A')}")
                    print(f"   Text length: {len(normalized.get('text', ''))}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = ConstitutionalCourtScraper()

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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
