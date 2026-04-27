#!/usr/bin/env python3
"""
DJ/JournalOfficiel -- Djibouti Journal Officiel (eJO)

Fetches legislation from the official Djibouti electronic Journal Officiel.

The platform runs on WordPress with a fully exposed REST API at:
  https://www.journalofficiel.dj/wp-json/wp/v2/texte-juridique

Content includes:
  - Lois (laws): ~2,461
  - Décrets (decrees): ~5,423
  - Arrêtés (orders): ~21,003
  - Décisions: ~22,033
  - Ordonnances, circulaires, délibérations, etc.
  - Total: ~57,100 legal texts

Strategy:
  - Paginate through the WordPress REST API (100 per page)
  - Full text is available as inline HTML in content.rendered
  - Rich ACF metadata: reference, visas, signature, institution
  - Bilingual: French + Arabic fields

Data Coverage:
  - ~57,100 legal texts dating back to 1900
  - French (primary) and Arabic
  - No authentication required

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from bs4 import BeautifulSoup
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DJ.JournalOfficiel")

API_BASE = "https://www.journalofficiel.dj/wp-json/wp/v2"
TEXTE_ENDPOINT = f"{API_BASE}/texte-juridique"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Taxonomy ID -> name mapping for nature_du_texte
NATURE_MAP = {
    247: "Loi",
    248: "Décret",
    249: "Accord",
    250: "Proclamation",
    251: "Rapport",
    252: "Déclaration",
    253: "Erratum",
    254: "Décision",
    255: "Résolution",
    256: "Arrêté",
    257: "Circulaire",
    258: "Décision Proclamation",
    259: "Loi Organique",
    260: "Loi de finances",
    261: "Rectificatif",
    262: "Additif",
    263: "Ordonnance",
    264: "Décret d'application",
    265: "Circulaire Présidentielle",
    266: "Amendement",
    267: "Allocution",
    268: "Arrêté additif",
    269: "Arrêté modificatif",
    1308: "Avis",
    1310: "Arrêté de Promulgation",
    1312: "Avis du Haut-Commissariat",
    1314: "Communiqué",
    1316: "Convention",
    1318: "Délibération",
    1320: "Décision additive",
    1322: "Instruction",
    1324: "Protocole",
    1372: "Approbation",
    1374: "Dépêche",
    1376: "Information",
    1378: "Notification",
    1380: "Ordre de service",
}

MIN_TEXT_CHARS = 50
PER_PAGE = 100


def _strip_html(html_str: str) -> str:
    """Strip HTML tags and decode entities, returning clean text."""
    if not html_str:
        return ""
    soup = BeautifulSoup(html_str, "html.parser")
    text = soup.get_text(separator="\n")
    text = html_module.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class DJJournalOfficielScraper(BaseScraper):
    """
    Scraper for DJ/JournalOfficiel -- Djibouti Journal Officiel.
    Country: DJ
    URL: https://www.journalofficiel.dj/

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def _fetch_page(self, page: int, per_page: int = PER_PAGE) -> list:
        """Fetch a page of texte-juridique from the WP REST API."""
        params = {
            "per_page": per_page,
            "page": page,
            "_fields": "id,title,content,acf,date,link",
        }
        try:
            self.rate_limiter.wait()
            resp = self.session.get(TEXTE_ENDPOINT, params=params, timeout=30)
            if resp.status_code == 400:
                return []  # Past last page
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                return []
            logger.warning(f"HTTP error fetching page {page}: {e}")
            return []
        except Exception as e:
            logger.warning(f"Failed to fetch page {page}: {e}")
            return []

    def _parse_record(self, item: dict) -> Optional[Dict[str, Any]]:
        """Parse a single WP REST API item into a raw record."""
        wp_id = item.get("id")
        if not wp_id:
            return None

        title_raw = item.get("title", {}).get("rendered", "")
        title = _strip_html(title_raw)
        if not title:
            return None

        content_html = item.get("content", {}).get("rendered", "")
        text = _strip_html(content_html)
        if len(text) < MIN_TEXT_CHARS:
            return None

        acf = item.get("acf", {}) or {}
        reference = acf.get("reference", "") or ""
        visas = _strip_html(acf.get("visas", "") or "")
        signature = _strip_html(acf.get("signature", "") or "")
        comment = _strip_html(acf.get("comment", "") or "")
        nature_id = acf.get("nature_du_texte")
        nature_name = NATURE_MAP.get(nature_id, "") if nature_id else ""

        # Build full text including visas and signature
        parts = []
        if text:
            parts.append(text)
        if visas:
            parts.append(f"\n\nVisas:\n{visas}")
        if signature:
            parts.append(f"\n\nSignature:\n{signature}")
        full_text = "\n".join(parts)

        date_str = item.get("date", "")
        if date_str:
            date_str = date_str[:10]  # YYYY-MM-DD

        return {
            "wp_id": wp_id,
            "title": title,
            "full_text": full_text,
            "reference": reference,
            "comment": comment,
            "nature": nature_name,
            "nature_id": nature_id,
            "date": date_str,
            "url": item.get("link", ""),
        }

    # ── BaseScraper interface ─────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legal texts from the WordPress REST API."""
        logger.info("Starting DJ/JournalOfficiel full crawl...")
        page = 1
        total_yielded = 0

        while True:
            items = self._fetch_page(page)
            if not items:
                logger.info(f"No more items at page {page}. Stopping.")
                break

            for item in items:
                record = self._parse_record(item)
                if record:
                    total_yielded += 1
                    yield record

            logger.info(f"Page {page}: {len(items)} items fetched, {total_yielded} total yielded")
            page += 1

        logger.info(f"DJ/JournalOfficiel crawl complete: {total_yielded} documents")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents modified after a given date."""
        logger.info(f"Fetching updates since {since.isoformat()}...")
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        page = 1
        total_yielded = 0

        while True:
            params = {
                "per_page": PER_PAGE,
                "page": page,
                "after": since_str,
                "_fields": "id,title,content,acf,date,link",
            }
            try:
                self.rate_limiter.wait()
                resp = self.session.get(TEXTE_ENDPOINT, params=params, timeout=30)
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
                items = resp.json()
            except Exception as e:
                logger.warning(f"Error fetching updates page {page}: {e}")
                break

            if not items:
                break

            for item in items:
                record = self._parse_record(item)
                if record:
                    total_yielded += 1
                    yield record

            page += 1

        logger.info(f"Update complete: {total_yielded} new/modified documents")

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        return {
            "_id": f"DJ_JO_{raw.get('wp_id', '')}",
            "_source": "DJ/JournalOfficiel",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("full_text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "reference": raw.get("reference", ""),
            "nature": raw.get("nature", ""),
            "language": "fr",
        }

    def test_api(self):
        """Quick connectivity and content test."""
        print("Testing DJ/JournalOfficiel API...\n")

        # Test basic endpoint
        items = self._fetch_page(1, per_page=3)
        print(f"API returned {len(items)} items on page 1\n")

        for item in items:
            record = self._parse_record(item)
            if record:
                print(f"  Title: {record['title'][:80]}")
                print(f"  Reference: {record['reference']}")
                print(f"  Nature: {record['nature']}")
                print(f"  Date: {record['date']}")
                print(f"  Text: {len(record['full_text'])} chars")
                print(f"  Preview: {record['full_text'][:150]}...")
                print()

        print("Test complete!")


def main():
    scraper = DJJournalOfficielScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

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
