#!/usr/bin/env python3
"""
UY/IMPO -- Uruguayan Legislation via Parlamento del Uruguay

Fetches full text of Uruguayan laws from the Parliament's open data.

Strategy:
  - Enumerate laws via Parlamento JSON API (date-range filtering)
  - For each law, fetch the Parlamento page to get the embedded iframe URL
  - Fetch the HTM document from infolegislativa.parlamento.gub.uy for full text
  - Clean HTML to plain text

Data: Public (Parlamento del Uruguay open access).
Rate limit: 2 sec between parlamento requests, 10 sec for IMPO (robots.txt).

Coverage: Laws ~9500 (1935) to ~20468 (2026), approximately 11,000 laws.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample laws
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

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UY.IMPO")

PARLAMENTO_BASE = "https://parlamento.gub.uy"
PARLAMENTO_JSON = f"{PARLAMENTO_BASE}/documentosyleyes/leyes/json"


def html_to_text(html_content: str) -> str:
    """Extract clean text from a Parlamento HTM law document."""
    if not html_content:
        return ""

    content = html_content

    # Remove script and style blocks
    content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)

    # Preserve headings
    content = re.sub(r'<h[1-6][^>]*>(.*?)</h[1-6]>', r'\n\n## \1\n', content, flags=re.DOTALL | re.IGNORECASE)

    # Preserve paragraph breaks
    content = re.sub(r'<p[^>]*>', '\n\n', content, flags=re.IGNORECASE)
    content = re.sub(r'</p>', '', content, flags=re.IGNORECASE)

    # Preserve line breaks and divs as newlines
    content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
    content = re.sub(r'<div[^>]*>', '\n', content, flags=re.IGNORECASE)
    content = re.sub(r'</div>', '', content, flags=re.IGNORECASE)

    # Preserve list items
    content = re.sub(r'<li[^>]*>', '\n  - ', content, flags=re.IGNORECASE)

    # Remove all remaining HTML tags
    content = re.sub(r'<[^>]+>', '', content)

    # Decode HTML entities
    content = html_module.unescape(content)

    # Clean whitespace
    content = re.sub(r'[ \t]+', ' ', content)
    content = re.sub(r'\n[ \t]+', '\n', content)
    content = re.sub(r'\n{3,}', '\n\n', content)

    return content.strip()


class IMPOScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html, application/json",
            },
            timeout=60,
        )

    def test_api(self):
        """Test connectivity to Parlamento JSON API."""
        logger.info("Testing Parlamento JSON API connectivity...")
        try:
            resp = self.http.get(
                PARLAMENTO_JSON,
                params={"Fechadesde": "2020-01-01", "Fechahasta": "2020-06-30"},
            )
            logger.info(f"  Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"  Laws returned: {len(data)}")
                if len(data) > 0 and "Numero_de_Ley" in data[0]:
                    logger.info("Connectivity test PASSED")
                    return True
            logger.error("Connectivity test FAILED: unexpected response")
            return False
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            return False

    def enumerate_laws(self, sample: bool = False) -> list:
        """Enumerate all laws via Parlamento JSON API using date ranges."""
        if sample:
            # For sample, get laws from a few different decades
            ranges = [
                ("2020-01-01", "2020-06-30"),  # Recent
                ("2010-01-01", "2010-06-30"),  # 2010s
                ("2000-01-01", "2000-06-30"),  # 2000s
            ]
        else:
            # Full enumeration: 5-year windows from 1935 to 2026
            ranges = []
            for start_year in range(1935, 2027, 5):
                end_year = min(start_year + 4, 2026)
                ranges.append((f"{start_year}-01-01", f"{end_year}-12-31"))

        all_laws = []
        seen_numbers = set()

        for date_from, date_to in ranges:
            logger.info(f"  Fetching laws {date_from} to {date_to}...")
            time.sleep(2)
            try:
                resp = self.http.get(
                    PARLAMENTO_JSON,
                    params={"Fechadesde": date_from, "Fechahasta": date_to},
                )
                if resp.status_code != 200:
                    logger.warning(f"  HTTP {resp.status_code} for range {date_from}-{date_to}")
                    continue

                data = resp.json()
                for law in data:
                    num = law.get("Numero_de_Ley", "").strip()
                    if num and num not in seen_numbers:
                        seen_numbers.add(num)
                        all_laws.append(law)

                logger.info(f"  Got {len(data)} laws, total unique: {len(all_laws)}")

                if sample and len(all_laws) >= 30:
                    break

            except Exception as e:
                logger.warning(f"  Failed to fetch range {date_from}-{date_to}: {e}")

        return all_laws

    def get_full_text_url(self, law_number: str) -> Optional[str]:
        """Get the infolegislativa HTM URL for a law from its Parlamento page."""
        url = f"{PARLAMENTO_BASE}/documentosyleyes/leyes/ley/{law_number}"
        try:
            resp = self.http.get(url)
            if resp.status_code != 200:
                logger.warning(f"  HTTP {resp.status_code} for law page {law_number}")
                return None

            # Extract iframe src pointing to infolegislativa
            match = re.search(
                r'<iframe[^>]*src="(https://infolegislativa\.parlamento\.gub\.uy/[^"]+)"',
                resp.text,
                re.IGNORECASE,
            )
            if match:
                return match.group(1)

            # Fallback: look for any infolegislativa link
            match = re.search(
                r'href="(https://infolegislativa\.parlamento\.gub\.uy/[^"]+\.htm)"',
                resp.text,
                re.IGNORECASE,
            )
            if match:
                return match.group(1)

            logger.warning(f"  No infolegislativa URL found for law {law_number}")
            return None

        except Exception as e:
            logger.warning(f"  Failed to get full text URL for law {law_number}: {e}")
            return None

    def fetch_law(self, law_meta: dict) -> Optional[dict]:
        """Fetch a single law with full text."""
        law_number = law_meta.get("Numero_de_Ley", "").strip()
        if not law_number:
            return None

        logger.info(f"  Fetching law {law_number}...")
        time.sleep(2)

        # Get the infolegislativa URL
        htm_url = self.get_full_text_url(law_number)
        if not htm_url:
            return None

        # Fetch the actual HTM content
        time.sleep(2)
        try:
            resp = self.http.get(htm_url)
            if resp.status_code != 200:
                logger.warning(f"  HTTP {resp.status_code} for HTM {law_number}")
                return None

            # Server reports ISO-8859-1 but content is actually UTF-8
            html_content = resp.content.decode("utf-8", errors="replace")

            text = html_to_text(html_content)
            if not text or len(text) < 20:
                logger.warning(f"  No text extracted for law {law_number}")
                return None

            # Parse promulgation date
            raw_date = law_meta.get("Promulgacion", "")
            iso_date = None
            if raw_date:
                try:
                    parts = raw_date.split("-")
                    if len(parts) == 3:
                        iso_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
                except Exception:
                    pass

            title = html_module.unescape(law_meta.get("Titulo", f"Ley N° {law_number}"))

            return {
                "law_number": law_number,
                "title": title,
                "text": text,
                "date": iso_date,
                "promulgation_raw": raw_date,
                "url_parlamento": law_meta.get("Texto_Original", ""),
                "url_impo": law_meta.get("Texto_Actualizado", ""),
                "htm_url": htm_url,
                "referenced_laws": law_meta.get("Leyes_Referenciadas", ""),
            }

        except Exception as e:
            logger.warning(f"  Failed to fetch HTM for law {law_number}: {e}")
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw law data into standard schema."""
        if not raw or not raw.get("text") or len(raw["text"]) < 20:
            return None

        law_num = raw["law_number"]

        return {
            "_id": f"UY-Ley-{law_num}",
            "_source": "UY/IMPO",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url_parlamento") or raw.get("url_impo", ""),
            "law_number": law_num,
            "url_impo": raw.get("url_impo", ""),
            "url_parlamento": raw.get("url_parlamento", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all Uruguayan laws with full text."""
        sample_limit = 15 if sample else None
        count = 0

        logger.info("Enumerating laws from Parlamento JSON API...")
        laws = self.enumerate_laws(sample=sample)
        logger.info(f"Found {len(laws)} laws to process")

        if sample:
            # Spread sample across the list
            step = max(1, len(laws) // 15)
            laws = laws[::step][:20]  # Take evenly spaced subset

        for law_meta in laws:
            raw = self.fetch_law(law_meta)
            if not raw:
                continue

            record = self.normalize(raw)
            if record:
                count += 1
                logger.info(f"  [{count}] Ley {record['law_number']} — {len(record['text'])} chars")
                yield record

                if sample_limit and count >= sample_limit:
                    logger.info(f"Sample limit ({sample_limit}) reached")
                    return

        logger.info(f"Total laws fetched: {count}")

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Fetch laws updated since a given date."""
        if hasattr(since, 'strftime'):
            date_str = since.strftime("%Y-%m-%d")
        else:
            date_str = str(since)

        logger.info(f"Fetching laws since {date_str}...")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            resp = self.http.get(
                PARLAMENTO_JSON,
                params={"Fechadesde": date_str, "Fechahasta": today},
            )
            if resp.status_code == 200:
                laws = resp.json()
                for law_meta in laws:
                    raw = self.fetch_law(law_meta)
                    if raw:
                        record = self.normalize(raw)
                        if record:
                            yield record
        except Exception as e:
            logger.error(f"Failed to fetch updates: {e}")

    def bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = Path(self.source_dir) / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in self.fetch_all(sample=sample):
            out_file = sample_dir / f"{record['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)
            count += 1
            logger.info(f"Saved: {out_file.name}")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="UY/IMPO bootstrapper")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 laws)")
    args = parser.parse_args()

    scraper = IMPOScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = scraper.bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        sys.exit(0)


if __name__ == "__main__":
    main()
