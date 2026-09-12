#!/usr/bin/env python3
"""
DK/Landsskatteretten -- Danish National Tax Tribunal Decisions

Fetches decisions from Landsskatteretten (the Danish Tax Tribunal) published
on info.skat.dk. These are principled decisions on tax, VAT, customs, and duties.

Strategy:
  - Scan year listing pages on info.skat.dk for entries with SKM suffix ".LSR"
  - Fetch individual decision pages to extract full text and metadata
  - Filter by SKM number suffix (LSR = Landsskatteretten)

Data source:
  - Year listing: https://info.skat.dk/data.aspx?oid={year_oid}
  - Individual: https://info.skat.dk/data.aspx?oid={decision_oid}

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (latest year)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
import time
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
logger = logging.getLogger("legal-data-hunter.DK.Landsskatteretten")

BASE_URL = "https://info.skat.dk"

# Year OIDs on info.skat.dk (discovered from navigation)
YEAR_OIDS = {
    2026: 939,
    2025: 491,
    2024: 69324,
    2023: 68564,
    2022: 68262,
    2021: 54223,
    2020: 16352,
    2019: 10921,
    2018: 9464,
    2017: 2924,
    2016: 2393,
}


class LandsskatterettenScraper(BaseScraper):
    """
    Scraper for DK/Landsskatteretten -- Danish National Tax Tribunal.
    Country: DK
    URL: https://info.skat.dk

    Data types: case_law (tax tribunal decisions)
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all Landsskatteretten decisions from all available years."""
        for year in sorted(YEAR_OIDS.keys(), reverse=True):
            logger.info(f"Scanning year {year} (oid={YEAR_OIDS[year]})")
            yield from self._fetch_year(year)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch decisions from the current year only."""
        current_year = datetime.now().year
        if current_year in YEAR_OIDS:
            yield from self._fetch_year(current_year)

    def _fetch_year(self, year: int) -> Generator[dict, None, None]:
        """Fetch all LSR decisions from a given year page."""
        oid = YEAR_OIDS[year]
        url = f"/data.aspx?oid={oid}"

        resp = self.client.get(url)
        if not resp or resp.status_code != 200:
            logger.warning(f"Failed to fetch year page {year} (oid={oid})")
            return

        html_content = resp.text
        # Extract LSR entries: SKM number and OID pairs
        entries = re.findall(
            r'<td[^>]*>(SKM\d+\.\d+\.LSR)</td><td[^>]*><a href="data\.aspx\?oid=(\d+)"',
            html_content,
        )

        logger.info(f"Found {len(entries)} Landsskatteretten decisions in {year}")

        for skm, decision_oid in entries:
            time.sleep(1)  # Rate limit
            record = self._fetch_decision(skm, decision_oid)
            if record:
                yield record

    def _fetch_decision(self, skm: str, oid: str) -> Optional[dict]:
        """Fetch and parse a single decision page."""
        url = f"/data.aspx?oid={oid}"
        resp = self.client.get(url)
        if not resp or resp.status_code != 200:
            logger.warning(f"Failed to fetch decision {skm} (oid={oid})")
            return None

        html_content = resp.text
        record = self._parse_decision(html_content, skm, oid)
        if record:
            normalized = self.normalize(record)
            if normalized and normalized.get("text"):
                return normalized
            else:
                logger.warning(f"No text extracted for {skm}")
        return None

    def _parse_decision(self, html_content: str, skm: str, oid: str) -> Optional[dict]:
        """Parse structured metadata and full text from a decision page."""
        raw = {"skm": skm, "oid": oid}

        # Extract metadata from the structured table
        meta_rows = re.findall(
            r'<div class="edge edge1">(.*?)</div></td><td class="edge edge2">(.*?)</td>',
            html_content,
            re.DOTALL,
        )
        for label, value in meta_rows:
            clean_label = self._strip_html(label).strip()
            clean_value = self._strip_html(value).strip()
            if "SKM-nummer" in clean_label:
                raw["skm_number"] = clean_value
            elif "Sagsnummer" in clean_label:
                raw["case_number"] = clean_value
            elif "Myndighed" in clean_label:
                raw["authority"] = clean_value
            elif "Dato for udgivelse" in clean_label:
                raw["publication_date"] = clean_value
            elif "Dato for afsagt" in clean_label:
                raw["decision_date"] = clean_value
            elif "Dokument type" in clean_label:
                raw["document_type"] = clean_value
            elif "Overordnede emner" in clean_label:
                raw["main_topic"] = clean_value
            elif "Overemner-emner" in clean_label:
                raw["sub_topic"] = clean_value
            elif "Emneord" in clean_label:
                raw["keywords"] = clean_value
            elif "Resum" in clean_label:
                raw["resume"] = clean_value
            elif "Reference" in clean_label:
                raw["references"] = clean_value

        # Extract title from the page
        title_match = re.search(r"<h1[^>]*>(.*?)</h1>", html_content)
        if title_match:
            raw["title"] = self._strip_html(title_match.group(1)).strip()

        # Extract full text from main content area (after metadata table)
        # The content is in a div after the closing </table> of the metadata
        text = self._extract_full_text(html_content)
        raw["full_text"] = text

        return raw

    def _extract_full_text(self, html_content: str) -> str:
        """Extract the full decision text from the page."""
        # Find content after the metadata table - look for the main text body
        # The text area starts after the edge-table and is in the content div
        # Strategy: find the last </table> before the main text, then get content until footer

        # Look for the content between the edge table end and the side menu
        match = re.search(
            r'</table>\s*</div>\s*<div[^>]*>(.*?)<div[^>]*class="[^"]*MPlink',
            html_content,
            re.DOTALL,
        )
        if match:
            text_html = match.group(1)
        else:
            # Fallback: get everything after the last edge2 cell
            match = re.search(
                r'Henvisning.*?</tr>\s*</tbody>\s*</table>\s*</div>\s*(.*?)<div[^>]*class="[^"]*MPlink',
                html_content,
                re.DOTALL,
            )
            if match:
                text_html = match.group(1)
            else:
                # Last resort: get everything in mainContent after the table
                match = re.search(
                    r'id="mainContent"[^>]*>.*?</table>(.*?)<div[^>]*class="[^"]*MPlink',
                    html_content,
                    re.DOTALL,
                )
                if match:
                    text_html = match.group(1)
                else:
                    return ""

        # Clean HTML to plain text
        text = self._strip_html(text_html)
        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _strip_html(self, text: str) -> str:
        """Remove HTML tags and decode entities."""
        text = re.sub(r"<br\s*/?>", "\n", text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = html_module.unescape(text)
        text = re.sub(r"&nbsp;", " ", text)
        return text

    def _parse_danish_date(self, date_str: str) -> Optional[str]:
        """Parse Danish date format to ISO 8601."""
        if not date_str:
            return None
        # Format: "08 maj 2026 13:33" or similar
        months = {
            "jan": "01", "feb": "02", "mar": "03", "apr": "04",
            "maj": "05", "jun": "06", "jul": "07", "aug": "08",
            "sep": "09", "okt": "10", "nov": "11", "dec": "12",
        }
        match = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_str)
        if match:
            day, month_str, year = match.groups()
            month = months.get(month_str.lower()[:3], "01")
            return f"{year}-{month}-{int(day):02d}"
        return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw parsed data into the standard schema."""
        skm = raw.get("skm_number") or raw.get("skm", "")
        title = raw.get("title", "")
        text = raw.get("full_text", "")

        if not text or len(text) < 100:
            return None

        decision_date = self._parse_danish_date(raw.get("decision_date", ""))
        publication_date = self._parse_danish_date(raw.get("publication_date", ""))

        # Build topics string
        topics = []
        if raw.get("main_topic"):
            topics.append(raw["main_topic"])
        if raw.get("sub_topic"):
            topics.append(raw["sub_topic"])

        return {
            "_id": skm or f"oid-{raw.get('oid', '')}",
            "_source": "DK/Landsskatteretten",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title or skm,
            "text": text,
            "date": decision_date or publication_date,
            "url": f"{BASE_URL}/data.aspx?oid={raw.get('oid', '')}",
            "skm_number": skm,
            "case_number": raw.get("case_number", ""),
            "authority": raw.get("authority", "Landsskatteretten"),
            "publication_date": publication_date,
            "decision_date": decision_date,
            "document_type": raw.get("document_type", "Afgørelse"),
            "topics": ", ".join(topics) if topics else "",
            "keywords": raw.get("keywords", ""),
            "resume": raw.get("resume", ""),
            "references": raw.get("references", ""),
        }

    # --- CLI interface ---

    def run_bootstrap(self, sample: bool = False):
        """Run full bootstrap or sample mode."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if sample else 10000

        for record in self.fetch_all():
            if count >= max_records:
                break

            # Save sample
            filename = re.sub(r"[^\w\-.]", "_", record["_id"]) + ".json"
            with open(sample_dir / filename, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            count += 1
            logger.info(
                f"[{count}] {record['_id']}: {record['title'][:60]}... "
                f"({len(record['text'])} chars)"
            )

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        return count

    def run_test_api(self):
        """Quick connectivity test."""
        logger.info("Testing connectivity to info.skat.dk...")
        resp = self.client.get("/data.aspx?oid=939")
        if resp and resp.status_code == 200:
            entries = re.findall(r"SKM\d+\.\d+\.LSR", resp.text)
            logger.info(f"OK - Year 2026 page has {len(entries)} LSR entries")
            return True
        else:
            logger.error("FAILED to connect to info.skat.dk")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="DK/Landsskatteretten scraper")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    args = parser.parse_args()

    scraper = LandsskatterettenScraper()

    if args.command == "test-api":
        scraper.run_test_api()
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        scraper.run_bootstrap(sample=True)  # Just fetch latest year


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
