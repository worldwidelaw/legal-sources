#!/usr/bin/env python3
"""
INTL/UNOOSA-SpaceLaw -- UN Office for Outer Space Affairs Space Law

Fetches the full text of:
  1. Five UN space treaties (Outer Space Treaty, Rescue Agreement, etc.)
  2. Five GA resolution principles/declarations
  3. National space legislation hosted on UNOOSA (~9 documents)

All content is on static HTML pages scraped via curl + BeautifulSoup.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.UNOOSA-SpaceLaw")

SOURCE_ID = "INTL/UNOOSA-SpaceLaw"
BASE_URL = "https://www.unoosa.org"
MIN_TEXT_CHARS = 200

# ----- Document definitions -----

TREATIES = [
    {
        "id": "outer-space-treaty",
        "path": "/oosa/en/ourwork/spacelaw/treaties/outerspacetreaty.html",
        "title": "Treaty on Principles Governing the Activities of States in the Exploration and Use of Outer Space, including the Moon and Other Celestial Bodies",
        "short_title": "Outer Space Treaty",
        "date": "1967-01-27",
        "ga_resolution": "2222 (XXI)",
    },
    {
        "id": "rescue-agreement",
        "path": "/oosa/en/ourwork/spacelaw/treaties/rescueagreement.html",
        "title": "Agreement on the Rescue of Astronauts, the Return of Astronauts and the Return of Objects Launched into Outer Space",
        "short_title": "Rescue Agreement",
        "date": "1968-04-22",
        "ga_resolution": "2345 (XXII)",
    },
    {
        "id": "liability-convention",
        "path": "/oosa/en/ourwork/spacelaw/treaties/liability-convention.html",
        "title": "Convention on International Liability for Damage Caused by Space Objects",
        "short_title": "Liability Convention",
        "date": "1972-03-29",
        "ga_resolution": "2777 (XXVI)",
    },
    {
        "id": "registration-convention",
        "path": "/oosa/en/ourwork/spacelaw/treaties/registration-convention.html",
        "title": "Convention on Registration of Objects Launched into Outer Space",
        "short_title": "Registration Convention",
        "date": "1975-01-14",
        "ga_resolution": "3235 (XXIX)",
    },
    {
        "id": "moon-agreement",
        "path": "/oosa/en/ourwork/spacelaw/treaties/moon-agreement.html",
        "title": "Agreement Governing the Activities of States on the Moon and Other Celestial Bodies",
        "short_title": "Moon Agreement",
        "date": "1979-12-18",
        "ga_resolution": "34/68",
    },
]

PRINCIPLES = [
    {
        "id": "legal-principles",
        "path": "/oosa/en/ourwork/spacelaw/principles/legal-principles.html",
        "title": "Declaration of Legal Principles Governing the Activities of States in the Exploration and Use of Outer Space",
        "short_title": "Declaration of Legal Principles",
        "date": "1963-12-13",
        "ga_resolution": "1962 (XVIII)",
    },
    {
        "id": "dbs-principles",
        "path": "/oosa/en/ourwork/spacelaw/principles/dbs-principles.html",
        "title": "Principles Governing the Use by States of Artificial Earth Satellites for International Direct Television Broadcasting",
        "short_title": "Direct Broadcasting Principles",
        "date": "1982-12-10",
        "ga_resolution": "37/92",
    },
    {
        "id": "remote-sensing-principles",
        "path": "/oosa/en/ourwork/spacelaw/principles/remote-sensing-principles.html",
        "title": "Principles Relating to Remote Sensing of the Earth from Outer Space",
        "short_title": "Remote Sensing Principles",
        "date": "1986-12-03",
        "ga_resolution": "41/65",
    },
    {
        "id": "nps-principles",
        "path": "/oosa/en/ourwork/spacelaw/principles/nps-principles.html",
        "title": "Principles Relevant to the Use of Nuclear Power Sources in Outer Space",
        "short_title": "Nuclear Power Sources Principles",
        "date": "1992-12-14",
        "ga_resolution": "47/68",
    },
    {
        "id": "space-benefits-declaration",
        "path": "/oosa/en/ourwork/spacelaw/principles/space-benefits-declaration.html",
        "title": "Declaration on International Cooperation in the Exploration and Use of Outer Space for the Benefit and in the Interest of All States",
        "short_title": "Space Benefits Declaration",
        "date": "1996-12-13",
        "ga_resolution": "51/122",
    },
]

NATIONAL_LEGISLATION = [
    {
        "id": "de-space-act-1998",
        "path": "/oosa/en/ourwork/spacelaw/nationalspacelaw/germany/raueg_1998G.html",
        "title": "Satellitendatensicherheitsgesetz (SatDSiG) — German Space Act",
        "country": "DE",
        "date": "1998-01-01",
        "language": "de",
    },
    {
        "id": "nl-space-activities-act",
        "path": "/oosa/en/ourwork/spacelaw/nationalspacelaw/netherlands/space_activities_actE.html",
        "title": "Rules concerning space activities and the establishment of a registry of space objects — Netherlands Space Activities Act",
        "country": "NL",
        "date": "2007-01-01",
        "language": "en",
    },
    {
        "id": "no-act-38-1969",
        "path": "/oosa/en/ourwork/spacelaw/nationalspacelaw/norway/act_38_1969E.html",
        "title": "Act No. 38 of 13 June 1969 relating to the Launching of Objects from Norwegian Territory into Outer Space",
        "country": "NO",
        "date": "1969-06-13",
        "language": "en",
    },
    {
        "id": "kr-space-development-act",
        "path": "/oosa/en/ourwork/spacelaw/nationalspacelaw/republic_of_korea/space_development_promotions_actE.html",
        "title": "Space Development Promotion Act — Republic of Korea",
        "country": "KR",
        "date": "2005-01-01",
        "language": "en",
    },
    {
        "id": "za-space-affairs-act-1993",
        "path": "/oosa/en/ourwork/spacelaw/nationalspacelaw/south_africa/space_affairs_act_1993E.html",
        "title": "Space Affairs Act, 1993 (Act No. 84 of 1993) — South Africa",
        "country": "ZA",
        "date": "1993-06-23",
        "language": "en",
    },
    {
        "id": "za-space-affairs-amendment-1995",
        "path": "/oosa/en/ourwork/spacelaw/nationalspacelaw/south_africa/space_affairs_amendment_act_1995E.html",
        "title": "Space Affairs Amendment Act, 1995 (Act No. 64 of 1995) — South Africa",
        "country": "ZA",
        "date": "1995-01-01",
        "language": "en",
    },
    {
        "id": "se-act-on-space-activities-1982",
        "path": "/oosa/en/ourwork/spacelaw/nationalspacelaw/sweden/act_on_space_activities_1982E.html",
        "title": "Act on Space Activities (1982:963) — Sweden",
        "country": "SE",
        "date": "1982-01-01",
        "language": "en",
    },
    {
        "id": "se-decree-on-space-activities-1982",
        "path": "/oosa/en/ourwork/spacelaw/nationalspacelaw/sweden/decree_on_space_activities_1982E.html",
        "title": "Decree on Space Activities (1982:1069) — Sweden",
        "country": "SE",
        "date": "1982-01-01",
        "language": "en",
    },
    {
        "id": "ua-ordinance-on-space-activity-1996",
        "path": "/oosa/en/ourwork/spacelaw/nationalspacelaw/ukraine/ordinance_on_space_activity_1996E.html",
        "title": "Ordinance of Ukraine on Space Activity, 1996 — Ukraine",
        "country": "UA",
        "date": "1996-11-15",
        "language": "en",
    },
]


def curl_get(url: str, timeout: int = 60, retries: int = 2) -> bytes:
    """Fetch a URL using curl subprocess with retries."""
    cmd = ["curl", "-sL", "--http1.1", "--max-time", str(timeout), url]
    for attempt in range(retries + 1):
        result = subprocess.run(cmd, capture_output=True, timeout=timeout + 30)
        if result.returncode == 0 and len(result.stdout) > 0:
            return result.stdout
        if attempt < retries:
            time.sleep(2)
    raise RuntimeError(f"curl failed (exit {result.returncode}) for {url}")


def extract_text_from_html(html_bytes: bytes) -> str:
    """Extract main content text from a UNOOSA HTML page."""
    html = html_bytes.decode("utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one("#contentContainer")
    if not content:
        # Fallback: find the largest div with treaty-relevant text
        for div in soup.find_all("div"):
            text = div.get_text(strip=True)
            if len(text) > 500:
                content = div
                break
    if not content:
        return ""
    # Remove navigation elements
    for nav in content.find_all(["nav", "header", "footer"]):
        nav.decompose()
    text = content.get_text(separator="\n", strip=True)
    # Clean up excessive whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class UNOOSASpaceLawScraper(BaseScraper):
    """Scraper for UNOOSA Space Law documents."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)

    def _fetch_document(self, doc_info: dict, doc_type: str) -> Optional[dict]:
        """Fetch and normalize a single document."""
        url = BASE_URL + doc_info["path"]
        logger.info(f"Fetching: {doc_info.get('short_title', doc_info['title'][:60])}")

        try:
            time.sleep(1)
            html_bytes = curl_get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        text = extract_text_from_html(html_bytes)
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text: {len(text)} chars for {doc_info['title'][:60]}")
            return None

        record = {
            "_id": doc_info["id"],
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": doc_info["title"],
            "text": text,
            "date": doc_info.get("date"),
            "url": url,
            "doc_type": doc_type,
            "body": "United Nations",
        }

        if doc_type == "treaty" or doc_type == "principle":
            record["ga_resolution"] = doc_info.get("ga_resolution", "")
            record["short_title"] = doc_info.get("short_title", "")
            record["country"] = "INTL"
        elif doc_type == "national_legislation":
            record["country"] = doc_info.get("country", "")
            record["language"] = doc_info.get("language", "en")

        return record

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all UNOOSA Space Law documents."""
        logger.info("=== Fetching UN Space Treaties ===")
        for doc in TREATIES:
            record = self._fetch_document(doc, "treaty")
            if record:
                yield record

        logger.info("=== Fetching GA Resolution Principles ===")
        for doc in PRINCIPLES:
            record = self._fetch_document(doc, "principle")
            if record:
                yield record

        logger.info("=== Fetching National Space Legislation ===")
        for doc in NATIONAL_LEGISLATION:
            record = self._fetch_document(doc, "national_legislation")
            if record:
                yield record

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/UNOOSA-SpaceLaw bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UNOOSASpaceLawScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        html = curl_get(BASE_URL + TREATIES[0]["path"])
        text = extract_text_from_html(html)
        logger.info(f"Outer Space Treaty: {len(text)} chars — OK")
        return

    sample_mode = args.sample and not args.full
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    for record in scraper.fetch_all():
        if sample_mode:
            fname = sample_dir / f"{record['_id']}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)
        count += 1
        logger.info(f"  Saved: {record['title'][:70]} ({len(record['text'])} chars)")

    logger.info(f"Done. Total records: {count}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
