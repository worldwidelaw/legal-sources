#!/usr/bin/env python3
"""
IN/IndiaCode — India Code Digital Repository of Central Acts

Fetches Indian central legislation (acts) with full text from indiacode.nic.in.

Strategy:
  - Browse all central acts via paginated DSpace browse endpoint
  - For each act, parse the act page to extract metadata and section links
  - For each section, fetch full text via the SectionPageContent AJAX endpoint
  - Combine all sections into a single full-text field per act

Data:
  - 843+ Central Acts (1836-present)
  - Full text at section-level granularity
  - Metadata: act number, year, ministry, enactment date

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent acts
  python bootstrap.py test               # Quick connectivity test
"""

import argparse
import html
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional

import requests
from bs4 import BeautifulSoup
import yaml

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.IndiaCode")

BASE_URL = "https://www.indiacode.nic.in"
CENTRAL_ACTS_HANDLE = "123456789/1362"
BROWSE_URL = f"{BASE_URL}/handle/{CENTRAL_ACTS_HANDLE}/browse"
SECTION_API = f"{BASE_URL}/SectionPageContent"


def clean_html(raw_html: str) -> str:
    """Strip HTML tags, decode entities, normalize whitespace."""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    # Remove script and style
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = html.unescape(text)
    # Normalize whitespace
    lines = [line.strip() for line in text.splitlines()]
    text = "\n".join(line for line in lines if line)
    return text.strip()


class IndiaCodeScraper(BaseScraper):
    """
    Scraper for IN/IndiaCode — Indian Central Acts.
    Country: IN
    URL: https://www.indiacode.nic.in/
    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._last_request = 0

    def _rate_limited_get(self, url: str, **kwargs) -> requests.Response:
        """Make a rate-limited GET request with retry."""
        elapsed = time.time() - self._last_request
        if elapsed < 2.0:
            time.sleep(2.0 - elapsed)
        self._last_request = time.time()

        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=30, **kwargs)
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning(f"Retry {attempt+1}/3 for {url}: {e}")
                time.sleep(5 * (attempt + 1))

    # ── Browse acts ──────────────────────────────────────────────────

    def _browse_acts(self) -> Generator[Dict, None, None]:
        """Iterate through the paginated browse page to list all central acts."""
        offset = 0
        rpp = 100
        while True:
            url = f"{BROWSE_URL}?type=shorttitle&order=ASC&rpp={rpp}&offset={offset}"
            logger.info(f"Browsing acts: offset={offset}")
            resp = self._rate_limited_get(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Parse table rows — each row has: enactment_date | act_number | title | view_link
            rows = soup.select("table.miscTable tr")
            if not rows:
                # Try alternative: look for the data in td elements
                rows = []
                for td in soup.find_all("td", class_=re.compile(r"(even|odd)Row")):
                    rows.append(td)

            count = 0
            # Parse rows in groups of 4 cells (date, number, title, view)
            cells = soup.find_all("td", class_=re.compile(r"(even|odd)Row"))
            for i in range(0, len(cells), 4):
                if i + 3 >= len(cells):
                    break
                date_cell = cells[i]
                number_cell = cells[i + 1]
                title_cell = cells[i + 2]
                view_cell = cells[i + 3]

                view_link = view_cell.find("a", href=True)
                if not view_link:
                    continue

                href = view_link["href"]
                handle_match = re.search(r"handle/123456789/(\d+)", href)
                if not handle_match:
                    continue

                handle_id = handle_match.group(1)
                title = title_cell.get_text(strip=True)
                act_number = number_cell.get_text(strip=True)
                enactment_date = date_cell.get_text(strip=True)

                yield {
                    "handle_id": handle_id,
                    "title": title,
                    "act_number": act_number,
                    "enactment_date_raw": enactment_date,
                }
                count += 1

            if count == 0:
                break
            offset += rpp
            if count < rpp:
                break

    # ── Fetch act details ────────────────────────────────────────────

    def _fetch_act_page(self, handle_id: str) -> Optional[Dict]:
        """Fetch act page to extract metadata and section links."""
        url = f"{BASE_URL}/handle/123456789/{handle_id}?view_type=browse"
        resp = self._rate_limited_get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract metadata from metadataFieldValue cells
        metadata = {}
        for row in soup.find_all("tr"):
            label_cell = row.find("td", class_="metadataFieldLabel")
            value_cell = row.find("td", class_="metadataFieldValue")
            if label_cell and value_cell:
                label = label_cell.get_text(strip=True).rstrip(":").strip()
                value = value_cell.get_text(strip=True)
                if label == "Act ID":
                    metadata["act_id_numeric"] = value
                elif label == "Enactment Date":
                    metadata["enactment_date"] = value
                elif label == "Ministry":
                    metadata["ministry"] = value
                elif label == "Department":
                    metadata["department"] = value
                elif label == "Enforcement Date":
                    metadata["enforcement_date"] = value

        # Extract section links: show-data?...actid=...&sectionId=...&orderno=...
        sections = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "show-data" not in href or "actid=" not in href:
                continue

            # Note: &sect in URLs gets decoded as § (section symbol) by HTML parsers
            # So sectionId becomes §ionId and sectionno becomes §ionno
            actid_m = re.search(r"actid=([^&§]+)", href)
            secid_m = re.search(r"(?:[sS]ection[iI]d|§ionId)=(\d+)", href)
            orderno_m = re.search(r"orderno=(\d+)", href)
            secno_m = re.search(r"(?:[sS]ection[nN]o|§ionno)=([^&§]+)", href)

            if actid_m and secid_m:
                section_title = link.get_text(strip=True)
                sections.append({
                    "actid": actid_m.group(1),
                    "section_id": secid_m.group(1),
                    "orderno": orderno_m.group(1) if orderno_m else None,
                    "section_no": secno_m.group(1) if secno_m else None,
                    "title": section_title,
                })

        metadata["sections"] = sections

        # Extract short title from page
        title_tag = soup.find("h2")
        if title_tag:
            metadata["page_title"] = title_tag.get_text(strip=True)

        return metadata

    # ── Fetch section full text ──────────────────────────────────────

    def _fetch_section_text(self, actid: str, section_id: str) -> Optional[str]:
        """Fetch section full text via the SectionPageContent AJAX endpoint."""
        url = f"{SECTION_API}?actid={actid}&sectionID={section_id}"
        try:
            resp = self._rate_limited_get(url)
            data = resp.json()
            content = data.get("content", "")
            footnote = data.get("footnote", "")
            text = clean_html(content)
            if footnote:
                fn_text = clean_html(footnote)
                if fn_text:
                    text += "\n\n[Footnotes]\n" + fn_text
            return text
        except Exception as e:
            logger.warning(f"Failed to fetch section {section_id}: {e}")
            return None

    # ── Combine into full act text ───────────────────────────────────

    def _fetch_act_full_text(self, sections: List[Dict]) -> str:
        """Fetch and combine all section texts for an act."""
        parts = []
        for sec in sections:
            sec_text = self._fetch_section_text(sec["actid"], sec["section_id"])
            if sec_text:
                header = f"Section {sec['section_no']}" if sec.get("section_no") else f"Section (order {sec.get('orderno', '?')})"
                if sec.get("title"):
                    header += f". {sec['title']}"
                parts.append(f"--- {header} ---\n{sec_text}")
        return "\n\n".join(parts)

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all central acts with full text."""
        for act_info in self._browse_acts():
            handle_id = act_info["handle_id"]
            title = act_info["title"]
            logger.info(f"Fetching act: {title} (handle={handle_id})")

            try:
                act_page = self._fetch_act_page(handle_id)
                if not act_page or not act_page.get("sections"):
                    logger.warning(f"No sections found for {title}")
                    continue

                sections = act_page["sections"]
                logger.info(f"  → {len(sections)} sections")

                full_text = self._fetch_act_full_text(sections)
                if not full_text:
                    logger.warning(f"  → No text extracted for {title}")
                    continue

                yield {
                    "handle_id": handle_id,
                    "title": title,
                    "act_number": act_info.get("act_number"),
                    "enactment_date_raw": act_info.get("enactment_date_raw"),
                    "enactment_date": act_page.get("enactment_date"),
                    "ministry": act_page.get("ministry"),
                    "department": act_page.get("department"),
                    "enforcement_date": act_page.get("enforcement_date"),
                    "act_id_numeric": act_page.get("act_id_numeric"),
                    "section_count": len(sections),
                    "text": full_text,
                }
            except Exception as e:
                logger.error(f"Error fetching {title}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recently enacted acts. IndiaCode doesn't have a date filter,
        so we browse all acts and filter by enactment date."""
        for raw in self.fetch_all():
            date_str = raw.get("enactment_date")
            if date_str:
                try:
                    act_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if act_date >= since:
                        yield raw
                except ValueError:
                    # Can't parse date, include it to be safe
                    yield raw

    def normalize(self, raw: dict) -> dict:
        """Transform raw act data into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        # Parse enactment date
        date = raw.get("enactment_date")
        if not date and raw.get("enactment_date_raw"):
            try:
                dt = datetime.strptime(raw["enactment_date_raw"], "%d-%b-%Y")
                date = dt.strftime("%Y-%m-%d")
            except ValueError:
                date = None

        # Build year from title or date
        year = None
        if date:
            year = date[:4]
        else:
            year_match = re.search(r",\s*(\d{4})", raw.get("title", ""))
            if year_match:
                year = year_match.group(1)

        act_id = raw.get("act_id_numeric") or raw.get("handle_id")

        return {
            "_id": f"IN_IndiaCode_{act_id}",
            "_source": "IN/IndiaCode",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", "").rstrip("."),
            "text": text,
            "date": date,
            "url": f"{BASE_URL}/handle/123456789/{raw['handle_id']}?view_type=browse",
            "act_number": raw.get("act_number"),
            "year": year,
            "ministry": raw.get("ministry"),
            "department": raw.get("department"),
            "enforcement_date": raw.get("enforcement_date"),
            "section_count": raw.get("section_count"),
            "country": "IN",
            "jurisdiction": "Central",
        }


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="IN/IndiaCode data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to execute")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only sample records")
    parser.add_argument("--sample-size", type=int, default=15,
                        help="Number of sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IndiaCodeScraper()

    if args.command == "test":
        logger.info("Testing connectivity to India Code...")
        resp = scraper.session.get(f"{BASE_URL}/handle/{CENTRAL_ACTS_HANDLE}/browse?type=shorttitle&rpp=5", timeout=30)
        logger.info(f"Status: {resp.status_code}, Length: {len(resp.text)}")
        # Try fetching one act from first page only
        acts = []
        for act in scraper._browse_acts():
            acts.append(act)
            if len(acts) >= 5:
                break
        if acts:
            logger.info(f"Found {len(acts)} acts in first batch")
            first = acts[0]
            logger.info(f"First act: {first['title']} (handle={first['handle_id']})")
            page = scraper._fetch_act_page(first["handle_id"])
            if page and page.get("sections"):
                logger.info(f"Sections: {len(page['sections'])}")
                sec = page["sections"][0]
                text = scraper._fetch_section_text(sec["actid"], sec["section_id"])
                logger.info(f"Section text length: {len(text) if text else 0}")
        logger.info("Test complete.")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
