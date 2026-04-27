#!/usr/bin/env python3
"""
CY/TaxTribunal -- Cyprus Administrative Court Tax Decisions

Fetches tax-related administrative court decisions from CyLaw (cylaw.org).
Targets AAD (Supreme/Appellate) decisions in the administrative jurisdiction
(meros_3) filtered for tax-related content.

Strategy:
  - Fetch yearly index pages for decisions
  - Filter for meros_3 (administrative) and meros_1 decisions with EDD suffix
  - Download each decision HTML, extract full text
  - Keep only those containing tax-related Greek keywords

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch current year
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CY.TaxTribunal")

BASE_URL = "https://www.cylaw.org"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

# Greek keywords indicating tax-related content
TAX_KEYWORDS = [
    "φορολογ",          # tax (root: φορολογία, φορολογικ, etc.)
    "Φορολογ",
    "εισόδημα",         # income
    "εισοδήματ",        # income (variant)
    "ΦΠΑ",             # VAT
    "Φ.Π.Α",           # VAT (with dots)
    "φόρο",            # tax (accusative)
    "Φόρο",
    "φόρου",           # tax (genitive)
    "Τμήμα Φορ",       # Tax Department
    "Έφορο",           # Commissioner (tax)
    "έφορο",
    "τελωνει",         # customs
    "Τελωνει",
]


def _is_tax_related(text: str) -> bool:
    """Check if decision text contains tax-related keywords."""
    for kw in TAX_KEYWORDS:
        if kw in text:
            return True
    return False


def _fetch_page(url: str, timeout: int = 30) -> Optional[bytes]:
    """Fetch raw bytes from a URL."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read()
    except (HTTPError, URLError, Exception) as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def _decode_page(data: bytes) -> str:
    """Decode page with correct encoding (Greek iso-8859-7)."""
    for enc in ["iso-8859-7", "windows-1253", "utf-8"]:
        try:
            text = data.decode(enc)
            # Quick check - if we get lots of replacement chars, wrong encoding
            if text.count("\ufffd") < 10:
                return text
        except (UnicodeDecodeError, ValueError):
            continue
    return data.decode("utf-8", errors="replace")


def _extract_text(html: str) -> str:
    """Extract clean text from HTML decision page."""
    # Remove script and style blocks
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML tags
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<p[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    # Decode HTML entities
    text = html_module.unescape(text)
    # Clean whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    # Strip navigation boilerplate: content starts at ECLI identifier
    ecli_match = re.search(r"(ECLI:CY:[A-Z0-9:]+)", text)
    if ecli_match:
        text = text[ecli_match.start():]
    else:
        # Fallback: find repeated court name (skip first nav occurrence)
        markers = ["ΑΝΩΤΑΤΟ ΔΙΚΑΣΤΗΡΙΟ ΚΥΠΡΟΥ", "ΔΙΟΙΚΗΤΙΚΟ ΔΙΚΑΣΤΗΡΙΟ", "ΕΦΕΤΕΙΟ"]
        for marker in markers:
            # Find second occurrence (first is in nav bar)
            first = text.find(marker)
            if first >= 0:
                second = text.find(marker, first + len(marker))
                if second >= 0:
                    text = text[second:]
                    break
                elif first > 200:
                    text = text[first:]
                    break
    # Strip trailing CyLaw footer
    footer = text.find("cylaw.org : Από το ΚΙΝOΠ")
    if footer > 0:
        text = text[:footer].rstrip()
    return text


def _extract_date(text: str, filename: str) -> Optional[str]:
    """Extract decision date from text or filename."""
    # Try to find date in text (Greek format: DD/MM/YYYY or DD.MM.YYYY)
    date_match = re.search(r"(\d{1,2})[/.·](\d{1,2})[/.·](20\d{2})", text[:500])
    if date_match:
        day, month, year = date_match.groups()
        try:
            return f"{year}-{int(month):02d}-{int(day):02d}"
        except ValueError:
            pass
    # Try from filename: e.g., 3-202403-146-21EDD.htm -> 2024-03
    fn_match = re.search(r"(\d{4})(\d{2})", filename)
    if fn_match:
        year, month = fn_match.groups()
        return f"{year}-{month}-01"
    return None


def _extract_title(text: str) -> str:
    """Extract case title (first line/parties) from decision text."""
    # Take first meaningful line (skip court name)
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    # Skip lines that are just court/jurisdiction identifiers
    for line in lines:
        if len(line) > 20 and " v. " in line or " ν. " in line or "κατά" in line.lower():
            return line[:200]
    # Fall back to first long line
    for line in lines[:5]:
        if len(line) > 20:
            return line[:200]
    return lines[0][:200] if lines else "Unknown"


class CYTaxTribunalScraper(BaseScraper):
    SOURCE_ID = "CY/TaxTribunal"

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir)

    def _get_year_decisions(self, year: int) -> List[Dict[str, str]]:
        """Get all decision links for a given year."""
        url = f"{BASE_URL}/apofaseis/aad/index_{year}.html"
        data = _fetch_page(url)
        if not data:
            return []

        html = _decode_page(data)
        # Find all decision links - focus on meros_3 (admin) but also check
        # meros_1/2 with "EDD" suffix (appeals against Administrative Court)
        pattern = r'href="(/cgi-bin/open\.pl\?file=(/apofaseis/aad/(meros_\d+)/\d{4}/([^"]+\.htm)))"'
        matches = re.findall(pattern, html)

        decisions = []
        for full_href, filepath, meros, filename in matches:
            # Keep: meros_3 (admin jurisdiction) + any with EDD suffix (admin court appeals)
            if meros == "meros_3" or "EDD" in filename:
                decisions.append({
                    "url": BASE_URL + full_href,
                    "filepath": filepath,
                    "meros": meros,
                    "filename": filename,
                    "year": str(year),
                })

        # Also get title text from the links
        link_pattern = (
            r'<a[^>]*href="(/cgi-bin/open\.pl\?file=/apofaseis/aad/meros_3/[^"]+)"[^>]*>'
            r'([^<]+)</a>'
        )
        title_matches = re.findall(link_pattern, html)
        title_map = {href: text.strip() for href, text in title_matches}

        for dec in decisions:
            href_key = dec["url"].replace(BASE_URL, "")
            dec["link_title"] = title_map.get(href_key, "")

        return decisions

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all tax-related decisions."""
        current_year = datetime.now().year
        # Fetch from 2015 onwards (when Administrative Court was established)
        years = list(range(current_year, 2014, -1))

        for year in years:
            logger.info(f"Fetching {year} index...")
            decisions = self._get_year_decisions(year)
            logger.info(f"  {year}: {len(decisions)} admin jurisdiction decisions found")
            time.sleep(2)

            for dec in decisions:
                time.sleep(2)  # Respectful rate limiting
                data = _fetch_page(dec["url"])
                if not data:
                    continue

                html = _decode_page(data)
                text = _extract_text(html)

                if len(text) < 200:
                    continue

                # Filter for tax-related content
                if not _is_tax_related(text):
                    continue

                title = dec.get("link_title") or _extract_title(text)
                date = _extract_date(text, dec["filename"])
                case_id = dec["filename"].replace(".htm", "")

                yield self.normalize({
                    "case_id": case_id,
                    "title": title,
                    "text": text,
                    "url": dec["url"],
                    "date": date,
                    "year": dec["year"],
                    "meros": dec["meros"],
                })

            yield None  # Signal year boundary (filtered out below)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions from the current year."""
        current_year = datetime.now().year
        decisions = self._get_year_decisions(current_year)
        for dec in decisions:
            time.sleep(2)
            data = _fetch_page(dec["url"])
            if not data:
                continue
            html = _decode_page(data)
            text = _extract_text(html)
            if len(text) < 200 or not _is_tax_related(text):
                continue
            title = dec.get("link_title") or _extract_title(text)
            date = _extract_date(text, dec["filename"])
            case_id = dec["filename"].replace(".htm", "")
            yield self.normalize({
                "case_id": case_id,
                "title": title,
                "text": text,
                "url": dec["url"],
                "date": date,
                "year": dec["year"],
                "meros": dec["meros"],
            })

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["case_id"],
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "case_number": raw["case_id"],
            "year": raw.get("year"),
        }


# ─── CLI Entry Point ─────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="CY/TaxTribunal bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    scraper = CYTaxTribunalScraper()

    if args.command == "test":
        decisions = scraper._get_year_decisions(2023)
        print(f"OK: Found {len(decisions)} admin jurisdiction decisions for 2023")
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else 9999

    for record in scraper.fetch_all():
        if record is None:
            continue
        count += 1
        fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        logger.info(f"[{count}] {record['title'][:60]} ({text_len} chars)")

        if count >= limit:
            logger.info(f"Sample limit reached ({limit} records)")
            break

    print(f"\nDone: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
