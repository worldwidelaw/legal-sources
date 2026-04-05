#!/usr/bin/env python3
"""
ZA/SAFLII-CaseLaw -- South Africa case law from all courts via SAFLII

Fetches full-text court decisions from the Southern African Legal Information
Institute (SAFLII). Covers 48 South African courts including Constitutional
Court, Supreme Court of Appeal, High Courts, Labour Courts, Tax Court, etc.

Data access:
  - Court index at /za/cases/ lists all courts
  - Each court has year-based listings: /za/cases/{COURT}/{YEAR}/
  - Each case has full HTML text: /za/cases/{COURT}/{YEAR}/{NUM}.html
  - Requires browser-like User-Agent header

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZA.SAFLII-CaseLaw")

BASE_URL = "https://www.saflii.org"
DELAY = 2.0

# Major courts to scrape (subset for sample, full list for bootstrap)
PRIORITY_COURTS = [
    "ZACC",      # Constitutional Court
    "ZASCA",     # Supreme Court of Appeal
    "ZAGPJHC",   # High Court - Gauteng (Johannesburg)
    "ZAWCHC",    # High Court - Western Cape
    "ZAKZDHC",   # High Court - KwaZulu-Natal
    "ZAECGHC",   # High Court - Eastern Cape (Grahamstown)
    "ZAFSHC",    # High Court - Free State
    "ZALMPHC",   # High Court - Limpopo
    "ZALAC",     # Labour Appeal Court
    "ZALCJHB",   # Labour Court - Johannesburg
    "ZACT",      # Competition Tribunal
]


class HTMLTextExtractor(HTMLParser):
    """Extract text content from HTML, stripping all tags."""

    def __init__(self):
        super().__init__()
        self.result = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False
        if tag in ("p", "br", "div", "h1", "h2", "h3", "h4", "tr", "li"):
            self.result.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self.result.append(data)

    def handle_entityref(self, name):
        from html import unescape
        self.result.append(unescape(f"&{name};"))

    def handle_charref(self, name):
        from html import unescape
        self.result.append(unescape(f"&#{name};"))

    def get_text(self):
        text = "".join(self.result)
        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


def strip_html(html_content: str) -> str:
    """Strip HTML tags and return clean text."""
    extractor = HTMLTextExtractor()
    extractor.feed(html_content)
    return extractor.get_text()


def parse_date_from_title(title: str) -> Optional[str]:
    """Extract date from case title like '(9 January 2025)'."""
    m = re.search(r"\((\d{1,2}\s+\w+\s+\d{4})\)\s*$", title)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def parse_citation(title: str) -> Optional[str]:
    """Extract citation like '[2025] ZASCA 1' from title."""
    m = re.search(r"\[(\d{4})\]\s+([A-Z]+)\s+(\d+)", title)
    if m:
        return f"[{m.group(1)}] {m.group(2)} {m.group(3)}"
    return None


class SAFLIICaseLawScraper(BaseScraper):
    """Scraper for South African case law from SAFLII."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
        )

    def _get_all_courts(self) -> List[str]:
        """Get all court codes from the South Africa index page."""
        url = f"{BASE_URL}/content/south-africa-index.html"
        resp = self.http.get(url)
        if resp.status_code != 200:
            logger.warning("Failed to fetch court index: %s", resp.status_code)
            return PRIORITY_COURTS
        codes = re.findall(r'href="/za/cases/([A-Z]+)/"', resp.text)
        return sorted(set(codes)) if codes else PRIORITY_COURTS

    def _get_years(self, court_code: str) -> List[str]:
        """Get available years for a court."""
        url = f"{BASE_URL}/za/cases/{court_code}/"
        time.sleep(DELAY)
        resp = self.http.get(url)
        if resp.status_code != 200:
            logger.warning("Failed to fetch years for %s: %s", court_code, resp.status_code)
            return []
        years = re.findall(r'href="(\d{4})/"', resp.text)
        return sorted(set(years), reverse=True)

    def _get_cases_for_year(self, court_code: str, year: str) -> List[Dict[str, str]]:
        """Get case links and titles from a year listing page."""
        url = f"{BASE_URL}/za/cases/{court_code}/{year}/"
        time.sleep(DELAY)
        resp = self.http.get(url)
        if resp.status_code != 200:
            logger.warning("Failed to fetch %s/%s: %s", court_code, year, resp.status_code)
            return []

        cases = []
        # Pattern: <a href="../2025/1.html" ...>Title (may contain nested tags)</a>
        pattern = r'<a\s+href="\.\./(\d{4}/\d+)\.html"[^>]*>(.*?)</a>'
        for match in re.finditer(pattern, resp.text, re.DOTALL):
            path = match.group(1)
            title_html = match.group(2)
            title = re.sub(r"<[^>]+>", "", title_html).strip()
            if title:
                cases.append({
                    "path": f"/za/cases/{court_code}/{path}.html",
                    "title": title,
                })
        return cases

    def _fetch_case(self, case_path: str) -> Optional[str]:
        """Fetch full text HTML for a single case."""
        url = f"{BASE_URL}{case_path}"
        time.sleep(DELAY)
        resp = self.http.get(url)
        if resp.status_code != 200:
            logger.warning("Failed to fetch case %s: %s", case_path, resp.status_code)
            return None
        return resp.text

    def _extract_judgment_text(self, html: str) -> str:
        """Extract the judgment text from the case HTML page.

        The judgment text is between the second <HR> and the last <HR> in the page.
        """
        # Split on <hr> or <HR> tags
        parts = re.split(r"<[Hh][Rr]\s*/?>", html)
        if len(parts) >= 3:
            # Main content is typically between 2nd and last HR
            content = "<HR>".join(parts[2:-1]) if len(parts) > 3 else parts[2]
        elif len(parts) >= 2:
            content = parts[-1]
        else:
            content = html

        return strip_html(content)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw SAFLII case into standard schema."""
        title = raw.get("title", "")
        court_code = raw.get("court_code", "")
        case_path = raw.get("path", "")
        html_content = raw.get("html", "")

        text = self._extract_judgment_text(html_content)
        date = parse_date_from_title(title)
        citation = parse_citation(title)

        # Build unique ID from court code and case number
        path_parts = case_path.rstrip("/").split("/")
        case_num = path_parts[-1].replace(".html", "") if path_parts else ""
        year = path_parts[-2] if len(path_parts) >= 2 else ""
        doc_id = f"SAFLII-{court_code}-{year}-{case_num}"

        return {
            "_id": doc_id,
            "_source": "ZA/SAFLII-CaseLaw",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}{case_path}",
            "court": court_code,
            "citation": citation,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all cases from all courts, all years."""
        courts = self._get_all_courts()
        logger.info("Found %d courts to scrape", len(courts))

        for court in courts:
            years = self._get_years(court)
            logger.info("Court %s: %d years available", court, len(years))

            for year in years:
                cases = self._get_cases_for_year(court, year)
                logger.info("Court %s/%s: %d cases", court, year, len(cases))

                for case_info in cases:
                    html = self._fetch_case(case_info["path"])
                    if html is None:
                        continue

                    raw = {
                        "title": case_info["title"],
                        "court_code": court,
                        "path": case_info["path"],
                        "html": html,
                    }
                    record = self.normalize(raw)
                    if record["text"] and len(record["text"]) > 100:
                        yield record
                    else:
                        logger.warning("Skipping %s: insufficient text", case_info["path"])

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Yield recent cases (current year) from priority courts."""
        current_year = str(datetime.now().year)
        courts = PRIORITY_COURTS

        for court in courts:
            cases = self._get_cases_for_year(court, current_year)
            logger.info("Updates: Court %s/%s: %d cases", court, current_year, len(cases))

            for case_info in cases:
                html = self._fetch_case(case_info["path"])
                if html is None:
                    continue

                raw = {
                    "title": case_info["title"],
                    "court_code": court,
                    "path": case_info["path"],
                    "html": html,
                }
                record = self.normalize(raw)
                if record["text"] and len(record["text"]) > 100:
                    yield record

    def test_connection(self) -> bool:
        """Test that we can access SAFLII."""
        url = f"{BASE_URL}/za/cases/ZASCA/2025/"
        resp = self.http.get(url)
        if resp.status_code == 200:
            logger.info("Connection test passed: SAFLII is accessible")
            return True
        logger.error("Connection test failed: status %s", resp.status_code)
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="ZA/SAFLII-CaseLaw bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10+ sample records")
    args = parser.parse_args()

    scraper = SAFLIICaseLawScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)

    if args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            scraper.storage.save(record)
            count += 1
            if count % 10 == 0:
                logger.info("Saved %d records", count)
        logger.info("Update complete: %d records", count)
        return

    # bootstrap (optionally with --sample)
    sample_dir = Path(__file__).resolve().parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.sample:
        # Sample mode: fetch from ZASCA (Supreme Court of Appeal) 2025
        count = 0
        target = 12
        courts_to_sample = ["ZASCA", "ZACC"]

        for court in courts_to_sample:
            if count >= target:
                break
            cases = scraper._get_cases_for_year(court, "2025")
            logger.info("Sample: Court %s/2025: %d cases available", court, len(cases))

            for case_info in cases[:target - count]:
                html = scraper._fetch_case(case_info["path"])
                if html is None:
                    continue

                raw = {
                    "title": case_info["title"],
                    "court_code": court,
                    "path": case_info["path"],
                    "html": html,
                }
                record = scraper.normalize(raw)
                if record["text"] and len(record["text"]) > 100:
                    fname = f"{record['_id']}.json"
                    with open(sample_dir / fname, "w", encoding="utf-8") as f:
                        json.dump(record, f, ensure_ascii=False, indent=2)
                    count += 1
                    logger.info(
                        "Sample %d/%d: %s (%d chars)",
                        count, target, record["_id"], len(record["text"])
                    )
                else:
                    logger.warning("Skipping %s: insufficient text", case_info["path"])

            if count >= target:
                break

        logger.info("Sample complete: %d records saved to %s", count, sample_dir)
    else:
        # Full bootstrap
        count = 0
        for record in scraper.fetch_all():
            scraper.storage.save(record)
            count += 1
            if count % 100 == 0:
                logger.info("Saved %d records", count)
        logger.info("Bootstrap complete: %d records", count)


if __name__ == "__main__":
    main()
