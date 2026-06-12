#!/usr/bin/env python3
"""
SI/AVK -- Slovenian Competition Protection Agency decisions.

Fetches published decision excerpts from the official AVK website. The site also
links some full PDFs; the HTML excerpts are used as the stable open route for
samples because they contain the official operative part of each decision.
"""

import hashlib
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import urllib3
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SI.AVK")

SOURCE_ID = "SI/AVK"
BASE_URL = "https://www.varstvo-konkurence.si"
SOURCE_PAGES = [
    {
        "path": "/omejevalna-ravnanja/odlocitve-agencije/",
        "category": "restrictive_practices",
    },
    {
        "path": "/koncentracije-podjetij/koncentracije-v-presoji-in-odlocitve-agencije/",
        "category": "merger_control",
    },
]


def clean_text(value: str) -> str:
    value = re.sub(r"[ \t]+", " ", value or "")
    value = re.sub(r"\n\s*\n+", "\n\n", value)
    return value.strip()


def parse_slovenian_date(value: str) -> str:
    match = re.search(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})", value or "")
    if not match:
        return ""
    day, month, year = match.groups()
    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"


def stable_id(*parts: str) -> str:
    raw = "|".join(part for part in parts if part)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"SI_AVK_{digest}"


class AVKScraper(BaseScraper):
    """Scraper for Slovenian AVK published competition decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
                "Accept-Language": "sl,en;q=0.5",
            },
            timeout=60,
            verify=False,
        )

    def _get_html(self, path_or_url: str) -> str:
        self.rate_limiter.wait()
        response = self.client.get(path_or_url)
        response.raise_for_status()
        response.encoding = "utf-8"
        return response.text

    def _discover_detail_links(self) -> list[dict]:
        links = []
        seen = set()
        for page in SOURCE_PAGES:
            logger.info("Fetching AVK source page: %s", page["path"])
            soup = BeautifulSoup(self._get_html(page["path"]), "html.parser")
            for anchor in soup.select("a[href]"):
                href = anchor.get("href", "")
                if "/ostali-dokumenti/arhiv-odlocb/odlocba" not in href:
                    continue
                url = urljoin(BASE_URL, href)
                if url in seen:
                    continue
                seen.add(url)
                links.append(
                    {
                        "detail_url": url,
                        "link_text": clean_text(anchor.get_text(" ", strip=True)),
                        "category": page["category"],
                    }
                )
        return links

    def _parse_detail(self, link: dict) -> dict:
        html = self._get_html(link["detail_url"])
        soup = BeautifulSoup(html, "html.parser")
        content = soup.select_one("#content") or soup.select_one("main") or soup.body
        text = clean_text(content.get_text("\n", strip=True) if content else "")

        # Drop global footer/accessibility boilerplate when present.
        for marker in [
            "Javna agencija Republike Slovenije za varstvo konkurence\nDunajska",
            "Za slepe in slabovidne",
        ]:
            if marker in text:
                text = text.split(marker, 1)[0].strip()

        title = self._extract_title(soup, text, link)
        date = self._extract_date(text)
        pdf_links = [
            urljoin(link["detail_url"], anchor["href"])
            for anchor in soup.select("a[href]")
            if ".pdf" in anchor.get("href", "").lower()
        ]
        case_number = self._extract_case_number(text)

        return {
            **link,
            "title": title,
            "text": text,
            "date": date,
            "case_number": case_number,
            "pdf_url": pdf_links[0] if pdf_links else "",
        }

    def _extract_title(self, soup: BeautifulSoup, text: str, link: dict) -> str:
        heading = soup.select_one("h1")
        if heading:
            value = clean_text(heading.get_text(" ", strip=True))
            if value:
                return value
        first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
        if first_line.startswith("Datum objave"):
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            return lines[1][:160] if len(lines) > 1 else link.get("link_text", "")
        return first_line[:160] or link.get("link_text", "") or "AVK decision"

    def _extract_date(self, text: str) -> str:
        for pattern in [
            r"Datum objave:\s*([0-9. ]+)",
            r"izdala odločbo.*?(\d{1,2}\.\s*\d{1,2}\.\s*\d{4})",
            r"z dne\s+(\d{1,2}\.\s*\d{1,2}\.\s*\d{4})",
        ]:
            match = re.search(pattern, text or "", re.IGNORECASE | re.DOTALL)
            if match:
                parsed = parse_slovenian_date(match.group(1))
                if parsed:
                    return parsed
        return ""

    def _extract_case_number(self, text: str) -> str:
        match = re.search(r"\b(30[67][0-9]-\d+/\d{4}(?:-\d+)?)\b", text or "")
        return match.group(1) if match else ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all AVK decision excerpt pages."""
        for link in self._discover_detail_links():
            record = self._parse_detail(link)
            if len(record.get("text", "")) >= 50:
                yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions published since ``since``."""
        for raw in self.fetch_all():
            date = raw.get("date")
            if not date:
                yield raw
                continue
            try:
                if datetime.fromisoformat(date).date() >= since.date():
                    yield raw
            except ValueError:
                yield raw

    def normalize(self, raw: dict) -> dict:
        """Normalize an AVK decision into the repository schema."""
        doc_id = raw.get("case_number") or stable_id(raw.get("detail_url", ""), raw.get("title", ""))
        return {
            "_id": stable_id(doc_id),
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("detail_url", ""),
            "doc_id": doc_id,
            "case_number": raw.get("case_number", ""),
            "category": raw.get("category", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "language": "sl",
            "jurisdiction": "Slovenia",
        }


def main():
    scraper = AVKScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        sample_size = int(sys.argv[sys.argv.index("--sample-size") + 1])

    if command == "bootstrap":
        stats = scraper.run_sample(n=sample_size) if sample_mode else scraper.bootstrap()
    elif command == "update":
        stats = scraper.update()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
