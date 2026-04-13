#!/usr/bin/env python3
"""
ID/PeraturanGO -- Database Peraturan Indonesia (DITJEN PP)

Fetches Indonesian legislation from peraturan.go.id.

Strategy:
  - HTML scraping of listing pages (20 per page, paginated)
  - Detail pages for metadata (jenis, nomor, tahun, date, status)
  - PDF download via /files/{slug}.pdf
  - Full text extraction from PDFs via pdfminer

Endpoints:
  - Listings: GET /uu?page={n}, /pp?page={n}, /perpres?page={n}, etc.
  - Detail: GET /id/{slug}
  - PDF: GET /files/{slug}.pdf

Data:
  - 62K+ regulations from 1926 to present
  - Language: Indonesian (Bahasa Indonesia)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from io import BytesIO

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ID.PeraturanGO")

BASE_URL = "https://peraturan.go.id"

# Regulation type URL paths (priority order for national legislation)
REG_TYPES = [
    ("uu", "UNDANG-UNDANG"),
    ("perppu", "PERATURAN PEMERINTAH PENGGANTI UNDANG-UNDANG"),
    ("pp", "PERATURAN PEMERINTAH"),
    ("perpres", "PERATURAN PRESIDEN"),
    ("permen", "PERATURAN MENTERI"),
    ("perda", "PERATURAN DAERAH"),
]

# Indonesian month names
BULAN = {
    "januari": "01", "februari": "02", "maret": "03", "april": "04",
    "mei": "05", "juni": "06", "juli": "07", "agustus": "08",
    "september": "09", "oktober": "10", "november": "11", "desember": "12",
}


def parse_indonesian_date(text: str) -> str:
    """Parse Indonesian date like '02 Januari 2026' to ISO format."""
    text = text.strip().lower()
    m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if m:
        day = m.group(1).zfill(2)
        month = BULAN.get(m.group(2), "")
        year = m.group(3)
        if month:
            return f"{year}-{month}-{day}"
    return ""


class PeraturanGOScraper(BaseScraper):
    """
    Scraper for ID/PeraturanGO -- Database Peraturan Indonesia.
    Country: ID
    URL: https://peraturan.go.id/

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "id,en;q=0.5",
            },
            timeout=60,
        )

    def _get_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page with retry logic."""
        for attempt in range(3):
            try:
                self.rate_limiter.wait()
                resp = self.client.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                if attempt < 2:
                    time.sleep((attempt + 1) * 3)
                else:
                    logger.warning(f"Failed to fetch {url}: {e}")
                    return None

    def _parse_listing(self, html: str) -> List[Dict]:
        """Parse a listing page to extract regulation slugs and basic info."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        results = []

        # Find links to detail pages
        for link in soup.select("a[href]"):
            href = link.get("href", "")
            if not href.startswith("/id/") or href == "/id/#":
                continue
            slug = href.replace("/id/", "").strip("/")
            if not slug or slug == "#":
                continue
            title = link.get_text(strip=True)
            if title:
                results.append({"slug": slug, "title": title})

        return results

    def _parse_detail(self, html: str) -> Dict:
        """Parse a detail page for metadata."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        meta = {}

        table = soup.select_one("table")
        if table:
            for row in table.select("tr"):
                cells = row.select("td")
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).lower()
                    val = cells[1].get_text(strip=True)
                    if "jenis" in key or "bentuk" in key:
                        meta["jenis"] = val
                    elif key == "nomor":
                        meta["nomor"] = val
                    elif key == "tahun":
                        meta["tahun"] = val
                    elif "tentang" in key:
                        meta["tentang"] = val
                    elif "ditetapkan tanggal" in key:
                        meta["tanggal"] = val
                    elif key == "status":
                        meta["status"] = val
                    elif "pemrakarsa" in key:
                        meta["pemrakarsa"] = val

        # Full title from h1 or title tag
        h1 = soup.select_one("h1, h2, h3")
        if h1:
            meta["full_title"] = h1.get_text(strip=True)

        return meta

    def _download_pdf_text(self, slug: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="ID/PeraturanGO",
            source_id="",
            pdf_bytes=slug,
            table="legislation",
        ) or ""

    def _scrape_listing_pages(self, reg_path: str) -> Generator[Dict, None, None]:
        """Yield items from all pages of a regulation type listing."""
        page = 1
        while True:
            url = f"{BASE_URL}/{reg_path}?page={page}"
            html = self._get_page(url)
            if not html:
                break

            items = self._parse_listing(html)
            if not items:
                break

            for item in items:
                yield item

            # Check if there are more pages
            if f"page={page + 1}" not in html and f"page={page+1}" not in html:
                break
            page += 1

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulations with slugs and basic info."""
        for reg_path, reg_name in REG_TYPES:
            logger.info(f"Scraping {reg_name} ({reg_path})...")
            count = 0
            for item in self._scrape_listing_pages(reg_path):
                item["reg_type"] = reg_name
                item["reg_path"] = reg_path
                yield item
                count += 1
            logger.info(f"  {reg_name}: {count} items")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recent regulations (first page of each type)."""
        for reg_path, reg_name in REG_TYPES:
            url = f"{BASE_URL}/{reg_path}?page=1"
            html = self._get_page(url)
            if html:
                for item in self._parse_listing(html):
                    item["reg_type"] = reg_name
                    item["reg_path"] = reg_path
                    yield item

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw listing item into standard schema with full text."""
        slug = raw.get("slug", "")
        if not slug:
            return None

        # Get detail page metadata
        detail_html = self._get_page(f"{BASE_URL}/id/{slug}")
        meta = self._parse_detail(detail_html) if detail_html else {}

        # Build full title
        jenis = meta.get("jenis", raw.get("reg_type", ""))
        nomor = meta.get("nomor", "")
        tahun = meta.get("tahun", "")
        tentang = meta.get("tentang", "")
        title = raw.get("title", "")
        if tentang and jenis:
            title = f"{jenis} Nomor {nomor} Tahun {tahun} tentang {tentang}"

        # Download and extract PDF text
        full_text = self._download_pdf_text(slug)
        if not full_text:
            logger.debug(f"No text for: {slug}")
            return None

        # Parse date
        date_iso = ""
        if meta.get("tanggal"):
            date_iso = parse_indonesian_date(meta["tanggal"])
        if not date_iso and tahun and str(tahun).isdigit():
            date_iso = f"{tahun}-01-01"

        return {
            "_id": f"ID-PPID-{slug}",
            "_source": "ID/PeraturanGO",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": f"{BASE_URL}/id/{slug}",
            "jenis": jenis,
            "nomor": nomor,
            "tahun": tahun,
            "tentang": tentang,
            "status": meta.get("status", ""),
            "pemrakarsa": meta.get("pemrakarsa", ""),
            "language": "ind",
        }

    def run_sample(self, n: int = 12) -> dict:
        """Fetch sample records from different regulation types."""
        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "sample_records_saved": 0,
            "errors": 0,
        }
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        # Get samples from top regulation types, continuing until we have enough
        samples_per_type = max(2, n // len(REG_TYPES) + 1)
        for reg_path, reg_name in REG_TYPES:
            url = f"{BASE_URL}/{reg_path}?page=1"
            html = self._get_page(url)
            if not html:
                continue

            items = self._parse_listing(html)
            type_count = 0

            for item in items:
                if type_count >= samples_per_type:
                    break

                item["reg_type"] = reg_name
                item["reg_path"] = reg_path
                record = self.normalize(item)
                if not record:
                    stats["errors"] += 1
                    continue

                fname = record["_id"].replace("/", "_").replace(" ", "_")[:80]
                out = sample_dir / f"{fname}.json"
                out.write_text(json.dumps(record, ensure_ascii=False, indent=2))
                stats["sample_records_saved"] += 1
                type_count += 1
                logger.info(
                    f"Sample {stats['sample_records_saved']}: [{reg_name}] "
                    f"{record['title'][:60]} ({len(record['text'])} chars)"
                )

                if stats["sample_records_saved"] >= n:
                    break

            if stats["sample_records_saved"] >= n:
                break

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        return stats

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing PeraturanGO (peraturan.go.id)...")

        print("\n1. Testing listing page...")
        html = self._get_page(f"{BASE_URL}/uu?page=1")
        if html:
            items = self._parse_listing(html)
            print(f"   Laws found on page 1: {len(items)}")
            if items:
                slug = items[0]["slug"]
                print(f"   First: {items[0]['title'][:80]} ({slug})")

                print("\n2. Testing detail page...")
                detail_html = self._get_page(f"{BASE_URL}/id/{slug}")
                if detail_html:
                    meta = self._parse_detail(detail_html)
                    for k, v in meta.items():
                        print(f"   {k}: {v[:60] if len(str(v)) > 60 else v}")

                print("\n3. Testing PDF download...")
                text = self._download_pdf_text(slug)
                print(f"   Text length: {len(text)} chars")
                if text:
                    print(f"   Preview: {text[:200]}...")
        else:
            print("   FAILED")

        print("\nTest complete!")


def main():
    scraper = PeraturanGOScraper()

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
    main()
