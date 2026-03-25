#!/usr/bin/env python3
"""
ID/DirektoriPutusan -- Indonesian Supreme Court Decision Directory Fetcher

Fetches case law from Direktori Putusan Mahkamah Agung at
putusan3.mahkamahagung.go.id.

Strategy:
  - HTML scraping of the public directory (CodeIgniter application)
  - Directory listing pages with pagination (25 results/page)
  - Individual case detail pages provide structured metadata table
  - Full text extracted from PDF attachments via pdfminer
  - PDF boilerplate disclaimer text is stripped

Endpoints:
  - Directory: https://putusan3.mahkamahagung.go.id/direktori.html
  - Category: https://putusan3.mahkamahagung.go.id/direktori/index/kategori/{slug}.html
  - Detail: https://putusan3.mahkamahagung.go.id/direktori/putusan/{hex-id}.html
  - PDF: https://putusan3.mahkamahagung.go.id/direktori/download_file/{file-id}/pdf/{case-id}

Data:
  - 10M+ court decisions from all Indonesian courts
  - Language: Indonesian (Bahasa Indonesia)
  - Rate limit: ~0.5 req/sec (site is slow, 5-15s response times)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
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
from typing import Generator, Optional, Dict, Any, List
from io import BytesIO

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ID.DirektoriPutusan")

BASE_URL = "https://putusan3.mahkamahagung.go.id"

# Boilerplate disclaimer text that appears on every PDF page
DISCLAIMER_PATTERNS = [
    r"Direktori Putusan Mahkamah Agung Republik Indonesia\s*\n\s*putusan\.mahkamahagung\.go\.id",
    r"Mahkamah Agung Republik Indonesia\n(?:Mahkamah Agung Republik Indonesia\n)+",
    r"Disclaimer\nKepaniteraan Mahkamah Agung.*?(?:pelaksanaan fungsi peradilan|kepaniteraan@mahkamahagung\.go\.id)\s*\.?\s*",
    r"Halaman \d+ dari \d+",
    r"h\s*\n\s*Disclaimer",
]


class IndonesianCourtScraper(BaseScraper):
    """
    Scraper for ID/DirektoriPutusan -- Indonesian Court Decisions.
    Country: ID
    URL: https://putusan3.mahkamahagung.go.id

    Data types: case_law
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "id,en",
            },
            timeout=90,
        )

    def _get_with_retry(self, url: str, max_retries: int = 3) -> Optional[Any]:
        """GET with retry logic for the frequently-timing-out site."""
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait()
                if url.startswith("http"):
                    resp = self.client.session.get(url, timeout=90)
                else:
                    resp = self.client.get(url)
                resp.raise_for_status()
                return resp
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 5
                    logger.debug(f"Retry {attempt+1} for {url}: {e}, waiting {wait}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"Failed after {max_retries} attempts: {url}: {e}")
                    return None

    def _extract_case_ids_from_listing(self, html_content: str) -> List[str]:
        """Extract case hex IDs from a directory listing page."""
        return list(set(re.findall(
            r'direktori/putusan/([a-f0-9]+)\.html', html_content
        )))

    def _extract_pagination_max(self, html_content: str) -> int:
        """Extract the maximum page number from pagination."""
        pages = re.findall(r'data-ci-pagination-page="(\d+)"', html_content)
        if pages:
            return max(int(p) for p in pages)
        return 1

    def _extract_categories(self, html_content: str) -> List[Dict[str, str]]:
        """Extract category slugs and names from the directory page."""
        cats = []
        pattern = re.compile(
            r'href="[^"]*direktori/index/kategori/([^/"]+)\.html"[^>]*>\s*([^<]+)',
        )
        seen = set()
        for match in pattern.finditer(html_content):
            slug = match.group(1).strip()
            name = match.group(2).strip()
            if slug not in seen and slug != "semua":
                seen.add(slug)
                cats.append({"slug": slug, "name": name})
        return cats

    def _fetch_case_detail(self, case_id: str) -> Optional[Dict[str, Any]]:
        """Fetch case detail page and extract metadata."""
        resp = self._get_with_retry(
            f"{BASE_URL}/direktori/putusan/{case_id}.html"
        )
        if not resp:
            return None

        if "500 - Exception" in resp.text or "kesalahan" in resp.text[:500]:
            logger.debug(f"Error page for case {case_id}")
            return None

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")

        meta = {"case_id": case_id}

        # Extract heading for case number and court
        h2 = soup.find("h2")
        if h2:
            heading = h2.get_text(strip=True)
            meta["heading"] = heading

        # Extract metadata from table rows
        field_map = {
            "Nomor": "nomor",
            "Tingkat Proses": "tingkat_proses",
            "Klasifikasi": "klasifikasi",
            "Kata Kunci": "kata_kunci",
            "Tahun": "tahun",
            "Tanggal Register": "tanggal_register",
            "Lembaga Peradilan": "lembaga_peradilan",
            "Jenis Lembaga Peradilan": "jenis_lembaga",
            "Hakim Ketua": "hakim_ketua",
            "Hakim Anggota": "hakim_anggota",
            "Panitera": "panitera",
            "Amar": "amar",
            "Catatan Amar": "catatan_amar",
            "Tanggal Musyawarah": "tanggal_musyawarah",
            "Tanggal Dibacakan": "tanggal_dibacakan",
            "Kaidah": "kaidah",
            "Status": "status_putusan",
            "Abstrak": "abstrak",
        }

        table = soup.find("table", class_="table")
        if table:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    label = tds[0].get_text(strip=True)
                    value = tds[1].get_text(strip=True)
                    if label in field_map and value and value != "—":
                        meta[field_map[label]] = value

        # Extract download links
        dl_links = re.findall(
            r'href="([^"]*download_file[^"]*)"', resp.text
        )
        meta["pdf_url"] = None
        for dl in dl_links:
            if "/pdf/" in dl:
                meta["pdf_url"] = dl
                break

        return meta

    def _download_full_text(self, pdf_url: str) -> str:
        """Download PDF and extract full text, stripping boilerplate."""
        resp = self._get_with_retry(pdf_url)
        if not resp or len(resp.content) < 500:
            return ""

        try:
            from pdfminer.high_level import extract_text
            text = extract_text(BytesIO(resp.content))
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

        # Strip boilerplate
        for pattern in DISCLAIMER_PATTERNS:
            text = re.sub(pattern, "", text, flags=re.DOTALL | re.IGNORECASE)

        # Clean up whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        return text

    def _enumerate_directory(
        self, sample_mode: bool = False, sample_size: int = 12
    ) -> Generator[str, None, None]:
        """
        Enumerate case IDs from the directory.
        In sample mode, just get from the first page.
        In full mode, paginate through categories.
        """
        seen = set()

        if sample_mode:
            # Just use the main directory page
            resp = self._get_with_retry(f"{BASE_URL}/direktori.html")
            if resp:
                ids = self._extract_case_ids_from_listing(resp.text)
                for cid in ids:
                    if cid not in seen:
                        seen.add(cid)
                        yield cid
                        if len(seen) >= sample_size * 2:
                            return
        else:
            # Get categories first
            resp = self._get_with_retry(f"{BASE_URL}/direktori.html")
            if not resp:
                return

            categories = self._extract_categories(resp.text)
            logger.info(f"Found {len(categories)} categories")

            for cat in categories:
                slug = cat["slug"]
                name = cat["name"]
                page = 1

                while True:
                    url = f"{BASE_URL}/direktori/index/kategori/{slug}/page/{page}.html"
                    resp = self._get_with_retry(url)
                    if not resp:
                        break

                    ids = self._extract_case_ids_from_listing(resp.text)
                    if not ids:
                        break

                    new_count = 0
                    for cid in ids:
                        if cid not in seen:
                            seen.add(cid)
                            new_count += 1
                            yield cid

                    if new_count == 0:
                        break

                    max_page = self._extract_pagination_max(resp.text)
                    if page >= max_page or page >= 499:
                        break

                    page += 1

                logger.info(f"Category {name}: enumerated through page {page}")

    def _fetch_cases(self, sample_mode: bool = False, sample_size: int = 12) -> Generator[dict, None, None]:
        """Yield court decisions with full text from PDFs."""
        for case_id in self._enumerate_directory(sample_mode=sample_mode, sample_size=sample_size):
            meta = self._fetch_case_detail(case_id)
            if not meta:
                continue

            pdf_url = meta.get("pdf_url")
            if not pdf_url:
                logger.debug(f"No PDF for {meta.get('nomor', case_id)}")
                continue

            full_text = self._download_full_text(pdf_url)
            if not full_text:
                logger.debug(
                    f"No text extracted for {meta.get('nomor', case_id)}"
                )
                continue

            meta["full_text"] = full_text
            yield meta

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all court decisions with full text from PDFs."""
        yield from self._fetch_cases(sample_mode=False)

    def fetch_sample(self, n: int = 12) -> Generator[dict, None, None]:
        """Yield a small sample of court decisions."""
        yield from self._fetch_cases(sample_mode=True, sample_size=n)

    def run_sample(self, n: int = 12) -> dict:
        """Override to use directory page instead of full category crawl."""
        from datetime import datetime, timezone
        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "sample_records_saved": 0,
            "errors": 0,
        }
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        for raw in self.fetch_sample(n=n):
            record = self.normalize(raw)
            if not record:
                stats["errors"] += 1
                continue
            fname = record["_id"].replace("/", "_").replace(" ", "_")[:80]
            out = sample_dir / f"{fname}.json"
            out.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            stats["sample_records_saved"] += 1
            logger.info(f"Sample {stats['sample_records_saved']}: {record.get('title', '')[:60]}")
            if stats["sample_records_saved"] >= n:
                break

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        return stats

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recent decisions from the main directory page."""
        resp = self._get_with_retry(f"{BASE_URL}/direktori.html")
        if not resp:
            return

        case_ids = self._extract_case_ids_from_listing(resp.text)

        for case_id in case_ids:
            meta = self._fetch_case_detail(case_id)
            if not meta:
                continue

            pdf_url = meta.get("pdf_url")
            if not pdf_url:
                continue

            full_text = self._download_full_text(pdf_url)
            if full_text:
                meta["full_text"] = full_text
                yield meta

    def _parse_indonesian_date(self, date_str: str) -> str:
        """Parse Indonesian date formats to ISO 8601."""
        if not date_str or date_str == "—":
            return ""

        # Try DD Month_ID YYYY format: "27 Agustus 2019"
        months_id = {
            "januari": "01", "februari": "02", "maret": "03",
            "april": "04", "mei": "05", "juni": "06",
            "juli": "07", "agustus": "08", "september": "09",
            "oktober": "10", "november": "11", "desember": "12",
        }

        m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_str)
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            year = int(m.group(3))
            month = months_id.get(month_name, "")
            if month:
                return f"{year:04d}-{month}-{day:02d}"

        # Try DD-MM-YYYY
        m = re.match(r"(\d{2})-(\d{2})-(\d{4})", date_str)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

        return date_str

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        case_id = raw.get("case_id", "")
        nomor = raw.get("nomor", "")
        full_text = raw.get("full_text", "")
        lembaga = raw.get("lembaga_peradilan", "")

        # Parse decision date
        date_str = raw.get("tanggal_dibacakan", raw.get("tanggal_musyawarah", ""))
        date_iso = self._parse_indonesian_date(date_str)

        # Build title
        title = nomor
        if lembaga:
            title = f"{nomor} - {lembaga}"

        url = f"{BASE_URL}/direktori/putusan/{case_id}.html"

        return {
            "_id": f"ID-MA-{nomor}" if nomor else f"ID-MA-{case_id}",
            "_source": "ID/DirektoriPutusan",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": url,
            "nomor": nomor,
            "tingkat_proses": raw.get("tingkat_proses", ""),
            "klasifikasi": raw.get("klasifikasi", ""),
            "kata_kunci": raw.get("kata_kunci", ""),
            "tahun": raw.get("tahun", ""),
            "lembaga_peradilan": lembaga,
            "jenis_lembaga": raw.get("jenis_lembaga", ""),
            "hakim_ketua": raw.get("hakim_ketua", ""),
            "hakim_anggota": raw.get("hakim_anggota", ""),
            "panitera": raw.get("panitera", ""),
            "amar": raw.get("amar", ""),
            "catatan_amar": raw.get("catatan_amar", ""),
            "status_putusan": raw.get("status_putusan", ""),
            "kaidah": raw.get("kaidah", ""),
            "abstrak": raw.get("abstrak", ""),
            "language": "ind",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Indonesian Court Decision Directory...")

        print("\n1. Testing directory page...")
        try:
            resp = self._get_with_retry(f"{BASE_URL}/direktori.html")
            if resp:
                case_ids = self._extract_case_ids_from_listing(resp.text)
                cats = self._extract_categories(resp.text)
                print(f"   Status: OK")
                print(f"   Cases on page: {len(case_ids)}")
                print(f"   Categories: {len(cats)}")
                if cats:
                    print(f"   Sample categories: {', '.join(c['name'] for c in cats[:5])}")
            else:
                print("   FAILED")
                return
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n2. Fetching a case detail...")
        if case_ids:
            for cid in case_ids[:5]:
                meta = self._fetch_case_detail(cid)
                if meta and meta.get("nomor"):
                    print(f"   Case: {meta.get('nomor')}")
                    print(f"   Court: {meta.get('lembaga_peradilan', 'N/A')}")
                    print(f"   Category: {meta.get('klasifikasi', 'N/A')}")
                    print(f"   Date: {meta.get('tanggal_dibacakan', 'N/A')}")
                    print(f"   Has PDF: {bool(meta.get('pdf_url'))}")

                    if meta.get("pdf_url"):
                        print("\n3. Testing PDF download...")
                        text = self._download_full_text(meta["pdf_url"])
                        print(f"   Text length: {len(text)} chars")
                        if text:
                            print(f"   Preview: {text[:200]}...")
                    break
            else:
                print("   No valid case found in first 5 attempts")

        print("\nTest complete!")


def main():
    scraper = IndonesianCourtScraper()

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
