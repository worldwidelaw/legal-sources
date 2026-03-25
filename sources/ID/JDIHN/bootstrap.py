#!/usr/bin/env python3
"""
ID/JDIHN -- Indonesia JDIHN National Legal Network Fetcher

Fetches legislation from JDIHN (Jaringan Dokumentasi dan Informasi Hukum Nasional)
at jdihn.go.id.

Strategy:
  - JSON API at /api/search with pagination (15 results/page, 1000 max per query)
  - Partition queries by document type (jenis) to cover full corpus
  - PDF download via /pencarian/download endpoint
  - Full text extracted from PDFs via pdfminer

Endpoints:
  - Search: GET /api/search?jenis={id}&page={n}
  - Types: GET /pencarian/getJenisDokumen
  - Download: GET /pencarian/download?id_dokumen={id}&id_anggota={id}
  - Stats: GET /home/countPeraturan

Data:
  - 639K+ regulations from 2600+ institutions
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
logger = logging.getLogger("legal-data-hunter.ID.JDIHN")

BASE_URL = "https://jdihn.go.id"

# Priority document types for national legislation
PRIORITY_JENIS_IDS = [
    9,   # UNDANG-UNDANG
    7,   # UNDANG-UNDANG DASAR
    10,  # PERATURAN PEMERINTAH PENGGANTI UNDANG-UNDANG
    11,  # PERATURAN PEMERINTAH
    12,  # PERATURAN PRESIDEN
    14,  # INSTRUKSI PRESIDEN
    16,  # UNDANG-UNDANG DARURAT
]


class JDIHNScraper(BaseScraper):
    """
    Scraper for ID/JDIHN -- Indonesian National Legal Network.
    Country: ID
    URL: https://jdihn.go.id

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json, text/html",
                "Accept-Language": "id,en",
            },
            timeout=60,
        )

    def _api_get(self, path: str, params: dict = None, max_retries: int = 3) -> Optional[Any]:
        """GET JSON from the API with retry logic."""
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait()
                resp = self.client.session.get(
                    f"{BASE_URL}{path}", params=params, timeout=60
                )
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 3
                    logger.debug(f"Retry {attempt+1} for {path}: {e}, waiting {wait}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"Failed after {max_retries} attempts: {path}: {e}")
                    return None

    def _get_document_types(self) -> List[Dict]:
        """Get all document types."""
        data = self._api_get("/pencarian/getJenisDokumen")
        return data if data else []

    def _search(self, jenis: int = None, tahun: str = None, page: int = 1) -> Optional[Dict]:
        """Search for documents."""
        params = {"page": page}
        if jenis:
            params["jenis"] = jenis
        if tahun:
            params["tahun"] = tahun
        return self._api_get("/api/search", params=params)

    def _download_pdf_text(self, id_dokumen: int, id_anggota: int) -> str:
        """Download PDF and extract text."""
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(
                f"{BASE_URL}/pencarian/download",
                params={"id_dokumen": id_dokumen, "id_anggota": id_anggota},
                timeout=90,
            )
            if resp.status_code != 200 or len(resp.content) < 500:
                return ""

            content_type = resp.headers.get("content-type", "")
            if "pdf" not in content_type and not resp.content[:5] == b"%PDF-":
                return ""

            from pdfminer.high_level import extract_text
            text = extract_text(BytesIO(resp.content))
            return text.strip()
        except Exception as e:
            logger.debug(f"PDF extraction failed for dokumen={id_dokumen}: {e}")
            return ""

    def _download_direct_pdf_text(self, url: str) -> str:
        """Download PDF from a direct URL and extract text."""
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(url, timeout=90)
            if resp.status_code != 200 or len(resp.content) < 500:
                return ""
            if not resp.content[:5] == b"%PDF-":
                return ""

            from pdfminer.high_level import extract_text
            text = extract_text(BytesIO(resp.content))
            return text.strip()
        except Exception as e:
            logger.debug(f"Direct PDF extraction failed for {url}: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation with full text from PDFs."""
        # Get all document types
        doc_types = self._get_document_types()
        if not doc_types:
            logger.error("Failed to get document types")
            return

        logger.info(f"Found {len(doc_types)} document types")

        for dtype in doc_types:
            jenis_id = dtype["id"]
            jenis_name = dtype["name"]

            page = 1
            while True:
                result = self._search(jenis=jenis_id, page=page)
                if not result or not result.get("data"):
                    break

                for item in result["data"]:
                    yield item

                meta = result.get("meta", {})
                if page >= meta.get("last_page", 1):
                    break
                page += 1

            logger.info(f"Type {jenis_name}: enumerated {page} pages")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recent legislation from the API (first pages)."""
        for jenis_id in PRIORITY_JENIS_IDS:
            result = self._search(jenis=jenis_id, page=1)
            if result and result.get("data"):
                for item in result["data"]:
                    yield item

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw API result into standard schema."""
        doc_id = raw.get("id", "")
        judul = raw.get("judul", "")
        id_dokumen = raw.get("id_dokumen")
        id_anggota = raw.get("id_anggota")

        if not judul:
            return None

        # Try to get full text from PDF
        full_text = ""
        download_url = raw.get("download", "")

        # Try direct download URL first (often faster)
        if download_url and download_url.startswith("http"):
            full_text = self._download_direct_pdf_text(download_url)

        # Fall back to JDIHN download endpoint
        if not full_text and id_dokumen and id_anggota:
            full_text = self._download_pdf_text(id_dokumen, id_anggota)

        if not full_text:
            logger.debug(f"No text for: {judul[:60]}")
            return None

        # Extract metadata
        jenis = raw.get("jenis_peraturan", {})
        jenis_name = jenis.get("name", "") if isinstance(jenis, dict) else ""
        instansi = raw.get("instansi", "")
        tahun = raw.get("tahun_terbit", "")
        nomor = raw.get("nomor", "")

        # Build date from year
        date_iso = f"{tahun}-01-01" if tahun and tahun.isdigit() else ""

        return {
            "_id": f"ID-JDIHN-{doc_id}",
            "_source": "ID/JDIHN",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": judul,
            "text": full_text,
            "date": date_iso,
            "url": f"{BASE_URL}/pencarian/detail/{doc_id}",
            "nomor": nomor or "",
            "jenis_peraturan": jenis_name,
            "instansi": instansi,
            "tahun_terbit": tahun or "",
            "language": "ind",
        }

    def run_sample(self, n: int = 12) -> dict:
        """Fetch sample records using priority national legislation types."""
        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "sample_records_saved": 0,
            "errors": 0,
        }
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        for jenis_id in PRIORITY_JENIS_IDS:
            result = self._search(jenis=jenis_id, page=1)
            if not result or not result.get("data"):
                continue

            for item in result["data"]:
                record = self.normalize(item)
                if not record:
                    stats["errors"] += 1
                    continue

                fname = record["_id"].replace("/", "_").replace(" ", "_")[:80]
                out = sample_dir / f"{fname}.json"
                out.write_text(json.dumps(record, ensure_ascii=False, indent=2))
                stats["sample_records_saved"] += 1
                logger.info(
                    f"Sample {stats['sample_records_saved']}: "
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
        print("Testing JDIHN National Legal Network...")

        print("\n1. Testing stats endpoint...")
        stats = self._api_get("/home/countPeraturan")
        if stats:
            print(f"   Regulations: {stats.get('puu', '?')}")
            print(f"   Monographs: {stats.get('monografi', '?')}")
            print(f"   Articles: {stats.get('artikel', '?')}")
            print(f"   Decisions: {stats.get('putusan', '?')}")
        else:
            print("   FAILED")
            return

        print("\n2. Testing search API...")
        result = self._search(jenis=9, page=1)
        if result and result.get("data"):
            print(f"   Results: {result['meta']['total']}")
            item = result["data"][0]
            print(f"   First: {item['judul'][:80]}")
            print(f"   Has download URL: {bool(item.get('download'))}")

            print("\n3. Testing PDF download...")
            text = ""
            dl = item.get("download", "")
            if dl and dl.startswith("http"):
                text = self._download_direct_pdf_text(dl)
            if not text:
                text = self._download_pdf_text(item["id_dokumen"], item["id_anggota"])
            print(f"   Text length: {len(text)} chars")
            if text:
                print(f"   Preview: {text[:200]}...")
        else:
            print("   FAILED")

        print("\nTest complete!")


def main():
    scraper = JDIHNScraper()

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
