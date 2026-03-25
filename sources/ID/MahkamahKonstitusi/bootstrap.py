#!/usr/bin/env python3
"""
ID/MahkamahKonstitusi -- Indonesian Constitutional Court Decisions

Fetches constitutional court (Mahkamah Konstitusi) decisions via JDIHN API.

Strategy:
  - JDIHN search API with jenis=93 (PUTUSAN MAHKAMAH KONSTITUSI)
  - PDF download via JDIHN /pencarian/download proxy (bypasses Cloudflare on mkri.id)
  - Full text extracted from PDFs via pdfminer

Endpoints:
  - Search: GET https://jdihn.go.id/api/search?jenis=93&page={n}
  - Download: GET https://jdihn.go.id/pencarian/download?id_dokumen={id}&id_anggota={id}

Data:
  - 1000+ constitutional review decisions
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
from typing import Generator, Optional, Dict, Any
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
logger = logging.getLogger("legal-data-hunter.ID.MahkamahKonstitusi")

JDIHN_BASE = "https://jdihn.go.id"
MK_JENIS_ID = 93  # PUTUSAN MAHKAMAH KONSTITUSI


class MahkamahKonstitusiScraper(BaseScraper):
    """
    Scraper for ID/MahkamahKonstitusi -- Indonesian Constitutional Court.
    Country: ID
    URL: https://mkri.id/

    Uses JDIHN API as data proxy (mkri.id is behind Cloudflare).
    Data types: case_law
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=JDIHN_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json, text/html",
                "Accept-Language": "id,en",
            },
            timeout=60,
        )

    def _api_get(self, path: str, params: dict = None, max_retries: int = 3) -> Optional[Any]:
        """GET JSON from the JDIHN API with retry logic."""
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait()
                resp = self.client.session.get(
                    f"{JDIHN_BASE}{path}", params=params, timeout=60
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

    def _search(self, page: int = 1) -> Optional[Dict]:
        """Search for MK decisions via JDIHN API."""
        return self._api_get("/api/search", params={"jenis": MK_JENIS_ID, "page": page})

    def _download_pdf_text(self, id_dokumen: int, id_anggota: int) -> str:
        """Download PDF via JDIHN proxy and extract text."""
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(
                f"{JDIHN_BASE}/pencarian/download",
                params={"id_dokumen": id_dokumen, "id_anggota": id_anggota},
                timeout=90,
            )
            if resp.status_code != 200 or len(resp.content) < 500:
                return ""

            if not resp.content[:5] == b"%PDF-":
                return ""

            from pdfminer.high_level import extract_text
            text = extract_text(BytesIO(resp.content))
            return text.strip()
        except Exception as e:
            logger.debug(f"PDF extraction failed for dokumen={id_dokumen}: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all MK decisions."""
        page = 1
        while True:
            result = self._search(page=page)
            if not result or not result.get("data"):
                break

            for item in result["data"]:
                yield item

            meta = result.get("meta", {})
            if page >= meta.get("last_page", 1):
                break
            page += 1

        logger.info(f"Enumerated {page} pages of MK decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recent MK decisions (first pages)."""
        for page in range(1, 4):
            result = self._search(page=page)
            if result and result.get("data"):
                for item in result["data"]:
                    yield item

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw JDIHN result into standard schema."""
        doc_id = raw.get("id", "")
        judul = raw.get("judul", "")
        id_dokumen = raw.get("id_dokumen")
        id_anggota = raw.get("id_anggota")

        if not judul:
            return None

        # Get full text from PDF via JDIHN download proxy
        full_text = ""
        if id_dokumen and id_anggota:
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
        date_iso = f"{tahun}-01-01" if tahun and str(tahun).isdigit() else ""

        return {
            "_id": f"ID-MK-{doc_id}",
            "_source": "ID/MahkamahKonstitusi",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": judul,
            "text": full_text,
            "date": date_iso,
            "url": f"https://mkri.id/index.php?page=web.Putusan&id={doc_id}",
            "nomor": nomor or "",
            "jenis_peraturan": jenis_name,
            "instansi": instansi,
            "tahun_terbit": str(tahun) if tahun else "",
            "language": "ind",
        }

    def run_sample(self, n: int = 12) -> dict:
        """Fetch sample records."""
        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "sample_records_saved": 0,
            "errors": 0,
        }
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        # Fetch from first few pages
        for page in range(1, 5):
            result = self._search(page=page)
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
        print("Testing MahkamahKonstitusi via JDIHN API...")

        print("\n1. Testing search API (jenis=93)...")
        result = self._search(page=1)
        if result and result.get("data"):
            meta = result.get("meta", {})
            print(f"   Total decisions: {meta.get('total')}")
            print(f"   Pages: {meta.get('last_page')}")
            item = result["data"][0]
            print(f"   First: {item['judul'][:80]}")

            print("\n2. Testing PDF download via JDIHN proxy...")
            text = self._download_pdf_text(item["id_dokumen"], item["id_anggota"])
            print(f"   Text length: {len(text)} chars")
            if text:
                print(f"   Preview: {text[:200]}...")
        else:
            print("   FAILED - no results")

        print("\nTest complete!")


def main():
    scraper = MahkamahKonstitusiScraper()

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
