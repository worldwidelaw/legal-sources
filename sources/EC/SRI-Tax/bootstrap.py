#!/usr/bin/env python3
"""
EC/SRI-Tax -- Ecuador Servicio de Rentas Internas Tax Guidance

Fetches tax doctrine from Ecuador's SRI (Internal Revenue Service):
  - Extractos de Consultas Tributarias Formales (2014-2024)
  - Normativa Institucional Vigente

Strategy:
  - Bootstrap: Downloads PDF documents from sri.gob.ec Alfresco portal,
    extracts full text via common/pdf_extract.extract_pdf_markdown.
  - Update: Re-fetches documents modified after a given date.
  - Sample: Fetches a subset of documents for validation.

Source: https://www.sri.gob.ec/extractos-de-consultas
Auth: none (open data)

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental update
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EC.SRI-Tax")

BASE_URL = "https://www.sri.gob.ec"
DOWNLOAD_BASE = f"{BASE_URL}/o/sri-portlet-biblioteca-alfresco-internet/descargar"

# Known PDF documents with direct download URLs
# Each entry: (uuid, filename, doc_id_slug, title, year, doc_type)
DOCUMENTS = [
    {
        "uuid": "c536bac6-7315-4c49-975a-3d55c60f4329",
        "filename": "Extractos+consultas+enero+-+diciembre+2014.pdf",
        "doc_id": "consultas-tributarias-2014",
        "title": "Extractos de Consultas Tributarias Formales - Enero a Diciembre 2014",
        "year": 2014,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "fd4c7aff-52c7-4b9d-b0c6-29f53b314893",
        "filename": "Extractos+consultas+enero+-+diciembre+2015.pdf",
        "doc_id": "consultas-tributarias-2015",
        "title": "Extractos de Consultas Tributarias Formales - Enero a Diciembre 2015",
        "year": 2015,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "5e828f65-743f-49a4-a769-bf3af882bdb2",
        "filename": "Extractos+consultas+enero+-+diciembre+2016.pdf",
        "doc_id": "consultas-tributarias-2016",
        "title": "Extractos de Consultas Tributarias Formales - Enero a Diciembre 2016",
        "year": 2016,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "238d9a6d-5f50-426d-bf8c-6a3c21ec10c9",
        "filename": "Extractos%20consulta%20enero%20-%20diciembre%202017.pdf",
        "doc_id": "consultas-tributarias-2017",
        "title": "Extractos de Consultas Tributarias Formales - Enero a Diciembre 2017",
        "year": 2017,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "40977c93-2ffd-4b64-9077-0fe3cade0218",
        "filename": "Extractos%20consultas%20enero%20-%20diciembre%202018.pdf",
        "doc_id": "consultas-tributarias-2018",
        "title": "Extractos de Consultas Tributarias Formales - Enero a Diciembre 2018",
        "year": 2018,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "9c3cdf84-5102-4cba-9344-129a6ab5be62",
        "filename": "Extractos%20consulta%20enero%20-%20diciembre%202019.pdf",
        "doc_id": "consultas-tributarias-2019",
        "title": "Extractos de Consultas Tributarias Formales - Enero a Diciembre 2019",
        "year": 2019,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "d074de83-517d-4a49-ac1b-7d7dc3ade3a3",
        "filename": "Extractos%20consulta%20enero%20-%20diciembre%202020.pdf",
        "doc_id": "consultas-tributarias-2020",
        "title": "Extractos de Consultas Tributarias Formales - Enero a Diciembre 2020",
        "year": 2020,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "240015d5-7c72-4ef5-8b75-20464168ee28",
        "filename": "Extractos%20consultas%20enero%20-%20diciembre%202021.pdf",
        "doc_id": "consultas-tributarias-2021",
        "title": "Extractos de Consultas Tributarias Formales - Enero a Diciembre 2021",
        "year": 2021,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "3dc62943-ac90-492e-a6d4-5d62b37fee10",
        "filename": "Extractos%20consultas%20enero%20-%20diciembre%202022.pdf",
        "doc_id": "consultas-tributarias-2022",
        "title": "Extractos de Consultas Tributarias Formales - Enero a Diciembre 2022",
        "year": 2022,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "4dd8cd7f-ba47-4206-b6c2-8ed4c367b46c",
        "filename": "EXTRACTOS_CONSULTAS_TRIBUTARIAS_FORMALES%202023.pdf",
        "doc_id": "consultas-tributarias-2023",
        "title": "Extractos de Consultas Tributarias Formales 2023",
        "year": 2023,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "41a82537-efa4-4e84-9d45-326f57dd8199",
        "filename": "EXTRACTOS_CONSULTAS_VINCULANTES_ISEM2024.pdf",
        "doc_id": "consultas-vinculantes-2024",
        "title": "Extractos de Consultas Vinculantes - Primer Semestre 2024",
        "year": 2024,
        "doc_type": "consulta_tributaria",
    },
    {
        "uuid": "fd3fa4a9-ab76-4426-9f4f-08dee57993d8",
        "filename": "normativa_institucional_vigente.pdf",
        "doc_id": "normativa-institucional-vigente",
        "title": "Normativa Institucional Vigente - Servicio de Rentas Internas",
        "year": 2025,
        "doc_type": "normativa",
    },
]


class SRITaxScraper(BaseScraper):
    """
    Scraper for EC/SRI-Tax -- Ecuador SRI Tax Doctrine.
    Country: EC
    URL: https://www.sri.gob.ec/
    Data types: doctrine
    Auth: none
    """

    SOURCE_ID = "EC/SRI-Tax"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url="",
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=120,
        )
        self.existing_ids: set[str] = set()

    def _build_pdf_url(self, doc: dict) -> str:
        """Build the full download URL for a document."""
        return f"{DOWNLOAD_BASE}/{doc['uuid']}/{doc['filename']}"

    def _download_and_extract(self, doc: dict) -> Optional[str]:
        """Download a PDF and extract text using the centralized extractor."""
        url = self._build_pdf_url(doc)
        doc_id = doc["doc_id"]

        # Use centralized PDF extraction (handles download + extraction + caching)
        text = extract_pdf_markdown(
            source=self.SOURCE_ID,
            source_id=doc_id,
            pdf_url=url,
            table="doctrine",
        )
        return text

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        doc_id = raw["doc_id"]
        year = raw.get("year", "")
        date = f"{year}-01-01" if year else None

        return {
            "_id": f"EC-SRI-{doc_id}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": date,
            "url": self._build_pdf_url(raw),
            "institution": "Servicio de Rentas Internas (SRI)",
            "document_type": raw.get("doc_type", "consulta_tributaria"),
            "year": year,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all SRI tax doctrine documents with full text."""
        self.existing_ids = preload_existing_ids(self.SOURCE_ID, table="doctrine")
        logger.info(f"Existing IDs in Neon: {len(self.existing_ids)}")

        for i, doc in enumerate(DOCUMENTS):
            doc_id = doc["doc_id"]
            if doc_id in self.existing_ids:
                logger.info(f"Skipping {doc_id} (already in Neon)")
                continue

            logger.info(f"[{i+1}/{len(DOCUMENTS)}] Downloading: {doc['title']}")
            text = self._download_and_extract(doc)

            if text:
                logger.info(f"  Extracted {len(text)} chars")
                record = {**doc, "text": text}
                yield self.normalize(record)
            else:
                logger.warning(f"  No text extracted for {doc_id}")

            time.sleep(2)  # Rate limiting

        logger.info("Fetch complete")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Re-fetch all documents (small corpus, always re-check)."""
        # With only ~12 documents, just re-fetch everything
        yield from self.fetch_all()

    def fetch_sample(self) -> list:
        """Fetch a sample of documents for validation."""
        samples = []
        # Sample a spread: first, middle, and last documents
        indices = [0, 2, 4, 6, 8, 10, 11]

        for idx in indices:
            if idx >= len(DOCUMENTS):
                continue
            doc = DOCUMENTS[idx]
            logger.info(f"Sampling: {doc['title']}")

            url = self._build_pdf_url(doc)
            try:
                import requests
                resp = requests.get(url, timeout=120, headers={
                    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)"
                })
                if resp.status_code != 200:
                    logger.warning(f"  HTTP {resp.status_code} for {doc['doc_id']}")
                    continue

                pdf_bytes = resp.content
                logger.info(f"  Downloaded {len(pdf_bytes)} bytes")

                # Extract text using centralized extractor
                text = extract_pdf_markdown(
                    source=self.SOURCE_ID,
                    source_id=doc["doc_id"],
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                    force=True,  # Always extract for sample validation
                )

                if text:
                    logger.info(f"  Extracted {len(text)} chars of text")
                    record = {**doc, "text": text}
                    samples.append(self.normalize(record))
                else:
                    logger.warning(f"  No text extracted from PDF")

            except Exception as e:
                logger.error(f"  Error processing {doc['doc_id']}: {e}")

            time.sleep(2)

        return samples

    def test_api(self):
        """Quick connectivity test for SRI document downloads."""
        print("Testing EC/SRI-Tax document access...")
        print(f"  Total documents cataloged: {len(DOCUMENTS)}")

        # Test downloading first document
        doc = DOCUMENTS[0]
        url = self._build_pdf_url(doc)
        print(f"\n  Testing download: {doc['title']}")
        print(f"  URL: {url}")

        try:
            import requests
            resp = requests.get(url, timeout=60, headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)"
            })
            print(f"  Status: {resp.status_code}")
            print(f"  Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
            print(f"  Size: {len(resp.content)} bytes")

            if resp.status_code == 200 and len(resp.content) > 1000:
                text = extract_pdf_markdown(
                    source=self.SOURCE_ID,
                    source_id=doc["doc_id"],
                    pdf_bytes=resp.content,
                    table="doctrine",
                    force=True,
                )
                if text:
                    print(f"  Text extracted: {len(text)} chars")
                    print(f"  First 300 chars:\n    {text[:300]}")
                else:
                    print("  Text extraction FAILED")
            else:
                print("  Download FAILED")
        except Exception as e:
            print(f"  Error: {e}")

        print("\nAPI test complete.")


def main():
    scraper = SRITaxScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample:
            logger.info("Fetching sample records...")
            samples = scraper.fetch_sample()

            # Save samples
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            for i, record in enumerate(samples):
                path = sample_dir / f"sample_{i:03d}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved {len(samples)} sample records to {sample_dir}")

            # Validation
            texts = [r for r in samples if r.get("text") and len(r["text"]) > 100]
            print(f"\nValidation: {len(texts)}/{len(samples)} records have full text")
            for r in samples:
                text_len = len(r.get("text", ""))
                print(f"  {r['_id']}: {r['title'][:70]} | text: {text_len} chars")
        else:
            logger.info("Starting full bootstrap...")
            count = 0
            output_dir = Path(__file__).parent / "data"
            output_dir.mkdir(exist_ok=True)

            for record in scraper.fetch_all():
                path = output_dir / f"{record['_id']}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info(f"  Saved {count} records...")

            logger.info(f"Bootstrap complete: {count} records saved")

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else "2026-01-01"
        logger.info(f"Fetching updates since {since}...")
        count = 0
        output_dir = Path(__file__).parent / "data"
        output_dir.mkdir(exist_ok=True)

        for record in scraper.fetch_updates(since):
            path = output_dir / f"{record['_id']}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Update complete: {count} new/updated records")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
