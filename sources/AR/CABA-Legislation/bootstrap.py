#!/usr/bin/env python3
"""
AR/CABA-Legislation -- Ciudad Autónoma de Buenos Aires Legislation

Fetches full-text legislation from the Boletín Oficial de la Ciudad de Buenos
Aires REST API. Covers laws (Leyes) and decrees (Decretos) from 1996-present.

Data access:
  - REST API at api-restboletinoficial.buenosaires.gob.ar
  - /obtenerResultado/{params} for paginated search
  - /download/{id} for PDF documents
  - Full text extracted from PDFs via pdfplumber

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10-15 sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests
import pdfplumber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.CABA-Legislation")

API_BASE = "https://api-restboletinoficial.buenosaires.gob.ar"
DELAY = 2.0
PAGE_SIZE = 50

# Norm type IDs from the API
NORM_TYPES = {
    1: "Ley",
    2: "Decreto",
}


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
        "Accept": "application/json",
    })
    return session


class CABAFetcher:
    SOURCE_ID = "AR/CABA-Legislation"

    def __init__(self):
        self.session = get_session()

    def _get_json(self, url: str, timeout: int = 30) -> Any:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except ValueError:
                        logger.warning("Non-JSON response from %s", url)
                        return None
                if resp.status_code == 429:
                    wait = 10 * (attempt + 1)
                    logger.warning("Rate limited, waiting %ds...", wait)
                    time.sleep(wait)
                    continue
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return None
            except requests.RequestException as e:
                logger.warning("Request error (attempt %d): %s", attempt + 1, e)
                time.sleep(5 * (attempt + 1))
        return None

    def _download_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                if resp.status_code != 200:
                    logger.warning("PDF download HTTP %d: %s", resp.status_code, url)
                    return None
                if len(resp.content) < 100:
                    return None
                pdf = pdfplumber.open(io.BytesIO(resp.content))
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                pdf.close()
                full_text = "\n\n".join(pages_text).strip()
                if len(full_text) > 50:
                    return full_text
                return None
            except Exception as e:
                logger.warning("PDF extraction error (attempt %d): %s", attempt + 1, e)
                time.sleep(3)
        return None

    def search_norms(self, norm_type: int, per_page: int = PAGE_SIZE,
                     offset: int = 0, year: Optional[int] = None) -> Dict[str, Any]:
        """Search norms by type with pagination."""
        params = f"perPage={per_page}&offset={offset}&tipoNorma={norm_type}"
        if year:
            params += f"&anio={year}"
        encoded = urllib.parse.quote(params)
        url = f"{API_BASE}/obtenerResultado/{encoded}"
        data = self._get_json(url, timeout=45)
        time.sleep(DELAY)
        if not data or not isinstance(data, dict):
            return {"normas": [], "total": 0}
        norms = data.get("normas", [])
        total = norms[0].get("total_count", 0) if norms else 0
        return {"normas": norms, "total": total}

    def _get_pdf_url(self, norm: Dict[str, Any]) -> Optional[str]:
        """Extract the best PDF download URL from a norm record."""
        # Try link_documento_normas first (list of [date, url] pairs)
        links = norm.get("link_documento_normas", [])
        if links and isinstance(links, list) and len(links) > 0:
            for link in links:
                if isinstance(link, list) and len(link) >= 2 and link[1]:
                    return link[1]
        # Try link_anexo
        anexo = norm.get("link_anexo")
        if anexo and isinstance(anexo, str) and anexo.startswith("http"):
            return anexo
        # Try constructing from archivo_norma
        archivo = norm.get("archivo_norma")
        if archivo:
            return f"http://documentosboletinoficial.buenosaires.gob.ar/publico/ck_{archivo}"
        return None

    def _parse_date(self, norm: Dict[str, Any]) -> Optional[str]:
        """Extract publication date from bulletin info."""
        boletines = norm.get("boletines", [])
        if boletines and isinstance(boletines, list):
            for b in boletines:
                if isinstance(b, list) and len(b) >= 2:
                    date_str = b[1]
                    try:
                        dt = datetime.strptime(date_str, "%d/%m/%Y")
                        return dt.strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        pass
        return None

    def normalize(self, norm: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Transform raw API norm into standard schema."""
        norm_type = norm.get("nombre_tipo", "")
        norm_num = norm.get("numero_norma", "")
        year = norm.get("anio_norma", "")
        # Year is 2-digit in API; convert to 4-digit
        if year and isinstance(year, int):
            year = year + 2000 if year < 96 else year + 1900

        norm_id = f"AR-CABA-{norm_type}-{norm_num}-{year}" if norm_num else f"AR-CABA-{norm.get('id', '')}"
        date = self._parse_date(norm)

        title_parts = []
        if norm_type:
            title_parts.append(norm_type)
        if norm_num:
            title_parts.append(f"N° {norm_num}")
        if year:
            title_parts.append(f"({year})")
        title = " ".join(title_parts)

        summary = norm.get("sumario", "")
        if summary:
            title = f"{title} - {summary}"

        boletin_url = None
        boletines = norm.get("boletines", [])
        if boletines and isinstance(boletines, list) and len(boletines) > 0:
            b = boletines[0]
            if isinstance(b, list) and len(b) >= 1:
                boletin_url = f"https://boletinoficial.buenosaires.gob.ar/normativa/buscar/{norm.get('id', '')}"

        return {
            "_id": norm_id,
            "_source": self.SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "norm_type": norm_type,
            "norm_number": str(norm_num) if norm_num else None,
            "year": year,
            "summary": summary,
            "issuing_body": norm.get("nombre_reparticion", ""),
            "issuing_org": norm.get("organismo_emisor", ""),
            "section": norm.get("nombre_seccion", ""),
            "url": boletin_url or f"https://boletinoficial.buenosaires.gob.ar/normativa",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation. If sample=True, fetch ~12 records."""
        sample_limit = 12 if sample else None
        count = 0

        for type_id, type_name in NORM_TYPES.items():
            logger.info("Fetching %s (type %d)...", type_name, type_id)
            offset = 0

            while True:
                if sample_limit and count >= sample_limit:
                    return

                batch_size = min(PAGE_SIZE, sample_limit - count) if sample_limit else PAGE_SIZE
                result = self.search_norms(type_id, per_page=batch_size, offset=offset)
                norms = result["normas"]
                total = result["total"]

                if not norms:
                    break

                logger.info("  %s: offset %d / %d total", type_name, offset, total)

                for norm in norms:
                    if sample_limit and count >= sample_limit:
                        return

                    pdf_url = self._get_pdf_url(norm)
                    if not pdf_url:
                        logger.warning("  No PDF URL for norm %s", norm.get("id"))
                        continue

                    text = self._download_pdf_text(pdf_url)
                    if not text:
                        logger.warning("  No text extracted for norm %s", norm.get("id"))
                        continue

                    record = self.normalize(norm, text)
                    count += 1
                    yield record

                offset += len(norms)
                if offset >= total:
                    break

        logger.info("Fetched %d records total", count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch norms published since a given date (YYYY-MM-DD)."""
        for type_id, type_name in NORM_TYPES.items():
            offset = 0
            while True:
                params = f"perPage={PAGE_SIZE}&offset={offset}&tipoNorma={type_id}&fecha_desde={since}"
                encoded = urllib.parse.quote(params)
                url = f"{API_BASE}/obtenerResultado/{encoded}"
                data = self._get_json(url, timeout=45)
                time.sleep(DELAY)

                if not data or not data.get("normas"):
                    break

                norms = data["normas"]
                total = norms[0].get("total_count", 0) if norms else 0

                for norm in norms:
                    pdf_url = self._get_pdf_url(norm)
                    if not pdf_url:
                        continue
                    text = self._download_pdf_text(pdf_url)
                    if not text:
                        continue
                    yield self.normalize(norm, text)

                offset += len(norms)
                if offset >= total:
                    break

    def test(self) -> bool:
        """Quick connectivity test."""
        result = self.search_norms(1, per_page=1)
        if result["normas"]:
            logger.info("API test OK: %d laws available", result["total"])
            return True
        logger.error("API test FAILED: no results")
        return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="AR/CABA-Legislation fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--since", help="Date for incremental update (YYYY-MM-DD)")
    args = parser.parse_args()

    fetcher = CABAFetcher()
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "test":
        ok = fetcher.test()
        sys.exit(0 if ok else 1)

    elif args.command == "bootstrap":
        count = 0
        for record in fetcher.fetch_all(sample=args.sample):
            fname = re.sub(r'[^\w\-.]', '_', record["_id"]) + ".json"
            out = sample_dir / fname
            out.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info("  [%d] %s (%d chars)", count, record["_id"], text_len)
        logger.info("Bootstrap complete: %d records saved to %s", count, sample_dir)

    elif args.command == "update":
        since = args.since or "2024-01-01"
        count = 0
        for record in fetcher.fetch_updates(since):
            fname = re.sub(r'[^\w\-.]', '_', record["_id"]) + ".json"
            out = sample_dir / fname
            out.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
        logger.info("Update complete: %d records since %s", count, since)


if __name__ == "__main__":
    main()
