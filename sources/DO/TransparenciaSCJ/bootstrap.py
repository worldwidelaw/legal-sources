#!/usr/bin/env python3
"""
DO/TransparenciaSCJ -- Dominican Republic Judicial Decisions

Fetches judicial decisions from Dominican Republic courts via official open APIs:
  - Juristeca API: discovery and metadata (POST /jurisprudencia/search)
  - Decisiones API: PDF URLs linked to case numbers (GET /ObtenerDecisiones)
  - Azure Blob Storage: Signed PDF documents with full text

Strategy:
  - Search Juristeca for all jurisprudencia with pagination
  - Parse "Expediente No." from title to get case number
  - Query Decisiones API with case number to get PDF URL
  - Download PDF and extract full text via pdfplumber
  - Normalize into standard schema

API docs: https://justicia.gob.do/apis-judiciales/
Rate limit: 100 requests/minute per IP (both APIs)

Usage:
  python bootstrap.py bootstrap              # Full pull (paginated)
  python bootstrap.py bootstrap --sample     # Fetch 12+ sample records
  python bootstrap.py update                 # Recent decisions (last 30 days)
"""

import sys
import json
import logging
import time
import re
import io
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DO.TransparenciaSCJ")

# API endpoints
JURISTECA_BASE = "https://api.poderjudicial.gob.do/Juristeca/api/v1"
DECISIONES_BASE = "https://api.poderjudicial.gob.do/Decisiones/Decisiones"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Pattern to extract Expediente number from title
EXPEDIENTE_RE = re.compile(r"Expediente\s+No\.?\s*(\d{4}-\d{5,9})")


class TransparenciaSCJScraper(BaseScraper):
    """
    Scraper for DO/TransparenciaSCJ -- Dominican Republic Judicial Decisions.
    Country: DO
    URL: https://justicia.gob.do/apis-judiciales/

    Data types: case_law
    Auth: none (Public open APIs)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=JURISTECA_BASE,
            headers=HEADERS,
            timeout=60,
        )
        self.decisiones_client = HttpClient(
            base_url=DECISIONES_BASE,
            headers=HEADERS,
            timeout=60,
        )

    def _search_jurisprudencia(self, page: int = 0, rows: int = 50,
                                search_tokens: list = None) -> list:
        """Search Juristeca jurisprudencia endpoint."""
        payload = {
            "searchTokens": search_tokens or ["Expediente"],
            "sort": "fecha desc",
            "page": page,
            "rows": rows,
        }
        try:
            self.rate_limiter.wait()
            resp = self.client.post("/jurisprudencia/search", json_data=payload)
            if resp.status_code == 429:
                logger.warning("Rate limited, waiting 60s...")
                time.sleep(60)
                resp = self.client.post("/jurisprudencia/search", json_data=payload)
            resp.raise_for_status()
            data = resp.json()
            return data.get("documentsJurisprudencia", [])
        except Exception as e:
            logger.error(f"Juristeca search failed (page {page}): {e}")
            return []

    def _get_expediente(self, title: str) -> Optional[str]:
        """Extract Expediente number from title."""
        match = EXPEDIENTE_RE.search(title)
        return match.group(1) if match else None

    def _get_decision_pdf_url(self, expediente: str) -> Optional[str]:
        """Get PDF URL from Decisiones API using expediente number."""
        try:
            self.rate_limiter.wait()
            resp = self.decisiones_client.get(
                "/ObtenerDecisiones",
                params={
                    "Nuc": expediente,
                    "PaginaActual": "1",
                    "RegistrosPorPagina": "1",
                },
            )
            if resp.status_code == 429:
                logger.warning("Decisiones rate limited, waiting 60s...")
                time.sleep(60)
                return None
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            datos = data.get("datos", [])
            if datos and datos[0].get("urlDocumentoFirmado"):
                return datos[0]["urlDocumentoFirmado"]
            return None
        except Exception as e:
            logger.debug(f"Decisiones API failed for {expediente}: {e}")
            return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text using pdfplumber."""
        try:
            import pdfplumber
            import requests as req_lib
            import warnings
            warnings.filterwarnings("ignore")

            resp = req_lib.get(
                pdf_url,
                headers={"User-Agent": HEADERS["User-Agent"]},
                timeout=30,
                verify=False,
            )
            if resp.status_code != 200:
                logger.debug(f"PDF download failed: HTTP {resp.status_code}")
                return None

            # Don't process huge PDFs (>50MB)
            if len(resp.content) > 50_000_000:
                logger.warning(f"PDF too large: {len(resp.content)} bytes")
                return None

            pdf = pdfplumber.open(io.BytesIO(resp.content))
            text_parts = []
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()

            text = "\n".join(text_parts)
            return text if len(text) > 100 else None

        except Exception as e:
            logger.debug(f"PDF extraction failed: {e}")
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw Juristeca + Decisiones data into standard schema."""
        doc_id = raw.get("id")
        if not doc_id:
            return None

        title = raw.get("title", "")
        fecha = raw.get("fecha", "")
        date_str = fecha[:10] if fecha else None

        record = {
            "_id": f"DO-TransparenciaSCJ-{doc_id}",
            "_source": "DO/TransparenciaSCJ",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "id": str(doc_id),
            "numero_tol": raw.get("numeroTOL", ""),
            "title": title,
            "numero": raw.get("numero", ""),
            "tribunal": raw.get("tribunal", ""),
            "tipo_decision": raw.get("tipoDecision", ""),
            "accion_o_recurso": raw.get("accionORecurso", ""),
            "organo": raw.get("organo"),
            "distrito_judicial": raw.get("distritoJudicial"),
            "ponente": raw.get("ponente"),
            "materia": raw.get("materia", []),
            "date": date_str,
            "expediente": raw.get("_expediente", ""),
            "text": raw.get("_text", ""),
            "url": raw.get("_pdf_url", ""),
        }
        return record

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all judicial decisions with full text."""
        page = 0
        rows = 50
        empty_pages = 0

        while empty_pages < 3:
            docs = self._search_jurisprudencia(page=page, rows=rows)
            if not docs:
                empty_pages += 1
                page += 1
                continue

            empty_pages = 0
            for doc in docs:
                title = doc.get("title", "")
                expediente = self._get_expediente(title)

                if not expediente:
                    page += 1
                    continue

                # Get PDF URL
                pdf_url = self._get_decision_pdf_url(expediente)
                if not pdf_url:
                    continue

                # Extract text
                text = self._extract_pdf_text(pdf_url)
                if not text:
                    continue

                doc["_expediente"] = expediente
                doc["_text"] = text
                doc["_pdf_url"] = pdf_url

                # BaseScraper contract: yield RAW; the framework calls
                # normalize(). Yielding an already-normalized record made the
                # framework re-normalize it, and since the normalized record has
                # `text`/`url` (not `_text`/`_pdf_url`) the second pass produced
                # empty text -> 0 written on the VPS pipeline (issue #915).
                yield doc

            page += 1
            logger.info(f"Processed page {page}")

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Fetch recent decisions (last 30 days by default)."""
        # Juristeca sorts by fecha desc, so first pages are most recent
        page = 0
        rows = 50
        cutoff = since or (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

        while page < 20:  # Max 20 pages for updates
            docs = self._search_jurisprudencia(page=page, rows=rows)
            if not docs:
                break

            for doc in docs:
                fecha = doc.get("fecha", "")
                if fecha and fecha[:10] < cutoff:
                    return

                title = doc.get("title", "")
                expediente = self._get_expediente(title)
                if not expediente:
                    continue

                pdf_url = self._get_decision_pdf_url(expediente)
                if not pdf_url:
                    continue

                text = self._extract_pdf_text(pdf_url)
                if not text:
                    continue

                doc["_expediente"] = expediente
                doc["_text"] = text
                doc["_pdf_url"] = pdf_url

                # Yield RAW (see fetch_all note re issue #915).
                yield doc

            page += 1

    def run_bootstrap(self, sample: bool = False, **kwargs):
        """Run bootstrap or sample mode."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        target = 12 if sample else 100000
        count = 0

        gen = self.fetch_all()
        for raw in gen:
            # fetch_all yields RAW now; normalize here for sample output.
            record = self.normalize(raw)
            if not record or not record.get("text"):
                continue

            count += 1
            # Save to sample directory
            filename = f"{record['_id']}.json"
            filepath = sample_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(
                f"[{count}/{target}] {record['numero']} | "
                f"{record['tribunal'][:40]} | "
                f"{len(record['text'])} chars"
            )

            if count >= target:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        return count


def main():
    import argparse

    parser = argparse.ArgumentParser(description="DO/TransparenciaSCJ bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only sample records (12+)")
    args = parser.parse_args()

    scraper = TransparenciaSCJScraper()

    if args.command == "test-api":
        print("Testing Juristeca API...")
        docs = scraper._search_jurisprudencia(page=0, rows=3)
        print(f"  Found {len(docs)} documents")
        if docs:
            print(f"  First: {docs[0].get('title', '')[:80]}")
            exp = scraper._get_expediente(docs[0].get("title", ""))
            print(f"  Expediente: {exp}")
        print("\nTesting Decisiones API...")
        if docs:
            for d in docs:
                exp = scraper._get_expediente(d.get("title", ""))
                if exp:
                    url = scraper._get_decision_pdf_url(exp)
                    print(f"  NUC {exp} -> PDF: {bool(url)}")
                    break
        print("\nAPI test complete.")

    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)

    elif args.command == "update":
        count = 0
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)
        for record in scraper.fetch_updates():
            count += 1
            filepath = sample_dir / f"{record['_id']}.json"
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            if count >= 50:
                break
        print(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
