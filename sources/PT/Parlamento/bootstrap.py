#!/usr/bin/env python3
"""
PT/Parlamento -- Portuguese Parliament Open Data (Assembleia da República)

Fetches legislative initiatives from the Portuguese Parliament's Open Data portal.
Each legislature (II–XVII, 1980–present) has a JSON feed listing all parliamentary
initiatives (bills, resolutions, motions) with metadata and PDF links to full text.

Strategy:
  - Fetch JSON feeds per legislature from app.parlamento.pt
  - Each initiative record contains IniLinkTexto (PDF URL) with the full text
  - Download PDFs and extract text via common/pdf_extract
  - Normalize into standard schema with full text

Endpoints:
  - JSON feed: https://app.parlamento.pt/webutils/docs/doc.txt?path={hex}&fich={fich}&Inline=true
  - PDF text: http://app.parlamento.pt/webutils/docs/doc.pdf?path={hex}&fich={fich}&Inline=true

Data:
  - Initiative types: Proposta de Lei, Projeto de Lei, Projeto de Resolução, etc.
  - Coverage: Legislatures II–XVII (1980–present)
  - License: Open Government Data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import base64
import binascii
import urllib.parse
import time
import re
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

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
logger = logging.getLogger("legal-data-hunter.PT.Parlamento")

# All legislatures available in the Open Data portal
LEGISLATURES = [
    "II", "III", "IV", "V", "VI", "VII", "VIII", "IX",
    "X", "XI", "XII", "XIII", "XIV", "XV", "XVI", "XVII",
]

# For sample mode, only fetch from the most recent legislature
SAMPLE_LEGISLATURES = ["XVII"]


def _build_url(category_path: str, filename: str, doc_type: str = "txt") -> str:
    """
    Build a parlamento.pt document URL by encoding the internal arnet path.

    The internal path is base64-encoded then hex-encoded to form the path parameter.
    """
    internal = f"http://arnet/opendata/DadosAbertos/{category_path}/{filename}"
    b64 = base64.b64encode(internal.encode("utf-8")).decode("ascii")
    hexpath = binascii.hexlify(b64.encode("ascii")).decode("ascii")
    fich = urllib.parse.quote(filename)
    return f"https://app.parlamento.pt/webutils/docs/doc.{doc_type}?path={hexpath}&fich={fich}&Inline=true"


def _clean_text(text: str) -> str:
    """Strip HTML tags and clean extracted text."""
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Decode HTML entities
    text = html_mod.unescape(text)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


class ParlamentoScraper(BaseScraper):
    """
    Scraper for PT/Parlamento -- Portuguese Parliament Open Data.
    Country: PT
    URL: https://www.parlamento.pt/Cidadania/Paginas/DadosAbertos.aspx

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="https://app.parlamento.pt",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept-Language": "pt,en",
            },
            timeout=120,
        )

    def _fetch_legislature_initiatives(self, leg: str) -> List[Dict[str, Any]]:
        """Fetch the JSON feed for a given legislature."""
        category = f"Iniciativas/{leg} Legislatura"
        filename = f"Iniciativas{leg}_json.txt"
        url = _build_url(category, filename)

        logger.info(f"Fetching initiatives for Legislature {leg}...")
        try:
            resp = self.client.get(url, timeout=180)
            if resp.status_code != 200:
                logger.warning(f"Legislature {leg}: HTTP {resp.status_code}")
                return []

            # Check if response is HTML error page
            text = resp.text
            if text.strip().startswith("<!DOCTYPE") or text.strip().startswith("<html"):
                logger.warning(f"Legislature {leg}: got HTML error page instead of JSON")
                return []

            data = resp.json()
            if not isinstance(data, list):
                logger.warning(f"Legislature {leg}: unexpected JSON structure")
                return []

            logger.info(f"Legislature {leg}: {len(data)} initiatives found")
            return data

        except Exception as e:
            logger.error(f"Legislature {leg}: failed to fetch: {e}")
            return []

    def _extract_date(self, record: Dict[str, Any]) -> Optional[str]:
        """Extract the most relevant date from an initiative record."""
        # Try events for publication or submission date
        eventos = record.get("IniEventos") or []
        for evento in eventos:
            data = evento.get("DataFase") or evento.get("DataEvento")
            if data:
                try:
                    # Parse date string (may be YYYY-MM-DD or other format)
                    if "T" in str(data):
                        dt = datetime.fromisoformat(str(data).replace("Z", "+00:00"))
                    else:
                        dt = datetime.strptime(str(data)[:10], "%Y-%m-%d")
                    return dt.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    continue

        # Fall back to legislature dates
        for field in ("DataInicioleg", "DataFimleg"):
            val = record.get(field)
            if val:
                try:
                    return str(val)[:10]
                except (ValueError, TypeError):
                    pass

        return None

    def _extract_authors(self, record: Dict[str, Any]) -> str:
        """Extract author names from initiative record."""
        authors = []

        # Deputies
        deps = record.get("IniAutorDeputados") or []
        for dep in deps:
            nome = dep.get("nome") or dep.get("Nome") or ""
            if nome:
                authors.append(nome)

        # Parliamentary groups
        grupos = record.get("IniAutorGruposParlamentares") or []
        for g in grupos:
            sigla = g.get("sigla") or g.get("Sigla") or ""
            if sigla:
                authors.append(sigla)

        # Other authors (e.g., Government)
        outros = record.get("IniAutorOutros")
        if isinstance(outros, dict):
            nome = outros.get("nome") or ""
            if nome:
                authors.append(nome)
        elif isinstance(outros, list):
            for o in outros:
                nome = o.get("nome") or ""
                if nome:
                    authors.append(nome)

        return "; ".join(authors) if authors else ""

    def _extract_pdf_text(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text using common/pdf_extract."""
        if not pdf_url:
            return None

        # Ensure HTTPS
        url = pdf_url.replace("http://", "https://")

        try:
            text = extract_pdf_markdown(
                source="PT/Parlamento",
                source_id=str(doc_id),
                pdf_url=url,
                table="legislation",
            )
            return text
        except Exception as e:
            logger.warning(f"PDF extraction failed for {doc_id}: {e}")
            # Fallback: try direct download + pdfplumber
            try:
                import io
                resp = self.client.get(url, timeout=60)
                if resp.status_code == 200 and resp.content[:4] == b"%PDF":
                    try:
                        import pdfplumber
                        pdf = pdfplumber.open(io.BytesIO(resp.content))
                        pages_text = []
                        for page in pdf.pages:
                            t = page.extract_text() or ""
                            if t:
                                pages_text.append(t)
                        pdf.close()
                        if pages_text:
                            return "\n\n".join(pages_text)
                    except ImportError:
                        pass
                    try:
                        from pypdf import PdfReader
                        reader = PdfReader(io.BytesIO(resp.content))
                        pages_text = []
                        for page in reader.pages:
                            t = page.extract_text() or ""
                            if t:
                                pages_text.append(t)
                        if pages_text:
                            return "\n\n".join(pages_text)
                    except ImportError:
                        pass
            except Exception as e2:
                logger.warning(f"Fallback PDF extraction also failed for {doc_id}: {e2}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw initiative record into standard schema."""
        doc_id = str(raw.get("IniId", ""))
        leg = raw.get("IniLeg", "")
        nr = raw.get("IniNr", "")
        tipo = raw.get("IniDescTipo", "")
        titulo = raw.get("IniTitulo") or raw.get("IniEpigrafe") or ""
        titulo = _clean_text(titulo)

        # Build a descriptive title if we have type and number
        if not titulo and tipo and nr:
            titulo = f"{tipo} n.º {nr}/{leg}"

        date = self._extract_date(raw)
        authors = self._extract_authors(raw)
        text = raw.get("text", "")

        # Source URL
        url = f"https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID={doc_id}"

        return {
            "_id": f"PT-Parlamento-{doc_id}",
            "_source": "PT/Parlamento",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": titulo,
            "text": _clean_text(text) if text else "",
            "date": date,
            "url": url,
            "initiative_type": tipo,
            "initiative_nr": str(nr) if nr else "",
            "legislature": str(leg),
            "authors": authors,
            "language": "pt",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all initiatives across all legislatures."""
        existing = preload_existing_ids("PT/Parlamento", table="legislation")

        for leg in LEGISLATURES:
            records = self._fetch_legislature_initiatives(leg)
            for rec in records:
                doc_id = str(rec.get("IniId", ""))
                if not doc_id:
                    continue
                full_id = f"PT-Parlamento-{doc_id}"
                if full_id in existing:
                    continue

                pdf_url = rec.get("IniLinkTexto", "")
                text = self._extract_pdf_text(pdf_url, doc_id)

                if not text:
                    logger.debug(f"Skipping {doc_id}: no text extracted")
                    continue

                rec["text"] = text
                yield self.normalize(rec)
                time.sleep(2)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent initiatives (last 2 legislatures)."""
        existing = preload_existing_ids("PT/Parlamento", table="legislation")
        recent_legs = LEGISLATURES[-2:]  # Last 2 legislatures

        for leg in recent_legs:
            records = self._fetch_legislature_initiatives(leg)
            for rec in records:
                doc_id = str(rec.get("IniId", ""))
                if not doc_id:
                    continue
                full_id = f"PT-Parlamento-{doc_id}"
                if full_id in existing:
                    continue

                pdf_url = rec.get("IniLinkTexto", "")
                text = self._extract_pdf_text(pdf_url, doc_id)

                if not text:
                    continue

                rec["text"] = text
                yield self.normalize(rec)
                time.sleep(2)

    def test(self) -> bool:
        """Quick connectivity test."""
        url = _build_url("Iniciativas/XVII Legislatura", "IniciativasXVII_json.txt")
        try:
            resp = self.client.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"Test OK: {len(data)} records in XVII Legislature")
                return True
        except Exception as e:
            logger.error(f"Test failed: {e}")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="PT/Parlamento Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to execute")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for testing")
    args = parser.parse_args()

    scraper = ParlamentoScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 15 if args.sample else 999999

        if args.sample:
            # In sample mode, only fetch from latest legislature
            existing = set()
            records = scraper._fetch_legislature_initiatives("XVII")
            for rec in records:
                if count >= max_records:
                    break
                doc_id = str(rec.get("IniId", ""))
                if not doc_id:
                    continue

                pdf_url = rec.get("IniLinkTexto", "")
                text = scraper._extract_pdf_text(pdf_url, doc_id)
                if not text or len(text) < 50:
                    continue

                rec["text"] = text
                normalized = scraper.normalize(rec)

                # Save sample
                safe_id = re.sub(r"[^\w\-]", "_", normalized["_id"])
                sample_path = sample_dir / f"{safe_id}.json"
                with open(sample_path, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

                count += 1
                logger.info(f"[{count}/{max_records}] Saved {normalized['_id']}: "
                            f"{normalized['title'][:60]}... ({len(text)} chars)")
                time.sleep(2)
        else:
            for record in scraper.fetch_all():
                if count >= max_records:
                    break
                count += 1
                if count % 50 == 0:
                    logger.info(f"Progress: {count} records fetched")

        logger.info(f"Bootstrap complete: {count} records")

    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} new records")


if __name__ == "__main__":
    main()
