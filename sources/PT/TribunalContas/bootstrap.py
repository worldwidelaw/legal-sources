#!/usr/bin/env python3
"""
PT/TribunalContas -- Portuguese Court of Auditors (Tribunal de Contas)

Fetches decisions, rulings, and acts from TCJure, the online database of
Portugal's Court of Auditors.

Strategy:
  - JSON search API: POST Search.aspx/GetDocumentsAdvanced
  - Document metadata page: GetDocument.aspx?rss=true&numero={ID}
  - Full text PDF: extracted URL from metadata page, downloaded and parsed
  - Especie types: 'J' (Jurisprudence), 'R' (Reports), 'A' (Acts)

Data:
  - ~4,400 records (acórdãos, sentenças, pareceres, despachos, etc.)
  - Language: Portuguese
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PT.TribunalContas")

TCJURE_BASE = "https://tcjure.tcontas.pt"
TCONTAS_BASE = "https://www.tcontas.pt"
SEARCH_URL = f"{TCJURE_BASE}/Search.aspx/GetDocumentsAdvanced"
DOCUMENT_URL = f"{TCJURE_BASE}/GetDocument.aspx"

PAGE_SIZE = 50
ESPECIE_ALL = "'R','J','A'"

# Section fallback order: TCJure metadata often says "1s" but the actual PDF
# is under "1sss" or "1spl".  Try alternatives until we get a real PDF.
SECTION_FALLBACKS = ['1sss', '1spl', '1s', '2s', '3s', 'pg', 'pg-rcj']


def clean_html(text: str) -> str:
    """Strip HTML tags and clean text."""
    if not text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_mod.unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' +', ' ', text)
    lines = [line.strip() for line in text.split('\n')]
    return '\n'.join(lines).strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="PT/TribunalContas",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

class TribunalContasScraper(BaseScraper):
    """
    Scraper for PT/TribunalContas -- Portuguese Court of Auditors.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'LegalDataHunter/1.0 (legal research; open data)',
            'Content-Type': 'application/json; charset=utf-8',
        })

    def _search_documents(self, page: int = 1, especie: str = ESPECIE_ALL,
                          year: str = "", page_size: int = PAGE_SIZE,
                          asc_desc: str = "2") -> Dict[str, Any]:
        """Query the TCJure search API. asc_desc: '1'=asc, '2'=desc."""
        data = {
            'CurrPage': page,
            'SearchText': '',
            'NumDoc': '',
            'AnoDoc': year,
            'Designacao': '',
            'Relator': '',
            'Descritores': '',
            'Entidade': '',
            'Especie': especie,
            'PageSize': page_size,
            'OrderBy': '1',
            'AscDesc': asc_desc,
            'TipoDocumento': '',
            'Orgao': '',
            'Data_Doc_From': '',
            'Data_Doc_To': ''
        }
        resp = self.session.post(SEARCH_URL, json=data, timeout=60)
        resp.raise_for_status()
        result = resp.json()
        d = result.get('d', [])
        if not d or len(d) < 5:
            return {'total': 0, 'pages': 0, 'items': []}
        return {
            'total': d[0],
            'pages': d[1],
            'page_size': d[2],
            'current_page': d[3],
            'items': d[4] if isinstance(d[4], list) else []
        }

    def _get_pdf_url(self, numero: int) -> Optional[str]:
        """Extract PDF URL from the GetDocument metadata page."""
        try:
            url = f"{DOCUMENT_URL}?rss=true&numero={numero}"
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            # PDF URL is in: var hasvalue = 'https://...pdf';
            match = re.search(r"var\s+hasvalue\s*=\s*'([^']+\.pdf)'", resp.text)
            if match:
                pdf_url = match.group(1)
                # Fix protocol if needed
                if pdf_url.startswith('http://'):
                    pdf_url = pdf_url.replace('http://', 'https://', 1)
                return pdf_url
        except Exception as e:
            logger.debug(f"Failed to get PDF URL for numero={numero}: {e}")
        return None

    def _resolve_pdf_url(self, raw_url: str) -> Optional[str]:
        """Try the raw URL first; if 404, cycle through section fallbacks."""
        # Quick check on the original URL
        try:
            resp = self.session.head(raw_url, timeout=15, allow_redirects=True)
            if resp.status_code == 200 and 'pdf' in resp.headers.get('content-type', ''):
                return raw_url
        except Exception:
            pass

        # Parse the URL to build alternatives
        # e.g. .../acordaos/1s/Documents/2024/ac001-2024-1s.pdf
        m = re.search(
            r'/ProdutosTC/(acordaos|Sentencas)/(\w+)/Documents/(\d{4})/(\w+)-(\d{4})-(\w+)\.pdf',
            raw_url, re.IGNORECASE,
        )
        if not m:
            return None

        doc_cat_orig, _sec_orig, year, prefix, _yr2, _sec_fname = m.groups()
        # Detect whether it's an acórdão or sentença from the filename prefix
        is_sentenca = prefix.startswith('st')
        categories = ['Sentencas', 'acordaos'] if is_sentenca else ['acordaos', 'Sentencas']

        base = f"{TCONTAS_BASE}/pt-pt/ProdutosTC"
        for cat in categories:
            for sec in SECTION_FALLBACKS:
                fname = f"{prefix}-{year}-{sec}.pdf"
                url = f"{base}/{cat}/{sec}/Documents/{year}/{fname}"
                try:
                    resp = self.session.head(url, timeout=10, allow_redirects=True)
                    if resp.status_code == 200 and 'pdf' in resp.headers.get('content-type', ''):
                        return url
                except Exception:
                    continue
        return None

    def _download_pdf_text(self, pdf_url: str) -> str:
        """Download a PDF and extract its text."""
        try:
            resp = self.session.get(pdf_url, timeout=60)
            if resp.status_code == 200 and resp.headers.get('content-type', '').startswith('application/pdf'):
                return extract_pdf_text(resp.content)
            elif resp.status_code == 200:
                # Might be HTML error page
                logger.debug(f"Non-PDF response from {pdf_url}")
        except Exception as e:
            logger.debug(f"PDF download failed for {pdf_url}: {e}")
        return ""

    def _get_document_metadata(self, numero: int) -> Dict[str, Any]:
        """Get additional metadata from the document page."""
        metadata = {}
        try:
            url = f"{DOCUMENT_URL}?rss=true&numero={numero}"
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            text = resp.text

            # Extract structured fields
            # Orgão (section)
            orgao_match = re.search(r'Org[aã]o:\s*</.*?>(.*?)</div', text, re.DOTALL | re.IGNORECASE)
            if orgao_match:
                metadata['orgao'] = clean_html(orgao_match.group(1)).strip()

            # Descritores (descriptors/keywords)
            desc_match = re.search(r'Descritores?:\s*</.*?>(.*?)</div', text, re.DOTALL | re.IGNORECASE)
            if desc_match:
                metadata['descriptors'] = clean_html(desc_match.group(1)).strip()

            # Notas (notes)
            notas_match = re.search(r'Notas?:\s*</.*?>(.*?)</div', text, re.DOTALL | re.IGNORECASE)
            if notas_match:
                metadata['notes'] = clean_html(notas_match.group(1)).strip()

            # PDF URL
            pdf_match = re.search(r"var\s+hasvalue\s*=\s*'([^']+\.pdf)'", text)
            if pdf_match:
                pdf_url = pdf_match.group(1)
                if pdf_url.startswith('http://'):
                    pdf_url = pdf_url.replace('http://', 'https://', 1)
                metadata['pdf_url'] = pdf_url

        except Exception as e:
            logger.debug(f"Failed to get metadata for numero={numero}: {e}")
        return metadata

    def _fetch_full_document(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch full document: metadata page + PDF text."""
        numero = item.get('Numero')
        if not numero:
            return item

        time.sleep(1)  # Rate limit

        # Get metadata page (includes PDF URL)
        metadata = self._get_document_metadata(numero)
        item['_metadata'] = metadata

        # Try to download and extract PDF text
        raw_pdf_url = metadata.get('pdf_url')
        if raw_pdf_url:
            resolved_url = self._resolve_pdf_url(raw_pdf_url)
            if resolved_url:
                time.sleep(1)  # Rate limit
                pdf_text = self._download_pdf_text(resolved_url)
                if pdf_text:
                    item['_pdf_text'] = pdf_text
                    item['_pdf_url'] = resolved_url
                    return item

        # Fallback: Sumario is often substantial legal text
        logger.debug(f"No PDF text for {numero}, using Sumario")
        return item

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from TCJure, newest first."""
        # Fetch jurisprudence year-by-year (newest first) so recent docs
        # with PDFs come first, then fall back to all remaining docs.
        current_year = datetime.now().year
        seen: set = set()

        # Phase 1: jurisprudence by year (most have PDFs)
        for year in range(current_year, 1989, -1):
            result = self._search_documents(page=1, year=str(year), especie="'J'")
            total_pages = result['pages']
            if result['total'] == 0:
                continue
            logger.info(f"Year {year}: {result['total']} jurisprudence docs")
            for item in result['items']:
                seen.add(item.get('Numero'))
                yield self._fetch_full_document(item)
            for page in range(2, total_pages + 1):
                time.sleep(1)
                result = self._search_documents(page=page, year=str(year), especie="'J'")
                for item in result['items']:
                    seen.add(item.get('Numero'))
                    yield self._fetch_full_document(item)

        # Phase 2: remaining docs (acts, reports) — all years
        result = self._search_documents(page=1)
        total_pages = result['pages']
        logger.info(f"Phase 2: {result['total']} total docs (filtering already-seen)")
        for item in result['items']:
            if item.get('Numero') not in seen:
                yield self._fetch_full_document(item)
        for page in range(2, total_pages + 1):
            time.sleep(1)
            result = self._search_documents(page=page)
            for item in result['items']:
                if item.get('Numero') not in seen:
                    yield self._fetch_full_document(item)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents from the current year (incremental)."""
        current_year = str(datetime.now().year)
        result = self._search_documents(page=1, year=current_year)
        total_pages = result['pages']

        for item in result['items']:
            doc = self._fetch_full_document(item)
            yield doc

        for page in range(2, total_pages + 1):
            time.sleep(1)
            result = self._search_documents(page=page, year=current_year)
            for item in result['items']:
                doc = self._fetch_full_document(item)
                yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform a raw TCJure document into standard schema."""
        numero = raw.get('Numero', '')
        num_doc = raw.get('Num_doc', '')
        ano_doc = raw.get('Ano_doc', '')
        designacao = raw.get('Designacao', '')
        data_doc = raw.get('DataDoc', '')
        sumario = raw.get('Sumario', '') or ''
        relator = raw.get('Relator', '')
        especie = raw.get('Especie', '')

        # Build unique ID
        doc_id = f"PT-TdC-{numero}"

        # Full text: prefer PDF, fall back to Sumario
        text = raw.get('_pdf_text', '') or sumario.strip()

        # Clean text
        text = clean_html(text)

        # URL
        pdf_url = raw.get('_pdf_url', '')
        doc_url = pdf_url or f"{TCJURE_BASE}/GetDocument.aspx?rss=true&numero={numero}"

        # Parse date
        date_str = None
        if data_doc:
            try:
                dt = datetime.strptime(data_doc[:10], '%Y-%m-%d')
                date_str = dt.strftime('%Y-%m-%d')
            except (ValueError, IndexError):
                date_str = data_doc

        # Extra metadata
        metadata = raw.get('_metadata', {})

        return {
            '_id': doc_id,
            '_source': 'PT/TribunalContas',
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': designacao,
            'text': text,
            'date': date_str,
            'url': doc_url,
            'document_number': num_doc,
            'year': ano_doc,
            'summary': clean_html(sumario) if raw.get('_pdf_text') else None,
            'rapporteur': relator,
            'document_type': especie,
            'section': metadata.get('orgao', ''),
            'descriptors': metadata.get('descriptors', ''),
            'notes': metadata.get('notes', ''),
        }

    # ── CLI ─────────────────────────────────────────────────────────

    def run_bootstrap(self, sample: bool = False):
        """Run full bootstrap or sample mode."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        target = 15 if sample else 999999
        errors = 0

        for raw in self.fetch_all():
            try:
                record = self.normalize(raw)
                text = record.get('text', '')
                if not text or len(text) < 50:
                    logger.warning(f"Skipping {record['_id']}: insufficient text ({len(text)} chars)")
                    errors += 1
                    continue

                fname = f"{record['_id'].replace('/', '_')}.json"
                out_path = sample_dir / fname if sample else self.source_dir / "data" / fname
                out_path.parent.mkdir(parents=True, exist_ok=True)

                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                count += 1
                logger.info(f"[{count}] {record['title'][:80]} ({len(text)} chars)")

                if count >= target:
                    break
            except Exception as e:
                logger.error(f"Error processing document: {e}")
                errors += 1
                if errors > 10 and count == 0:
                    logger.error("Too many errors with no successes, aborting")
                    break

        logger.info(f"Done: {count} documents saved, {errors} errors")
        return count

    def run_test(self):
        """Quick connectivity test."""
        result = self._search_documents(page=1, page_size=5)
        total = result['total']
        items = result['items']
        logger.info(f"API test: {total} total documents, got {len(items)} items")
        if items:
            logger.info(f"First: {items[0].get('Designacao', 'N/A')}")
        return total > 0


def main():
    import argparse
    parser = argparse.ArgumentParser(description="PT/TribunalContas bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TribunalContasScraper()

    if args.command == "test":
        ok = scraper.run_test()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        count = scraper.run_bootstrap(sample=args.sample)
        sys.exit(0 if count > 0 else 1)
    elif args.command == "update":
        count = scraper.run_bootstrap(sample=False)
        sys.exit(0 if count > 0 else 1)


if __name__ == "__main__":
    main()
