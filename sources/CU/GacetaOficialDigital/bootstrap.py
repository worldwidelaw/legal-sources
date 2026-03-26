#!/usr/bin/env python3
"""
CU/GacetaOficialDigital -- Cuba Official Gazette Digital Archive

Fetches Cuban legislation from the Gaceta Oficial de la República de Cuba.

Strategy:
  - Paginate norms listing at /es/normas-juridicas?page=N
  - Visit each norm detail page for metadata + gazette reference
  - Visit gazette page for PDF download URL
  - Download gazette PDF, extract full text with PyPDF2
  - Each gazette PDF contains multiple norms; we extract the full
    gazette text and store it per-norm (the gazette text IS the norm's
    full text since it's the official publication)

Data:
  - ~30,000 norms from 1990 to present
  - Types: laws, decrees, resolutions, agreements, proclamations, etc.
  - Full text from gazette PDFs
  - Language: Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Dict, Any, Optional, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CU.GacetaOficialDigital")

BASE_URL = "https://www.gacetaoficial.gob.cu"
NORMS_URL = BASE_URL + "/es/normas-juridicas?page={page}"
CRAWL_DELAY = 10  # robots.txt requires 10s delay

# Cache of gazette slug -> (pdf_url, pdf_text)
_gazette_cache: Dict[str, Tuple[str, str]] = {}


class CubaGacetaOficialScraper(BaseScraper):
    """
    Scraper for CU/GacetaOficialDigital -- Cuba Official Gazette.
    Country: CU
    URL: https://www.gacetaoficial.gob.cu/

    Data types: legislation
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "es-CU,es;q=0.9,en;q=0.5",
            },
            timeout=60,
        )

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page with crawl delay."""
        try:
            time.sleep(CRAWL_DELAY)
            resp = self.client.session.get(url, timeout=self.client.timeout)
            if resp.status_code == 403:
                logger.warning(f"403 Forbidden: {url}")
                return None
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file."""
        try:
            time.sleep(CRAWL_DELAY)
            resp = self.client.session.get(url, timeout=120)
            resp.raise_for_status()
            if len(resp.content) < 100:
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF {url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from a PDF using PyPDF2."""
        try:
            import PyPDF2
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            pages_text = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            full_text = "\n".join(pages_text)
            # Clean up common OCR/PDF artifacts
            full_text = re.sub(r'[ \t]+', ' ', full_text)
            full_text = re.sub(r' *\n *', '\n', full_text)
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            return full_text.strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    def _parse_norms_listing(self, html: str) -> list:
        """Parse a norms listing page, returning list of norm stubs."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        norms = []
        seen_slugs = set()

        # Find all norm links (pattern: /es/{type}-{number}-de-{year}-de-{issuer})
        for a in soup.find_all('a', href=True):
            href = a['href']
            if not re.match(r'/es/\w+.*-de-\d{4}', href):
                continue
            if href in seen_slugs:
                continue
            # Skip "Leer más" duplicate links
            text = a.get_text(strip=True)
            if text.startswith('Leer más'):
                continue
            seen_slugs.add(href)
            norms.append({
                'slug': href,
                'title': text,
                'url': BASE_URL + href,
            })
        return norms

    def _parse_norm_detail(self, html: str) -> Dict[str, Any]:
        """Parse a norm detail page for metadata."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        meta = {}

        # Extract fields by label
        fields = soup.select('.field')
        for field in fields:
            label_el = field.select_one('.field-label')
            if not label_el:
                continue
            label = label_el.get_text(strip=True).rstrip(':').lower()
            # Get value from the sibling or next element
            items = field.select('.field-item, .field-items')
            value = ''
            if items:
                value = items[0].get_text(strip=True)
            else:
                # Get text minus the label
                full = field.get_text(strip=True)
                lbl_text = label_el.get_text(strip=True)
                value = full[len(lbl_text):].strip()

            if 'identificador' in label:
                meta['goc_id'] = value
            elif 'publicado' in label:
                meta['gazette_ref'] = value
                # Find gazette link
                gazette_link = field.select_one('a[href*="gaceta-oficial"]')
                if gazette_link:
                    meta['gazette_slug'] = gazette_link['href']
            elif 'resumen' in label:
                meta['summary'] = value
            elif 'número' in label and 'número' == label:
                meta['norm_number'] = value
            elif 'año' in label:
                meta['year'] = value
            elif 'palabras' in label:
                meta['keywords'] = value

        return meta

    def _get_gazette_text(self, gazette_slug: str) -> Tuple[str, str]:
        """Get full text from a gazette PDF, with caching."""
        if gazette_slug in _gazette_cache:
            return _gazette_cache[gazette_slug]

        gazette_url = BASE_URL + gazette_slug
        html = self._fetch_page(gazette_url)
        if not html:
            _gazette_cache[gazette_slug] = ("", "")
            return ("", "")

        # Find PDF download link
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        pdf_url = ""
        for a in soup.find_all('a', href=True):
            if '.pdf' in a['href']:
                href = a['href']
                if href.startswith('/'):
                    href = BASE_URL + href
                elif not href.startswith('http'):
                    href = BASE_URL + '/' + href
                pdf_url = href
                break

        if not pdf_url:
            logger.warning(f"No PDF link found on gazette page: {gazette_slug}")
            _gazette_cache[gazette_slug] = ("", "")
            return ("", "")

        logger.info(f"Downloading gazette PDF: {pdf_url}")
        pdf_bytes = self._fetch_pdf(pdf_url)
        if not pdf_bytes:
            _gazette_cache[gazette_slug] = (pdf_url, "")
            return (pdf_url, "")

        # Check size - skip if > 50MB
        if len(pdf_bytes) > 50 * 1024 * 1024:
            logger.warning(f"Gazette PDF too large ({len(pdf_bytes)} bytes), skipping text extraction")
            _gazette_cache[gazette_slug] = (pdf_url, "")
            return (pdf_url, "")

        text = self._extract_pdf_text(pdf_bytes)
        logger.info(f"Extracted {len(text)} chars from gazette PDF")
        _gazette_cache[gazette_slug] = (pdf_url, text)
        return (pdf_url, text)

    def _extract_norm_text_from_gazette(self, gazette_text: str, goc_id: str, title: str) -> str:
        """Extract the specific norm's text from the full gazette.

        Gazette PDFs have structure:
          - SUMARIO (table of contents) at the top, ending with ___________
          - Then body text with each norm preceded by its GOC ID
          - GOC IDs in the body mark norm boundaries

        The GOC ID typically appears twice: once in the SUMARIO and once
        before the actual body text. We use the LAST occurrence to find
        the body text.
        """
        if not gazette_text:
            return ""

        if goc_id:
            # Find ALL occurrences of this GOC ID
            pattern = re.escape(goc_id)
            matches = list(re.finditer(pattern, gazette_text))
            if matches:
                # Use the LAST occurrence (body text, not SUMARIO)
                last_match = matches[-1]
                start = last_match.end()
                # Find the next different GOC ID to delimit the end
                next_goc = re.search(
                    r'GOC-\d{4}-\d+-[A-Z]+\d+',
                    gazette_text[start:]
                )
                if next_goc:
                    # Check it's a different GOC ID
                    next_id = next_goc.group()
                    if next_id == goc_id:
                        # Same ID again, skip and look further
                        further = start + next_goc.end()
                        next_goc2 = re.search(
                            r'GOC-\d{4}-\d+-[A-Z]+\d+',
                            gazette_text[further:]
                        )
                        if next_goc2:
                            end = further + next_goc2.start()
                        else:
                            end = len(gazette_text)
                    else:
                        end = start + next_goc.start()
                else:
                    end = len(gazette_text)
                section = gazette_text[start:end].strip()
                # Clean: remove leading dots/page numbers from SUMARIO remnants
                section = re.sub(r'^[.\s\d]+\n', '', section)
                if len(section) > 100:
                    return section

        # Fallback: skip SUMARIO separator and return full body
        sumario_end = gazette_text.find('_________________')
        body = gazette_text[sumario_end:] if sumario_end > 0 else gazette_text
        return body.strip()

    def _parse_norm_type_from_slug(self, slug: str) -> str:
        """Extract norm type from URL slug."""
        # /es/resolucion-63-de-2026-de-ministerio-de-justicia
        m = re.match(r'/es/([a-z-]+?)-\d', slug)
        if m:
            raw = m.group(1).rstrip('-')
            # Capitalize and fix common types
            type_map = {
                'resolucion': 'Resolución',
                'resolucion-conjunta': 'Resolución Conjunta',
                'ley': 'Ley',
                'decreto': 'Decreto',
                'decreto-ley': 'Decreto-Ley',
                'acuerdo': 'Acuerdo',
                'proclama': 'Proclama',
                'constitucion': 'Constitución',
                'instruccion': 'Instrucción',
                'indicacion': 'Indicación',
            }
            return type_map.get(raw, raw.replace('-', ' ').title())
        return ""

    def _parse_issuer_from_slug(self, slug: str) -> str:
        """Extract issuing body from URL slug."""
        # /es/resolucion-63-de-2026-de-ministerio-de-justicia
        parts = slug.split('-de-')
        if len(parts) >= 3:
            # Last part(s) after year are the issuer
            year_idx = None
            for i, p in enumerate(parts):
                if re.match(r'^\d{4}$', p.strip()):
                    year_idx = i
                    break
            if year_idx is not None and year_idx + 1 < len(parts):
                issuer_parts = parts[year_idx + 1:]
                issuer = ' de '.join(issuer_parts)
                return issuer.replace('-', ' ').title()
        return ""

    def _parse_date_from_gazette_text(self, gazette_text: str) -> Optional[str]:
        """Try to extract a publication date from gazette header text."""
        # Look for patterns like "16 DE MARZO DE 2026"
        months = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12',
        }
        m = re.search(
            r'(\d{1,2})\s+DE\s+(\w+)\s+DE\s+(\d{4})',
            gazette_text[:2000], re.IGNORECASE
        )
        if m:
            day = m.group(1).zfill(2)
            month_name = m.group(2).lower()
            year = m.group(3)
            month = months.get(month_name)
            if month:
                return f"{year}-{month}-{day}"
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document into the standard schema."""
        return {
            "_id": raw.get("goc_id") or raw.get("slug", ""),
            "_source": "CU/GacetaOficialDigital",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "goc_id": raw.get("goc_id", ""),
            "norm_type": raw.get("norm_type", ""),
            "norm_number": raw.get("norm_number", ""),
            "issuing_body": raw.get("issuing_body", ""),
            "summary": raw.get("summary", ""),
            "gazette_ref": raw.get("gazette_ref", ""),
            "keywords": raw.get("keywords", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all norms with full text from gazette PDFs."""
        page = 0
        max_pages = 1493
        while page < max_pages:
            logger.info(f"Fetching norms listing page {page}/{max_pages}")
            html = self._fetch_page(NORMS_URL.format(page=page))
            if not html:
                logger.warning(f"Failed to fetch page {page}, skipping")
                page += 1
                continue

            stubs = self._parse_norms_listing(html)
            if not stubs:
                logger.info(f"No norms on page {page}, stopping")
                break

            for stub in stubs:
                doc = self._fetch_single_norm(stub)
                if doc:
                    yield self.normalize(doc)

            page += 1

    def _fetch_single_norm(self, stub: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch a single norm: detail page + gazette PDF text."""
        slug = stub['slug']
        logger.info(f"Processing norm: {stub['title'][:80]}")

        # Fetch norm detail page
        detail_html = self._fetch_page(BASE_URL + slug)
        if not detail_html:
            return None

        meta = self._parse_norm_detail(detail_html)
        goc_id = meta.get('goc_id', '')
        gazette_slug = meta.get('gazette_slug', '')

        # Get full text from gazette PDF
        text = ""
        if gazette_slug:
            pdf_url, gazette_text = self._get_gazette_text(gazette_slug)
            if gazette_text:
                text = self._extract_norm_text_from_gazette(gazette_text, goc_id, stub['title'])

        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for norm: {stub['title'][:60]} ({len(text) if text else 0} chars)")
            return None

        # Parse norm type and issuer from slug if not in metadata
        norm_type = self._parse_norm_type_from_slug(slug)
        issuer = self._parse_issuer_from_slug(slug)

        # Parse date from year field or gazette text
        date = None
        if meta.get('year'):
            date = f"{meta['year']}-01-01"  # Year only
        if gazette_slug and gazette_slug in _gazette_cache:
            _, gt = _gazette_cache[gazette_slug]
            if gt:
                parsed_date = self._parse_date_from_gazette_text(gt)
                if parsed_date:
                    date = parsed_date

        return {
            'slug': slug,
            'title': stub['title'],
            'url': stub['url'],
            'goc_id': goc_id,
            'norm_type': norm_type,
            'norm_number': meta.get('norm_number', ''),
            'issuing_body': issuer,
            'summary': meta.get('summary', ''),
            'gazette_ref': meta.get('gazette_ref', ''),
            'keywords': meta.get('keywords', ''),
            'date': date,
            'text': text,
        }

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch norms updated since a given date."""
        # The norms listing appears to be sorted by recency
        # Fetch recent pages until we hit norms older than 'since'
        since_dt = datetime.fromisoformat(since)
        for doc in self.fetch_all():
            doc_date = doc.get("date")
            if doc_date:
                try:
                    dt = datetime.fromisoformat(doc_date)
                    if dt < since_dt:
                        return
                except ValueError:
                    pass
            yield doc

    def test(self) -> bool:
        """Quick connectivity test."""
        html = self._fetch_page(NORMS_URL.format(page=0))
        if not html:
            logger.error("Cannot reach norms listing page")
            return False
        stubs = self._parse_norms_listing(html)
        logger.info(f"Connectivity OK: found {len(stubs)} norms on page 0")
        return len(stubs) > 0

    def bootstrap(self, sample: bool = False):
        """Run initial bootstrap, optionally just a sample."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)
        data_dir = self.source_dir / "data"
        data_dir.mkdir(exist_ok=True)

        if sample:
            logger.info("Running sample bootstrap (15 records)")
            count = 0
            target = 15

            # Fetch first page of norms listing
            html = self._fetch_page(NORMS_URL.format(page=0))
            if not html:
                logger.error("Cannot fetch norms listing")
                return

            stubs = self._parse_norms_listing(html)
            logger.info(f"Found {len(stubs)} norms on first page")

            for stub in stubs:
                if count >= target:
                    break
                doc = self._fetch_single_norm(stub)
                if doc:
                    record = self.normalize(doc)
                    safe_id = re.sub(r'[^\w-]', '_', record['_id'])[:80]
                    out_path = sample_dir / f"{safe_id}.json"
                    with open(out_path, 'w', encoding='utf-8') as f:
                        json.dump(record, f, ensure_ascii=False, indent=2)
                    count += 1
                    text_len = len(record.get('text', ''))
                    logger.info(f"[{count}/{target}] Saved: {record['title'][:60]} ({text_len} chars)")

            logger.info(f"Sample bootstrap complete: {count} records saved to {sample_dir}")
        else:
            logger.info("Running full bootstrap")
            count = 0
            for record in self.fetch_all():
                safe_id = re.sub(r'[^\w-]', '_', record['_id'])[:80]
                out_path = data_dir / f"{safe_id}.json"
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 100 == 0:
                    logger.info(f"Progress: {count} records saved")
            logger.info(f"Full bootstrap complete: {count} records saved to {data_dir}")


if __name__ == "__main__":
    scraper = CubaGacetaOficialScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [test|bootstrap] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)
    elif cmd == "bootstrap":
        sample = "--sample" in sys.argv
        scraper.bootstrap(sample=sample)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
