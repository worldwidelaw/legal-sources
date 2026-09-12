#!/usr/bin/env python3
"""
CL/Aduanas -- Servicio Nacional de Aduanas (Chilean National Customs Service)

Fetches customs regulations from aduana.cl:
  1. Compendio de Normas Aduaneras — 7 HTML chapters (legislation)
  2. Resoluciones Anticipadas — advance rulings with full HTML text (case_law)
  3. Oficios Circulares — yearly PDF circulars (legislation)
  4. Dictámenes de Clasificación — classification rulings by year (case_law)

Strategy:
  - Compendio: scrape full-text HTML from chapter pages
  - Oficios Circulares: parse year index pages for PDF links, download + extract
  - Dictámenes: parse year index pages for individual dictamen links, follow to PDF

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15+ sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import gzip
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.Aduanas")

BASE_URL = "https://www.aduana.cl"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

# ── Compendio de Normas Aduaneras chapters ────────────────────────
COMPENDIO_CHAPTERS = [
    {
        "id": "compendio-cap1",
        "title": "Compendio de Normas Aduaneras — Capítulo 1: Normas Generales",
        "path": "/aduana/site/artic/20070215/pags/20070215151856.html",
    },
    {
        "id": "compendio-cap2",
        "title": "Compendio de Normas Aduaneras — Capítulo 2: Valoración en Aduana de las Mercancías",
        "path": "/aduana/site/artic/20070215/pags/20070215153316.html",
    },
    {
        "id": "compendio-cap3",
        "title": "Compendio de Normas Aduaneras — Capítulo 3: Ingreso de Mercancías",
        "path": "/aduana/site/artic/20070216/pags/20070216135454.html",
    },
    {
        "id": "compendio-cap4",
        "title": "Compendio de Normas Aduaneras — Capítulo 4: Salida de Mercancías",
        "path": "/aduana/site/artic/20070216/pags/20070216180251.html",
    },
    {
        "id": "compendio-cap5",
        "title": "Compendio de Normas Aduaneras — Capítulo 5: Anulación y Modificación o Aclaración de las Declaraciones",
        "path": "/aduana/site/artic/20070219/pags/20070219104840.html",
    },
    {
        "id": "compendio-cap6",
        "title": "Compendio de Normas Aduaneras — Capítulo 6: Subasta Aduanera de Mercancías",
        "path": "/aduana/site/artic/20070219/pags/20070219113240.html",
    },
    {
        "id": "compendio-cap7",
        "title": "Compendio de Normas Aduaneras — Capítulo 7: Mercancías sujetas a Despacho Especial",
        "path": "/capitulo-7-mercancias-sujetas-a-despacho-especial/aduana/2016-11-22/164057.html",
    },
]

# ── Oficios Circulares year index pages ───────────────────────────
OFICIOS_YEARS = {
    2026: "/oficios-circulares-2026/aduana/2026-01-08/104353.html",
    2025: "/oficios-circulares-2025/aduana/2025-01-02/100116.html",
    2024: "/oficios-circulares-2024/aduana/2024-01-03/092040.html",
    2023: "/oficios-circulares-2023/aduana/2023-01-03/150849.html",
    2022: "/oficios-circulares-2022/aduana/2022-01-03/141454.html",
    2021: "/oficios-circulares-2021/aduana/2021-01-04/153226.html",
    2020: "/oficios-circulares-2020/aduana/2020-01-02/101702.html",
    2019: "/oficios-circulares-2019/aduana/2019-02-08/114337.html",
    2018: "/oficios-circulares-2018-y-anos-anteriores/aduana/2018-01-02/155822.html",
    2017: "/oficios-circulares-2017-y-anos-anteriores/aduana/2017-01-03/154524.html",
    2016: "/oficios-circulares-2016/aduana/2016-01-04/140543.html",
    2015: "/oficios-circulares-2015-y-anos-anteriores/aduana/2015-01-06/102736.html",
    2014: "/oficios-circulares-2014-y-anos-anteriores/aduana/2014-01-03/171015.html",
    2013: "/oficios-circulares-2013/aduana/2013-04-05/173617.html",
    2012: "/oficios-circulares-2012/aduana/2012-01-04/173509.html",
    2011: "/oficios-circulares-2011/aduana/2011-01-05/102342.html",
    2010: "/oficios-circulares-2010/aduana/2010-02-08/133411.html",
    2009: "/oficios-circulares-2009/aduana/2009-01-14/123051.html",
    2008: "/oficios-circulares-2008/aduana/2008-01-08/152738.html",
    2007: "/oficios-circulares-2007/aduana/2007-03-03/195353.html",
}

# ── Dictámenes de Clasificación year index pages ──────────────────
DICTAMENES_YEARS = {
    2009: "/aduana/site/artic/20090121/pags/20090121114037.html",
    2008: "/aduana/site/artic/20080204/pags/20080204145715.html",
    2007: "/aduana/site/artic/20070223/pags/20070223103743.html",
    2006: "/aduana/site/artic/20070223/pags/20070223110314.html",
    2005: "/aduana/site/artic/20070223/pags/20070223113723.html",
    2004: "/aduana/site/artic/20070223/pags/20070223122955.html",
    2003: "/aduana/site/artic/20070223/pags/20070223130959.html",
    2002: "/aduana/site/artic/20070223/pags/20070223132421.html",
    2001: "/aduana/site/artic/20070223/pags/20070223135148.html",
    2000: "/aduana/site/artic/20070223/pags/20070223153010.html",
    1999: "/aduana/site/artic/20070223/pags/20070223161310.html",
    1998: "/aduana/site/artic/20070223/pags/20070223162633.html",
    1997: "/aduana/site/artic/20070223/pags/20070223164348.html",
    1996: "/aduana/site/artic/20070223/pags/20070223170147.html",
    1995: "/aduana/site/artic/20070223/pags/20070223171502.html",
    1994: "/aduana/site/artic/20070223/pags/20070223173014.html",
    1993: "/aduana/site/artic/20070223/pags/20070223180518.html",
}


# ── HTTP helpers ──────────────────────────────────────────────────
def _get(url: str, timeout: int = 120) -> str:
    """GET a URL and return decoded text."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    raw = resp.read()
    for enc in ("utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _get_bytes(url: str, timeout: int = 120) -> bytes:
    """GET a URL and return raw bytes."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    return resp.read()


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_body_content(html: str) -> str:
    """Extract the main content area from an aduana.cl HTML page."""
    # Best: CUERPO class div (resoluciones anticipadas) — greedy to capture nested divs
    m = re.search(r'class="CUERPO"[^>]*>(.*)</div>\s*<!--/CUERPO-->', html, re.DOTALL)
    if not m:
        m = re.search(r'class="CUERPO"[^>]*>((?:(?!</div>\s*</div>\s*</div>).)*)', html, re.DOTALL)
    if m:
        cleaned = _clean_html(m.group(1))
        if len(cleaned) > 500:
            return cleaned

    # Try to find the main article/content div
    m = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL | re.IGNORECASE)
    if m and len(m.group(1)) > 200:
        return _clean_html(m.group(1))

    # Try articulo class div
    m = re.search(r'class="articulo"[^>]*>(.*?)</div>\s*</div>', html, re.DOTALL | re.IGNORECASE)
    if m and len(m.group(1)) > 200:
        return _clean_html(m.group(1))

    # Fallback: extract everything between first <h1>/<h2> and footer
    m = re.search(r'(<h[12][^>]*>.*?)<footer', html, re.DOTALL | re.IGNORECASE)
    if m:
        return _clean_html(m.group(1))

    # Last resort: extract body
    m = re.search(r'<body[^>]*>(.*?)</body>', html, re.DOTALL | re.IGNORECASE)
    if m:
        return _clean_html(m.group(1))

    return _clean_html(html)


def _parse_date(date_str: str) -> Optional[str]:
    """Parse various date formats to ISO format."""
    if not date_str:
        return None
    date_str = date_str.strip()

    # DD.MM.YYYY (common in aduana.cl)
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    # DD/MM/YYYY or DD-MM-YYYY
    m = re.match(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    # YYYY-MM-DD already
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        return date_str[:10]

    return None


# ── Sitemap-based discovery for resoluciones anticipadas ──────────
SITEMAP_INDEX = "https://www.aduana.cl/aduana/site/sitemap_pags.xml"


def _discover_resoluciones_anticipadas(max_months: int = 228) -> List[str]:
    """Scan monthly sitemaps for resolución anticipada page URLs."""
    results = []
    try:
        xml = _get(SITEMAP_INDEX, timeout=30)
    except Exception as e:
        logger.error(f"Failed to fetch sitemap index: {e}")
        return results

    # Extract monthly sitemap URLs
    sitemap_urls = re.findall(r'<loc>([^<]+\.xml\.gz)</loc>', xml)
    # Sort reverse chronological (newest first)
    sitemap_urls.sort(reverse=True)

    for sm_url in sitemap_urls[:max_months]:
        try:
            req = Request(sm_url, headers={"User-Agent": USER_AGENT})
            raw = urlopen(req, timeout=30).read()
            sm_xml = gzip.decompress(raw).decode("utf-8", errors="replace")
            urls = re.findall(
                r'<loc>([^<]*resolucion-anticipada[^<]*)</loc>',
                sm_xml, re.IGNORECASE,
            )
            results.extend(urls)
        except Exception as e:
            logger.debug(f"Skipping sitemap {sm_url}: {e}")
            continue

    logger.info(f"Discovered {len(results)} resoluciones anticipadas from sitemaps")
    return results


def _parse_resolucion_anticipada(url: str) -> Optional[Dict[str, Any]]:
    """Fetch a resolución anticipada page and extract metadata + full text."""
    try:
        html = _get(url, timeout=60)
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None

    text = _extract_body_content(html)
    if len(text) < 200:
        return None

    # Extract resolution number and date from URL slug
    slug = url.rsplit("/", 1)[-1].replace(".html", "")
    num_match = re.search(r'resolucion-anticipada-n-(\d+)', url, re.IGNORECASE)
    number = num_match.group(1) if num_match else slug

    # Extract date from URL path (YYYY-MM-DD pattern)
    date_match = re.search(r'/(\d{4}-\d{2}-\d{2})/', url)
    date_str = date_match.group(1) if date_match else None

    # Try to extract date from "del DD.MM.YYYY" in the URL slug
    if not date_str:
        d = re.search(r'del-(\d{1,2})-(\d{1,2})-(\d{4})', url)
        if d:
            date_str = f"{d.group(3)}-{d.group(2).zfill(2)}-{d.group(1).zfill(2)}"
        else:
            d = re.search(r'de-(\d{1,2})-(\d{1,2})-(\d{4})', url)
            if d:
                date_str = f"{d.group(3)}-{d.group(2).zfill(2)}-{d.group(1).zfill(2)}"

    # Extract title from page
    title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL | re.IGNORECASE)
    title = _clean_html(title_match.group(1)) if title_match else f"Resolución Anticipada N° {number}"

    doc_id = f"res-anticipada-{number}"

    return {
        "doc_id": doc_id,
        "title": title,
        "text": text,
        "date": date_str,
        "url": url,
        "section": "Resolución Anticipada",
        "data_type": "case_law",
        "number": number,
    }


# ── Parse oficios circulares from a year page ─────────────────────
def _parse_oficios_page(html: str, year: int) -> List[Dict[str, Any]]:
    """Extract oficio circular entries (PDF links + metadata) from a year page."""
    results = []

    # Find all PDF links with surrounding text describing the oficio
    # Pattern: text like "Oficio Circular N° 257 de 30.12.2024" followed by a PDF link
    # The HTML typically has: description text + <a href="...pdf">
    segments = re.split(r'(?=Oficio\s+Circular)', html, flags=re.IGNORECASE)

    for seg in segments[1:]:  # skip first segment (before any oficio mention)
        # Extract circular number
        num_match = re.search(r'Oficio\s+Circular\s+N[°º]?\s*(\d+)', seg, re.IGNORECASE)
        if not num_match:
            continue
        number = num_match.group(1)

        # Extract date from "de DD.MM.YYYY" pattern
        date_match = re.search(r'de\s+(\d{1,2}\.\d{1,2}\.\d{4})', seg)
        if not date_match:
            date_match = re.search(r'de\s+(\d{1,2}/\d{1,2}/\d{4})', seg)
        date_str = _parse_date(date_match.group(1)) if date_match else f"{year}-01-01"

        # Extract PDF link
        pdf_match = re.search(
            r'href=["\']?(/aduana/site/docs/[^\s"\'<>]+\.pdf)',
            seg, re.IGNORECASE
        )
        if not pdf_match:
            # Try absolute URL
            pdf_match = re.search(
                r'href=["\']?(https?://[^\s"\'<>]+\.pdf)',
                seg, re.IGNORECASE
            )
        if not pdf_match:
            continue

        pdf_path = pdf_match.group(1)
        if pdf_path.startswith("/"):
            pdf_url = BASE_URL + pdf_path
        else:
            pdf_url = pdf_path

        # Extract brief description (text after the number/date, before PDF link)
        desc_text = _clean_html(seg[:500])
        # Trim to just the first sentence or meaningful chunk
        desc_text = re.sub(r'\s+', ' ', desc_text).strip()
        title = f"Oficio Circular N° {number}"
        if date_match:
            title += f" de {date_match.group(1)}"

        doc_id = f"oficio-{year}-{number.zfill(3)}"

        results.append({
            "doc_id": doc_id,
            "title": title,
            "description": desc_text[:300],
            "date": date_str,
            "pdf_url": pdf_url,
            "section": "Oficio Circular",
            "year": year,
            "number": number,
        })

    return results


# ── Parse dictámenes from a year page ─────────────────────────────
def _parse_dictamenes_page(html: str, year: int) -> List[Dict[str, Any]]:
    """Extract dictamen entries from a year index page."""
    results = []

    # Individual dictamen links: "Dictamen N° 001" with an <a href> to the detail page
    pattern = r'<a[^>]+href=["\']?(/aduana/site/artic/[^\s"\'<>]+\.html)["\']?[^>]*>.*?Dictamen.*?N[°º]?\s*(\d+).*?</a>'
    for m in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
        detail_path = m.group(1)
        number = m.group(2)

        # Try to find associated date
        context = html[max(0, m.start()-100):m.end()+100]
        date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', context)
        if not date_match:
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', context)
        date_str = _parse_date(date_match.group(1)) if date_match else f"{year}-01-01"

        doc_id = f"dictamen-{year}-{number.zfill(3)}"
        detail_url = BASE_URL + detail_path

        results.append({
            "doc_id": doc_id,
            "title": f"Dictamen de Clasificación N° {number} ({year})",
            "date": date_str,
            "detail_url": detail_url,
            "section": "Dictamen de Clasificación",
            "year": year,
            "number": number,
        })

    # Also check for direct PDF links if no detail pages found
    if not results:
        for m in re.finditer(
            r'href=["\']?(/aduana/site/[^\s"\'<>]+\.pdf)["\']?',
            html, re.IGNORECASE
        ):
            pdf_path = m.group(1)
            # Try to extract dictamen number from filename
            num_match = re.search(r'(\d+)', pdf_path.rsplit('/', 1)[-1])
            if num_match:
                number = num_match.group(1)
                doc_id = f"dictamen-{year}-{number.zfill(3)}"
                results.append({
                    "doc_id": doc_id,
                    "title": f"Dictamen de Clasificación N° {number} ({year})",
                    "date": f"{year}-01-01",
                    "pdf_url": BASE_URL + pdf_path,
                    "section": "Dictamen de Clasificación",
                    "year": year,
                    "number": number,
                })

    return results


def _get_dictamen_pdf_url(detail_url: str) -> Optional[str]:
    """Fetch a dictamen detail page and extract the PDF download link."""
    try:
        html = _get(detail_url, timeout=60)
        # Look for "Ver Dictamen (PDF)" or similar link
        m = re.search(
            r'href=["\']?(/aduana/site/[^\s"\'<>]+\.pdf|https?://[^\s"\'<>]+\.pdf)',
            html, re.IGNORECASE
        )
        if m:
            url = m.group(1)
            return BASE_URL + url if url.startswith("/") else url
    except Exception as e:
        logger.warning(f"Failed to fetch dictamen detail {detail_url}: {e}")
    return None


class AduanasScraper(BaseScraper):
    SOURCE_ID = "CL/Aduanas"

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Yield all customs regulation documents."""
        count = 0

        # 1. Compendio chapters (HTML full text)
        logger.info("=== Fetching Compendio de Normas Aduaneras ===")
        for ch in COMPENDIO_CHAPTERS:
            if max_records and count >= max_records:
                return
            record = self._fetch_compendio_chapter(ch)
            if record:
                yield record
                count += 1
            time.sleep(1.5)

        # 2. Resoluciones Anticipadas (HTML full text from sitemaps)
        logger.info("=== Fetching Resoluciones Anticipadas ===")
        sm_months = 12 if max_records else 228
        ra_urls = _discover_resoluciones_anticipadas(max_months=sm_months)
        seen_ra = set()
        for ra_url in ra_urls:
            if max_records and count >= max_records:
                return
            if ra_url in seen_ra:
                continue
            seen_ra.add(ra_url)
            parsed = _parse_resolucion_anticipada(ra_url)
            if parsed and len(parsed.get("text", "")) >= 100:
                yield parsed  # RAW; normalized downstream
                count += 1
            time.sleep(1.5)

        # 3. Oficios Circulares (PDFs by year)
        logger.info("=== Fetching Oficios Circulares ===")
        for year in sorted(OFICIOS_YEARS.keys(), reverse=True):
            if max_records and count >= max_records:
                return
            url = BASE_URL + OFICIOS_YEARS[year]
            logger.info(f"Fetching oficios circulares {year} from {url}")
            try:
                html = _get(url)
                oficios = _parse_oficios_page(html, year)
                logger.info(f"  Found {len(oficios)} oficios for {year}")
            except Exception as e:
                logger.error(f"  Error fetching oficios {year}: {e}")
                continue

            for oficio in oficios:
                if max_records and count >= max_records:
                    return
                record = self._process_pdf_document(oficio, "legislation")
                if record:
                    yield record
                    count += 1
            time.sleep(1)

        # 4. Dictámenes de Clasificación (case law)
        logger.info("=== Fetching Dictámenes de Clasificación ===")
        for year in sorted(DICTAMENES_YEARS.keys(), reverse=True):
            if max_records and count >= max_records:
                return
            url = BASE_URL + DICTAMENES_YEARS[year]
            logger.info(f"Fetching dictámenes {year}")
            try:
                html = _get(url)
                dictamenes = _parse_dictamenes_page(html, year)
                logger.info(f"  Found {len(dictamenes)} dictámenes for {year}")
            except Exception as e:
                logger.error(f"  Error fetching dictámenes {year}: {e}")
                continue

            for dic in dictamenes:
                if max_records and count >= max_records:
                    return
                # Need to get PDF URL from detail page
                if "pdf_url" not in dic and "detail_url" in dic:
                    pdf_url = _get_dictamen_pdf_url(dic["detail_url"])
                    if pdf_url:
                        dic["pdf_url"] = pdf_url
                    else:
                        logger.warning(f"No PDF found for {dic['doc_id']}")
                        continue
                    time.sleep(1)

                record = self._process_pdf_document(dic, "case_law")
                if record:
                    yield record
                    count += 1

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent oficios circulares (current year)."""
        since = as_date_str(since)  # update() passes a datetime; #1512
        current_year = datetime.now().year
        for year in (current_year, current_year - 1):
            if year not in OFICIOS_YEARS:
                continue
            url = BASE_URL + OFICIOS_YEARS[year]
            try:
                html = _get(url)
                oficios = _parse_oficios_page(html, year)
            except Exception as e:
                logger.error(f"Error fetching oficios {year}: {e}")
                continue

            for oficio in oficios:
                if since and oficio.get("date") and oficio["date"] < since:
                    continue
                record = self._process_pdf_document(oficio, "legislation")
                if record:
                    yield record

    def _fetch_compendio_chapter(self, chapter: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch a Compendio chapter (full-text HTML)."""
        url = BASE_URL + chapter["path"]
        logger.info(f"Fetching {chapter['id']}: {chapter['title']}")
        try:
            html = _get(url)
        except Exception as e:
            logger.error(f"Failed to fetch {chapter['id']}: {e}")
            return None

        text = _extract_body_content(html)
        if len(text) < 100:
            logger.warning(f"Insufficient text for {chapter['id']}: {len(text)} chars")
            return None

        # Yield RAW per BaseScraper contract; the framework/main() normalizes.
        return {
            "doc_id": chapter["id"],
            "title": chapter["title"],
            "text": text,
            "date": None,
            "url": url,
            "section": "Compendio de Normas Aduaneras",
            "data_type": "legislation",
        }

    def _process_pdf_document(self, doc: Dict[str, Any], data_type: str) -> Optional[Dict[str, Any]]:
        """Download a PDF and extract text."""
        doc_id = doc["doc_id"]
        pdf_url = doc.get("pdf_url")
        if not pdf_url:
            return None

        logger.info(f"Processing {doc_id}: {doc['title'][:60]}")
        time.sleep(1)

        try:
            pdf_data = _get_bytes(pdf_url)
        except Exception as e:
            logger.warning(f"PDF download failed for {doc_id}: {e}")
            return None

        table = "case_law" if data_type == "case_law" else "legislation"
        text = extract_pdf_markdown(
            source=self.SOURCE_ID,
            source_id=doc_id,
            pdf_bytes=pdf_data,
            table=table,
        )

        if not text or len(text) < 50:
            logger.warning(f"No/insufficient text for {doc_id} ({len(text) if text else 0} chars)")
            return None

        # Yield RAW per BaseScraper contract; the framework/main() normalizes.
        return {
            **doc,
            "text": text,
            "url": pdf_url,
            "data_type": data_type,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        data_type = raw.get("data_type", "legislation")
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": data_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "section": raw.get("section", ""),
            "description": raw.get("description", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/Aduanas data fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all years)")
    args = parser.parse_args()

    source_dir = Path(__file__).resolve().parent
    scraper = AduanasScraper(str(source_dir))

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            html = _get(BASE_URL + "/aduana/site/edic/base/port/normativas.html", timeout=30)
            logger.info(f"  Main page: {len(html)} bytes")
            html2 = _get(BASE_URL + COMPENDIO_CHAPTERS[0]["path"], timeout=30)
            logger.info(f"  Compendio Cap 1: {len(html2)} bytes")
            logger.info("Connectivity OK")
        except Exception as e:
            logger.error(f"Test failed: {e}")
            sys.exit(1)
        return

    sample_dir = source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    # Also write to data/records.jsonl for full bootstrap
    data_dir = source_dir / "data"
    data_dir.mkdir(exist_ok=True)
    jsonl_path = data_dir / "records.jsonl"

    count = 0
    limit = 15 if args.sample else 999999

    gen = scraper.fetch_all(max_records=limit) if args.command in ("bootstrap", "bootstrap-fast") else scraper.fetch_updates()

    jsonl_file = open(jsonl_path, "a", encoding="utf-8") if not args.sample else None

    try:
        for raw in gen:
            if count >= limit:
                break
            record = scraper.normalize(raw)
            text_len = len(record.get("text", ""))
            if text_len < 100:
                logger.warning(f"Skipping {record['_id']}: text too short ({text_len} chars)")
                continue

            # Save to sample/
            if count < 20 or args.sample:
                fname = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"]) + ".json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            # Write to JSONL for full bootstrap
            if jsonl_file:
                jsonl_file.write(json.dumps(record, ensure_ascii=False) + "\n")

            count += 1
            logger.info(f"[{count}/{limit}] Saved {record['_id']} ({text_len} chars)")
    finally:
        if jsonl_file:
            jsonl_file.close()

    logger.info(f"Done: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
