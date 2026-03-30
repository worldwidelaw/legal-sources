#!/usr/bin/env python3
"""
EC/AsambleaNacional -- Ecuador Legislation (oficial.ec)

Fetches Ecuadorian laws, decrees, agreements, resolutions, and ordinances
from oficial.ec, an open-access Drupal site publishing Registro Oficial content.

Strategy:
  1. Parse sitemaps (page=1 and page=2) for all document URLs (~6,900)
  2. Fetch each page and extract full text from the Drupal body field
  3. Extract metadata: institution, Registro Oficial number, date

Content structure (Drupal 7):
  - Body text in div.field-name-body > div.field-item.even
  - Institution in div.field-name-field-instituciones
  - RO date in div.field-name-field-ro-date
  - Title in og:title or <h1>

Respects robots.txt Crawl-delay: 10

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick API connectivity test
"""

import sys
import re
import html
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EC.AsambleaNacional")

BASE_URL = "https://www.oficial.ec"

# URL prefixes that are NOT legal documents (skip these)
SKIP_PREFIXES = (
    "/temas/", "/instituciones/", "/acerca-", "/user", "/contacto",
    "/leyes", "/decretos", "/acuerdos", "/resoluciones", "/ordenanzas",
    "/sentencias", "/ediciones-especiales", "/registros-oficiales",
)

# Spanish month mapping
SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def _parse_spanish_date(date_str: str) -> Optional[str]:
    """Parse dates like '21 de Abril de 2020' or 'Jueves 10 de Julio de 2014'."""
    if not date_str:
        return None
    # Remove day-of-week prefix if present (e.g., "Jueves ")
    cleaned = re.sub(r"^(?:lunes|martes|miércoles|jueves|viernes|sábado|domingo)\s+",
                     "", date_str.strip(), flags=re.IGNORECASE)
    match = re.match(
        r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})",
        cleaned.strip(), re.IGNORECASE
    )
    if match:
        day = int(match.group(1))
        month_str = match.group(2).lower()
        year = int(match.group(3))
        month = SPANISH_MONTHS.get(month_str)
        if month:
            try:
                return f"{year:04d}-{month:02d}-{day:02d}"
            except ValueError:
                pass
    return None


def _classify_document_type(slug: str) -> str:
    """Classify document type from URL slug."""
    if slug.startswith("ley-"):
        return "ley"
    elif slug.startswith("decreto-"):
        return "decreto"
    elif slug.startswith("acuerdo-"):
        return "acuerdo"
    elif slug.startswith("resolucion-"):
        return "resolucion"
    elif slug.startswith("ordenanza-"):
        return "ordenanza"
    elif slug.startswith("sentencia-"):
        return "sentencia"
    elif slug.startswith("registro-oficial-"):
        return "registro_oficial"
    elif slug.startswith("edicion-especial-"):
        return "edicion_especial"
    return "otro"


class AsambleaNacionalScraper(BaseScraper):
    """
    Scraper for EC/AsambleaNacional -- Ecuadorian legislation from oficial.ec.
    Country: EC
    URL: https://www.oficial.ec/

    Data types: legislation (laws, decrees, agreements, resolutions)
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    def _fetch_sitemap_urls(self) -> list[dict]:
        """
        Parse sitemap pages and return all document URLs.
        Filters out non-document pages (temas, instituciones, etc.).
        """
        all_urls = []

        for page in [1, 2]:
            logger.info(f"Fetching sitemap page {page}...")
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"/sitemap.xml?page={page}")
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Sitemap page {page} failed: {e}")
                continue

            root = ET.fromstring(resp.content)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            for url_elem in root.findall("sm:url", ns):
                loc = url_elem.find("sm:loc", ns)
                if loc is None or loc.text is None:
                    continue

                url = loc.text.strip()
                path = url.replace(BASE_URL, "")

                # Skip non-document pages
                if path == "/" or not path:
                    continue
                if any(path.startswith(p) for p in SKIP_PREFIXES):
                    continue

                lastmod = None
                lastmod_elem = url_elem.find("sm:lastmod", ns)
                if lastmod_elem is not None and lastmod_elem.text:
                    lastmod = lastmod_elem.text.strip()

                all_urls.append({"url": url, "path": path, "lastmod": lastmod})

        logger.info(f"Found {len(all_urls)} document URLs in sitemap")
        return all_urls

    def _parse_page(self, url: str, path: str) -> Optional[dict]:
        """
        Fetch and parse a single document page.
        Returns raw dict with extracted content or None on failure.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)

            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            content = resp.text
        except Exception as e:
            logger.warning(f"Error fetching {path}: {e}")
            return None

        # Extract title from <h1> or <title>
        title = ""
        h1_match = re.search(r'<h1[^>]*>([^<]+)</h1>', content)
        if h1_match:
            title = html.unescape(h1_match.group(1)).strip()
        if not title:
            title_match = re.search(r"<title>([^<]+)</title>", content)
            if title_match:
                title = html.unescape(title_match.group(1)).strip()
                title = re.sub(r"\s*\|\s*Oficial\s*$", "", title)

        # Helper to extract a Drupal field's text content
        def extract_field(field_name: str) -> str:
            pat = (
                f'field-name-{field_name}.*?field-item even[^>]*>(.*?)</div>'
            )
            match = re.search(pat, content, re.DOTALL)
            if match:
                text = re.sub(r"<[^>]+>", " ", match.group(1))
                text = html.unescape(text)
                return re.sub(r"\s+", " ", text).strip()
            return ""

        # Combine content fields into full text
        # Pages use: field-header, field-considerando, body, field-firmas
        text_parts = []

        header = extract_field("field-header")
        if header:
            text_parts.append(header)

        considerando = extract_field("field-considerando")
        if considerando:
            text_parts.append(considerando)

        # Body field (main content — may be large)
        body_text = ""
        body_start = content.find("field-name-body")
        if body_start != -1:
            body_section = content[body_start:]
            item_start = body_section.find("field-item even")
            if item_start != -1:
                content_start = body_section.index(">", item_start) + 1
                body_content = body_section[content_start:]

                end_patterns = [
                    '<div class="ds-',
                    "<footer",
                    '<div id="block-',
                    '<div class="region',
                ]
                end_pos = len(body_content)
                for pat in end_patterns:
                    pos = body_content.find(pat)
                    if pos != -1 and pos < end_pos:
                        end_pos = pos

                body_html = body_content[:end_pos]
                body_text = re.sub(r"<[^>]+>", " ", body_html)
                body_text = html.unescape(body_text)
                body_text = re.sub(r"\s+", " ", body_text).strip()

                for trail in [
                    "Inicie sesión o regístrese para comentar",
                    "Inicie sesión",
                ]:
                    idx = body_text.rfind(trail)
                    if idx != -1:
                        body_text = body_text[:idx].strip()

        if body_text:
            text_parts.append(body_text)

        firmas = extract_field("field-firmas")
        if firmas:
            text_parts.append(firmas)

        full_text = "\n\n".join(text_parts)

        # Extract institution
        institution = extract_field("field-insti")

        # Extract RO date and number
        ro_info = ""
        date_str = ""
        ro_match = re.search(
            r'field-name-field-ro-date.*?field-item even[^>]*>(.*?)</div>',
            content, re.DOTALL
        )
        if ro_match:
            ro_section = ro_match.group(1)
            lines = re.sub(r"<[^>]+>", "\n", ro_section)
            lines = html.unescape(lines).strip().split("\n")
            lines = [l.strip() for l in lines if l.strip()]
            if lines:
                ro_info = lines[0]
            if len(lines) > 1:
                date_str = lines[-1]

        # Also check field-date
        if not date_str:
            date_str = extract_field("field-date")

        # Extract slug from path
        slug = path.lstrip("/")

        # Classify document type
        doc_type = _classify_document_type(slug)

        return {
            "url": url,
            "slug": slug,
            "title": title,
            "text": full_text,
            "institution": institution,
            "registro_oficial": ro_info,
            "date_raw": date_str,
            "document_type": doc_type,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from the sitemap."""
        urls = self._fetch_sitemap_urls()

        for i, entry in enumerate(urls):
            if i > 0 and i % 100 == 0:
                logger.info(f"Progress: {i}/{len(urls)} documents fetched")

            raw = self._parse_page(entry["url"], entry["path"])
            if raw is None:
                continue

            raw["_sitemap_lastmod"] = entry.get("lastmod")
            yield raw

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given date."""
        urls = self._fetch_sitemap_urls()
        since_str = since.strftime("%Y-%m-%d")

        for entry in urls:
            lastmod = entry.get("lastmod", "")
            if lastmod and lastmod[:10] >= since_str:
                raw = self._parse_page(entry["url"], entry["path"])
                if raw is not None:
                    raw["_sitemap_lastmod"] = entry.get("lastmod")
                    yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document data into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 100:
            logger.debug(f"Skipping {raw.get('slug', '?')}: no/insufficient text")
            return None

        date = _parse_spanish_date(raw.get("date_raw", ""))

        return {
            "_id": raw["slug"],
            "_source": "EC/AsambleaNacional",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": date,
            "url": raw.get("url", ""),
            "institution": raw.get("institution", ""),
            "registro_oficial": raw.get("registro_oficial", ""),
            "document_type": raw.get("document_type", ""),
        }


# ── CLI ─────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="EC/AsambleaNacional scraper")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample", action="store_true", help="Sample mode (10 records)"
    )
    args = parser.parse_args()

    scraper = AsambleaNacionalScraper()

    if args.command == "test-api":
        logger.info("Testing oficial.ec access...")
        urls = scraper._fetch_sitemap_urls()
        logger.info(f"Sitemap: {len(urls)} document URLs found")
        if urls:
            logger.info(f"First URL: {urls[0]['url']}")
            raw = scraper._parse_page(urls[0]["url"], urls[0]["path"])
            if raw:
                logger.info(f"Title: {raw['title'][:60]}...")
                logger.info(f"Text length: {len(raw.get('text', ''))}")
                logger.info(f"Institution: {raw.get('institution')}")
                logger.info(f"RO: {raw.get('registro_oficial')}")
                logger.info(f"Date: {raw.get('date_raw')}")
                logger.info("API test PASSED")
            else:
                logger.error("Failed to parse page")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        last_run = scraper.status.get("last_run")
        if last_run:
            since = datetime.fromisoformat(last_run)
        else:
            since = datetime(2020, 1, 1, tzinfo=timezone.utc)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
