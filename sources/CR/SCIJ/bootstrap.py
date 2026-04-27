#!/usr/bin/env python3
"""
CR/SCIJ -- Costa Rica Sistema Costarricense de Información Jurídica

Fetches Costa Rican legislation from SCIJ (Attorney General's office).

Strategy:
  - Enumerate numeric IDs from 1 to ~107000
  - Skip IDs that return 302 (invalid/not found)
  - Fetch metadata page for structured fields (type, number, title, date)
  - Fetch full text page for the legislation body

Data:
  - ~70,000-90,000 norms from 1821 to present
  - Types: Constitution, laws, decrees, treaties, regulations, etc.
  - Full text in HTML (Word-exported), cleaned to plain text
  - Language: Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Dict, Any, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CR.SCIJ")

BASE_URL = "https://pgrweb.go.cr/scij"
META_URL = BASE_URL + "/Busqueda/Normativa/Normas/nrm_norma.aspx?param1=NRM&nValor1=1&nValor2={id}&nValor3=0&strTipM=FN"
TEXT_URL = BASE_URL + "/Busqueda/Normativa/Normas/nrm_texto_completo.aspx?param1=NRTC&nValor1=1&nValor2={id}&nValor3=0&strTipM=TC"

# Sample IDs known to have content (laws, decrees, treaties)
SAMPLE_IDS = [100, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000,
              60000, 70000, 80000, 85000, 90000, 95000, 100000, 105000]

MAX_ID = 107000


class CostaRicaSCIJScraper(BaseScraper):
    """
    Scraper for CR/SCIJ -- Costa Rica SCIJ legislation.
    Country: CR
    URL: https://pgrweb.go.cr/scij/

    Data types: legislation
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "es-CR,es;q=0.9,en;q=0.5",
            },
            timeout=30,
        )
    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page, returning None on redirect (invalid ID) or error."""
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(
                url,
                timeout=self.client.timeout,
                allow_redirects=False,
            )
            if resp.status_code in (301, 302, 303, 307):
                return None  # Invalid ID — redirects to error page
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return None

    def _parse_metadata(self, html: str) -> Dict[str, Any]:
        """Extract structured metadata from the nrm_norma.aspx page."""
        meta = {}

        # Norm type and number from tabla_titulo
        m = re.search(
            r'class="tabla_titulo"[^>]*>\s*'
            r'([\w\s]+?)&nbsp;:&nbsp;\s*(\d+)',
            html
        )
        if m:
            meta["norm_type"] = m.group(1).strip()
            meta["norm_number"] = m.group(2).strip()

        # Title: between <!--Nombre de la norma--> and <br> or </td>
        m = re.search(
            r'<!--Nombre de la norma-->\s*(.*?)(?:<br|</td>)',
            html, re.DOTALL
        )
        if m:
            title = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            title = unescape(title)
            title = re.sub(r'\s+', ' ', title)
            meta["title"] = title

        # Estado (vigencia)
        m = re.search(
            r'<!--Estado de la norma-->\s*(.*?)</td>',
            html, re.DOTALL
        )
        if m:
            estado = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            if estado:
                meta["estado"] = estado

        # Ente emisor
        m = re.search(
            r'Ente emisor:</nobr>\s*</td>\s*<td[^>]*>\s*(.*?)\s*</td>',
            html, re.DOTALL
        )
        if m:
            meta["issuing_body"] = re.sub(r'<[^>]+>', '', m.group(1)).strip()

        # Fecha de vigencia
        m = re.search(
            r'Fecha de vigencia desde:</nobr>\s*</td>\s*<td[^>]*>\s*(.*?)\s*</td>',
            html, re.DOTALL
        )
        if m:
            date_str = m.group(1).strip()
            meta["date_raw"] = date_str
            # Parse DD/MM/YYYY
            dm = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
            if dm:
                try:
                    dt = datetime(int(dm.group(3)), int(dm.group(2)), int(dm.group(1)))
                    meta["date"] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Date from header (del DD/MM/YYYY)
        if "date" not in meta:
            m = re.search(
                r'class="tabla_titulo"[^>]*>.*?del\s+(\d{1,2}/\d{1,2}/\d{4})',
                html, re.DOTALL
            )
            if m:
                dm = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', m.group(1))
                if dm:
                    try:
                        dt = datetime(int(dm.group(3)), int(dm.group(2)), int(dm.group(1)))
                        meta["date"] = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass

        # Gaceta info
        m = re.search(
            r'Gaceta:</nobr>\s*</td>\s*<td[^>]*>\s*(.*?)\s*</td>',
            html, re.DOTALL
        )
        if m:
            meta["gaceta"] = re.sub(r'<[^>]+>', '', m.group(1)).strip()

        return meta

    def _extract_fulltext(self, html: str) -> str:
        """Extract full text from nrm_texto_completo.aspx page."""
        # Find dvTextoCompleto div
        m = re.search(
            r'<div\s+id="dvTextoCompleto"[^>]*>(.*?)(?:</div>\s*</div>|<div\s+id="dvVersionesIntermedias")',
            html, re.DOTALL | re.IGNORECASE
        )
        if not m:
            # Fallback: look for the content after dvTextoCompleto
            m = re.search(
                r'id="dvTextoCompleto"[^>]*>(.*)',
                html, re.DOTALL | re.IGNORECASE
            )
        if not m:
            return ""

        content = m.group(1)

        # Remove embedded Word HTML head/style sections
        content = re.sub(r'<head[^>]*>.*?</head>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)

        # Convert block elements to newlines
        content = re.sub(r'<(?:br|p|div|tr|li|h[1-6])[^>]*/?\s*>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</(?:p|div|tr|li|h[1-6])>', '\n', content, flags=re.IGNORECASE)

        # Strip remaining tags
        content = re.sub(r'<[^>]+>', ' ', content)

        # Decode HTML entities
        content = unescape(content)

        # Clean up whitespace
        content = re.sub(r'[ \t]+', ' ', content)
        content = re.sub(r' *\n *', '\n', content)
        lines = [line.strip() for line in content.split('\n')]
        lines = [line for line in lines if line]
        text = '\n'.join(lines)

        # Remove leading/trailing boilerplate
        text = text.strip()
        # Remove "Texto Completo Norma NNNNN" header
        text = re.sub(r'^Texto Completo Norma \d+\n?', '', text)
        # Remove trailing "Ficha articulo ... Ir al principio del documento"
        text = re.sub(
            r'\n?Ficha articulo\nFecha de generación:.*$', '', text, flags=re.DOTALL
        )
        return text.strip()

    def _fetch_document(self, norm_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single norm by ID (metadata + full text)."""
        # Fetch metadata page
        meta_html = self._fetch_page(META_URL.format(id=norm_id))
        if meta_html is None:
            return None

        meta = self._parse_metadata(meta_html)
        if not meta.get("title") and not meta.get("norm_type"):
            # Page exists but has no parseable content
            return None

        # Fetch full text page
        text_html = self._fetch_page(TEXT_URL.format(id=norm_id))
        text = ""
        if text_html:
            text = self._extract_fulltext(text_html)

        if not text or len(text) < 30:
            logger.debug(f"ID {norm_id}: no full text (len={len(text) if text else 0})")
            return None

        # Build title from type + number + title
        parts = []
        if meta.get("norm_type"):
            parts.append(meta["norm_type"])
        if meta.get("norm_number"):
            parts.append(f"No. {meta['norm_number']}")
        header = " ".join(parts)
        title = meta.get("title", "")
        if header and title:
            full_title = f"{header} - {title}"
        elif header:
            full_title = header
        elif title:
            full_title = title
        else:
            full_title = f"Norm ID {norm_id}"

        return {
            "norm_id": norm_id,
            "norm_type": meta.get("norm_type", ""),
            "norm_number": meta.get("norm_number", ""),
            "title": full_title,
            "text": text,
            "date": meta.get("date"),
            "issuing_body": meta.get("issuing_body", ""),
            "estado": meta.get("estado", ""),
            "gaceta": meta.get("gaceta", ""),
            "url": TEXT_URL.format(id=norm_id),
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document to standard schema."""
        return {
            "_id": f"CR/SCIJ:{raw['norm_id']}",
            "_source": "CR/SCIJ",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "norm_id": raw.get("norm_id"),
            "norm_type": raw.get("norm_type", ""),
            "norm_number": raw.get("norm_number", ""),
            "issuing_body": raw.get("issuing_body", ""),
            "estado": raw.get("estado", ""),
            "gaceta": raw.get("gaceta", ""),
            "jurisdiction": "CR",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all norms by iterating through ID range."""
        logger.info(f"Starting full fetch: IDs 1 to {MAX_ID}")
        count = 0
        skipped = 0

        for norm_id in range(1, MAX_ID + 1):
            raw = self._fetch_document(norm_id)
            if raw is None:
                skipped += 1
                continue

            normalized = self.normalize(raw)
            count += 1
            yield normalized

            if count % 100 == 0:
                logger.info(
                    f"Progress: {count} fetched, {skipped} skipped, "
                    f"ID {norm_id}/{MAX_ID}"
                )

        logger.info(f"Completed: {count} norms fetched, {skipped} skipped")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent norms (high IDs likely to be newer)."""
        start_id = MAX_ID - 5000
        logger.info(f"Fetching updates: IDs {start_id} to {MAX_ID}")
        count = 0

        for norm_id in range(start_id, MAX_ID + 1):
            raw = self._fetch_document(norm_id)
            if raw is None:
                continue

            if since and raw.get("date") and raw["date"] < since:
                continue

            normalized = self.normalize(raw)
            count += 1
            yield normalized

        logger.info(f"Updates: {count} norms fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            raw = self._fetch_document(70000)
            if raw and raw.get("text") and len(raw["text"]) > 100:
                logger.info(
                    f"Test passed: ID 70000 - {raw['title'][:80]} "
                    f"({len(raw['text'])} chars)"
                )
                return True
            logger.error("Test failed: no text returned for ID 70000")
            return False
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CR/SCIJ data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CostaRicaSCIJScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "update"):
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
