#!/usr/bin/env python3
"""
CO/SUINJuriscol -- Colombian SUIN-Juriscol Legal Database Fetcher

Fetches Colombian legislation from SUIN-Juriscol (Ministry of Justice).

Strategy:
  - Parse sitemap XML to get all document URLs
  - Fetch each HTML page and extract text from div#TextoNorma
  - Extract metadata (title, date, law number) from page content

Data:
  - 11,679 laws + decrees, resolutions, etc. from 1864 to present
  - Full text HTML
  - Language: Spanish
  - robots.txt allows access

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.SUINJuriscol")

BASE_URL = "https://www.suin-juriscol.gov.co"
SITEMAP_LAWS = f"{BASE_URL}/sitemapleyes.xml"


class ColombiaSUINScraper(BaseScraper):
    """
    Scraper for CO/SUINJuriscol -- Colombian SUIN-Juriscol.
    Country: CO
    URL: https://www.suin-juriscol.gov.co/

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
                "Accept-Language": "es-CO,es;q=0.9,en;q=0.5",
            },
            timeout=30,
            verify=False,
        )

    def _get_sitemap_urls(self, sitemap_url: str) -> List[str]:
        """Parse a sitemap XML and return all document URLs."""
        try:
            self.rate_limiter.wait()
            # Use Accept: */* for XML sitemaps (server rejects text/html)
            resp = self.client.session.get(
                sitemap_url,
                headers={"Accept": "*/*"},
                timeout=self.client.timeout,
            )
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            ns = {"sm": "https://www.sitemaps.org/schemas/sitemap/0.9"}
            urls = []
            for url_el in root.findall(".//sm:url/sm:loc", ns):
                if url_el.text:
                    urls.append(url_el.text.strip())
            logger.info(f"Sitemap {sitemap_url}: {len(urls)} URLs")
            return urls
        except Exception as e:
            logger.error(f"Failed to fetch sitemap {sitemap_url}: {e}")
            return []

    def _extract_text(self, html: str) -> str:
        """Extract law text from HTML, cleaning tags and scripts."""
        # Try to find TextoNorma div first
        match = re.search(
            r'id=["\']TextoNorma["\'][^>]*>(.*?)(?=<div[^>]*id=["\']|<footer|</body)',
            html,
            re.DOTALL | re.IGNORECASE,
        )
        if match:
            content = match.group(1)
        else:
            # Fall back to body content after header
            match = re.search(
                r'(?:DECRETA|RESUELVE|CONSIDERA)[:\s]*(.*?)(?=<footer|<script[^>]*>var\s+_gaq|</body)',
                html,
                re.DOTALL | re.IGNORECASE,
            )
            if match:
                content = match.group(0)
            else:
                content = html

        # Remove scripts, styles, and tags
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL)
        content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
        # Convert <br>, <p>, <div> to newlines
        content = re.sub(r'<(?:br|p|div)[^>]*/?>', '\n', content, flags=re.IGNORECASE)
        # Strip remaining tags
        content = re.sub(r'<[^>]+>', ' ', content)
        # Decode HTML entities
        from html import unescape
        content = unescape(content)
        # Normalize whitespace
        lines = [line.strip() for line in content.split('\n')]
        lines = [line for line in lines if line]
        return '\n'.join(lines)

    def _extract_title(self, html: str, text: str) -> str:
        """Extract law title from page."""
        # Try <title> tag
        match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
        if match:
            title = match.group(1).strip()
            title = re.sub(r'\s+', ' ', title)
            if title and title.lower() != 'suin-juriscol':
                return title

        # Try first meaningful line of text
        for line in text.split('\n')[:10]:
            line = line.strip()
            if re.match(r'(?:LEY|DECRETO|ACTO LEGISLATIVO|RESOLUCI[OÓ]N)\s+\d+', line, re.IGNORECASE):
                return line[:300]
        return text.split('\n')[0][:300] if text else "Unknown"

    def _extract_date(self, html: str, text: str, title: str) -> Optional[str]:
        """Extract date from HTML, text, or title."""
        # Look for DD/MM/YYYY in raw HTML (Fecha de expedición)
        match = re.search(r'Fecha de expedici[oó]n[^<]*?(\d{1,2}/\d{1,2}/\d{4})', html)
        if match:
            try:
                dt = datetime.strptime(match.group(1), "%d/%m/%Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Try any DD/MM/YYYY in HTML metadata area
        dates = re.findall(r'(\d{2}/\d{2}/\d{4})', html[:5000])
        if dates:
            try:
                dt = datetime.strptime(dates[0], "%d/%m/%Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Extract year from title (LEY X DE YYYY)
        match = re.search(r'DE\s+(\d{4})', title)
        if match:
            return f"{match.group(1)}-01-01"

        return None

    def _extract_ruta(self, url: str) -> str:
        """Extract ruta from URL."""
        match = re.search(r'ruta=([^&\s]+)', url)
        return match.group(1) if match else url

    def _fetch_document(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a single document and extract its content."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            html = resp.text

            if len(html) < 100:
                return None

            text = self._extract_text(html)
            if not text or len(text.strip()) < 50:
                return None

            title = self._extract_title(html, text)
            date = self._extract_date(html, text, title)
            ruta = self._extract_ruta(url)

            return {
                "ruta": ruta,
                "title": title,
                "text": text,
                "date": date,
                "url": url,
                "html_length": len(html),
            }
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document to standard schema."""
        ruta = raw.get("ruta", "")
        doc_type = "legislation"

        return {
            "_id": f"CO/SUINJuriscol:{ruta}",
            "_source": "CO/SUINJuriscol",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "ruta": ruta,
            "jurisdiction": "CO",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all laws from sitemap."""
        urls = self._get_sitemap_urls(SITEMAP_LAWS)
        if not urls:
            logger.error("No URLs from sitemap")
            return

        logger.info(f"Starting fetch of {len(urls)} law documents")
        count = 0
        errors = 0

        for i, url in enumerate(urls):
            raw = self._fetch_document(url)
            if raw is None:
                errors += 1
                continue

            normalized = self.normalize(raw)
            count += 1
            yield normalized

            if count % 100 == 0:
                logger.info(f"Progress: {count} fetched, {errors} errors, {i+1}/{len(urls)} processed")

        logger.info(f"Completed: {count} laws fetched, {errors} errors")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent laws (re-fetches from sitemap, checks dates)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            urls = self._get_sitemap_urls(SITEMAP_LAWS)
            if not urls:
                logger.error("No URLs in sitemap")
                return False
            raw = self._fetch_document(urls[0])
            if raw and raw.get("text"):
                logger.info(f"Test passed: fetched {urls[0]} ({len(raw['text'])} chars)")
                return True
            return False
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


# === CLI ===
def main():
    import argparse

    parser = argparse.ArgumentParser(description="CO/SUINJuriscol data fetcher")
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
    args = parser.parse_args()

    scraper = ColombiaSUINScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        max_records = 15 if args.sample else None
        count = 0

        for record in scraper.fetch_all():
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', 'unknown')[:80]} "
                f"({text_len:,} chars)"
            )

            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
