#!/usr/bin/env python3
"""
NI/Digesto -- Digesto Jurídico Nicaragüense (Consolidated Legal Norms)

Fetches consolidated legislation from Nicaragua's Digesto Jurídico, maintained
by the National Assembly. 41,000+ norms organized by subject matter.

Strategy:
  - List norms via POST to /consultas/util/ws/proxy.php (paginated, 100/page)
  - Fetch full text via getNormaHtmlAccordion query type
  - Document IDs are base64-encoded integers (iunpid field)

API:
  - Base: http://digesto.asamblea.gob.ni
  - List: POST proxy.php with hddQueryType=getJuridicNorms
  - Text: POST proxy.php with hddQueryType=getNormaHtmlAccordion&iunp={b64_id}
  - No auth required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as htmlmod
import base64
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NI.Digesto")

BASE_URL = "http://digesto.asamblea.gob.ni"
PROXY_PATH = "/consultas/util/ws/proxy.php"
PAGE_SIZE = 100


def decode_b64_id(b64_str: str) -> str:
    """Decode base64-encoded numeric ID."""
    try:
        return base64.b64decode(b64_str).decode("utf-8")
    except Exception:
        return b64_str


def clean_html_text(html_str: str) -> str:
    """Strip HTML/XML tags and clean text."""
    if not html_str:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_str, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_str, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = htmlmod.unescape(text)
    lines = [line.strip() for line in text.split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines).strip()


def parse_date_dmy(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY to ISO 8601."""
    if not date_str:
        return None
    match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str.strip())
    if match:
        d, m, y = match.groups()
        return f"{y}-{int(m):02d}-{int(d):02d}"
    return None


class NIDigestoScraper(BaseScraper):
    """Scraper for NI/Digesto -- Nicaraguan consolidated legal norms."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "es-NI,es;q=0.9,en;q=0.5",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=60,
        )

    def _list_norms(self, max_entries: int = 0) -> List[Dict]:
        """List all norms via the search API with pagination."""
        entries = []
        page = 1

        while True:
            self.rate_limiter.wait()
            form_data = (
                f"hddQueryType=getJuridicNorms"
                f"&slcRange=0&slcCategory=0&MateriaPrin=0&MateriaRel=0"
                f"&txtDatePublishFrom=&txtDatePublishTo="
                f"&infoNorm=&txtContentSearch="
                f"&isavancedsearch=true"
                f"&hddPageSize={PAGE_SIZE}"
                f"&hddCurrentPage={page}"
                f"&hddFieldSort="
            )

            try:
                resp = self.client.post(PROXY_PATH, data=form_data)
                if not resp or resp.status_code != 200:
                    logger.warning(f"Failed to fetch page {page}: status={getattr(resp, 'status_code', 'None')}")
                    break

                data = resp.json()
                iunps = data.get("iunps", [])

                if not iunps:
                    break

                for item in iunps:
                    iunpid = item.get("iunpid", "")
                    titulo = item.get("titulo", "")

                    # Skip the summary record (id="MA==" = "0")
                    if not iunpid or not titulo or decode_b64_id(iunpid) == "0":
                        continue

                    entries.append({
                        "iunpid": iunpid,
                        "numeric_id": decode_b64_id(iunpid),
                        "titulo": titulo,
                        "categoria": item.get("categoria", ""),
                        "numero": item.get("numero", ""),
                        "fechaPublicacion": item.get("fechaPublicacion", ""),
                        "fechaAprobacion": item.get("fechaAprobacion", ""),
                        "rango": item.get("rango", ""),
                        "registro": item.get("registro", ""),
                    })

                logger.info(f"Listed {len(entries)} norms so far (page {page})")

                if max_entries and len(entries) >= max_entries:
                    entries = entries[:max_entries]
                    break

                # Check if we got fewer results than page size (last page)
                real_entries = [i for i in iunps if decode_b64_id(i.get("iunpid", "")) != "0"]
                if len(real_entries) < PAGE_SIZE:
                    break

                page += 1

            except Exception as e:
                logger.warning(f"Error listing page {page}: {e}")
                break

        return entries

    def _fetch_full_text(self, iunpid: str) -> Optional[str]:
        """Fetch full text of a norm. Tries getNormaHtmlAccordion first,
        then falls back to loadVersionsXML + getVersionHtmlAccordion."""
        # Approach 1: direct HTML accordion
        self.rate_limiter.wait()
        try:
            form_data = f"hddQueryType=getNormaHtmlAccordion&iunp={quote(iunpid)}"
            resp = self.client.post(PROXY_PATH, data=form_data)
            if resp and resp.status_code == 200:
                content = resp.text
                if content and len(content) >= 50:
                    text = clean_html_text(content)
                    if len(text) >= 50:
                        return text
        except Exception as e:
            logger.debug(f"getNormaHtmlAccordion failed for {iunpid}: {e}")

        # Approach 2: version-based (loadVersionsXML + getVersionHtmlAccordion)
        self.rate_limiter.wait()
        try:
            form_data = f"hddQueryType=loadVersionsXML&iunp={quote(iunpid)}"
            resp = self.client.post(PROXY_PATH, data=form_data)
            if resp and resp.status_code == 200 and resp.text.strip():
                vdata = resp.json()
                docs = vdata.get("documents", [])
                if docs:
                    self.rate_limiter.wait()
                    name = docs[0]["name"]
                    form_data2 = f"hddQueryType=getVersionHtmlAccordion&iunp={quote(iunpid)}&name={quote(name)}"
                    resp2 = self.client.post(PROXY_PATH, data=form_data2)
                    if resp2 and resp2.status_code == 200:
                        content = resp2.text
                        if content and len(content) >= 50:
                            text = clean_html_text(content)
                            if len(text) >= 50:
                                return text
        except Exception as e:
            logger.debug(f"getVersionHtmlAccordion failed for {iunpid}: {e}")

        return None

    def fetch_all(self, max_entries: int = 0) -> Generator[Dict[str, Any], None, None]:
        """Yield all norm entries from the listing API."""
        entries = self._list_norms(max_entries=max_entries)
        for entry in entries:
            yield entry

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all (no reliable date filtering in the API)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema, fetching full text."""
        iunpid = raw.get("iunpid", "")
        titulo = raw.get("titulo", "")
        numeric_id = raw.get("numeric_id", "")

        if not iunpid or not titulo:
            return None

        text = self._fetch_full_text(iunpid)
        if not text:
            return None

        date_str = parse_date_dmy(raw.get("fechaAprobacion", "")) or \
                   parse_date_dmy(raw.get("fechaPublicacion", ""))

        return {
            "_id": f"NI-DIG-{numeric_id}",
            "_source": "NI/Digesto",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": titulo,
            "text": text,
            "date": date_str,
            "url": f"{BASE_URL}/consultas/normas/shownorms.php?idnorm={iunpid}",
            "number": raw.get("numero", ""),
            "category": raw.get("categoria", ""),
            "rango": raw.get("rango", ""),
            "registro": raw.get("registro", ""),
            "jurisdiction": "NI",
            "language": "es",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing NI/Digesto...")

        entries = self._list_norms(max_entries=5)
        print(f"Listing: {len(entries)} entries retrieved")

        if entries:
            for i, entry in enumerate(entries[:3]):
                print(f"\n--- Entry {i+1} ---")
                print(f"  ID: {entry['numeric_id']} ({entry['iunpid']})")
                print(f"  Title: {entry.get('titulo', 'N/A')[:80]}")
                print(f"  Category: {entry.get('categoria', 'N/A')}")
                print(f"  Date: {entry.get('fechaAprobacion', 'N/A')}")

                text = self._fetch_full_text(entry['iunpid'])
                if text:
                    print(f"  Full text: {len(text)} chars")
                    print(f"  Sample: {text[:150]}...")
                else:
                    print("  No text available")

        print("\nTest complete!")


def main():
    scraper = NIDigestoScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
