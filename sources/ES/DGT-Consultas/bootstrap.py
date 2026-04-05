#!/usr/bin/env python3
"""
ES/DGT-Consultas -- Spanish Tax Binding Rulings (Consultas Vinculantes DGT)

Fetches binding tax rulings from the PETETE database of the Direccion General
de Tributos (DGT). Contains 100,000+ rulings from 1997 to present.

Strategy:
  - Establish session by visiting main page (gets JSESSIONID cookie)
  - Search via POST to /consultas/do/search with date range filters
  - Fetch full document via POST to /consultas/do/document
  - Document endpoint requires X-Requested-With: XMLHttpRequest header
  - Two databases: tab=1 (general), tab=2 (vinculantes/binding)
  - 20 results per page, paginated

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import argparse
import html as html_mod
import http.cookiejar
import json
import logging
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Generator, Optional

from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ES.DGT-Consultas")

SOURCE_ID = "ES/DGT-Consultas"
BASE_URL = "https://petete.tributos.hacienda.gob.es"
MAIN_URL = f"{BASE_URL}/consultas/"
SEARCH_URL = f"{BASE_URL}/consultas/do/search"
DOCUMENT_URL = f"{BASE_URL}/consultas/do/document"

REQUEST_DELAY = 2.0  # seconds between requests


class PETETEClient:
    """HTTP client for the PETETE system using stdlib only."""

    def __init__(self):
        # Create SSL context that doesn't verify (site has cert issues)
        self.ssl_ctx = ssl.create_default_context()
        self.ssl_ctx.check_hostname = False
        self.ssl_ctx.verify_mode = ssl.CERT_NONE

        # Cookie jar for session management
        self.cookie_jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar),
            urllib.request.HTTPSHandler(context=self.ssl_ctx),
        )

    def _request(self, url: str, data: dict = None, extra_headers: dict = None,
                 retries: int = 3) -> str:
        """Make an HTTP request with retry logic, return response text."""
        headers = {
            "Accept": "*/*",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        if extra_headers:
            headers.update(extra_headers)

        if data is not None:
            encoded = urllib.parse.urlencode(data).encode("utf-8")
        else:
            encoded = None

        last_err = None
        for attempt in range(retries):
            try:
                req = urllib.request.Request(url, data=encoded, headers=headers)
                resp = self.opener.open(req, timeout=60)
                return resp.read().decode("utf-8", errors="replace")
            except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
                last_err = e
                code = getattr(e, 'code', None)
                if code and code < 500:
                    raise  # Don't retry client errors
                logger.warning(f"Request attempt {attempt+1}/{retries} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(REQUEST_DELAY * (attempt + 1))
        raise last_err

    def init_session(self) -> bool:
        """Visit main page to get JSESSIONID cookie."""
        try:
            self._request(MAIN_URL)
            cookies = list(self.cookie_jar)
            logger.info(f"Session initialized, {len(cookies)} cookie(s)")
            return True
        except Exception as e:
            logger.error(f"Failed to init session: {e}")
            return False

    def search(self, tab: int = 2, page: int = 1,
               from_date: str = None, to_date: str = None) -> dict:
        """
        Search PETETE.

        Args:
            tab: 1=general, 2=vinculantes (binding)
            page: 1-indexed page number
            from_date: DD/MM/YYYY
            to_date: DD/MM/YYYY

        Returns:
            dict with 'total', 'total_pages', 'query', 'documents'
        """
        params = {
            f"type{tab}": "on",
            "NMCMP_1": "NUM-CONSULTA",
            "VLCMP_1": "",
            "OPCMP_1": ".Y",
            "NMCMP_2": "FECHA-SALIDA",
            "VLCMP_2": "",
            "OPCMP_2": ".Y",
            "NMCMP_3": "NORMATIVA",
            "VLCMP_3": "",
            "OPCMP_3": ".Y",
            "NMCMP_4": "CUESTION-PLANTEADA",
            "VLCMP_4": "",
            "OPCMP_4": ".Y",
            "NMCMP_5": "DESCRIPCION-HECHOS",
            "VLCMP_5": "",
            "OPCMP_5": ".Y",
            "NMCMP_6": "FreeText",
            "VLCMP_6": "",
            "OPCMP_6": ".Y",
            "cmpOrder": "FECHA-SALIDA",
            "dirOrder": "1",  # descending
            "tab": str(tab),
            "page": str(page),
        }

        if from_date and to_date:
            params["VLCMP_2"] = f"{from_date}..{to_date}"

        html_text = self._request(SEARCH_URL, data=params)
        return self._parse_search(html_text, tab)

    def _parse_search(self, html_text: str, tab: int) -> dict:
        """Parse search results HTML."""
        result = {"total": 0, "total_pages": 1, "query": "", "documents": []}

        # Total count
        m = re.search(rf'updateNumResults\("{tab}",\s*"(\d+)"\)', html_text)
        if m:
            result["total"] = int(m.group(1))

        # Total pages
        m = re.search(r'<span id="total_pages">(\d+)</span>', html_text)
        if m:
            result["total_pages"] = int(m.group(1))

        # Query string (needed for document fetch)
        m = re.search(r'<input[^>]*id="query"[^>]*value="([^"]*)"', html_text)
        if m:
            result["query"] = html_mod.unescape(m.group(1))

        # Document entries: extract doc_id and num_consulta
        for m in re.finditer(
            r'onClick="return viewDocument\((\d+),\s*\d+\);".*?'
            r'<span class="NUM-CONSULTA"><strong>\s*([^<]+?)\s*</strong>',
            html_text, re.DOTALL
        ):
            result["documents"].append({
                "doc_id": m.group(1),
                "num_consulta": m.group(2).strip(),
            })

        return result

    def fetch_document(self, doc_id: str, query: str, tab: int = 2) -> dict:
        """
        Fetch full document content.

        The document endpoint requires X-Requested-With header.
        """
        params = {
            "query": query or ".T",
            "doc": doc_id,
            "tab": str(tab),
        }

        extra_headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Referer": MAIN_URL,
            "Origin": BASE_URL,
        }

        html_text = self._request(DOCUMENT_URL, data=params, extra_headers=extra_headers)

        if "HTTP Status 401" in html_text:
            logger.warning(f"401 for doc {doc_id}, re-initializing session")
            self.init_session()
            time.sleep(REQUEST_DELAY)
            html_text = self._request(DOCUMENT_URL, data=params, extra_headers=extra_headers)
            if "HTTP Status 401" in html_text:
                logger.error(f"Still 401 for doc {doc_id} after session refresh")
                return {}

        return self._parse_document(html_text, doc_id)

    def _parse_document(self, html_text: str, doc_id: str) -> dict:
        """Parse document HTML using BeautifulSoup."""
        soup = BeautifulSoup(html_text, "html.parser")
        doc = {"doc_id": doc_id}

        table = soup.find("table", class_="document")
        if not table:
            logger.warning(f"No document table found for {doc_id}")
            return doc

        # Map CSS class to field name
        field_map = {
            "NUM-CONSULTA": "num_consulta",
            "ORGANO": "organo",
            "FECHA-SALIDA": "fecha_salida",
            "NORMATIVA": "normativa",
            "DESCRIPCION-HECHOS": "descripcion_hechos",
            "CUESTION-PLANTEADA": "cuestion_planteada",
            "CONTESTACION-COMPL": "contestacion_completa",
        }

        for css_class, field_name in field_map.items():
            row = table.find("tr", class_=css_class)
            if not row:
                continue
            td = row.find("td", class_="value")
            if not td:
                continue

            # Get all <p> elements in this cell
            paragraphs = td.find_all("p", class_=css_class)
            if paragraphs:
                texts = []
                for p in paragraphs:
                    t = p.get_text(separator=" ", strip=True)
                    if t:
                        texts.append(t)
                doc[field_name] = "\n\n".join(texts)
            else:
                # Fallback: get all text in the td
                t = td.get_text(separator=" ", strip=True)
                if t:
                    doc[field_name] = t

        return doc


def normalize(raw: dict) -> dict:
    """Transform raw document into standard schema."""
    num_consulta = raw.get("num_consulta", "").strip()
    doc_id_str = num_consulta or f"DOC_{raw.get('doc_id', 'unknown')}"

    # Parse date DD/MM/YYYY -> YYYY-MM-DD
    fecha = raw.get("fecha_salida", "").strip()
    date_iso = ""
    if fecha:
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})", fecha)
        if m:
            date_iso = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # Build full text from all substantive sections
    text_parts = []
    if raw.get("descripcion_hechos"):
        text_parts.append("DESCRIPCION DE HECHOS:\n" + raw["descripcion_hechos"])
    if raw.get("cuestion_planteada"):
        text_parts.append("CUESTION PLANTEADA:\n" + raw["cuestion_planteada"])
    if raw.get("contestacion_completa"):
        text_parts.append("CONTESTACION COMPLETA:\n" + raw["contestacion_completa"])
    full_text = "\n\n".join(text_parts)

    # Title
    normativa = raw.get("normativa", "")
    title = f"Consulta Vinculante {num_consulta}"
    if normativa:
        title += f" - {normativa[:120]}"

    # URL: direct link using num_consulta parameter
    url = f"{MAIN_URL}?num_consulta={urllib.parse.quote(num_consulta)}" if num_consulta else MAIN_URL

    return {
        "_id": doc_id_str,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date_iso,
        "url": url,
        "num_consulta": num_consulta,
        "organo": raw.get("organo", ""),
        "normativa": normativa,
        "language": "es",
        "jurisdiction": "ES",
    }


def fetch_all(client: PETETEClient, tab: int = 2,
              from_date: str = None, to_date: str = None,
              max_pages: int = None) -> Generator[dict, None, None]:
    """
    Fetch all consultas, paginating through results.

    Yields normalized records with full text.
    """
    page = 1
    total_fetched = 0
    query_str = ""

    while True:
        logger.info(f"Fetching search page {page}...")
        try:
            result = client.search(tab=tab, page=page,
                                   from_date=from_date, to_date=to_date)
        except Exception as e:
            logger.error(f"Search page {page} failed: {e}")
            break

        if page == 1:
            logger.info(f"Total results: {result['total']}, pages: {result['total_pages']}")
            query_str = result.get("query", "")

        if not result["documents"]:
            logger.info("No more documents")
            break

        for doc_meta in result["documents"]:
            doc_id = doc_meta["doc_id"]
            num = doc_meta.get("num_consulta", doc_id)

            try:
                time.sleep(REQUEST_DELAY)
                raw = client.fetch_document(doc_id, query=query_str, tab=tab)

                if not raw or not raw.get("contestacion_completa"):
                    logger.warning(f"No full text for {num}")
                    continue

                record = normalize(raw)
                if record.get("text"):
                    yield record
                    total_fetched += 1
                    if total_fetched % 50 == 0:
                        logger.info(f"Fetched {total_fetched} records so far...")
                else:
                    logger.warning(f"Empty text after normalize for {num}")

            except Exception as e:
                logger.error(f"Failed to fetch doc {num}: {e}")

        if page >= result["total_pages"]:
            break

        if max_pages and page >= max_pages:
            logger.info(f"Reached max pages ({max_pages})")
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    logger.info(f"Total fetched: {total_fetched}")


def bootstrap_sample(client: PETETEClient, count: int = 15) -> list:
    """Fetch a sample of records for testing."""
    samples = []

    logger.info(f"Fetching {count} sample records...")

    # Init session
    if not client.init_session():
        logger.error("Cannot init session")
        return samples

    time.sleep(REQUEST_DELAY)

    # Search for recent binding rulings
    result = client.search(tab=2, page=1)
    logger.info(f"Total available: {result['total']}")
    query_str = result.get("query", "")

    docs_to_fetch = result["documents"][:count]

    for doc_meta in docs_to_fetch:
        doc_id = doc_meta["doc_id"]
        num = doc_meta.get("num_consulta", doc_id)

        try:
            time.sleep(REQUEST_DELAY)
            logger.info(f"Fetching {num} (id={doc_id})...")

            raw = client.fetch_document(doc_id, query=query_str, tab=2)

            if not raw or not raw.get("contestacion_completa"):
                logger.warning(f"  No full text for {num}")
                continue

            record = normalize(raw)
            text_len = len(record.get("text", ""))

            if text_len > 0:
                samples.append(record)
                logger.info(f"  OK: {text_len} chars")
            else:
                logger.warning(f"  Empty text for {num}")

        except Exception as e:
            logger.error(f"  Failed: {e}")

    # If we need more, try page 2
    if len(samples) < count:
        logger.info(f"Got {len(samples)}/{count}, trying page 2...")
        time.sleep(REQUEST_DELAY)
        try:
            result2 = client.search(tab=2, page=2)
            remaining = count - len(samples)
            for doc_meta in result2["documents"][:remaining]:
                doc_id = doc_meta["doc_id"]
                num = doc_meta.get("num_consulta", doc_id)
                try:
                    time.sleep(REQUEST_DELAY)
                    logger.info(f"Fetching {num} (id={doc_id})...")
                    raw = client.fetch_document(doc_id, query=query_str, tab=2)
                    if raw and raw.get("contestacion_completa"):
                        record = normalize(raw)
                        text_len = len(record.get("text", ""))
                        if text_len > 0:
                            samples.append(record)
                            logger.info(f"  OK: {text_len} chars")
                except Exception as e:
                    logger.error(f"  Failed: {e}")
        except Exception as e:
            logger.error(f"Page 2 failed: {e}")

    return samples


def main():
    parser = argparse.ArgumentParser(description="ES/DGT-Consultas Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Maximum pages to fetch (bootstrap)")
    parser.add_argument("--tab", type=int, default=2, choices=[1, 2],
                        help="1=general, 2=vinculantes")

    args = parser.parse_args()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    client = PETETEClient()

    if args.command == "test":
        print("Testing PETETE connectivity...")

        print("\n1. Initializing session...")
        if not client.init_session():
            print("   FAILED")
            sys.exit(1)
        print("   OK")

        time.sleep(REQUEST_DELAY)

        print("\n2. Searching...")
        result = client.search(tab=2, page=1)
        print(f"   Total: {result['total']} rulings")
        print(f"   Pages: {result['total_pages']}")
        print(f"   Docs on page 1: {len(result['documents'])}")
        if result["documents"]:
            print(f"   First: {result['documents'][0]['num_consulta']}")

        if result["documents"]:
            time.sleep(REQUEST_DELAY)
            print("\n3. Fetching document...")
            doc_id = result["documents"][0]["doc_id"]
            raw = client.fetch_document(doc_id, query=result.get("query", ""), tab=2)
            if raw:
                print(f"   Num: {raw.get('num_consulta', 'N/A')}")
                print(f"   Date: {raw.get('fecha_salida', 'N/A')}")
                ct = raw.get("contestacion_completa", "")
                print(f"   Full text: {len(ct)} chars")
                if ct:
                    print(f"   Preview: {ct[:300]}...")
            else:
                print("   FAILED to fetch document")

        print("\nTest complete!")

    elif args.command == "bootstrap":
        if args.sample:
            samples = bootstrap_sample(client, count=args.count)

            if not samples:
                logger.error("No samples fetched!")
                sys.exit(1)

            # Save samples
            for i, record in enumerate(samples):
                filepath = sample_dir / f"sample_{i + 1:03d}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            # Summary
            print(f"\n{'=' * 60}")
            print(f"ES/DGT-Consultas Sample Bootstrap Complete")
            print(f"{'=' * 60}")
            print(f"Records: {len(samples)}")

            text_lengths = [len(r.get("text", "")) for r in samples]
            avg_len = sum(text_lengths) / len(text_lengths) if text_lengths else 0
            min_len = min(text_lengths) if text_lengths else 0
            max_len = max(text_lengths) if text_lengths else 0
            print(f"Text lengths: min={min_len:,}, avg={avg_len:,.0f}, max={max_len:,}")

            all_have_text = all(len(r.get("text", "")) > 200 for r in samples)
            print(f"All have text >200 chars: {'YES' if all_have_text else 'NO'}")

            print(f"\nSample IDs:")
            for r in samples:
                print(f"  {r['_id']} ({r.get('date', 'no date')}) "
                      f"- {len(r.get('text', '')):,} chars")

            print(f"\nSamples saved to: {sample_dir}")

        else:
            # Full bootstrap
            if not client.init_session():
                logger.error("Cannot init session")
                sys.exit(1)

            output_file = script_dir / "records.jsonl"
            count = 0

            with open(output_file, "w", encoding="utf-8") as f:
                for record in fetch_all(client, tab=args.tab,
                                        max_pages=args.max_pages):
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1

            print(f"\nBootstrap complete: {count} records -> {output_file}")


if __name__ == "__main__":
    main()
