#!/usr/bin/env python3
"""
PE/OSCE -- Peru Public Procurement Tribunal Resolutions

Fetches resolutions from the Tribunal de Contrataciones del Estado (formerly
under OSCE, now OECE) via the gob.pe collection. Each resolution is published
as an individual PDF on gob.pe CDN.

Strategy:
  1. Paginate gob.pe collection HTML pages (25 items/page)
  2. Extract resolution page links and metadata from each page
  3. Fetch each resolution detail page to get the PDF URL
  4. Download PDF and extract full text using zlib decompression
     (PDFs are text-based, not scanned)

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Connectivity test
"""

import sys
import json
import logging
import re
import time
import zlib
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PE.OSCE")

BASE_URL = "https://www.gob.pe"
COLLECTION_PATH = "/institucion/oece/colecciones/716-resoluciones-del-tribunal-de-contrataciones-del-estado"
SOURCE_ID = "PE/OSCE"
DELAY = 1.5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.5",
}

MONTH_MAP = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "setiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


def _get(url, timeout=30):
    """Fetch a URL with browser headers, return bytes."""
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(3):
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)
            return resp.read()
        except urllib.error.HTTPError as e:
            if e.code in (418, 429):
                logger.warning("HTTP %d on %s, retrying...", e.code, url)
                time.sleep(10 if e.code == 429 else 3)
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            logger.warning("Network error on %s: %s (attempt %d/3)", url, e, attempt + 1)
            time.sleep(3)
            if attempt == 2:
                raise
    return None


def _get_text(url, timeout=30):
    data = _get(url, timeout)
    if data is None:
        return ""
    return data.decode("utf-8", errors="replace")


def extract_text_from_pdf(pdf_bytes):
    """Extract text from a text-based PDF using stdlib zlib."""
    all_text = []
    idx = 0
    while idx < len(pdf_bytes):
        start = pdf_bytes.find(b"stream", idx)
        if start == -1:
            break
        data_start = start + 6
        if data_start < len(pdf_bytes) and pdf_bytes[data_start:data_start + 1] == b"\r":
            data_start += 1
        if data_start < len(pdf_bytes) and pdf_bytes[data_start:data_start + 1] == b"\n":
            data_start += 1
        end = pdf_bytes.find(b"endstream", data_start)
        if end == -1:
            break
        stream_data = pdf_bytes[data_start:end]
        try:
            decompressed = zlib.decompress(stream_data)
            decoded = decompressed.decode("latin-1", errors="replace")
            for arr_match in re.finditer(r"\[([^\]]+)\]\s*TJ", decoded):
                arr = arr_match.group(1)
                parts = re.findall(r"\(([^)]*)\)", arr)
                line = "".join(parts)
                if line.strip():
                    all_text.append(line)
            for tj_match in re.finditer(r"\(([^)]+)\)\s*Tj", decoded):
                t = tj_match.group(1)
                if t.strip():
                    all_text.append(t)
        except zlib.error:
            pass
        idx = end + 9

    text = "\n".join(all_text)
    text = text.replace("\\n", "\n").replace("\\r", "").replace("\\t", "\t")
    text = text.replace("\\(", "(").replace("\\)", ")")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_spanish_date(text):
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", text)
    if not m:
        return None
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = MONTH_MAP.get(month_name)
    if not month:
        return None
    return f"{year}-{month}-{int(day):02d}"


def fetch_collection_page(page_num):
    url = f"{BASE_URL}{COLLECTION_PATH}?sheet={page_num}"
    html = _get_text(url)
    if not html:
        return []

    results = []
    for m in re.finditer(
        r'href="(/institucion/oece/normas-legales/(\d+)-([^"]+))"', html
    ):
        path = m.group(1)
        gob_id = m.group(2)
        slug = m.group(3)
        results.append({"path": path, "gob_id": gob_id, "slug": slug})
    return results


def fetch_resolution_detail(path):
    url = f"{BASE_URL}{path}"
    html = _get_text(url)
    if not html:
        return None

    info = {"page_url": url}

    pdf_match = re.search(r'(https://cdn\.www\.gob\.pe/[^"]+\.pdf[^"]*)', html)
    if pdf_match:
        info["pdf_url"] = pdf_match.group(1).replace("&amp;", "&")

    article = re.search(r"<article[^>]*>(.*?)</article>", html, re.DOTALL)
    if article:
        content = article.group(1)
        plain = re.sub(r"<[^>]+>", " ", content)
        plain = " ".join(plain.split())

        title_match = re.search(r"(Resolución\s+N\.?°?\s*[\d]+-[\d]+-TCP-S\d+)", plain, re.I)
        if title_match:
            info["title"] = title_match.group(1)
        else:
            h_match = re.search(r"<h[12][^>]*>([^<]+)</h[12]>", content)
            if h_match:
                info["title"] = h_match.group(1).strip()

        info["date"] = parse_spanish_date(plain)

        res_match = re.search(r"(\d{1,5}-\d{4}-TCP-S\d+)", plain, re.I)
        if res_match:
            info["resolution_number"] = res_match.group(1).upper()

        sala_match = re.search(r"TCP-S(\d+)", plain, re.I)
        if sala_match:
            info["sala"] = f"S{sala_match.group(1)}"

    return info


class OSCEScraper(BaseScraper):
    """Scraper for PE/OSCE - Peru Public Procurement Tribunal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        res_num = raw.get("resolution_number", raw.get("slug", "unknown")).upper()
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        return {
            "_id": f"PE/OSCE/{res_num}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", f"Resolución N.° {res_num}"),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("page_url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "resolution_number": res_num,
            "sala": raw.get("sala", ""),
            "gob_id": raw.get("gob_id", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        page = 1
        while True:
            logger.info("Fetching collection page %d...", page)
            self.rate_limiter.wait()
            items = fetch_collection_page(page)
            if not items:
                logger.info("No more items on page %d, stopping.", page)
                break

            for item in items:
                self.rate_limiter.wait()
                logger.info("Fetching detail for %s...", item["slug"])

                detail = fetch_resolution_detail(item["path"])
                if not detail:
                    continue

                detail["gob_id"] = item["gob_id"]
                detail["slug"] = item["slug"]

                if "resolution_number" not in detail:
                    detail["resolution_number"] = item["slug"].upper()

                if detail.get("pdf_url"):
                    self.rate_limiter.wait()
                    logger.info("Downloading PDF for %s...", item["slug"])
                    try:
                        pdf_bytes = _get(detail["pdf_url"], timeout=60)
                        if pdf_bytes:
                            text = extract_text_from_pdf(pdf_bytes)
                            detail["text"] = text
                            logger.info("Extracted %d chars of text.", len(text))
                    except Exception as e:
                        logger.warning("Failed to download PDF for %s: %s", item["slug"], e)

                if detail.get("text"):
                    yield detail

            page += 1

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = OSCEScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command in ("test", "test-api"):
        logger.info("Testing gob.pe collection page...")
        items = fetch_collection_page(1)
        logger.info("Found %d resolution links on page 1.", len(items))
        if items:
            item = items[0]
            logger.info("Fetching detail for %s...", item["slug"])
            time.sleep(DELAY)
            detail = fetch_resolution_detail(item["path"])
            if detail:
                logger.info("Title: %s", detail.get("title", "N/A"))
                logger.info("Date: %s", detail.get("date", "N/A"))
                logger.info("PDF URL: %s", detail.get("pdf_url", "N/A")[:100])
                print("Test PASSED")
            else:
                print("Test FAILED")
                sys.exit(1)
        else:
            print("Test FAILED: no items found")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
