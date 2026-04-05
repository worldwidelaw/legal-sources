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

Source: https://www.gob.pe/institucion/oece/colecciones/716-resoluciones-del-tribunal-de-contrataciones-del-estado
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import os
import json
import logging
import re
import time
import zlib
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("PE/OSCE")

BASE_URL = "https://www.gob.pe"
COLLECTION_PATH = "/institucion/oece/colecciones/716-resoluciones-del-tribunal-de-contrataciones-del-estado"
ITEMS_PER_PAGE = 25
SOURCE_ID = "PE/OSCE"
DELAY = 1.5  # seconds between requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.5",
}

# ---------- HTTP helpers ----------

def _get(url, timeout=30):
    """Fetch a URL with browser headers, return bytes."""
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(3):
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)
            return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 418:
                log.warning("Got 418 (bot detected) on %s, retrying...", url)
                time.sleep(3)
                continue
            if e.code == 429:
                log.warning("Rate limited on %s, waiting 10s...", url)
                time.sleep(10)
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            log.warning("Network error on %s: %s (attempt %d/3)", url, e, attempt + 1)
            time.sleep(3)
            if attempt == 2:
                raise
    return None


def _get_text(url, timeout=30):
    """Fetch URL and return decoded text."""
    data = _get(url, timeout)
    if data is None:
        return ""
    return data.decode("utf-8", errors="replace")


# ---------- PDF text extraction ----------

def extract_text_from_pdf(pdf_bytes):
    """
    Extract text from a PDF using only stdlib (zlib + regex).
    Works for text-based PDFs (not scanned images).
    """
    all_text = []
    idx = 0
    while idx < len(pdf_bytes):
        start = pdf_bytes.find(b"stream", idx)
        if start == -1:
            break
        # Skip past "stream\r\n" or "stream\n"
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
            # Extract text from TJ arrays: [(text1) -kern (text2)] TJ
            for arr_match in re.finditer(r"\[([^\]]+)\]\s*TJ", decoded):
                arr = arr_match.group(1)
                parts = re.findall(r"\(([^)]*)\)", arr)
                line = "".join(parts)
                if line.strip():
                    all_text.append(line)
            # Extract text from Tj operator: (text) Tj
            for tj_match in re.finditer(r"\(([^)]+)\)\s*Tj", decoded):
                t = tj_match.group(1)
                if t.strip():
                    all_text.append(t)
        except zlib.error:
            pass
        idx = end + 9

    # Join and clean up
    text = "\n".join(all_text)
    # Unescape PDF string escapes
    text = text.replace("\\n", "\n").replace("\\r", "").replace("\\t", "\t")
    text = text.replace("\\(", "(").replace("\\)", ")")
    # Collapse excessive whitespace but preserve paragraph breaks
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ---------- Collection page parsing ----------

MONTH_MAP = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


def parse_spanish_date(text):
    """Parse date like '2 de octubre de 2025' to ISO 8601."""
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", text)
    if not m:
        return None
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = MONTH_MAP.get(month_name)
    if not month:
        return None
    return f"{year}-{month}-{int(day):02d}"


def fetch_collection_page(page_num):
    """
    Fetch one page of the gob.pe collection and return list of dicts:
    [{path, title, date_text}, ...]
    """
    url = f"{BASE_URL}{COLLECTION_PATH}?sheet={page_num}"
    html = _get_text(url)
    if not html:
        return []

    results = []
    # Resolution links follow pattern: /institucion/oece/normas-legales/{id}-{slug}
    for m in re.finditer(
        r'href="(/institucion/oece/normas-legales/(\d+)-([^"]+))"', html
    ):
        path = m.group(1)
        gob_id = m.group(2)
        slug = m.group(3)
        results.append({
            "path": path,
            "gob_id": gob_id,
            "slug": slug,
        })
    return results


def fetch_resolution_detail(path):
    """
    Fetch a resolution detail page and extract metadata + PDF URL.
    Returns dict with title, date, pdf_url, resolution_number, sala.
    """
    url = f"{BASE_URL}{path}"
    html = _get_text(url)
    if not html:
        return None

    info = {"page_url": url}

    # Extract PDF URL from CDN link
    pdf_match = re.search(r'(https://cdn\.www\.gob\.pe/[^"]+\.pdf[^"]*)', html)
    if pdf_match:
        info["pdf_url"] = pdf_match.group(1).replace("&amp;", "&")

    # Extract article content for metadata
    article = re.search(r"<article[^>]*>(.*?)</article>", html, re.DOTALL)
    if article:
        content = article.group(1)
        # Strip HTML tags for plain text
        plain = re.sub(r"<[^>]+>", " ", content)
        plain = " ".join(plain.split())

        # Title: usually "Resolución N.° XXXX-YYYY-TCP-SZ"
        title_match = re.search(r"(Resolución\s+N\.?°?\s*[\d]+-[\d]+-TCP-S\d+)", plain, re.I)
        if title_match:
            info["title"] = title_match.group(1)
        else:
            # Fallback: get first heading-like text
            h_match = re.search(r"<h[12][^>]*>([^<]+)</h[12]>", content)
            if h_match:
                info["title"] = h_match.group(1).strip()

        # Date
        info["date"] = parse_spanish_date(plain)

        # Resolution number
        res_match = re.search(r"(\d{1,5}-\d{4}-TCP-S\d+)", plain, re.I)
        if res_match:
            info["resolution_number"] = res_match.group(1).upper()

        # Sala
        sala_match = re.search(r"TCP-S(\d+)", plain, re.I)
        if sala_match:
            info["sala"] = f"S{sala_match.group(1)}"

    return info


# ---------- Normalization ----------

def normalize(raw):
    """Transform raw resolution data into standard schema."""
    res_num = raw.get("resolution_number", raw.get("slug", "unknown")).upper()

    record = {
        "_id": f"PE/OSCE/{res_num}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", f"Resolución N.° {res_num}"),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("page_url", ""),
        "pdf_url": raw.get("pdf_url", ""),
        "resolution_number": res_num,
        "sala": raw.get("sala", ""),
        "gob_id": raw.get("gob_id", ""),
    }
    return record


# ---------- Main fetcher ----------

def fetch_all(sample=False):
    """Yield normalized resolution records."""
    max_records = 15 if sample else 999999
    max_pages = 3 if sample else 99999
    count = 0

    page = 1
    while page <= max_pages and count < max_records:
        log.info("Fetching collection page %d...", page)
        items = fetch_collection_page(page)
        if not items:
            log.info("No more items on page %d, stopping.", page)
            break

        for item in items:
            if count >= max_records:
                break

            time.sleep(DELAY)
            log.info("Fetching detail for %s (%d/%d)...", item["slug"], count + 1, max_records)

            detail = fetch_resolution_detail(item["path"])
            if not detail:
                log.warning("Could not fetch detail for %s, skipping.", item["slug"])
                continue

            detail["gob_id"] = item["gob_id"]
            detail["slug"] = item["slug"]

            # Set resolution_number from slug if not found in page
            if "resolution_number" not in detail:
                # slug like "6608-2025-tcp-s4"
                detail["resolution_number"] = item["slug"].upper()

            # Download and extract PDF text
            if detail.get("pdf_url"):
                time.sleep(DELAY)
                log.info("Downloading PDF for %s...", item["slug"])
                try:
                    pdf_bytes = _get(detail["pdf_url"], timeout=60)
                    if pdf_bytes:
                        text = extract_text_from_pdf(pdf_bytes)
                        detail["text"] = text
                        log.info("Extracted %d chars of text.", len(text))
                    else:
                        log.warning("Empty PDF response for %s.", item["slug"])
                except Exception as e:
                    log.warning("Failed to download PDF for %s: %s", item["slug"], e)

            record = normalize(detail)
            if record["text"]:
                yield record
                count += 1
            else:
                log.warning("No text extracted for %s, skipping.", item["slug"])

        page += 1

    log.info("Done. Yielded %d records.", count)


# ---------- CLI ----------

def cmd_test_api():
    """Test connectivity to gob.pe."""
    log.info("Testing gob.pe collection page...")
    items = fetch_collection_page(1)
    log.info("Found %d resolution links on page 1.", len(items))

    if items:
        item = items[0]
        log.info("Fetching detail for %s...", item["slug"])
        time.sleep(DELAY)
        detail = fetch_resolution_detail(item["path"])
        if detail:
            log.info("Title: %s", detail.get("title", "N/A"))
            log.info("Date: %s", detail.get("date", "N/A"))
            log.info("PDF URL: %s", detail.get("pdf_url", "N/A")[:100])
            if detail.get("pdf_url"):
                time.sleep(DELAY)
                pdf_bytes = _get(detail["pdf_url"], timeout=60)
                if pdf_bytes:
                    text = extract_text_from_pdf(pdf_bytes)
                    log.info("Extracted %d chars of text from PDF.", len(text))
                    log.info("First 300 chars: %s", text[:300])
        else:
            log.error("Failed to fetch resolution detail.")
    else:
        log.error("No resolution links found on collection page.")


def cmd_bootstrap(sample=False):
    """Run the bootstrap process."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        fname = re.sub(r"[^\w\-]", "_", record["_id"]) + ".json"
        out_path = sample_dir / fname
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        log.info("Saved %s (%d chars text)", fname, len(record.get("text", "")))

    log.info("Bootstrap complete: %d records saved to %s", count, sample_dir)
    return count


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("Usage: python bootstrap.py [test-api|bootstrap] [--sample]")
        sys.exit(1)

    cmd = args[0]
    sample = "--sample" in args

    if cmd == "test-api":
        cmd_test_api()
    elif cmd == "bootstrap":
        n = cmd_bootstrap(sample=sample)
        if n == 0:
            log.error("No records produced!")
            sys.exit(1)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
