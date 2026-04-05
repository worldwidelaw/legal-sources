#!/usr/bin/env python3
"""
CO/DIAN-DoctrinaTributaria — Colombian Tax Doctrine Fetcher

Fetches tax doctrine (oficios, conceptos) from the DIAN Normograma.
Full text HTML documents from normograma.dian.gov.co.

Data source: https://normograma.dian.gov.co/dian/compilacion/tributario.html
License: Open government data (Colombia)
"""

import json
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# Constants
BASE_URL = "https://normograma.dian.gov.co/dian/compilacion/"
DOCS_URL = BASE_URL + "docs/"
RATE_LIMIT_DELAY = 1.5  # seconds between requests

# Index "parte" pages that list documents by section/year
# These are AJAX fragments loaded by the normograma tree UI
# Tax doctrine (tributaria): 13 partes covering 1987-2026, ~15,800 docs
INDEX_PAGES = [
    f"t_2_doctrina_tributaria_parte_{i:02d}.html" for i in range(1, 14)
]


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    if HAS_BS4:
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
    else:
        text = re.sub(r"<script[^>]*>.*?</script>", " ", html_text, flags=re.DOTALL)
        text = re.sub(r"<style[^>]*>.*?</style>", " ", html_text, flags=re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", text)
        text = unescape(text)
    text = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def http_get(url: str, retries: int = 2) -> str:
    """Fetch a URL and return the response as text."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open-data-research)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-CO,es;q=0.9,en;q=0.5",
    }
    req = urllib.request.Request(url, headers=headers)
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="replace")
        except Exception as e:
            if attempt < retries:
                print(f"Retry {attempt + 1} for {url}: {e}", file=sys.stderr)
                time.sleep(2)
            else:
                raise


def discover_doc_urls_from_index(index_url: str) -> list:
    """Scrape an index page to discover document URLs."""
    html = http_get(index_url)
    links = re.findall(r'href="(docs/[^"]+\.htm)"', html)
    return list(dict.fromkeys(links))  # deduplicate preserving order


def discover_all_doc_urls() -> list:
    """Discover document URLs from all known index pages."""
    all_urls = []
    seen = set()
    for page in INDEX_PAGES:
        url = BASE_URL + page
        try:
            links = discover_doc_urls_from_index(url)
            for link in links:
                if link not in seen:
                    seen.add(link)
                    all_urls.append(link)
            print(f"Index {page}: found {len(links)} docs", file=sys.stderr)
        except Exception as e:
            print(f"Error fetching index {page}: {e}", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)
    return all_urls


def extract_doc_metadata(filename: str) -> dict:
    """Extract metadata from the document filename."""
    # Pattern: oficio_dian_{number}_{year}.htm or concepto_tributario_dian_{number}_{year}.htm
    m = re.match(r"docs/(oficio_dian|concepto_tributario_dian|concepto_dian)_(\d+)_(\d{4})\.htm", filename)
    if m:
        doc_type_raw = m.group(1)
        number = m.group(2)
        year = m.group(3)
        if "concepto" in doc_type_raw:
            doc_type = "concepto"
        else:
            doc_type = "oficio"
        return {"doc_type": doc_type, "number": number, "year": year, "filename": filename}

    # Fallback: extract what we can
    parts = filename.replace("docs/", "").replace(".htm", "")
    return {"doc_type": "unknown", "number": parts, "year": "", "filename": filename}


def fetch_document(doc_path: str) -> dict:
    """Fetch a single document and extract its content."""
    url = BASE_URL + doc_path
    html = http_get(url)

    # Extract title from the HTML
    title = ""
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.DOTALL | re.IGNORECASE)
    if title_match:
        title = clean_html(title_match.group(1)).strip()
        # Clean common prefix
        title = re.sub(r"^Compilación Jurídica de la DIAN\s*-\s*", "", title).strip()

    # Extract the main content body
    # The normograma uses a specific content area
    content_html = html
    # Try to extract just the document body (between specific markers)
    body_match = re.search(
        r'<div[^>]*class="[^"]*contenido[^"]*"[^>]*>(.*?)</div>\s*(?:<div[^>]*class="[^"]*pie|<footer)',
        html, re.DOTALL | re.IGNORECASE
    )
    if body_match:
        content_html = body_match.group(1)
    else:
        # Try another common pattern
        body_match = re.search(
            r'<div[^>]*id="[^"]*contenido[^"]*"[^>]*>(.*?)</div>',
            html, re.DOTALL | re.IGNORECASE
        )
        if body_match:
            content_html = body_match.group(1)

    text = clean_html(content_html)

    # Remove navigation/header noise at the start
    # Look for the ALL-CAPS formal header (OFICIO/CONCEPTO) which follows nav noise
    doc_start = re.search(
        r"\n((?:OFICIO|CONCEPTO|CIRCULAR)\s+\d+)",
        text
    )
    if doc_start:
        text = text[doc_start.start() + 1:]
    else:
        # Fallback: case-insensitive, skip "Anotaciones" nav noise
        nav_end = re.search(r"(?:ÍNDICE|INDICE)\s*\n", text)
        if nav_end:
            text = text[nav_end.end():]

    # Remove footer noise
    noise_patterns = [
        r"©.*?DIAN.*",
        r"Compilación Jurídica DIAN.*",
        r"Novedades y boletines.*",
        r"Encuesta y enlaces.*",
    ]
    for pattern in noise_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()

    # Extract date from the document header area (first 800 chars)
    date = ""
    months = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
    }
    header_text = text[:800]

    # Try "(day de month de year)" in parentheses
    date_match = re.search(
        r"\((\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(?:de\s+)?(\d{4})\)",
        header_text, re.IGNORECASE
    )
    if not date_match:
        # Try "day de month de year" without parentheses (e.g. "24 de enero de 2020")
        date_match = re.search(
            r"(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(?:de\s+)?(\d{4})",
            header_text, re.IGNORECASE
        )
    if not date_match:
        # Try "(month day)" format (e.g. "(enero 24)")
        m = re.search(
            r"\((enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(\d{1,2})\)",
            header_text, re.IGNORECASE
        )
        if m:
            # Need to get year from filename or header
            year_m = re.search(r"DE\s+(\d{4})", header_text)
            if year_m:
                day = m.group(2).zfill(2)
                month = months.get(m.group(1).lower(), "01")
                date = f"{year_m.group(1)}-{month}-{day}"

    if date_match and not date:
        day = date_match.group(1).zfill(2)
        month = months.get(date_match.group(2).lower(), "01")
        year = date_match.group(3)
        date = f"{year}-{month}-{day}"

    return {
        "title": title,
        "text": text,
        "date": date,
        "url": url,
        "html_length": len(html),
    }


def normalize(raw: dict, meta: dict) -> dict:
    """Transform raw document data into the normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    doc_id = f"DIAN-{meta.get('doc_type', 'doc')}-{meta.get('number', 'unknown')}-{meta.get('year', '')}"
    date = raw.get("date", "")
    if not date and meta.get("year"):
        date = f"{meta['year']}-01-01"

    return {
        "_id": doc_id,
        "_source": "CO/DIAN-DoctrinaTributaria",
        "_type": "doctrine",
        "_fetched_at": now,
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": date,
        "url": raw.get("url", ""),
        "doc_type": meta.get("doc_type", "unknown"),
        "doc_number": meta.get("number", ""),
        "year": meta.get("year", ""),
        "language": "es",
    }


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all documents from discovered index pages."""
    doc_paths = discover_all_doc_urls()
    print(f"Discovered {len(doc_paths)} document URLs", file=sys.stderr)

    count = 0
    for doc_path in doc_paths:
        if max_docs and count >= max_docs:
            break
        meta = extract_doc_metadata(doc_path)
        try:
            raw = fetch_document(doc_path)
            if raw["text"] and len(raw["text"]) >= 50:
                record = normalize(raw, meta)
                yield record
                count += 1
            else:
                print(f"Skipping {doc_path}: insufficient text ({len(raw.get('text', ''))} chars)",
                      file=sys.stderr)
        except Exception as e:
            print(f"Error fetching {doc_path}: {e}", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

    print(f"Done: {count} records fetched.", file=sys.stderr)


def bootstrap_sample(sample_dir: Path, count: int = 15) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for record in fetch_all(max_docs=count + 5):
        if not record["text"] or len(record["text"]) < 100:
            continue

        samples.append(record)

        safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
        filename = f"{safe_id}.json"
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text']):,} chars)", file=sys.stderr)

        if len(samples) >= count:
            break

    if samples:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        text_lengths = [len(s["text"]) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)

        by_type = {}
        for s in samples:
            dt = s.get("doc_type", "unknown")
            by_type[dt] = by_type.get(dt, 0) + 1
        print(f"\nBy doc_type:", file=sys.stderr)
        for dt, cnt in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"  {dt}: {cnt}", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="DIAN tax doctrine fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Generate sample data only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of samples to generate")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (YYYY-MM-DD)")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            count = 0
            for record in fetch_all():
                print(json.dumps(record, ensure_ascii=False))
                count += 1
            print(f"Full bootstrap: {count} records emitted.", file=sys.stderr)

    elif args.command == "fetch":
        limit = args.count if args.sample else None
        for record in fetch_all(max_docs=limit):
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("Error: --since is required for updates command", file=sys.stderr)
            sys.exit(1)
        since_year = int(args.since[:4])
        count = 0
        for record in fetch_all():
            if record.get("year") and int(record["year"]) >= since_year:
                print(json.dumps(record, ensure_ascii=False))
                count += 1
        print(f"Updates: {count} records since {args.since}.", file=sys.stderr)


if __name__ == "__main__":
    main()
