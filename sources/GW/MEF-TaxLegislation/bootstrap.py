#!/usr/bin/env python3
"""
Guinea-Bissau Ministry of Economy & Finance — Tax Legislation (Kontaktu)

Fetches full-text tax legislation from the Kontaktu portal at kontaktu.mef.gw.
All legislation is embedded in a Semantic UI accordion on a single page.

Data source: https://kontaktu.mef.gw/legislation
License: Public domain (government legislation)
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, Iterator, List, Optional

SOURCE_ID = "GW/MEF-TaxLegislation"
BASE_URL = "https://kontaktu.mef.gw"
LEGISLATION_URL = f"{BASE_URL}/legislation"

# Minimum text length to include a document (skip empty category headers)
MIN_TEXT_LENGTH = 200

# Category headers to skip (these are section groupings, not documents)
CATEGORY_HEADERS = {
    "Contribuição Industrial",
    "Contribuição Predial Urbana",
    "Decretos do MEF",
    "Despachos do MEF",
    "Imposto de  Capitais",
    "Imposto de Capitais",
    "Imposto Especial de Consumo",
    "Imposto Geral sobre Vendas e Serviços (IGV)",
    "Imposto Profissional",
    "Imposto sobre o Valor Acrescentado -  IVA",
    "Imposto sobre o Valor Acrescentado - IVA",
    "Instruções de Serviço",
    "Lei Geral Tributária",
}


def curl_get(url: str, timeout: int = 60) -> Optional[str]:
    """Fetch URL content using curl."""
    try:
        result = subprocess.run(
            ['curl', '-sL', '--compressed', '--max-time', str(timeout),
             '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
             '-H', 'Accept: text/html',
             url],
            capture_output=True, text=True, timeout=timeout + 10
        )
        if result.returncode == 0 and result.stdout:
            return result.stdout
    except Exception as e:
        print(f"  curl error: {e}")
    return None


def clean_html(html_text: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_text:
        return ""
    text = unescape(html_text)
    # Remove style/script blocks
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    # Convert block elements to newlines
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</?p[^>]*>', '\n', text)
    text = re.sub(r'</?li[^>]*>', '\n- ', text)
    text = re.sub(r'</?(?:ul|ol)[^>]*>', '\n', text)
    text = re.sub(r'</?h[1-6][^>]*>', '\n', text)
    text = re.sub(r'</?(?:div|section|article|blockquote|pre)[^>]*>', '\n', text)
    text = re.sub(r'</?tr[^>]*>', '\n', text)
    text = re.sub(r'</?t[dh][^>]*>', ' | ', text)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


def make_slug(title: str) -> str:
    """Create a URL-safe slug from a title."""
    slug = title.lower()
    slug = re.sub(r'[àáâãä]', 'a', slug)
    slug = re.sub(r'[èéêë]', 'e', slug)
    slug = re.sub(r'[ìíîï]', 'i', slug)
    slug = re.sub(r'[òóôõö]', 'o', slug)
    slug = re.sub(r'[ùúûü]', 'u', slug)
    slug = re.sub(r'[ç]', 'c', slug)
    slug = re.sub(r'[^\w\s-]', '', slug)
    slug = re.sub(r'[\s_]+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug.strip('-')[:80]


def extract_date(title: str, text: str) -> Optional[str]:
    """Try to extract a date from title or text."""
    combined = title + ' ' + text[:3000]

    months_pt = {
        'janeiro': '01', 'fevereiro': '02', 'março': '03', 'marco': '03',
        'abril': '04', 'maio': '05', 'junho': '06',
        'julho': '07', 'agosto': '08', 'setembro': '09',
        'outubro': '10', 'novembro': '11', 'dezembro': '12',
    }

    # "de DD de Month de YYYY"
    match = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', combined, re.IGNORECASE)
    if match:
        day = int(match.group(1))
        month = months_pt.get(match.group(2).lower())
        year = int(match.group(3))
        if month and 1900 <= year <= 2030:
            return f"{year}-{month}-{day:02d}"

    # "DD/MM/YYYY"
    match = re.search(r'(\d{2})/(\d{2})/(\d{4})', combined)
    if match:
        year = int(match.group(3))
        if 1900 <= year <= 2030:
            return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"

    # "DD-MM-YYYY"
    match = re.search(r'(\d{2})-(\d{2})-(\d{4})', combined)
    if match:
        year = int(match.group(3))
        if 1900 <= year <= 2030:
            return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"

    # "até DD-MM-YYYY" or "até YYYY"
    match = re.search(r'até\s+(\d{2})-(\d{2})-(\d{4})', combined, re.IGNORECASE)
    if match:
        return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"

    # Year in title like "Lei nº X/YYYY"
    match = re.search(r'[Ll]ei\s+n[.ºo°]+\s*\d+[-/](\d{4})', combined)
    if match:
        year = int(match.group(1))
        if 1990 <= year <= 2030:
            return f"{year}-01-01"

    return None


def determine_category(title: str) -> str:
    """Determine the tax category from the title."""
    t = title.lower()
    if 'contribuição industrial' in t or 'cci' in t or 'aci' in t:
        return "Industrial Contribution"
    if 'predial urbana' in t or 'cpu' in t:
        return "Urban Property Tax"
    if 'imposto de capitais' in t:
        return "Capital Tax"
    if 'especial de consumo' in t:
        return "Special Consumption Tax"
    if 'igv' in t or 'imposto geral sobre vendas' in t:
        return "General Sales Tax (IGV)"
    if 'imposto profissional' in t:
        return "Professional Tax"
    if 'iva' in t or 'valor acrescentado' in t:
        return "Value Added Tax (IVA)"
    if 'lei geral tributária' in t or 'lgt' in t:
        return "General Tax Law"
    if 'decreto' in t:
        return "Ministry Decrees"
    if 'despacho' in t:
        return "Ministry Orders"
    if 'instrução' in t or 'is n' in t:
        return "Service Instructions"
    return "Tax Legislation"


def parse_accordion(html: str) -> List[Dict]:
    """Parse the Semantic UI accordion to extract title/content pairs."""
    documents = []

    # Split HTML by title divs
    parts = re.split(r'(<div[^>]*class="[^"]*title[^"]*"[^>]*>)', html)

    for i in range(1, len(parts) - 1, 2):
        rest = parts[i + 1]

        # Extract title text from the content after the opening tag
        title_end = rest.find('</div>')
        if title_end < 0:
            continue
        title_html = rest[:title_end]
        title = re.sub(r'<[^>]+>', '', unescape(title_html)).strip()

        if not title or title in CATEGORY_HEADERS:
            continue

        # Find the content div that follows
        after_title = rest[title_end + 6:]  # skip </div>
        content_match = re.search(
            r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)$',
            after_title, re.DOTALL
        )
        if not content_match:
            continue

        content_html = content_match.group(1)
        text = clean_html(content_html)

        if len(text) < MIN_TEXT_LENGTH:
            continue

        slug = make_slug(title)
        doc_id = f"GW_MEF_{slug}"
        category = determine_category(title)
        date = extract_date(title, text)

        documents.append({
            '_id': doc_id,
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': text,
            'date': date,
            'url': LEGISLATION_URL,
            'category': category,
            'language': 'pt',
        })

    return documents


def fetch_all() -> Iterator[Dict]:
    """Yield all tax legislation documents."""
    print("Fetching legislation page...")
    html = curl_get(LEGISLATION_URL)
    if not html:
        print("ERROR: Failed to fetch legislation page")
        return

    print(f"Page size: {len(html)} chars")
    documents = parse_accordion(html)
    print(f"Parsed {len(documents)} documents with full text")

    for doc in documents:
        print(f"  {doc['title'][:60]:60s} | {len(doc['text']):>6d} chars")
        yield doc


def fetch_updates(since: str) -> Iterator[Dict]:
    """Yield documents — single-page source, same as fetch_all."""
    yield from fetch_all()


def bootstrap_sample(max_records: int = 20) -> List[Dict]:
    """Fetch sample records."""
    samples = []
    for record in fetch_all():
        samples.append(record)
        if len(samples) >= max_records:
            break
    return samples


def main():
    parser = argparse.ArgumentParser(description='GW/MEF-TaxLegislation Data Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'])
    parser.add_argument('--sample', action='store_true')
    parser.add_argument('--since', type=str)
    parser.add_argument('--output', type=str)
    parser.add_argument('--full', action='store_true')
    args = parser.parse_args()

    output_dir = args.output or str(Path(__file__).parent / 'sample')
    os.makedirs(output_dir, exist_ok=True)

    if args.command == 'bootstrap':
        records = bootstrap_sample() if args.sample else list(fetch_all())

        saved = 0
        for record in records:
            filename = re.sub(r'[^\w-]', '_', record['_id'])[:100] + '.json'
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            saved += 1

        print(f"\n=== Summary ===")
        print(f"Records saved: {saved}")
        if records:
            texts = [r['text'] for r in records if r.get('text')]
            avg_len = sum(len(t) for t in texts) / len(texts) if texts else 0
            print(f"Average text length: {avg_len:.0f} chars")
            print(f"All have text: {all(r.get('text') for r in records)}")

    elif args.command == 'fetch':
        count = 0
        for record in fetch_all():
            filename = re.sub(r'[^\w-]', '_', record['_id'])[:100] + '.json'
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"Fetched {count} records")

    elif args.command == 'updates':
        if not args.since:
            print("ERROR: --since required")
            sys.exit(1)
        count = 0
        for record in fetch_updates(args.since):
            filename = re.sub(r'[^\w-]', '_', record['_id'])[:100] + '.json'
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"Fetched {count} updates since {args.since}")


if __name__ == '__main__':
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
