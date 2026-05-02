#!/usr/bin/env python3
"""
CL/TribunalesAmbientales - Chilean Environmental Court Rulings

Fetches sentencias (rulings) from Chile's three Environmental Courts:
  - 1TA: Primer Tribunal Ambiental (Antofagasta)
  - 2TA: Segundo Tribunal Ambiental (Santiago)
  - 3TA: Tercer Tribunal Ambiental (Valdivia)

Data sources:
  - 3TA: https://3ta.cl/sentencias/ (WordPress page with PDF links)
  - 2TA: https://tribunalambiental.cl (WordPress API news posts with sentencia PDFs)
  - 1TA: https://www.1ta.cl (WordPress site, may have limited access)

License: Public domain (official tribunal decisions)
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SOURCE_ID = "CL/TribunalesAmbientales"
SAMPLE_DIR = Path(__file__).parent / "sample"

# Tribunal URLs
TA3_SENTENCIAS_URL = "https://3ta.cl/sentencias/"
TA2_API_BASE = "https://www.tribunalambiental.cl/wp-json/wp/v2"
TA1_API_BASE = "https://www.1ta.cl/wp-json/wp/v2"


class TribunalesAmbientalesFetcher:
    """Fetcher for Chilean Environmental Court rulings."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html, application/json, */*',
        })

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and decode entities."""
        if not text:
            return ""
        text = unescape(text)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    # ─── 3TA: Tercer Tribunal Ambiental (Valdivia) ───

    def _fetch_3ta_sentencias(self) -> List[Dict[str, Any]]:
        """Scrape the 3TA sentencias page for metadata and PDF links."""
        results = []
        try:
            resp = self.session.get(TA3_SENTENCIAS_URL, timeout=30)
            resp.raise_for_status()
            html = resp.text
        except requests.RequestException as e:
            logger.error(f"3TA page fetch failed: {e}")
            return results

        # Extract table rows with sentencias
        # Pattern: links to PDFs in wp-content/uploads
        pdf_links = re.findall(
            r'href=["\']([^"\']*wp-content/uploads[^"\']*\.pdf)["\']',
            html, re.IGNORECASE
        )

        # Extract rows: each sentencia has a Rol, title/parties, date, and PDF link
        # The page has a table structure with Rol links and PDF download links
        rows = re.findall(
            r'<tr[^>]*>(.*?)</tr>',
            html, re.DOTALL
        )

        for row in rows:
            # Extract Rol number
            rol_match = re.search(r'(?:R|D)-\d+-\d{4}', row)
            if not rol_match:
                continue
            rol = rol_match.group(0)

            # Extract date (DD de mes de YYYY format or YYYY-MM-DD)
            date_str = ""
            date_match = re.search(
                r'(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})',
                row, re.IGNORECASE
            )
            if date_match:
                day, month_name, year = date_match.groups()
                months = {
                    'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
                    'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
                    'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
                }
                month = months.get(month_name.lower(), '01')
                date_str = f"{year}-{month}-{day.zfill(2)}"

            # Extract PDF links from row
            all_pdfs = re.findall(
                r'href=["\']([^"\']*\.pdf)["\']',
                row, re.IGNORECASE
            )
            # Prioritize: actual sentencia PDF matching the Rol (not Sintesis)
            pdf_url = None
            rol_pattern = rol.replace('-', '[-_]?')
            for pdf in all_pdfs:
                if 'sintesis' in pdf.lower():
                    continue
                if re.search(rol_pattern, pdf, re.IGNORECASE):
                    pdf_url = pdf
                    break
            if not pdf_url:
                # Fall back to any non-synthesis sentencia PDF
                for pdf in all_pdfs:
                    if 'sintesis' in pdf.lower():
                        continue
                    if 'sentencia' in pdf.lower():
                        pdf_url = pdf
                        break
            if not pdf_url:
                # Last resort: any non-synthesis PDF
                for pdf in all_pdfs:
                    if 'sintesis' not in pdf.lower():
                        pdf_url = pdf
                        break
            if pdf_url and not pdf_url.startswith('http'):
                pdf_url = urljoin("https://3ta.cl/", pdf_url)

            # Extract title/parties from cell content
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            title = ""
            if len(cells) >= 3:
                # Cell 2 has parties/title; clean out link text artifacts
                raw_title = self._clean_html(cells[2])
                # Remove trailing "Síntesis Sentencia..." link text
                raw_title = re.sub(r'\s*S[ií]ntesis\s+Sentencia.*$', '', raw_title)
                title = raw_title.strip()
            # Extract judge name from cell 4
            judge = ""
            if len(cells) >= 5:
                judge = self._clean_html(cells[4])
            # Extract matter type from cell 5
            matter = ""
            if len(cells) >= 6:
                matter = self._clean_html(cells[5])

            results.append({
                'tribunal': '3TA',
                'rol': rol,
                'title': title or f"Sentencia {rol}",
                'date': date_str,
                'pdf_url': pdf_url,
                'source_url': TA3_SENTENCIAS_URL,
                'judge': judge,
                'matter': matter,
            })

        logger.info(f"3TA: Found {len(results)} sentencias")
        return results

    # ─── 2TA: Segundo Tribunal Ambiental (Santiago) ───

    def _fetch_2ta_sentencias(self) -> List[Dict[str, Any]]:
        """Fetch 2TA sentencias from WordPress API news posts."""
        results = []
        page = 1
        while True:
            url = f"{TA2_API_BASE}/posts"
            params = {
                'per_page': 100,
                'page': page,
                'categories': '5',  # noticias
                'search': 'sentencia',
            }
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
                posts = resp.json()
                if not posts:
                    break
            except requests.RequestException as e:
                logger.error(f"2TA API page {page} failed: {e}")
                break

            for post in posts:
                content = post.get('content', {}).get('rendered', '')
                title = self._clean_html(post.get('title', {}).get('rendered', ''))
                date_str = post.get('date', '')[:10]
                link = post.get('link', '')

                # Extract PDF links from content
                pdf_matches = re.findall(
                    r'href=["\']([^"\']*wp-content/uploads[^"\']*[Ss]entencia[^"\']*\.pdf)["\']',
                    content, re.IGNORECASE
                )
                if not pdf_matches:
                    # Try any PDF in content
                    pdf_matches = re.findall(
                        r'href=["\']([^"\']*\.pdf)["\']',
                        content, re.IGNORECASE
                    )
                    # Filter to likely sentencia PDFs
                    pdf_matches = [p for p in pdf_matches if 'sentencia' in p.lower() or 'R-' in p]

                for pdf_url in pdf_matches:
                    if not pdf_url.startswith('http'):
                        pdf_url = urljoin("https://www.tribunalambiental.cl/", pdf_url)

                    # Extract Rol from PDF filename
                    rol_match = re.search(r'(?:R|D)-\d+-\d{4}', pdf_url)
                    rol = rol_match.group(0) if rol_match else ""

                    results.append({
                        'tribunal': '2TA',
                        'rol': rol or f"2TA-{post.get('id', '')}",
                        'title': title,
                        'date': date_str,
                        'pdf_url': pdf_url,
                        'source_url': link,
                    })

            page += 1
            time.sleep(1)

        logger.info(f"2TA: Found {len(results)} sentencias")
        return results

    # ─── 1TA: Primer Tribunal Ambiental (Antofagasta) ───

    def _fetch_1ta_sentencias(self) -> List[Dict[str, Any]]:
        """Try to fetch 1TA sentencias from WordPress API."""
        results = []
        page = 1
        while True:
            url = f"{TA1_API_BASE}/posts"
            params = {
                'per_page': 100,
                'page': page,
                'search': 'sentencia',
            }
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code in (400, 403, 404):
                    if page == 1:
                        logger.warning("1TA API not accessible (may be blocked)")
                    break
                resp.raise_for_status()
                posts = resp.json()
                if not posts:
                    break
            except requests.RequestException as e:
                if page == 1:
                    logger.warning(f"1TA API failed: {e}")
                break

            for post in posts:
                content = post.get('content', {}).get('rendered', '')
                title = self._clean_html(post.get('title', {}).get('rendered', ''))
                date_str = post.get('date', '')[:10]
                link = post.get('link', '')

                # Extract PDF links from content
                pdf_matches = re.findall(
                    r'href=["\']([^"\']*\.pdf)["\']',
                    content, re.IGNORECASE
                )
                pdf_matches = [p for p in pdf_matches if 'sentencia' in p.lower() or 'R-' in p or 'D-' in p]

                for pdf_url in pdf_matches:
                    if not pdf_url.startswith('http'):
                        pdf_url = urljoin("https://www.1ta.cl/", pdf_url)

                    rol_match = re.search(r'(?:R|D)-\d+-\d{4}', pdf_url)
                    rol = rol_match.group(0) if rol_match else ""

                    results.append({
                        'tribunal': '1TA',
                        'rol': rol or f"1TA-{post.get('id', '')}",
                        'title': title,
                        'date': date_str,
                        'pdf_url': pdf_url,
                        'source_url': link,
                    })

            page += 1
            time.sleep(1)

        logger.info(f"1TA: Found {len(results)} sentencias")
        return results

    # ─── Combined fetch ───

    def _get_all_sentencias_metadata(self) -> List[Dict[str, Any]]:
        """Fetch metadata from all three tribunals."""
        all_items = []
        all_items.extend(self._fetch_3ta_sentencias())
        all_items.extend(self._fetch_2ta_sentencias())
        all_items.extend(self._fetch_1ta_sentencias())

        # Deduplicate by Rol
        seen = set()
        unique = []
        for item in all_items:
            key = f"{item['tribunal']}-{item['rol']}"
            if key not in seen:
                seen.add(key)
                unique.append(item)

        logger.info(f"Total unique sentencias: {len(unique)}")
        return unique

    def normalize(self, item: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Normalize a sentencia into standard schema."""
        tribunal = item.get('tribunal', '')
        rol = item.get('rol', '')
        doc_id = f"CL-{tribunal}-{rol}" if rol else f"CL-{tribunal}-{item.get('date', 'unknown')}"
        # Clean ID for filesystem
        doc_id = re.sub(r'[^\w\-]', '_', doc_id)

        return {
            '_id': doc_id,
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': item.get('title', ''),
            'text': text.strip(),
            'date': item.get('date', ''),
            'url': item.get('source_url', ''),
            'pdf_url': item.get('pdf_url', ''),
            'tribunal': tribunal,
            'rol': rol,
            'language': 'spa',
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all normalized sentencias with full text."""
        items = self._get_all_sentencias_metadata()

        for i, item in enumerate(items):
            pdf_url = item.get('pdf_url')
            if not pdf_url:
                logger.warning(f"  No PDF for {item.get('rol')}, skipping")
                continue

            logger.info(f"Processing {i+1}/{len(items)}: {item.get('tribunal')} {item.get('rol')}")
            text = self._extract_pdf_text(pdf_url)
            if not text:
                logger.warning(f"  No text extracted from PDF, skipping")
                continue

            yield self.normalize(item, text)
            time.sleep(2)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield sentencias published since a given date."""
        since_dt = datetime.fromisoformat(since)
        for doc in self.fetch_all():
            if doc.get('date'):
                try:
                    doc_dt = datetime.fromisoformat(doc['date'])
                    if doc_dt >= since_dt:
                        yield doc
                except (ValueError, TypeError):
                    yield doc

    def bootstrap_sample(self, n: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample of sentencias."""
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        items = self._get_all_sentencias_metadata()
        if not items:
            logger.error("No sentencias found")
            return []

        # Group by tribunal for diverse sampling
        by_tribunal = {}
        for item in items:
            t = item['tribunal']
            by_tribunal.setdefault(t, []).append(item)

        # Pick samples from each tribunal proportionally
        sample_items = []
        for tribunal, tribunal_items in by_tribunal.items():
            count = max(3, n * len(tribunal_items) // len(items))
            # Take newest + spread
            step = max(1, len(tribunal_items) // count)
            for i in range(0, len(tribunal_items), step):
                sample_items.append(tribunal_items[i])
                if len(sample_items) >= n + 5:
                    break

        sample_items = sample_items[:n + 5]
        results = []

        for item in sample_items:
            if len(results) >= n:
                break
            pdf_url = item.get('pdf_url')
            if not pdf_url:
                continue

            logger.info(f"Sample {len(results)+1}: {item.get('tribunal')} {item.get('rol')}")
            text = self._extract_pdf_text(pdf_url)
            if not text:
                logger.warning(f"  No text from PDF, skipping")
                continue

            doc = self.normalize(item, text)
            results.append(doc)

            fname = f"{doc['_id']}.json"
            with open(SAMPLE_DIR / fname, 'w', encoding='utf-8') as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
            logger.info(f"  Saved: {fname} ({len(text)} chars)")
            time.sleep(2)

        logger.info(f"Sample complete: {len(results)} documents saved to {SAMPLE_DIR}")
        return results


def main():
    parser = argparse.ArgumentParser(description='CL/TribunalesAmbientales - Environmental Court Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'bootstrap-fast', 'fetch', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only (for bootstrap)')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (ISO format)')
    parser.add_argument('--limit', type=int, default=15,
                        help='Max documents for sample')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    fetcher = TribunalesAmbientalesFetcher()

    if args.command == 'bootstrap':
        if args.sample:
            results = fetcher.bootstrap_sample(n=args.limit)
            print(f"\nSample: {len(results)} documents fetched")
            for r in results:
                text_len = len(r.get('text', ''))
                print(f"  {r['_id']} | {text_len:>6} chars | {r['tribunal']} | {r['title'][:50]}")
        else:
            count = 0
            for doc in fetcher.fetch_all():
                count += 1
                if count % 10 == 0:
                    logger.info(f"Fetched {count} documents")
            print(f"Total: {count} documents fetched")

    elif args.command == 'bootstrap-fast':
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        written = 0
        errors = 0
        for doc in fetcher.fetch_all():
            count += 1
            try:
                fname = f"{doc['_id']}.json"
                with open(SAMPLE_DIR / fname, 'w', encoding='utf-8') as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)
                written += 1
            except Exception as e:
                logger.error(f"Write error for {doc.get('_id')}: {e}")
                errors += 1
            if count % 10 == 0:
                logger.info(f"Progress: {count} fetched, {written} written")
        print(json.dumps({"records": count, "written": written, "errors": errors}))

    elif args.command == 'updates':
        if not args.since:
            print("Error: --since required for updates command", file=sys.stderr)
            sys.exit(1)
        count = 0
        for doc in fetcher.fetch_updates(args.since):
            count += 1
        print(f"Updates: {count} documents fetched since {args.since}")

    elif args.command == 'fetch':
        count = 0
        for doc in fetcher.fetch_all():
            count += 1
        print(f"Total: {count} documents fetched")


if __name__ == '__main__':
    main()
