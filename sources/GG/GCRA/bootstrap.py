#!/usr/bin/env python3
"""
Guernsey Competition & Regulatory Authority (GCRA) Decision Fetcher

Scrapes competition and regulatory decisions from gcra.gg.
GCRA is the successor to CICRA (dissolved July 2020).

Covers: Competition, Telecoms, Electricity, Post sectors.
~322 cases from 2007-present, with PDF full text extraction.

Data source: https://www.gcra.gg/cases
License: Public government data
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

SOURCE_ID = "GG/GCRA"
BASE_URL = "https://www.gcra.gg"
CASES_URL = f"{BASE_URL}/cases"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter; Legal Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}


def parse_date(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY date string to ISO 8601."""
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()
    for fmt in ("%d/%m/%Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class GCRAFetcher:
    """Fetcher for GCRA competition and regulatory decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse an HTML page."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            print(f"  [WARN] Failed to fetch {url}: {e}")
            return None

    def _scrape_case_list(self, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        """Scrape case listing pages to discover all cases."""
        cases = []
        page = 0

        while True:
            if max_pages is not None and page >= max_pages:
                break

            url = f"{CASES_URL}?page={page}"
            print(f"  Fetching case list page {page}: {url}")
            soup = self._get_page(url)
            if not soup:
                break

            # Find case links - they're in heading elements linking to /case/...
            case_links = soup.find_all("a", href=re.compile(r"^/case/"))
            if not case_links:
                break

            found_new = False
            for link in case_links:
                href = link.get("href", "")
                if not href.startswith("/case/"):
                    continue

                case_slug = href.replace("/case/", "").strip("/")
                title = link.get_text(strip=True)

                # Extract metadata from surrounding context
                parent = link.find_parent()
                if parent:
                    # Walk up to find the case entry container
                    container = parent
                    for _ in range(5):
                        if container.parent:
                            container = container.parent
                        else:
                            break

                    text_block = container.get_text(" ", strip=True)
                else:
                    text_block = ""

                # Parse sector from text
                sector = None
                for s in ("Competition", "Telecoms", "Electricity", "Post", "General"):
                    if s in text_block:
                        sector = s
                        break

                # Parse dates from text
                open_date = None
                close_date = None
                status = None

                opened_match = re.search(r"OPENED:\s*(\d{2}/\d{2}/\d{4})", text_block)
                if opened_match:
                    open_date = parse_date(opened_match.group(1))

                closed_match = re.search(r"CLOSED:\s*(\d{2}/\d{2}/\d{4})", text_block)
                if closed_match:
                    close_date = parse_date(closed_match.group(1))

                if "Status: Closed" in text_block:
                    status = "Closed"
                elif "Status: Open" in text_block:
                    status = "Open"

                cases.append({
                    "slug": case_slug,
                    "title": title,
                    "sector": sector,
                    "open_date": open_date,
                    "close_date": close_date,
                    "status": status,
                    "url": f"{BASE_URL}/case/{case_slug}",
                })
                found_new = True

            if not found_new:
                break

            page += 1
            time.sleep(1.5)

        # Deduplicate by slug
        seen = set()
        unique = []
        for c in cases:
            if c["slug"] not in seen:
                seen.add(c["slug"])
                unique.append(c)

        print(f"  Found {len(unique)} unique cases")
        return unique

    def _scrape_case_detail(self, case: Dict[str, Any]) -> Dict[str, Any]:
        """Scrape individual case page for PDF links and additional metadata."""
        url = case["url"]
        soup = self._get_page(url)
        if not soup:
            return case

        # Find PDF links
        pdf_links = []
        for a_tag in soup.find_all("a", href=re.compile(r"\.pdf", re.IGNORECASE)):
            href = a_tag.get("href", "")
            if not href:
                continue
            if href.startswith("/"):
                href = BASE_URL + href
            pdf_name = a_tag.get_text(strip=True)
            pdf_links.append({"url": href, "name": pdf_name})

        case["pdf_links"] = pdf_links

        # Try to get case reference from title (e.g. "T1640G - ...")
        ref_match = re.match(r"^([A-Z]\d+[A-Z]?)\s*[-–—]", case.get("title", ""))
        if ref_match:
            case["case_reference"] = ref_match.group(1)
        else:
            case["case_reference"] = case["slug"]

        return case

    def _extract_pdf_text(self, case: Dict[str, Any]) -> str:
        """Download and extract text from all PDFs for a case."""
        texts = []
        for pdf_info in case.get("pdf_links", []):
            pdf_url = pdf_info["url"]
            pdf_name = pdf_info.get("name", "")
            try:
                resp = self.session.get(pdf_url, timeout=60)
                resp.raise_for_status()
                if len(resp.content) < 100:
                    continue

                md = extract_pdf_markdown(
                    source=SOURCE_ID,
                    source_id=case.get("case_reference", case["slug"]),
                    pdf_bytes=resp.content,
                    table="doctrine",
                )
                if md and md.strip():
                    header = f"## {pdf_name}\n\n" if pdf_name else ""
                    texts.append(header + md)
            except requests.RequestException as e:
                print(f"    [WARN] Failed to download PDF {pdf_url}: {e}")
            time.sleep(1.0)

        return "\n\n---\n\n".join(texts)

    def normalize(self, case: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Normalize a case record to standard schema."""
        case_ref = case.get("case_reference", case["slug"])
        return {
            "_id": f"GG/GCRA/{case_ref}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": case.get("title", ""),
            "text": text,
            "case_reference": case_ref,
            "sector": case.get("sector"),
            "date": case.get("close_date") or case.get("open_date"),
            "open_date": case.get("open_date"),
            "close_date": case.get("close_date"),
            "status": case.get("status"),
            "url": case.get("url", ""),
            "pdf_urls": [p["url"] for p in case.get("pdf_links", [])],
        }

    def fetch_all(self, sample: bool = False) -> Iterator[Dict[str, Any]]:
        """Fetch all GCRA decisions with full text."""
        max_pages = 2 if sample else None
        cases = self._scrape_case_list(max_pages=max_pages)

        if sample:
            cases = cases[:15]

        for i, case in enumerate(cases):
            print(f"  [{i+1}/{len(cases)}] {case['title']}")
            case = self._scrape_case_detail(case)
            time.sleep(1.5)

            if not case.get("pdf_links"):
                print(f"    [SKIP] No PDFs found")
                continue

            text = self._extract_pdf_text(case)
            if not text.strip():
                print(f"    [SKIP] No text extracted from PDFs")
                continue

            record = self.normalize(case, text)
            print(f"    OK: {len(text)} chars, {len(case['pdf_links'])} PDFs")
            yield record


def main():
    parser = argparse.ArgumentParser(description="GCRA Decision Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--output", default=None, help="Output directory")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    source_dir = Path(__file__).parent
    output_dir = Path(args.output) if args.output else source_dir / "sample"
    output_dir.mkdir(parents=True, exist_ok=True)

    fetcher = GCRAFetcher()
    count = 0

    for record in fetcher.fetch_all(sample=args.sample):
        out_path = output_dir / f"{record['_id'].replace('/', '_')}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        print(f"  Saved: {out_path.name}")

    print(f"\nDone. {count} records saved to {output_dir}")


if __name__ == "__main__":
    main()
