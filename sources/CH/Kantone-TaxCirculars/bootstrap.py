#!/usr/bin/env python3
"""
CH/Kantone-TaxCirculars -- Swiss Cantonal Tax Circulars (Schwyz, Nidwalden)

Fetches tax directives (Weisungen) and guidelines (Richtlinien) from:
  - Schwyz (SZ): Schwyzer Steuerbuch sections at sz.ch
  - Nidwalden (NW): Weisungen pages at steuern-nw.ch

Strategy:
  - Scrape listing pages to discover PDF URLs
  - Download PDFs and extract full text via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CH.Kantone-TaxCirculars")

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

# ─── Schwyz (SZ) Configuration ─────────────────────────────────────────────

SZ_BASE = "https://www.sz.ch"
SZ_PAGES = [
    # (section_name, URL path)
    (
        "Steuerrechtliche Weisungen",
        "/finanzdepartement/steuerverwaltung/rechtliche-grundlagen/schwyzer-steuerbuch"
        "/steuerrechtliche-weisungen-der-steuerverwaltung.html"
        "/8756-8758-8802-10332-10354-10358-10359-10368",
    ),
    (
        "Weisungen an die Gemeinden",
        "/finanzdepartement/steuerverwaltung/rechtliche-grundlagen/schwyzer-steuerbuch"
        "/weisungen-der-steuerverwaltung-an-die-gemeinden-etc.html"
        "/8756-8758-8802-10332-10354-10358-10359-10369",
    ),
    (
        "Weisungen des Finanzdepartements",
        "/finanzdepartement/steuerverwaltung/rechtliche-grundlagen/schwyzer-steuerbuch"
        "/weisungen-des-finanzdepartements.html"
        "/8756-8758-8802-10332-10354-10358-10359-10366",
    ),
    (
        "Beschlüsse des Regierungsrates",
        "/finanzdepartement/steuerverwaltung/rechtliche-grundlagen/schwyzer-steuerbuch"
        "/beschluesse-des-regierungsrates.html"
        "/8756-8758-8802-10332-10354-10358-10359-10364",
    ),
]

# ─── Nidwalden (NW) Configuration ──────────────────────────────────────────

NW_PAGES = [
    ("NP Weisungen & Richtlinien", "https://www.steuern-nw.ch/services/np-weisungen-richtlinine/"),
    ("JP Weisungen & Richtlinien", "https://www.steuern-nw.ch/services/jp-weisungen-richtlinien/"),
]


def _fetch_html(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch HTML content from a URL."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError) as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def _extract_sz_pdfs(page_html: str, section: str) -> List[Dict[str, Any]]:
    """Extract PDF links from a Schwyz Steuerbuch page."""
    results = []
    seen_urls = set()

    # SZ uses full URLs: href="https://www.sz.ch/public/upload/assets/NNNN/filename.pdf?fp=N"
    # Try with link text first (table cells: <td>ref</td><td><a href="...">title</a></td>)
    link_pattern = (
        r'<a[^>]*href="(https://www\.sz\.ch/public/upload/assets/(\d+)/([^"]+\.pdf)[^"]*)"'
        r'[^>]*>([^<]*)</a>'
    )
    link_matches = re.findall(link_pattern, page_html)

    for href, asset_id, filename, link_text in link_matches:
        url = href.split("?")[0]
        if url in seen_urls:
            continue
        seen_urls.add(url)

        # Extract reference number from filename (e.g., Steuerbuch_70_10)
        ref_match = re.search(r"(?:Steuerbuch_|stb_)(\d+_\d+(?:_\d+)?)", filename)
        ref = ref_match.group(1).replace("_", ".") if ref_match else ""

        # Use link text as title; fall back to cleaned filename
        title = link_text.strip()
        if not title or title == filename:
            title = re.sub(r"(?:Schwyzer_)?Steuerbuch_\d+_\d+(?:_\d+)?_?", "", filename)
            title = title.replace(".pdf", "").replace("_", " ").strip()
        title = html.unescape(title)

        results.append({
            "url": url,
            "title": title,
            "reference": ref,
            "section": section,
            "asset_id": asset_id,
        })

    # Fallback: catch any PDF links not matched by the <a> pattern
    fallback_pattern = r'href="(https://www\.sz\.ch/public/upload/assets/\d+/[^"]+\.pdf[^"]*)"'
    for href in re.findall(fallback_pattern, page_html):
        url = href.split("?")[0]
        if url in seen_urls:
            continue
        seen_urls.add(url)
        filename = url.split("/")[-1]
        ref_match = re.search(r"(?:Steuerbuch_|stb_)(\d+_\d+(?:_\d+)?)", filename)
        ref = ref_match.group(1).replace("_", ".") if ref_match else ""
        title = re.sub(r"(?:Schwyzer_)?Steuerbuch_\d+_\d+(?:_\d+)?_?", "", filename)
        title = title.replace(".pdf", "").replace("_", " ").strip()
        title = html.unescape(title)

        results.append({
            "url": url,
            "title": title,
            "reference": ref,
            "section": section,
            "asset_id": url.split("/")[-2],
        })

    return results


def _extract_nw_pdfs(page_html: str, section: str) -> List[Dict[str, Any]]:
    """Extract PDF links from a Nidwalden Weisungen page."""
    results = []
    # Pattern: links to /app/uploads/YYYY/MM/filename.pdf
    pattern = r'href="(https://www\.steuern-nw\.ch/app/uploads/[^"]+\.pdf)"'
    matches = re.findall(pattern, page_html)

    # Also try relative URLs
    rel_pattern = r'href="(/app/uploads/[^"]+\.pdf)"'
    rel_matches = re.findall(rel_pattern, page_html)
    for rel in rel_matches:
        full_url = "https://www.steuern-nw.ch" + rel
        if full_url not in matches:
            matches.append(full_url)

    # Get link text for titles
    link_pattern = r'<a[^>]*href="((?:https://www\.steuern-nw\.ch)?/app/uploads/[^"]+\.pdf)"[^>]*>([^<]*)</a>'
    link_matches = re.findall(link_pattern, page_html)

    seen_urls = set()
    if link_matches:
        for href, link_text in link_matches:
            url = href if href.startswith("http") else "https://www.steuern-nw.ch" + href
            if url in seen_urls:
                continue
            seen_urls.add(url)

            filename = url.split("/")[-1]
            title = link_text.strip() if link_text.strip() else filename
            title = title.replace(".pdf", "").replace("_", " ").replace("-", " ").strip()
            title = html.unescape(title)

            # Extract date from filename if possible
            date_match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
            date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}" if date_match else None

            results.append({
                "url": url,
                "title": title,
                "reference": "",
                "section": section,
                "date": date,
            })
    else:
        for url in matches:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            filename = url.split("/")[-1]
            title = filename.replace(".pdf", "").replace("_", " ").replace("-", " ").strip()
            title = html.unescape(title)

            date_match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
            date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}" if date_match else None

            results.append({
                "url": url,
                "title": title,
                "reference": "",
                "section": section,
                "date": date,
            })

    return results


class CHKantoneScraper(BaseScraper):
    SOURCE_ID = "CH/Kantone-TaxCirculars"

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir)

    def _discover_all_pdfs(self) -> List[Dict[str, Any]]:
        """Discover all PDFs from SZ and NW listing pages."""
        all_pdfs = []

        # Schwyz pages
        for section, path in SZ_PAGES:
            url = SZ_BASE + path
            logger.info(f"Fetching SZ section: {section}")
            page_html = _fetch_html(url)
            if page_html:
                pdfs = _extract_sz_pdfs(page_html, section)
                for pdf in pdfs:
                    pdf["canton"] = "SZ"
                all_pdfs.extend(pdfs)
                logger.info(f"  Found {len(pdfs)} PDFs in {section}")
            time.sleep(3)  # Avoid 429 rate limiting from sz.ch

        # Nidwalden pages
        for section, url in NW_PAGES:
            logger.info(f"Fetching NW section: {section}")
            page_html = _fetch_html(url)
            if page_html:
                pdfs = _extract_nw_pdfs(page_html, section)
                for pdf in pdfs:
                    pdf["canton"] = "NW"
                all_pdfs.extend(pdfs)
                logger.info(f"  Found {len(pdfs)} PDFs in {section}")
            time.sleep(1)

        return all_pdfs

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all tax circulars with full text."""
        pdfs = self._discover_all_pdfs()
        logger.info(f"Total PDFs to process: {len(pdfs)}")

        for i, pdf in enumerate(pdfs):
            time.sleep(1.5)
            canton = pdf["canton"]
            ref = pdf.get("reference", "")
            # Create unique ID from canton + reference or URL hash
            if ref:
                doc_id = f"{canton}_{ref.replace('.', '_')}"
            else:
                # Use filename from URL
                filename = pdf["url"].split("/")[-1].split("?")[0]
                doc_id = f"{canton}_{filename.replace('.pdf', '')}"

            # Clean doc_id
            doc_id = re.sub(r'[^\w\-]', '_', doc_id)[:80]

            text = extract_pdf_markdown(
                source=self.SOURCE_ID,
                source_id=doc_id,
                pdf_url=pdf["url"],
                table="doctrine",
            )
            if not text or len(text.strip()) < 100:
                logger.warning(f"Skipping {pdf['title'][:50]}: insufficient text ({len(text) if text else 0} chars)")
                continue

            yield self.normalize({
                "doc_id": doc_id,
                "title": pdf["title"],
                "text": text,
                "url": pdf["url"],
                "date": pdf.get("date"),
                "canton": canton,
                "reference": ref,
                "category": pdf.get("section", ""),
            })

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch all (no date-based filtering available for these static pages)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "canton": raw["canton"],
            "reference": raw.get("reference", ""),
            "category": raw.get("category", ""),
        }


# ─── CLI Entry Point ─────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="CH/Kantone-TaxCirculars bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    scraper = CHKantoneScraper()

    if args.command == "test":
        pdfs = scraper._discover_all_pdfs()
        print(f"OK: Found {len(pdfs)} tax circular PDFs (SZ + NW)")
        for canton in ("SZ", "NW"):
            count = sum(1 for p in pdfs if p["canton"] == canton)
            print(f"  {canton}: {count} PDFs")
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else 9999

    for record in scraper.fetch_all():
        count += 1
        fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        logger.info(f"[{count}] [{record['canton']}] {record['title'][:50]} ({text_len} chars)")

        if count >= limit:
            logger.info(f"Sample limit reached ({limit} records)")
            break

    print(f"\nDone: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
