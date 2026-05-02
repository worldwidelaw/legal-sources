#!/usr/bin/env python3
"""
MX/IFT -- Instituto Federal de Telecomunicaciones - Resoluciones del Pleno

Fetches plenary resolutions from IFT with full text extracted from PDFs.

Strategy:
  - Crawl paginated session listing at ift.org.mx/conocenos/pleno/sesiones-del-pleno
  - For each session page, parse HTML to extract resolution numbers and PDF links
  - Download each resolution PDF and extract text via pdf_extract
  - Normalize into standard schema with full text

Data:
  - ~6000+ resolutions from 2013-2025
  - Telecom regulation, competition, spectrum, broadcasting decisions
  - Full text in Spanish, extracted from PDF

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental (not implemented)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from html import unescape

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.IFT")

BASE_URL = "https://www.ift.org.mx"
SESSIONS_URL = f"{BASE_URL}/conocenos/pleno/sesiones-del-pleno"
DELAY = 1.5

# Spanish month names for date parsing
MONTH_MAP = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


class IFTScraper(BaseScraper):
    """Scraper for MX/IFT plenary resolutions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
        })

    def _parse_date_from_slug(self, slug: str) -> Optional[str]:
        """Extract date from session URL slug like 'xx-ordinaria-del-pleno-24-de-septiembre-de-2025'."""
        # Match patterns like "24-de-septiembre-de-2025" or "7-de-mayo-de-2014"
        m = re.search(
            r'(\d{1,2})-de-(\w+)-(?:de-)?(\d{4})',
            slug,
        )
        if m:
            day, month_name, year = m.groups()
            month = MONTH_MAP.get(month_name.lower())
            if month:
                try:
                    return datetime(int(year), month, int(day)).strftime("%Y-%m-%d")
                except ValueError:
                    pass
        return None

    def _parse_date_from_resolution_id(self, res_id: str) -> Optional[str]:
        """Extract date from resolution ID like 'P/IFT/240925/321' (DDMMYY)."""
        m = re.search(r'P/IFT(?:/EXT)?/(\d{6})/', res_id)
        if m:
            date_str = m.group(1)
            try:
                day = int(date_str[0:2])
                month = int(date_str[2:4])
                year = int(date_str[4:6])
                year += 2000 if year < 50 else 1900
                return datetime(year, month, day).strftime("%Y-%m-%d")
            except (ValueError, IndexError):
                pass
        return None

    def _get_session_urls(self, max_pages: int = 60) -> List[Dict[str, str]]:
        """Crawl session listing pages to get all session URLs."""
        sessions = []
        for page in range(max_pages):
            url = f"{SESSIONS_URL}?page={page}"
            try:
                time.sleep(DELAY)
                r = self.session.get(url, timeout=30)
                r.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch page {page}: {e}")
                break

            soup = BeautifulSoup(r.text, "html.parser")
            links = soup.find_all("a", href=re.compile(r"/conocenos/pleno/sesiones/"))
            if not links:
                logger.info(f"No more sessions on page {page}, stopping")
                break

            for a in links:
                href = a.get("href", "")
                if not href.startswith("http"):
                    href = BASE_URL + href
                slug = href.rstrip("/").split("/")[-1]
                title = a.get_text(strip=True)
                date = self._parse_date_from_slug(slug)
                sessions.append({
                    "url": href,
                    "slug": slug,
                    "title": title,
                    "date": date,
                })

            logger.info(f"Page {page}: found {len(links)} sessions (total: {len(sessions)})")

        # Deduplicate by URL
        seen = set()
        unique = []
        for s in sessions:
            if s["url"] not in seen:
                seen.add(s["url"])
                unique.append(s)
        logger.info(f"Total unique sessions: {len(unique)}")
        return unique

    def _parse_session_resolutions(self, session_info: Dict[str, str]) -> List[Dict[str, Any]]:
        """Parse a session page to extract resolution numbers and PDF links."""
        url = session_info["url"]
        try:
            time.sleep(DELAY)
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch session {url}: {e}")
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        html = r.text
        resolutions = []

        # Find all resolution IDs (P/IFT/... pattern)
        res_ids = re.findall(r'P/IFT(?:/EXT)?/\d{6}/\d+', html)
        # Deduplicate while preserving order
        seen_ids = set()
        unique_ids = []
        for rid in res_ids:
            if rid not in seen_ids:
                seen_ids.add(rid)
                unique_ids.append(rid)

        # Find all resolution PDF links
        pdf_links = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower() and ("acuerdo_liga" in href or "acuerdoliga" in href):
                # Skip accessible versions
                if "_acc.pdf" in href.lower():
                    continue
                # Try to match this PDF to a resolution ID
                # Newer: p_ift_240925.321.pdf -> P/IFT/240925/321
                # Older: pift07051496.pdf -> P/IFT/070514/96
                fname = href.split("/")[-1].lower()
                for rid in unique_ids:
                    # Extract date and number parts from the resolution ID
                    m = re.match(r'P/IFT(?:/EXT)?/(\d{6})/(\d+)', rid)
                    if m:
                        date_part, num_part = m.groups()
                        # Check both naming patterns
                        patterns = [
                            f"p_ift_{date_part}.{num_part}.pdf",
                            f"p_ift_ext_{date_part}.{num_part}.pdf",
                            f"pift{date_part}{num_part}.pdf",
                            f"dofpift{date_part}{num_part}.pdf",
                            f"vppift{date_part}{num_part}.pdf",
                        ]
                        if any(fname == p for p in patterns):
                            if not href.startswith("http"):
                                href = BASE_URL + href
                            pdf_links[rid] = href
                            break
                # If no match yet, store it generically
                if href not in pdf_links.values():
                    if not href.startswith("http"):
                        href = BASE_URL + href
                    # Try to associate by proximity (this PDF likely belongs to
                    # the resolution ID mentioned just before it in HTML)
                    pass

        # For PDFs not matched via filename, try proximity matching
        # Parse the HTML sequentially to associate PDFs with their resolution IDs
        current_rid = None
        for line in html.split("\n"):
            rid_match = re.search(r'(P/IFT(?:/EXT)?/\d{6}/\d+)', line)
            if rid_match:
                current_rid = rid_match.group(1)
            pdf_match = re.search(r'href="([^"]*(?:acuerdo_liga|acuerdoliga)[^"]*\.pdf)"', line, re.IGNORECASE)
            if pdf_match and current_rid and current_rid not in pdf_links:
                href = pdf_match.group(1)
                if "_acc.pdf" not in href.lower():
                    if not href.startswith("http"):
                        href = BASE_URL + href
                    pdf_links[current_rid] = href

        # Build resolution records
        for rid in unique_ids:
            pdf_url = pdf_links.get(rid)
            if not pdf_url:
                continue  # Skip resolutions without PDF links

            date = self._parse_date_from_resolution_id(rid)
            if not date:
                date = session_info.get("date")

            resolutions.append({
                "resolution_id": rid,
                "pdf_url": pdf_url,
                "session_title": session_info.get("title", ""),
                "session_url": url,
                "date": date,
            })

        logger.info(f"  Session {session_info.get('title', '')[:60]}: {len(resolutions)} resolutions with PDFs")
        return resolutions

    def fetch_all(self, sample_mode: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all IFT plenary resolutions with full text from PDFs."""
        max_pages = 2 if sample_mode else 60
        logger.info(f"Step 1: Crawling session listing pages (max {max_pages})...")
        sessions = self._get_session_urls(max_pages=max_pages)

        logger.info(f"Step 2: Parsing {len(sessions)} sessions for resolution PDFs...")
        yielded = 0
        for i, session_info in enumerate(sessions):
            resolutions = self._parse_session_resolutions(session_info)

            for res in resolutions:
                try:
                    logger.info(f"[{yielded+1}] {res['resolution_id']}")
                    time.sleep(DELAY)

                    # Download PDF
                    try:
                        pdf_resp = self.session.get(res["pdf_url"], timeout=120)
                        pdf_resp.raise_for_status()
                    except requests.RequestException as e:
                        logger.warning(f"  Failed to download PDF: {e}")
                        continue

                    pdf_bytes = pdf_resp.content
                    if len(pdf_bytes) < 500:
                        logger.warning(f"  PDF too small ({len(pdf_bytes)} bytes), skipping")
                        continue

                    # Extract text from PDF
                    doc_id = res["resolution_id"].replace("/", "-")
                    text = extract_pdf_markdown(
                        "MX/IFT", doc_id,
                        pdf_bytes=pdf_bytes,
                        table="case_law",
                        force=True,
                    )
                    if not text or len(text.strip()) < 100:
                        logger.warning(f"  Insufficient text extracted ({len(text) if text else 0} chars)")
                        continue

                    res["text"] = text
                    yield self.normalize(res)
                    yielded += 1

                except Exception as e:
                    logger.error(f"  Error processing {res['resolution_id']}: {e}")
                    continue

            if (i + 1) % 10 == 0:
                logger.info(f"  Parsed {i+1}/{len(sessions)} sessions, {yielded} records yielded")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates since a given date (not implemented)."""
        logger.warning("Incremental updates not implemented, use bootstrap")
        return
        yield

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw resolution data into standard schema."""
        rid = raw["resolution_id"]
        date = raw.get("date") or ""

        return {
            "_id": rid.replace("/", "-"),
            "_source": "MX/IFT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"Resolución {rid}",
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("pdf_url", ""),
            "resolution_id": rid,
            "session_title": raw.get("session_title", ""),
            "session_url": raw.get("session_url", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ─────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="MX/IFT bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    scraper = IFTScraper()

    if args.command == "test":
        logger.info("Testing connectivity to IFT session listing...")
        r = scraper.session.get(SESSIONS_URL, timeout=30)
        r.raise_for_status()
        links = re.findall(r'href="/conocenos/pleno/sesiones/[^"]+', r.text)
        logger.info(f"Found {len(links)} session links on first page")
        logger.info("Test passed!")
        return

    if args.command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if args.sample else 999999

        for record in scraper.fetch_all(sample_mode=args.sample):
            text_len = len(record.get("text", ""))
            logger.info(
                f"  => {record['_id'][:60]} | "
                f"text={text_len} chars | date={record.get('date', 'N/A')}"
            )

            safe_name = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
            out_path = sample_dir / f"{safe_name}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            count += 1
            if count >= limit:
                break

        logger.info(f"Done. Saved {count} records to {sample_dir}")


if __name__ == "__main__":
    main()
