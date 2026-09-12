#!/usr/bin/env python3
"""
INTL/ACERWC -- African Committee of Experts on the Rights and Welfare
               of the Child — Decisions and General Comments

Fetches Communications (case_law) and General Comments (doctrine) from
the ACERWC website at acerwc.africa.

Strategy:
  - Paginate /en/communications/list?page=N to collect detail URLs.
  - For each communication detail page, download decision PDF(s) and
    extract full text via common/pdf_extract.py.
  - Scrape /en/key-documents/general-comments for GC PDFs.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap
  python bootstrap.py update             # Re-scan listing
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

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
logger = logging.getLogger("legal-data-hunter.INTL.ACERWC")

BASE = "https://www.acerwc.africa"
COMMS_LISTING = BASE + "/en/communications/list?page={page}"
GC_URL = BASE + "/en/key-documents/general-comments"
RESOLUTIONS_URL = BASE + "/en/resolutions"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
}

MIN_TEXT_CHARS = 300


class ACERWCScraper(BaseScraper):
    """
    Scraper for INTL/ACERWC.
    Country: INTL
    URL: https://www.acerwc.africa/
    Data types: case_law, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── communications listing ────────────────────────────────────
    def _list_communication_urls(self, max_pages: int = 10) -> list[str]:
        """Walk paginated communications listing and return detail URLs."""
        seen: set[str] = set()
        ordered: list[str] = []
        empty_streak = 0
        for page in range(0, max_pages):
            url = COMMS_LISTING.format(page=page)
            try:
                r = self.session.get(url, timeout=60)
                r.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Listing page {page} failed: {e}")
                break
            soup = BeautifulSoup(r.text, "html.parser")
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/en/communications/" in href and href != "/en/communications/list":
                    if "/en/communications/table" in href:
                        continue
                    if "?page=" in href or href.endswith("/list"):
                        continue
                    if href.startswith("/"):
                        href = BASE + href
                    if href not in seen:
                        seen.add(href)
                        links.append(href)
                        ordered.append(href)
            logger.info(f"  comms listing page {page}: {len(links)} new links")
            if not links:
                empty_streak += 1
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
            time.sleep(1.5)
        logger.info(f"Collected {len(ordered)} communication detail URLs")
        return ordered

    # ── communication detail ──────────────────────────────────────
    def _parse_communication(self, url: str) -> Optional[dict]:
        """Parse a communication detail page for metadata + PDF links."""
        try:
            r = self.session.get(url, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Detail fetch failed {url}: {e}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Title: prefer <title> tag, fall back to case-style h2
        article = soup.find("article") or soup
        title = ""
        title_tag = soup.find("title")
        if title_tag:
            raw_title = title_tag.get_text(strip=True)
            # Strip site suffix like "| ACERWC ..."
            raw_title = re.sub(r"\s*\|.*$", "", raw_title).strip()
            if len(raw_title) > 10:
                title = raw_title
        if not title:
            for h2 in soup.find_all("h2"):
                txt = h2.get_text(strip=True)
                if len(txt) > 20 and any(kw in txt.lower() for kw in ("v.", "vs", "against", "behalf")):
                    title = txt
                    break
        title = re.sub(r"\s+", " ", title).strip()

        # Extract metadata from page text using regex (Drupal layout)
        page_text = article.get_text()
        case_number = ""
        m = re.search(r"(\d{3,4}/Com/\d{3}/\d{4})", page_text)
        if m:
            case_number = m.group(1)

        status = ""
        m = re.search(r"Status\s*:?\s*(.+)", page_text)
        if m:
            status = m.group(1).strip().split("\n")[0].strip()

        date_received = ""
        m = re.search(r"Date Received\s*:?\s*(.+)", page_text)
        if m:
            date_received = m.group(1).strip().split("\n")[0].strip()

        respondent = ""
        # Note: site uses "Respondant" (typo)
        m = re.search(r"Respond[ae]nt?\s*State\s*:?\s*(.+)", page_text, re.IGNORECASE)
        if m:
            respondent = m.group(1).strip().split("\n")[0].strip()

        # Collect PDFs
        pdfs = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                if href.startswith("http"):
                    pdfs.append(href)
                elif href.startswith("/"):
                    pdfs.append(BASE + href)

        if not pdfs:
            return None

        return {
            "title": title,
            "url": url,
            "case_number": case_number,
            "status": status,
            "date_received": date_received,
            "respondent_state": respondent,
            "pdf_urls": pdfs[:5],
            "doc_type": "case_law",
        }

    # ── general comments ──────────────────────────────────────────
    def _list_general_comments(self) -> list[dict]:
        """Scrape the general comments page for GC metadata + PDF links."""
        try:
            r = self.session.get(GC_URL, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"General Comments page failed: {e}")
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        results = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if href.lower().endswith(".pdf") and "general comment" in text.lower():
                pdf_url = href if href.startswith("http") else BASE + href
                gc_num = ""
                m = re.search(r"No\.?\s*(\d+)", text)
                if m:
                    gc_num = m.group(1)
                results.append({
                    "title": re.sub(r"\s+", " ", text).strip(),
                    "url": GC_URL,
                    "case_number": f"GC-{gc_num}" if gc_num else f"GC-{len(results)+1}",
                    "pdf_urls": [pdf_url],
                    "doc_type": "doctrine",
                    "status": "Adopted",
                    "date_received": "",
                    "respondent_state": "",
                })

        # Deduplicate by GC number (English preferred)
        seen_gc: dict[str, dict] = {}
        for gc in results:
            key = gc["case_number"]
            existing = seen_gc.get(key)
            if existing is None:
                seen_gc[key] = gc
            else:
                # Prefer English
                fname = gc["pdf_urls"][0].lower()
                if "eng" in fname or "en" in fname:
                    seen_gc[key] = gc
        deduped = list(seen_gc.values())
        logger.info(f"Collected {len(deduped)} general comments")
        return deduped

    # ── resolutions ────────────────────────────────────────────────
    def _list_resolutions(self) -> list[dict]:
        """Scrape the resolutions page for resolution metadata + PDF links."""
        try:
            r = self.session.get(RESOLUTIONS_URL, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Resolutions page failed: {e}")
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        results = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if href.lower().endswith(".pdf") and ("resolution" in text.lower() or "no." in text.lower()):
                pdf_url = href if href.startswith("http") else BASE + href
                res_num = ""
                m = re.search(r"No\.?\s*(\d+)", text)
                if m:
                    res_num = m.group(1)
                results.append({
                    "title": re.sub(r"\s+", " ", text).strip(),
                    "url": RESOLUTIONS_URL,
                    "case_number": f"RES-{res_num}" if res_num else f"RES-{len(results)+1}",
                    "pdf_urls": [pdf_url],
                    "doc_type": "doctrine",
                    "status": "Adopted",
                    "date_received": "",
                    "respondent_state": "",
                })

        # Deduplicate by resolution number (English preferred)
        seen_res: dict[str, dict] = {}
        for res in results:
            key = res["case_number"]
            existing = seen_res.get(key)
            if existing is None:
                seen_res[key] = res
            else:
                fname = res["pdf_urls"][0].lower()
                if "eng" in fname or "en" in fname:
                    seen_res[key] = res
        deduped = list(seen_res.values())
        logger.info(f"Collected {len(deduped)} resolutions")
        return deduped

    # ── PDF text extraction ───────────────────────────────────────
    def _extract_text(self, doc_id: str, pdf_urls: list[str]) -> str:
        parts = []
        for pdf_url in pdf_urls:
            # force=True: the Arabic half of this corpus was stored
            # character-reversed (issue #1560), so a refresh has to re-extract
            # the rows already in Neon rather than skip them as present. It also
            # keeps a multi-PDF document from stopping after its first file,
            # since every PDF of one document shares this source_id.
            md = extract_pdf_markdown(
                source="INTL/ACERWC",
                source_id=doc_id,
                pdf_url=pdf_url,
                table="case_law",
                force=True,
            )
            if md and md.strip():
                label = pdf_url.rsplit("/", 1)[-1]
                parts.append(f"--- {label} ---\n\n{md.strip()}")
            time.sleep(1.0)
        return "\n\n".join(parts).strip()

    # ── date parsing ──────────────────────────────────────────────
    @staticmethod
    def _guess_date(text: str) -> Optional[str]:
        months = {m.lower(): i for i, m in enumerate(
            ["January", "February", "March", "April", "May", "June", "July",
             "August", "September", "October", "November", "December"], start=1)}
        for scope in (text[:4000], text[:20000]):
            m = re.search(
                r"(\d{1,2})\s*(?:st|nd|rd|th)?\s+"
                r"(January|February|March|April|May|June|July|August|"
                r"September|October|November|December)[,\s]+(\d{4})",
                scope, re.IGNORECASE)
            if m:
                day = int(m.group(1))
                mon = months.get(m.group(2).lower())
                year = int(m.group(3))
                if mon and 1 <= day <= 31 and 1985 <= year <= 2100:
                    return f"{year:04d}-{mon:02d}-{day:02d}"
        return None

    # ── normalize ─────────────────────────────────────────────────
    def normalize(self, raw: dict) -> Optional[dict]:
        text = (raw.get("text") or "").strip()
        if len(text) < MIN_TEXT_CHARS:
            return None

        title = raw.get("title", "").strip()
        case_number = raw.get("case_number", "")
        doc_type = raw.get("doc_type", "case_law")
        _type = "case_law" if doc_type == "case_law" else "doctrine"

        date = self._guess_date(text)
        _id = "ACERWC-" + re.sub(r"[^0-9A-Za-z]+", "-", case_number or title[:40]).strip("-")

        return {
            "_id": _id,
            "_source": "INTL/ACERWC",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("url", ""),
            "case_number": case_number,
            "status": raw.get("status", ""),
            "respondent_state": raw.get("respondent_state", ""),
            "pdf_urls": raw.get("pdf_urls", []),
            "court": "African Committee of Experts on the Rights and Welfare of the Child",
            "jurisdiction": "African Union",
        }

    # ── fetch ─────────────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        # 1. Communications
        comm_urls = self._list_communication_urls()
        total = len(comm_urls)
        yielded = 0
        for i, url in enumerate(comm_urls):
            logger.info(f"[comm {i+1}/{total}] {url}")
            meta = self._parse_communication(url)
            if not meta:
                logger.info("  no decision PDF — skipping")
                time.sleep(1)
                continue
            text = self._extract_text(meta["case_number"] or f"comm-{i}", meta["pdf_urls"])
            if len(text) < MIN_TEXT_CHARS:
                logger.info(f"  insufficient text ({len(text)} chars) — skipping")
                time.sleep(1)
                continue
            yield {**meta, "text": text}
            yielded += 1
            logger.info(f"  yielded comm ({len(text)} chars)")
            time.sleep(1)
        logger.info(f"Communications: {yielded}/{total} with full text")

        # 2. General Comments
        gcs = self._list_general_comments()
        gc_yielded = 0
        for i, gc in enumerate(gcs):
            logger.info(f"[GC {i+1}/{len(gcs)}] {gc['title'][:80]}")
            text = self._extract_text(gc["case_number"], gc["pdf_urls"])
            if len(text) < MIN_TEXT_CHARS:
                logger.info(f"  insufficient text ({len(text)} chars) — skipping")
                time.sleep(1)
                continue
            yield {**gc, "text": text}
            gc_yielded += 1
            logger.info(f"  yielded GC ({len(text)} chars)")
            time.sleep(1)
        logger.info(f"General Comments: {gc_yielded}/{len(gcs)} with full text")

        # 3. Resolutions
        resolutions = self._list_resolutions()
        res_yielded = 0
        for i, res in enumerate(resolutions):
            logger.info(f"[RES {i+1}/{len(resolutions)}] {res['title'][:80]}")
            text = self._extract_text(res["case_number"], res["pdf_urls"])
            if len(text) < MIN_TEXT_CHARS:
                logger.info(f"  insufficient text ({len(text)} chars) — skipping")
                time.sleep(1)
                continue
            yield {**res, "text": text}
            res_yielded += 1
            logger.info(f"  yielded RES ({len(text)} chars)")
            time.sleep(1)
        logger.info(f"Resolutions: {res_yielded}/{len(resolutions)} with full text")
        logger.info(f"Total yielded: {yielded + gc_yielded + res_yielded}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ACERWC fetcher")
    sub = parser.add_subparsers(dest="command")

    bp = sub.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of samples")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    # `bootstrap-fast` is the fleet runner's entry point for a FULL crawl, not a
    # sampling one — it was registered here with `--sample` defaulted to True,
    # so every fleet run fetched 15 records and the corpus never grew past its
    # bundled samples (issue #602/#1113 class). It is an alias for the full
    # bootstrap; the corpus is a few hundred documents, so there is no separate
    # fast path to route to.
    bf = sub.add_parser("bootstrap-fast", help="Alias for the full bootstrap (fleet entry point)")
    bf.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bf.add_argument("--sample-size", type=int, default=15, help="Number of samples")
    bf.add_argument("--full", action="store_true", help="Fetch all records")

    sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = ACERWCScraper()

    if args.command == "test":
        urls = scraper._list_communication_urls(max_pages=2)
        logger.info(f"OK: {len(urls)} communication URLs (first 2 pages)")
        gcs = scraper._list_general_comments()
        logger.info(f"OK: {len(gcs)} general comments")
        if urls:
            meta = scraper._parse_communication(urls[0])
            logger.info(f"First comm: {meta}")
    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(
            sample_mode=getattr(args, "sample", False),
            sample_size=getattr(args, "sample_size", 15),
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
