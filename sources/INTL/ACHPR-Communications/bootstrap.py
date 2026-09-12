#!/usr/bin/env python3
"""
INTL/ACHPR-Communications -- African Commission on Human and Peoples' Rights
                             Decisions on Communications

Fetches the quasi-judicial "Decisions on Communications" of the African
Commission on Human and Peoples' Rights (the Banjul Charter organ of the
African Union). These are merits / admissibility / inadmissibility decisions
on individual and inter-state complaints — distinct from the African Court
on Human and Peoples' Rights (INTL/AfCHPR).

Strategy:
  - Paginate the listing at
        https://achpr.au.int/en/category/decisions-communications?page=N
    until a page yields no communication links.
  - For each communication detail page, read the og:title and collect the
    linked decision PDF(s).
  - Prefer the adopted decision PDF(s) (engdecision-*-merits / -admissible /
    -inadmissible); skip pure transmittal/cover letters. English preferred.
  - Download + extract full text from the PDF(s) via common/pdf_extract.py.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Re-scan listing (idempotent via Neon)
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

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ACHPR-Communications")

BASE = "https://achpr.au.int"
LISTING_URL = BASE + "/en/category/decisions-communications?page={page}"
DETAIL_RE = re.compile(r'href="(/en/decisions-communications/[^"#?]+)"')

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
}

# Filename hints
_EXCLUDE = ("transmit", "letter", "cover", "press", "agenda", "provisional",
            "order-of", "noteverbale", "note-verbale")
_DECISION = ("decision", "merits", "admiss", "inadmis", "ruling", "decisio")

MIN_TEXT_CHARS = 500  # below this we treat the communication as no-full-text


class ACHPRCommunicationsScraper(BaseScraper):
    """
    Scraper for INTL/ACHPR-Communications.
    Country: INTL
    URL: https://achpr.au.int/en/category/decisions-communications
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── listing ────────────────────────────────────────────────────
    def _list_detail_urls(self, max_pages: int = 40) -> list[str]:
        """Walk the paginated listing and return distinct detail-page URLs."""
        seen: set[str] = set()
        ordered: list[str] = []
        empty_streak = 0
        for page in range(0, max_pages):
            url = LISTING_URL.format(page=page)
            try:
                r = self.session.get(url, timeout=60)
                r.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Listing page {page} failed: {e}")
                break
            paths = [m for m in DETAIL_RE.findall(r.text)
                     if m != "/en/category/decisions-communications"]
            new = 0
            for p in paths:
                full = BASE + p
                if full not in seen:
                    seen.add(full)
                    ordered.append(full)
                    new += 1
            logger.info(f"  listing page {page}: {len(paths)} links, {new} new")
            if new == 0:
                empty_streak += 1
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
            time.sleep(1.5)
        logger.info(f"Collected {len(ordered)} communication detail URLs")
        return ordered

    # ── detail page ────────────────────────────────────────────────
    def _parse_detail(self, url: str) -> Optional[dict]:
        """Return {title, pdf_urls} for a communication detail page."""
        try:
            r = self.session.get(url, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Detail fetch failed {url}: {e}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        title = ""
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            title = og["content"].strip()
        if not title:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)
        title = re.sub(r"\s+", " ", title).strip()

        pdfs = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                if href.startswith("http"):
                    pdfs.append(href)
                elif href.startswith("/"):
                    pdfs.append(BASE + href)

        selected = self._select_pdfs(pdfs)
        if not selected:
            return None
        return {"title": title, "url": url, "pdf_urls": selected}

    @staticmethod
    def _select_pdfs(pdfs: list[str]) -> list[str]:
        """Choose decision PDF(s); prefer English, skip transmittal letters."""
        seen: set[str] = set()
        uniq: list[str] = []
        for h in pdfs:
            if h not in seen:
                seen.add(h)
                uniq.append(h)

        def fname(h: str) -> str:
            return h.rsplit("/", 1)[-1].lower()

        # Drop favicons / images that slipped through and excluded doc types
        candidates = [h for h in uniq
                      if not any(x in fname(h) for x in _EXCLUDE)]
        decisions = [h for h in candidates
                     if any(k in fname(h) for k in _DECISION)]
        selected = decisions or candidates
        if not selected:
            return []
        english = [h for h in selected if fname(h).startswith("eng")]
        chosen = english if english else selected
        return chosen[:6]

    # ── parsing helpers ────────────────────────────────────────────
    @staticmethod
    def _communication_numbers(title: str, url: str) -> list[str]:
        """Extract communication reference numbers, e.g. ['424/12']."""
        nums = re.findall(r"\b(\d{1,4}\s*/\s*\d{2,4})\b", title)
        nums = [re.sub(r"\s+", "", n) for n in nums]
        if nums:
            # de-dup preserving order
            out = []
            for n in nums:
                if n not in out:
                    out.append(n)
            return out
        # fallback: digits in the slug, e.g. communication-42412 -> 424/12
        slug = url.rsplit("/", 1)[-1]
        m = re.search(r"(\d{3,5})", slug)
        if m:
            d = m.group(1)
            if len(d) >= 4:
                return [f"{d[:-2]}/{d[-2:]}"]
            return [d]
        return []

    @staticmethod
    def _guess_date(text: str) -> Optional[str]:
        """Best-effort decision/adoption date as ISO from the (OCR) text."""
        months = {m.lower(): i for i, m in enumerate(
            ["January", "February", "March", "April", "May", "June", "July",
             "August", "September", "October", "November", "December"], start=1)}
        # Look near an "Adopted ..." phrase first, then anywhere.
        head = text[:4000]
        for scope in (head, text[:20000]):
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

    # ── extraction ─────────────────────────────────────────────────
    def _extract_text(self, comm_id: str, pdf_urls: list[str]) -> str:
        parts = []
        for pdf_url in pdf_urls:
            md = extract_pdf_markdown(
                source="INTL/ACHPR-Communications",
                source_id=comm_id,
                pdf_url=pdf_url,
                table="case_law",
            )
            if md and md.strip():
                label = pdf_url.rsplit("/", 1)[-1]
                parts.append(f"--- {label} ---\n\n{md.strip()}")
            time.sleep(1.0)
        return "\n\n".join(parts).strip()

    # ── schema ─────────────────────────────────────────────────────
    def normalize(self, raw: dict) -> Optional[dict]:
        text = (raw.get("text") or "").strip()
        if len(text) < MIN_TEXT_CHARS:
            return None

        title = raw.get("title", "").strip()
        nums = raw.get("communication_numbers") or []
        comm_no = " & ".join(nums) if nums else (raw.get("comm_id") or "")
        comm_id = raw.get("comm_id") or (nums[0] if nums else title[:40])

        date = self._guess_date(text)
        filing_year = None
        if nums:
            m = re.search(r"/(\d{2,4})$", nums[0])
            if m:
                yy = m.group(1)
                if len(yy) == 2:
                    filing_year = 1900 + int(yy) if int(yy) > 50 else 2000 + int(yy)
                else:
                    filing_year = int(yy)

        return {
            "_id": "ACHPR-" + re.sub(r"[^0-9A-Za-z]+", "-", comm_id).strip("-"),
            "_source": "INTL/ACHPR-Communications",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("url", ""),
            "communication_number": comm_no,
            "filing_year": filing_year,
            "pdf_urls": raw.get("pdf_urls", []),
            "court": "African Commission on Human and Peoples' Rights",
            "jurisdiction": "African Union",
        }

    # ── fetch ──────────────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        urls = self._list_detail_urls()
        total = len(urls)
        yielded = 0
        for i, url in enumerate(urls):
            logger.info(f"[{i+1}/{total}] {url}")
            meta = self._parse_detail(url)
            if not meta:
                logger.info("  no decision PDF — skipping")
                time.sleep(1)
                continue
            nums = self._communication_numbers(meta["title"], url)
            comm_id = nums[0] if nums else url.rsplit("/", 1)[-1]
            text = self._extract_text(comm_id, meta["pdf_urls"])
            if len(text) < MIN_TEXT_CHARS:
                logger.info(f"  insufficient text ({len(text)} chars) — skipping")
                time.sleep(1)
                continue
            yield {
                **meta,
                "comm_id": comm_id,
                "communication_numbers": nums,
                "text": text,
            }
            yielded += 1
            logger.info(f"  yielded ({len(text)} chars)")
            time.sleep(1)
        logger.info(f"Finished: {yielded}/{total} communications with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No reliable per-item modified date; re-scan listing.

        Idempotency is handled downstream via common/pdf_extract.py's
        Neon preload (records already ingested with text are skipped).
        """
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ACHPR-Communications fetcher")
    sub = parser.add_subparsers(dest="command")

    bp = sub.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=12, help="Number of samples")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = ACHPRCommunicationsScraper()

    if args.command == "test":
        urls = scraper._list_detail_urls(max_pages=3)
        logger.info(f"OK: {len(urls)} detail URLs (first 3 pages)")
        if urls:
            meta = scraper._parse_detail(urls[0])
            logger.info(f"First: {meta}")
            if meta:
                nums = scraper._communication_numbers(meta["title"], urls[0])
                cid = nums[0] if nums else "test"
                txt = scraper._extract_text(cid, meta["pdf_urls"][:1])
                logger.info(f"Extracted {len(txt)} chars; preview: {txt[:200]!r}")
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
