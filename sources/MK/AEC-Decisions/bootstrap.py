#!/usr/bin/env python3
"""
MK/AEC-Decisions — North Macedonia Agency for Electronic Communications (AEK)

The AEK (Агенција за електронски комуникации, https://aek.mk) is the national
telecom/electronic-communications regulator. It publishes its regulatory
output — decisions (одлуки/решенија), rulebooks and regulations (правилници),
sector legislation, and plans — on a WordPress site, each post linking to the
official PDF (most are reprints from the Official Gazette, "Службен весник").

Strategy:
  1. List posts per relevant category via the WordPress REST API
     (/wp-json/wp/v2/posts?categories=ID), which yields structured JSON
     with title, date and the post body.
  2. Extract the PDF link from each post body, download it, and pull the
     full text with pdfplumber.
  3. Keep records whose extracted text is clean and substantial; some older
     items are image-only scans (1-2 chars extracted) and are dropped by a
     quality filter.

Content is in Macedonian (Cyrillic). Free access, no authentication.

Usage:
  python bootstrap.py bootstrap --sample   # sample records for validation
  python bootstrap.py bootstrap            # full pull
  python bootstrap.py update               # incremental (re-crawl newest first)
  python bootstrap.py test-api             # connectivity / post-count check
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MK.AEC-Decisions")

BASE_URL = "https://aek.mk"
SOURCE_ID = "MK/AEC-Decisions"

# WordPress category id -> (slug, human label). Born-digital regulation pages
# (rulebooks, sector legislation) are listed first so a --sample run reaches
# its quota with the cleanest content before touching the older scan-heavy
# decision archives.
CATEGORIES = [
    (128, "Legislation"),                 # legislativa — laws & rulebooks (Правилници)
    (167, "Competition Legislation"),     # konkurencija-legislativa
    (288, "Plans"),                       # planovi
    (112, "Decisions"),                   # odluki-i-reshenija — одлуки/решенија
]

PER_PAGE = 50
MAX_PDF_BYTES = 25_000_000
MAX_PDF_PAGES = 60
MIN_TEXT_CHARS = 450
MIN_CYRILLIC = 200


def _clean_text(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _is_clean(text: str) -> bool:
    """Reject image-only scans. Clean Cyrillic prose has many long word
    tokens; bad scans extract to a handful of stray characters."""
    if len(text) < MIN_TEXT_CHARS:
        return False
    cyr = sum(1 for c in text if "Ѐ" <= c <= "ӿ")
    if cyr < MIN_CYRILLIC:
        return False
    tokens = [t for t in text.split() if any(ch.isalpha() for ch in t)]
    if not tokens:
        return False
    long_tokens = [t for t in tokens if len(t) >= 5]
    return (len(long_tokens) / len(tokens)) >= 0.28


def _strip_html(html: str) -> str:
    txt = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", unescape(txt)).strip()


class AECDecisionsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/json,application/pdf,*/*",
                "Accept-Language": "mk,sq;q=0.8,en;q=0.6",
            },
            timeout=90,
            respect_robots=False,
        )
        self._seen_pdfs: set[str] = set()

    def _list_posts(self, category_id: int) -> Generator[dict, None, None]:
        """Yield WP posts for a category, oldest-relevant pages handled by the
        REST API's default newest-first ordering."""
        page = 1
        while True:
            url = (f"{BASE_URL}/wp-json/wp/v2/posts"
                   f"?categories={category_id}&per_page={PER_PAGE}&page={page}")
            try:
                resp = self.http.get(url, timeout=60)
            except Exception as e:
                logger.warning("Post list error cat=%s page=%d: %s", category_id, page, e)
                return
            if resp.status_code != 200:
                if resp.status_code != 400:  # 400 = page past the end
                    logger.warning("cat=%s page=%d -> HTTP %d", category_id, page, resp.status_code)
                return
            try:
                posts = resp.json()
            except Exception:
                return
            if not posts:
                return
            for p in posts:
                yield p
            total_pages = int(resp.headers.get("X-WP-TotalPages", page))
            if page >= total_pages:
                return
            page += 1
            time.sleep(1)

    def _first_pdf(self, post: dict) -> Optional[str]:
        body = post.get("content", {}).get("rendered", "") or ""
        for href in re.findall(r'href="([^"]+\.pdf)"', body, re.IGNORECASE):
            full = urljoin(BASE_URL + "/", unescape(href))
            if full not in self._seen_pdfs:
                return full
        return None

    def _extract_pdf(self, url: str) -> Optional[str]:
        try:
            resp = self.http.get(url, timeout=90)
        except Exception as e:
            logger.warning("PDF fetch error: %s (%s)", url[:90], e)
            return None
        if resp.status_code != 200:
            return None
        data = resp.content
        if not data or not data[:5].startswith(b"%PDF"):
            return None
        if len(data) > MAX_PDF_BYTES:
            logger.info("Skip oversized PDF (%d bytes): %s", len(data), url[:90])
            return None
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                pages = pdf.pages[:MAX_PDF_PAGES]
                parts = [(p.extract_text() or "") for p in pages]
        except Exception as e:
            logger.warning("PDF parse error: %s (%s)", url[:90], e)
            return None
        return _clean_text("\n".join(parts))

    def _build(self, post: dict, category: str) -> Optional[dict]:
        pdf_url = self._first_pdf(post)
        if not pdf_url:
            return None
        self._seen_pdfs.add(pdf_url)
        text = self._extract_pdf(pdf_url)
        if not text or not _is_clean(text):
            return None

        title = _strip_html(post.get("title", {}).get("rendered", "")) or \
            unquote(pdf_url.rsplit("/", 1)[-1]).rsplit(".", 1)[0]
        date = (post.get("date") or "")[:10] or None
        doc_id = "mk-aec-" + str(post.get("id", "")) + "-" + re.sub(
            r"[^a-z0-9]+", "-",
            unquote(pdf_url.rsplit("/", 1)[-1]).lower()).strip("-")[:60]
        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": post.get("link") or pdf_url,
            "pdf_url": pdf_url,
            "category": category,
            "language": "mk",
        }

    # ── BaseScraper interface ───────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        self._seen_pdfs = set()
        for cat_id, category in CATEGORIES:
            count = 0
            for post in self._list_posts(cat_id):
                rec = self._build(post, category)
                if rec:
                    count += 1
                    yield rec
                time.sleep(1.2)
            logger.info("%s: %d clean records", category, count)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ─────────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="MK/AEC-Decisions scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = AECDecisionsScraper()

    if args.command == "test-api":
        for cat_id, cat in CATEGORIES:
            n = sum(1 for _ in scraper._list_posts(cat_id))
            logger.info("%s (cat %d): %d posts", cat, cat_id, n)
            time.sleep(1)
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    limit = 15 if args.sample else None
    count = 0
    for record in scraper.fetch_all():
        count += 1
        if args.sample or count <= 15:
            with open(sample_dir / f"{count:04d}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info("[%d] %s — %d chars (%s)", count,
                    record["title"][:55], len(record["text"]), record.get("date"))
        if limit and count >= limit:
            break
    logger.info("Done: %d records", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
