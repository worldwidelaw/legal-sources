#!/usr/bin/env python3
"""
CH/ENSI — Swiss Federal Nuclear Safety Inspectorate document fetcher.

ENSI (Eidgenössisches Nuklearsicherheitsinspektorat) is Switzerland's national
regulator for the nuclear safety and security of Swiss nuclear facilities. Under
the Nuclear Energy Act (KEG) it publishes binding regulatory guidelines
(Richtlinien, ENSI-A/B/G series), requirements (Anforderungen), expert reports
(Gutachten), advisory opinions (Stellungnahmen), safety-review assessments and
annual oversight reports (Aufsichtsberichte).

Enumeration: the public document database at /de/dokumente/ is a WordPress
"document" post-type archive paginated at /de/dokumente/page/N/. Each list item
(`<li class="wp-block-post ... document ...">`) carries, inline, the document
number (`download-item__number`), title, its taxonomy categories, and one or
more direct PDF download links (`download-item__files`) under
`/de/wp-content/uploads/...`. No per-document page visit is required. Full text
is extracted from the PDF with the shared common.pdf_extract backend.

Classification: guideline / requirement documents (Richtlinien / Anforderungen)
are binding regulatory instruments → `legislation`; everything else (expert
reports, opinions, oversight/annual reports) → `doctrine`.

License: Swiss federal official works — Art. 5 URG (not copyright-protected),
reuse permitted with attribution.

Usage:
  python bootstrap.py test                # verify listing + one PDF download
  python bootstrap.py bootstrap --sample  # fetch 15 sample records
  python bootstrap.py bootstrap           # full run
"""

import html as html_lib
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

SOURCE_DIR = Path(__file__).parent
sys.path.insert(0, str(SOURCE_DIR.parent.parent.parent))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CH.ENSI")

BASE_URL = "https://ensi.admin.ch"
LISTING_PATH = "/de/dokumente/"

# Category slugs that mark a binding regulatory instrument (→ legislation).
LEGISLATION_CATEGORIES = {"richtlinien", "anforderungen"}

# Whole-page item block: one document list entry. The full class list (which
# carries the document-category-* taxonomy) is captured in `cls`.
ITEM_RE = re.compile(
    r'<li class="wp-block-post post-(?P<pid>\d+) document (?P<cls>[^"]*)">(?P<body>.*?)</li>',
    re.DOTALL,
)
NUMBER_RE = re.compile(r'download-item__number">\s*([^<]*?)\s*<', re.DOTALL)
TITLE_RE = re.compile(
    r'download-item__title">\s*<a href="([^"]+)">\s*(.*?)\s*</a>', re.DOTALL
)
PDF_RE = re.compile(r'href="([^"]*/wp-content/uploads/[^"]+\.pdf)"', re.IGNORECASE)
CAT_RE = re.compile(r'document-category-([a-z0-9\-]+)')
# Last /YYYY/MM/ segment right before the filename in the upload path.
DATE_IN_URL_RE = re.compile(r'/(\d{4})/(\d{2})/[^/]+$')


def _norm_path(url: str) -> str:
    url = html_lib.unescape(url.split("?")[0])
    if url.startswith("http"):
        url = re.sub(r"^https?://[^/]+", "", url)
    if not url.startswith("/"):
        url = "/" + url
    return url


class ENSIScraper(BaseScraper):
    """Scraper for CH/ENSI."""

    def __init__(self):
        super().__init__(str(SOURCE_DIR))
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            },
        )

    def _parse_items(self, html: str) -> list[dict]:
        """Parse document entries from one listing page."""
        items: list[dict] = []
        for m in ITEM_RE.finditer(html):
            body = m.group("body")
            classes = m.group("cls") or ""
            tm = TITLE_RE.search(body)
            if not tm:
                continue
            doc_page = _norm_path(tm.group(1))
            title = html_lib.unescape(re.sub(r"\s+", " ", tm.group(2))).strip()

            nm = NUMBER_RE.search(body)
            number = html_lib.unescape(nm.group(1)).strip() if nm else ""

            pm = PDF_RE.search(body)
            if not pm:
                # No inline PDF (e.g. external-only or archived) — skip; body has no full text.
                continue
            pdf_path = _norm_path(pm.group(1))

            cats = CAT_RE.findall(classes)
            category = cats[0] if cats else ""

            # Date from the PDF upload path /uploads/.../YYYY/MM/
            dm = DATE_IN_URL_RE.search(pdf_path)
            if dm:
                date = f"{dm.group(1)}-{dm.group(2)}-01"
            else:
                ym = re.search(r"\b(19|20)\d{2}\b", title)
                date = f"{ym.group(0)}-01-01" if ym else None

            items.append(
                {
                    "post_id": m.group("pid"),
                    "doc_number": number,
                    "title": title,
                    "doc_page": doc_page,
                    "pdf_path": pdf_path,
                    "categories": cats,
                    "category": category,
                    "date": date,
                }
            )
        return items

    def _list_page(self, page: int) -> list[dict]:
        path = LISTING_PATH if page == 1 else f"{LISTING_PATH}page/{page}/"
        resp = self.client.get(path)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        return self._parse_items(resp.content.decode("utf-8", errors="replace"))

    def _is_legislation(self, categories: list[str]) -> bool:
        return any(
            any(lc in c for lc in LEGISLATION_CATEGORIES) for c in categories
        )

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1
        empty_streak = 0
        while True:
            try:
                self.rate_limiter.wait()
                items = self._list_page(page)
            except Exception as e:
                logger.warning("  Listing page %d failed: %s", page, e)
                break
            if not items:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                page += 1
                continue
            empty_streak = 0
            for it in items:
                key = it["pdf_path"]
                if key in seen:
                    continue
                seen.add(key)
                yield it
            logger.info("  Listed page %d (%d items, %d total)", page, len(items), len(seen))
            page += 1
            if page > 200:  # hard safety ceiling (~1,800 docs)
                break

    def normalize(self, raw: dict) -> Optional[dict]:
        is_leg = self._is_legislation(raw.get("categories", []))
        table = "legislation" if is_leg else "doctrine"
        doc_id = raw["post_id"]
        try:
            self.rate_limiter.wait()
            resp = self.client.get(raw["pdf_path"])
            resp.raise_for_status()
            pdf_bytes = resp.content
        except Exception as e:
            logger.warning("  PDF download failed for %s: %s", raw.get("doc_number") or doc_id, e)
            return None

        if not pdf_bytes or len(pdf_bytes) < 1000:
            logger.warning("  Tiny/empty PDF for %s", raw.get("doc_number") or doc_id)
            return None

        text = (
            extract_pdf_markdown(
                source="CH/ENSI",
                source_id=f"ensi-{doc_id}",
                pdf_bytes=pdf_bytes,
                table=table,
            )
            or ""
        )
        if not text or len(text) < 500:
            logger.warning(
                "  Skipping %s — no/short text (%d chars)",
                raw.get("doc_number") or doc_id,
                len(text),
            )
            return None

        title = raw["title"]
        if raw.get("doc_number") and raw["doc_number"] not in title:
            title = f"{raw['doc_number']} — {title}"

        return {
            "_id": f"CH/ENSI/ensi-{doc_id}",
            "_source": "CH/ENSI",
            "_type": "legislation" if is_leg else "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": BASE_URL + raw["doc_page"],
            "doc_id": f"ensi-{doc_id}",
            "doc_number": raw.get("doc_number", ""),
            "category": raw.get("category", ""),
            "pdf_url": BASE_URL + raw["pdf_path"],
        }

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for doc in self.fetch_all():
            yield doc


if __name__ == "__main__":
    scraper = ENSIScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        items = scraper._parse_items(
            scraper.client.get(LISTING_PATH).content.decode("utf-8", errors="replace")
        )
        if not items:
            print("FAILED - no items parsed from listing")
            sys.exit(1)
        print(f"Parsed {len(items)} document items from page 1.")
        it = items[0]
        print(f"  First: [{it['doc_number']}] {it['title'][:70]}")
        print(f"  Category: {it['category']} | date: {it['date']}")
        print(f"  PDF: {it['pdf_path']}")
        rec = scraper.normalize(it)
        if rec and rec.get("text"):
            print(f"  Extracted text: {len(rec['text'])} chars, type={rec['_type']}")
        else:
            print("  FAILED - no text extracted")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
