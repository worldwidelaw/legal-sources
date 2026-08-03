#!/usr/bin/env python3
"""
Japan Nuclear Regulation Authority (NRA / 原子力規制委員会) — Related Laws & Ordinances.

The NRA is Japan's post-Fukushima independent nuclear/radiation regulator. Under
the Radioisotope Regulation Law and the Reactor Regulation Law it issues binding
ordinances (告示), enforcement regulations (施行規則/施行令), and regulatory guides
(審査ガイド/立入検査ガイド) that carry the force of law — i.e. legislation.

Enumeration: the "Related Laws & Ordinances" (関連法令) index page is static,
server-rendered HTML whose full document list ships inline. Each list item is an
anchor to a flat, stable PDF endpoint:

    <a href="/data/000261992.pdf">特定放射性同位元素の数量を定める告示【PDF：35KB】</a>

Full text: the linked PDFs are born-digital and extracted (Japanese text layer)
with the shared common.pdf_extract backend. The promulgation date printed on the
first lines of each ordinance (Japanese era format, e.g. 平成三十年十一月二十六日)
is parsed to ISO 8601 where possible.

License: NRA content is published under the Government of Japan Standard Terms of
Use v2.0, which the NRA states is compatible with CC BY 4.0 (reuse, adaptation
and commercial use permitted with attribution).

Usage:
  python bootstrap.py test                # verify listing + one PDF download
  python bootstrap.py bootstrap --sample  # fetch sample records
  python bootstrap.py bootstrap           # full run
  python bootstrap.py bootstrap-fast      # alias for full run (VPS wrapper)
"""

import hashlib
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
logger = logging.getLogger("legal-data-hunter.JP.NRA")

BASE_URL = "https://www.nra.go.jp"
# "関連法令" — Related Laws & Ordinances (radiation-safety regulatory instruments).
LISTING_PATH = "/activity/ri_kisei/kanrenhourei/index.html"

# Skip header/footer logos and other non-document assets (ids beginning 9000…).
_PDF_RE = re.compile(r'href="(/data/(\d{6,}))\.pdf"[^>]*>([^<]{0,120})', re.I)

# --- Japanese era / kanji-numeral date parsing -------------------------------

_ERA_BASE = {"令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867}
_KANJI_DIGIT = {
    "〇": 0, "零": 0, "一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9,
}


def _kanji_to_int(s: str) -> Optional[int]:
    """Parse a kanji numeral 1..99 (handles 十/廿 tens). Returns None on failure."""
    if not s:
        return None
    if s in ("元",):  # 元年 = year 1
        return 1
    # Pure arabic (some notices mix scripts)
    if s.isdigit():
        return int(s)
    total = 0
    if "十" in s:
        tens_part, _, ones_part = s.partition("十")
        tens = _KANJI_DIGIT.get(tens_part, 1) if tens_part else 1
        ones = _KANJI_DIGIT.get(ones_part, 0) if ones_part else 0
        total = tens * 10 + ones
    else:
        for ch in s:
            if ch not in _KANJI_DIGIT:
                return None
            total = total * 10 + _KANJI_DIGIT[ch]
    return total if 0 < total < 100 else None


def _parse_jp_date(text: str) -> Optional[str]:
    """Best-effort: extract the promulgation date near the top of an ordinance."""
    head = text[:400]
    m = re.search(
        r"(令和|平成|昭和|大正|明治)\s*([元〇零一二三四五六七八九十\d]+)\s*年"
        r"\s*([〇零一二三四五六七八九十\d]+)\s*月"
        r"\s*([〇零一二三四五六七八九十\d]+)\s*日",
        head,
    )
    if not m:
        return None
    era, y, mo, d = m.group(1), m.group(2), m.group(3), m.group(4)
    yr = _kanji_to_int(y)
    mon = _kanji_to_int(mo)
    day = _kanji_to_int(d)
    if not yr or not mon or not day:
        return None
    year = _ERA_BASE[era] + yr
    if not (1 <= mon <= 12 and 1 <= day <= 31):
        return None
    try:
        return datetime(year, mon, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _clean_title(anchor_text: str) -> str:
    # Drop the trailing 【PDF：…】 / 【EXCEL：…】 size annotation and whitespace.
    t = re.sub(r"【[^】]*】", "", anchor_text)
    t = t.replace("&amp;", "&").replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", t).strip()


def _doc_id(pdf_id: str) -> str:
    return pdf_id


class NRAScraper(BaseScraper):
    """Scraper for JP/NRA related laws & ordinances (legislation)."""

    def __init__(self):
        super().__init__(str(SOURCE_DIR))
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
                ),
            },
        )

    def _load_listing(self) -> list[dict]:
        """Parse the inline PDF list from the 関連法令 index page."""
        resp = self.client.get(LISTING_PATH)
        resp.raise_for_status()
        html = resp.content.decode("utf-8", errors="replace")

        docs: list[dict] = []
        seen: set[str] = set()
        for m in _PDF_RE.finditer(html):
            path, pdf_id, anchor = m.group(1), m.group(2), m.group(3)
            if pdf_id.startswith("9000"):  # header/footer logo assets
                continue
            title = _clean_title(anchor)
            if not title:
                continue
            if pdf_id in seen:
                continue
            seen.add(pdf_id)
            docs.append(
                {
                    "doc_id": _doc_id(pdf_id),
                    "title": title,
                    "pdf_url": BASE_URL + path + ".pdf",
                }
            )
        logger.info("Parsed %d ordinance/guide entries from listing", len(docs))
        return docs

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"JP/NRA/{raw['doc_id']}",
            "_source": "JP/NRA",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_prefetched_text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "doc_id": raw["doc_id"],
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        all_docs = self._load_listing()
        limit = 15 if sample else None
        count = 0

        for doc in all_docs:
            if limit and count >= limit:
                break
            try:
                self.rate_limiter.wait()
                resp = self.client.get(doc["pdf_url"])
                resp.raise_for_status()
                pdf_bytes = resp.content
            except Exception as e:
                logger.warning("  Failed to download %s: %s", doc["title"][:50], e)
                continue

            if not pdf_bytes or len(pdf_bytes) < 100:
                logger.warning("  Tiny/empty PDF for %s", doc["title"][:50])
                continue

            text = (
                extract_pdf_markdown(
                    source="JP/NRA",
                    source_id=doc["doc_id"],
                    pdf_bytes=pdf_bytes,
                    table="legislation",
                )
                or ""
            )
            if not text or len(text) < 200:
                logger.warning(
                    "  Skipping %s — no/short text (%d chars)", doc["title"][:50], len(text)
                )
                continue

            doc["_prefetched_text"] = text
            doc["date"] = _parse_jp_date(text)
            yield doc
            count += 1
            logger.info("  [%d] %s (%d chars)", count, doc["title"][:50], len(text))

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for doc in self.fetch_all():
            yield doc


if __name__ == "__main__":
    scraper = NRAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        docs = scraper._load_listing()
        if not docs:
            print("FAILED - no documents found")
            sys.exit(1)
        print(f"Loaded {len(docs)} ordinance/guide entries.")
        test_doc = docs[0]
        print(f"  Testing download: {test_doc['title'][:60]}...")
        resp = scraper.client.get(test_doc["pdf_url"])
        resp.raise_for_status()
        print(f"  Download OK: {len(resp.content)} bytes")
    elif command in ("bootstrap", "bootstrap-fast"):
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
