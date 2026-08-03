#!/usr/bin/env python3
"""
US/MD-COMAR -- Code of Maryland Regulations (COMAR)

Fetches the full text of the Code of Maryland Regulations (COMAR), the
official compilation of the administrative regulations adopted by the
agencies of the State of Maryland. COMAR is published by the Office of the
Secretary of State's Division of State Documents and mirrored, as a
free public "Library of Maryland Regulations", at regs.maryland.gov. Each
regulation is an administrative rule with the force of law = legislation.

Access (no JavaScript, no CAPTCHA, no auth):
  regs.maryland.gov exposes the whole code as a JSON tree plus per-subtitle
  born-digital "full.html" documents:

      https://regs.maryland.gov/us/md/exec/comar/index.json      (the tree)
      https://regs.maryland.gov/us/md/exec/comar/{TT.SS}/index.full.html

  The tree is a nested container of Title -> Subtitle -> Chapter -> Section
  nodes.  Every SUBTITLE node carries an "fh" field pointing at its
  index.full.html, which contains the complete, real-text (no OCR) body of
  every chapter and regulation in that subtitle together with its
  Administrative History (adoption / amendment effective dates).  There are
  ~1,850 such subtitle documents across all COMAR titles.

Strategy:
  GET the master index.json, walk it to collect every subtitle node that has
  an "fh" (recording its short code, human page path and parent Title name),
  fetch each index.full.html, extract the <article class="content"> body as
  clean text, parse the earliest "Effective date: Month DD, YYYY" from the
  Administrative History as the record date, and normalize.  The subtitle is
  the natural document unit (the site's own canonical full-text file).

Usage:
  python bootstrap.py bootstrap            # Full pull (all COMAR subtitles)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MD-COMAR")

BASE_URL = "https://regs.maryland.gov"
INDEX_URL = "https://regs.maryland.gov/us/md/exec/comar/index.json"

ARTICLE_RE = re.compile(r"<article[^>]*>(?P<body>.*?)</article>", re.S | re.I)
EFF_DATE_RE = re.compile(
    r"[Ee]ffective(?:\s+date)?:?\s*"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})"
)
_MONTH_IDX = {
    m: i + 1
    for i, m in enumerate(
        "January February March April May June July August September "
        "October November December".split()
    )
}


def _clean(fragment: str) -> str:
    """Strip HTML tags/scripts/entities from a fragment -> readable text."""
    frag = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", fragment or "",
                  flags=re.S | re.I)
    frag = re.sub(r"<[^>]+>", " ", frag)
    frag = _html.unescape(frag)
    frag = re.sub(r"[ \t]+", " ", frag)
    frag = re.sub(r"\s*\n\s*", "\n", frag)
    frag = re.sub(r"\n{3,}", "\n\n", frag)
    return frag.strip()


def _first_effective_date(text: str) -> str | None:
    """Earliest 'Effective date: Month DD, YYYY' in the body -> ISO date."""
    best = None
    for m in EFF_DATE_RE.finditer(text or ""):
        mo = _MONTH_IDX.get(m.group(1))
        d, y = int(m.group(2)), int(m.group(3))
        if mo and 1 <= d <= 31:
            iso = f"{y:04d}-{mo:02d}-{d:02d}"
            if best is None or iso < best:
                best = iso
    return best


class MDCOMARScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
        )

    # ---------------------------------------------------------------- http
    def _curl(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_all(self) -> list[dict]:
        """Return [{sc, path, title_name, subtitle_name, fh_url, page_url}]."""
        raw = self._curl(INDEX_URL)
        if not raw:
            logger.error("could not fetch the COMAR index.json")
            return []
        try:
            tree = json.loads(raw)
        except Exception as e:
            logger.error(f"index.json parse failed: {e}")
            return []

        items: list[dict] = []

        def walk(node: dict, title_name: str):
            t = (node.get("t") or "").strip()
            # A Title node names the current title context for its descendants.
            if t.upper().startswith("TITLE "):
                title_name = t
            if "fh" in node:
                items.append({
                    "sc": node.get("sc") or node.get("p", "").rsplit("/", 1)[-1],
                    "path": node.get("p"),
                    "title_name": title_name,
                    "subtitle_name": t,
                    "fh_url": BASE_URL + node["fh"],
                    "page_url": BASE_URL + node.get("p", ""),
                })
            for c in node.get("c", []):
                walk(c, title_name)

        walk(tree, "")

        # Interleave subtitles across titles (round-robin) so any prefix of the
        # list spans many COMAR titles rather than exhausting Title 01 first.
        # Document/ingest order is otherwise irrelevant.
        by_title: dict[str, list[dict]] = {}
        for it in items:
            by_title.setdefault(it["title_name"], []).append(it)
        buckets = list(by_title.values())
        interleaved: list[dict] = []
        i = 0
        while any(i < len(b) for b in buckets):
            for b in buckets:
                if i < len(b):
                    interleaved.append(b[i])
            i += 1
        return interleaved

    def _fetch_text(self, fh_url: str) -> str | None:
        html = self._curl(fh_url)
        if not html:
            return None
        m = ARTICLE_RE.search(html)
        body = m.group("body") if m else html
        return _clean(body)

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing COMAR index + full.html extraction...")
        items = self._list_all()
        if not items:
            logger.error("API test FAILED: no subtitle documents found")
            return False
        logger.info(f"  discovered {len(items)} COMAR subtitle documents")
        ok = 0
        for it in items[:5]:
            text = self._fetch_text(it["fh_url"])
            if text and len(text) > 400:
                logger.info(f"  COMAR {it['sc']} OK ({len(text)} chars) "
                            f"date={_first_effective_date(text)}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        sc = raw.get("sc")
        subtitle_name = raw.get("subtitle_name") or ""
        title_name = raw.get("title_name") or ""
        parts = [p for p in (title_name, subtitle_name) if p]
        caption = " — ".join(parts) if parts else subtitle_name
        title = f"COMAR {sc}: {caption}" if caption else f"COMAR {sc}"
        return {
            "_id": f"US/MD-COMAR/{sc}",
            "_source": "US/MD-COMAR",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "comar_code": sc,
            "issuer": "State of Maryland — Division of State Documents",
            "title": title,
            "title_name": title_name,
            "subtitle_name": subtitle_name,
            "text": raw["text"],
            "url": raw["page_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-MD",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        items = self._list_all()
        emitted = 0
        for it in items:
            text = self._fetch_text(it["fh_url"])
            if not text or len(text) < 400:
                logger.warning(f"  COMAR {it.get('sc')}: insufficient text "
                               f"({len(text) if text else 0} chars), skipping")
                continue
            date = _first_effective_date(text)
            yield {**it, "text": text, "date": date}
            emitted += 1
            if sample and emitted >= 12:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/MD-COMAR bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MDCOMARScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
