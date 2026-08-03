#!/usr/bin/env python3
"""
US/CA-PERB-Digest -- California PERB Decisional-Law Digest (Headnotes)

Fetches the full text of every headnote in the official decisional-law
digest of the California Public Employment Relations Board (PERB). PERB
maintains an official annotated digest of its own case law: each Board
Decision is broken into one or more headnotes, and every headnote states a
discrete legal principle established by that decision, classified under a
hierarchical topic code (e.g. 1000.02163 -- Work Rules, under 1000.00000 --
SCOPE OF REPRESENTATION). This official state-authored digest of public-
sector labor-law principles = doctrine. Public domain California state-
government works (government edicts).

Access (no CAPTCHA, no auth):
  perb.ca.gov is a WordPress site. Headnotes are the custom post type
  `decision-headnote`, enumerable via the public WP REST API:

      /wp-json/wp/v2/decision-headnote?per_page=100&page={N}   (~15,019 posts)

  The headnote's substantive text is server-rendered on its page (the ACF
  fields are empty over REST), so each headnote page is fetched and the
  topic-classification <strong> lines + the holding <p>(s) are extracted
  from the <article> region.

Strategy:
  1. Enumerate every `decision-headnote` post (title encodes the decision
     number + topic code + topic name; the post link is the page URL).
  2. Fetch each headnote page and extract: the topic classification (the
     1000.00000-style code hierarchy), the parent-decision caption, and the
     holding text (the digest principle statement).
  3. Normalize into the doctrine schema (text = topic classification +
     holding statement).

Usage:
  python bootstrap.py bootstrap            # Full pull (~15,019 headnotes)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
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
logger = logging.getLogger("legal-data-hunter.US.CA-PERB-Digest")

BASE_URL = "https://perb.ca.gov"
POST_TEMPLATE = (
    BASE_URL + "/wp-json/wp/v2/decision-headnote"
    "?per_page=100&page={page}&_fields=id,slug,date,link,title"
)

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
ARTICLE_RE = re.compile(r"<article\b.*?</article>", re.S | re.I)
STRONG_RE = re.compile(r"<strong>(.*?)</strong>", re.S | re.I)
P_RE = re.compile(r"<p>(.*?)</p>", re.S | re.I)
TOPIC_CODE_RE = re.compile(r"\d{4}\.\d+")
# "Headnote for 3017E, 1000.02163 – Work Rules"
TITLE_RE = re.compile(
    r"Headnote for\s+([0-9]+[A-Za-z]?)\s*,\s*([\d.]+)\s*[–—-]\s*(.+)",
    re.I,
)
CAPTION_RE = re.compile(r"Decision\s+[0-9]+[A-Za-z]?\s*[–—-]\s*[^<\n]{3,120}")


class CAPERBDigestScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.7
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
    def _curl_text(self, url: str) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_json(self, url: str):
        b = self._curl_text(url)
        if not b:
            return None
        try:
            return json.loads(b)
        except Exception:
            return None

    # ------------------------------------------------------------- helpers
    @classmethod
    def _clean(cls, s: str) -> str:
        s = TAG_RE.sub(" ", s or "")
        s = _html.unescape(s)
        return WS_RE.sub(" ", s).strip()

    @staticmethod
    def _doc_id(slug: str, pid) -> str:
        stem = re.sub(r"[^A-Za-z0-9_-]", "-", slug or f"post-{pid}")
        return stem.strip("-") or f"post-{pid}"

    def _parse_page(self, htmltext: str) -> dict:
        """Extract topic classification, decision caption, and holding text."""
        m = ARTICLE_RE.search(htmltext)
        art = m.group(0) if m else htmltext
        # topic classification: <strong> lines carrying a NNNN.NNN code
        topics = []
        for sm in STRONG_RE.finditer(art):
            t = self._clean(sm.group(1))
            if TOPIC_CODE_RE.search(t):
                topics.append(t)
        # holding paragraph(s): <p> inside the article, excluding the two
        # navigation <p>s ("View all topics ...", "Full Decision Text ...")
        # which always carry an <a> link; the holding paragraph has none.
        holdings = []
        for pm in P_RE.finditer(art):
            inner = pm.group(1)
            if "<a " in inner.lower() or "<a>" in inner.lower():
                continue
            t = self._clean(inner)
            low = t.lower()
            if (not t or len(t) <= 8
                    or low.startswith("view all topics")
                    or low.startswith("full decision text")
                    or low.startswith("full text")
                    or "click on the link" in low):
                continue
            holdings.append(t)
        # parent decision caption
        cap_m = CAPTION_RE.search(self._clean(art))
        caption = self._clean(cap_m.group(0)) if cap_m else None
        return {
            "topics": topics,
            "holding": " ".join(holdings).strip(),
            "caption": caption,
        }

    # --------------------------------------------------------- discovery
    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        total = 0
        page = 1
        while True:
            data = self._curl_json(POST_TEMPLATE.format(page=page))
            if not isinstance(data, list) or not data:
                break
            for post in data:
                total += 1
                yield {
                    "post_id": post.get("id"),
                    "slug": post.get("slug"),
                    "title": self._clean((post.get("title") or {}).get("rendered", "")),
                    "link": post.get("link"),
                    "post_date": (post.get("date") or "")[:10] or None,
                }
                if sample and total >= 16:
                    return
            logger.info(f"  post page {page}: total {total} headnotes")
            if len(data) < 100:  # last page
                break
            page += 1
            if page > 200:  # safety (~20,000 posts)
                logger.warning("Reached post page safety cap (200)")
                break
        logger.info(f"Discovered {total} CA PERB digest headnotes")

    # ------------------------------------------------------- build record
    def _build_raw(self, doc: dict) -> dict | None:
        htmltext = self._curl_text(doc["link"])
        if not htmltext:
            logger.warning(f"Fetch failed: {doc['link']}")
            return None
        parsed = self._parse_page(htmltext)
        if not parsed["holding"] or len(parsed["holding"]) < 20:
            logger.warning(f"No usable holding text for {doc['link']}")
            return None
        # decision number + topic code + topic name from the WP title
        tm = TITLE_RE.search(doc["title"] or "")
        decision_number = tm.group(1).upper() if tm else None
        topic_code = tm.group(2) if tm else None
        topic_name = self._clean(tm.group(3)) if tm else None
        classification = "\n".join(parsed["topics"]) if parsed["topics"] else (
            f"{topic_code} – {topic_name}" if topic_code else None)
        text_parts = []
        if classification:
            text_parts.append(classification)
        text_parts.append(parsed["holding"])
        doc = dict(doc)
        doc["doc_id"] = self._doc_id(doc["slug"], doc["post_id"])
        doc["decision_number"] = decision_number
        doc["topic_code"] = topic_code
        doc["topic_name"] = topic_name
        doc["classification"] = classification
        doc["caption"] = parsed["caption"]
        doc["holding"] = parsed["holding"]
        doc["text"] = "\n\n".join(text_parts).strip()
        doc["date"] = doc.get("post_date")
        return doc

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CA PERB digest enumeration + headnote extraction...")
        try:
            docs = list(self.discover_documents(sample=True))
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} headnotes (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 20:
                logger.info(f"  Headnote extraction OK ({len(raw['text'])} chars) — "
                            f"Decision {raw.get('decision_number')} "
                            f"[{raw.get('topic_code')}] {raw.get('topic_name')}")
                logger.info(f"    holding: {raw['holding'][:120]}")
            else:
                logger.error("  Headnote extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        dn = raw.get("decision_number")
        tn = raw.get("topic_name")
        title = raw.get("title") or (
            f"PERB Headnote {dn}: {tn}" if dn else "PERB Decisional-Law Digest Headnote")
        return {
            "_id": f"US/CA-PERB-Digest/{raw['doc_id']}",
            "_source": "US/CA-PERB-Digest",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "decision_number": dn,
            "topic_code": raw.get("topic_code"),
            "topic_name": tn,
            "classification": raw.get("classification"),
            "decision_caption": raw.get("caption"),
            "issuer": "California Public Employment Relations Board (PERB)",
            "title": title,
            "text": raw["text"],
            "url": raw.get("link"),
            "date": raw.get("date") or None,
            "jurisdiction": "US-CA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
            if raw:
                yield raw
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

    parser = argparse.ArgumentParser(description="US/CA-PERB-Digest bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CAPERBDigestScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
