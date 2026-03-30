#!/usr/bin/env python3
"""
Laws of Bangladesh Data Fetcher

Fetches 1,571 Bangladesh Acts from bdlaws.minlaw.gov.bd (Ministry of Law).
Each act page lists sections; each section page has the actual text.
Full text is assembled by fetching all sections per act.

Site uses UTF-16 encoding.
"""

import html as html_mod
import json
import logging
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "http://bdlaws.minlaw.gov.bd"
DELAY = 1.0
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0"}


def http_get(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL and decode from UTF-16."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        # Try UTF-16 first (site default), fall back to UTF-8
        for enc in ("utf-16", "utf-8", "latin-1"):
            try:
                return raw.decode(enc)
            except (UnicodeDecodeError, UnicodeError):
                continue
        return raw.decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"HTTP GET failed for {url[:100]}: {e}")
        return None


def strip_html(text: str) -> str:
    """Remove HTML tags and clean whitespace."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(?:p|div|tr|li|h[1-6])>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n", "\n\n", text)
    return text.strip()


def extract_content(html_text: str) -> str:
    """Extract main content from a page, skipping navigation."""
    # Find the Print View marker or section content
    markers = ["Print View", "print-view", "Section Index"]
    start = 0
    for marker in markers:
        idx = html_text.find(marker)
        if idx != -1:
            start = idx
            break

    # Find end markers
    end_markers = ["Contact Us", "footer", "Copyright", "বাংলা"]
    end = len(html_text)
    for marker in end_markers:
        idx = html_text.find(marker, start + 100)
        if idx != -1 and idx < end:
            end = idx

    chunk = html_text[start:end]
    return strip_html(chunk)


class BdLawsFetcher:
    """Fetcher for Bangladesh legislation."""

    def __init__(self):
        self.delay = DELAY

    def get_act_list(self) -> List[Dict[str, str]]:
        """Get list of all acts from the chronological index."""
        url = f"{BASE_URL}/laws-of-bangladesh-chronological-index.html"
        data = http_get(url)
        if not data:
            return []

        acts = []
        seen = set()
        for m in re.finditer(r'href="/(act-(\d+)\.html)"', data):
            act_path = m.group(1)
            act_id = m.group(2)
            if act_id not in seen:
                seen.add(act_id)
                acts.append({"act_id": act_id, "path": act_path})

        logger.info(f"Found {len(acts)} unique acts in index")
        return acts

    def get_act_info(self, act_id: str) -> Tuple[str, str, List[str]]:
        """Get act title, date hint, and section URLs."""
        url = f"{BASE_URL}/act-{act_id}.html"
        data = http_get(url)
        if not data:
            return "", "", []

        # Title
        title_m = re.search(r"<title>(.*?)</title>", data, re.DOTALL)
        title = strip_html(title_m.group(1)).strip() if title_m else f"Act {act_id}"

        # Date from title (e.g., "The Districts Act, 1836")
        date = ""
        date_m = re.search(r",?\s*(1[789]\d{2}|20[012]\d)", title)
        if date_m:
            date = f"{date_m.group(1)}-01-01"

        # Section URLs
        sections = []
        for m in re.finditer(r'href="/(act-\d+/section-\d+\.html)"', data):
            sections.append(m.group(1))

        return title, date, sections

    def fetch_section(self, section_path: str) -> str:
        """Fetch text of a single section."""
        url = f"{BASE_URL}/{section_path}"
        data = http_get(url)
        if not data:
            return ""
        return extract_content(data)

    def fetch_act(self, act_id: str) -> Optional[Dict[str, Any]]:
        """Fetch complete act with all sections."""
        title, date, section_paths = self.get_act_info(act_id)
        if not section_paths:
            logger.warning(f"No sections found for act-{act_id}")
            return None

        time.sleep(self.delay)

        # Fetch all sections
        text_parts = []
        for i, sp in enumerate(section_paths):
            section_text = self.fetch_section(sp)
            if section_text:
                text_parts.append(section_text)
            if i < len(section_paths) - 1:
                time.sleep(self.delay)

        if not text_parts:
            return None

        full_text = "\n\n".join(text_parts)
        if len(full_text) < 50:
            return None

        return {
            "_id": f"BD-BDLAWS-{act_id}",
            "_source": "BD/BdLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date or None,
            "url": f"{BASE_URL}/act-{act_id}.html",
            "act_id": act_id,
            "section_count": len(section_paths),
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        acts = self.get_act_list()
        for i, act in enumerate(acts):
            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(acts)}")
            doc = self.fetch_act(act["act_id"])
            if doc:
                yield doc

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        # No date filtering available, yield all
        yield from self.fetch_all()


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample acts (preferring small ones for speed)."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    fetcher = BdLawsFetcher()

    acts = fetcher.get_act_list()
    logger.info(f"Total acts: {len(acts)}")

    # First, probe a few acts to find small ones (few sections)
    candidates = []
    for act in acts[:80]:
        url = f"{BASE_URL}/act-{act['act_id']}.html"
        data = http_get(url)
        if not data:
            continue
        sections = re.findall(r'href="/(act-\d+/section-\d+\.html)"', data)
        title_m = re.search(r"<title>(.*?)</title>", data, re.DOTALL)
        title = strip_html(title_m.group(1)).strip() if title_m else ""
        candidates.append({
            "act_id": act["act_id"],
            "section_count": len(sections),
            "title": title[:80],
        })
        time.sleep(0.5)

    # Sort by section count, pick smallest
    candidates.sort(key=lambda x: x["section_count"])
    logger.info(f"Probed {len(candidates)} acts, section counts: {[c['section_count'] for c in candidates[:20]]}")

    saved = 0
    for cand in candidates:
        if saved >= count:
            break
        if cand["section_count"] == 0:
            continue

        act_id = cand["act_id"]
        logger.info(f"Fetching act-{act_id} ({cand['section_count']} sections): {cand['title']}")

        doc = fetcher.fetch_act(act_id)
        if not doc:
            continue

        text_len = len(doc.get("text", ""))
        logger.info(f"  Text: {text_len} chars, Sections: {doc.get('section_count')}")

        out_file = sample_dir / f"{doc['_id']}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        saved += 1
        logger.info(f"  Saved ({saved}/{count})")

    logger.info(f"Bootstrap complete: {saved} documents saved to {sample_dir}")
    return saved


if __name__ == "__main__":
    source_dir = Path(__file__).parent
    sample_dir = source_dir / "sample"

    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        sample_flag = "--sample" in sys.argv
        count = 15 if sample_flag else 50
        saved = bootstrap_sample(sample_dir, count)
        if saved < 10:
            logger.error(f"Only {saved} documents saved, expected at least 10")
            sys.exit(1)
    else:
        print("Usage: python3 bootstrap.py bootstrap [--sample]")
