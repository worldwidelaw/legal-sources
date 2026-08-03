#!/usr/bin/env python3
"""
US/WY-AGOpinions -- Wyoming Attorney General Formal Opinions

Fetches the FULL TEXT of the Formal Opinions of the Wyoming Attorney General.
These are authoritative interpretations of Wyoming law issued by the state's
chief legal officer — a doctrine corpus.

Source / access
---------------
The Wyoming Attorney General publishes its Formal Opinions on a single,
server-rendered index page at https://attorneygeneral.wyo.gov/formal-opinions
(reachable, no WAF/CAPTCHA). Each opinion links to a Google Drive file; the
file is downloaded directly via the Drive ``uc?export=download`` endpoint
(passing the ``resourcekey`` for the older ``0B...``-format IDs). All opinion
PDFs carry a real text layer (born-digital, no OCR needed).

Coverage: ~19 documents 1998-2018. The 1998 and 1999 entries are compiled
multi-opinion volumes (one PDF holding the year's numbered opinions); the rest
are one numbered formal opinion per entry. Text is extracted via the shared,
OOM-hardened ``common.pdf_extract.extract_pdf_markdown`` helper.

Usage:
  python3 bootstrap.py bootstrap            # Full pull (all PDFs)
  python3 bootstrap.py bootstrap --sample   # Fetch sample documents
  python3 bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python3 bootstrap.py test-api             # Connectivity / extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WY-AGOpinions")

SOURCE_ID = "US/WY-AGOpinions"
INDEX_URL = "https://attorneygeneral.wyo.gov/formal-opinions"

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)


def clean_text(text: str) -> str:
    """Normalize whitespace; strip pdfplumber (cid:N) artefacts."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\(cid:\d+\)", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_opinion_label(label: str) -> tuple[str, Optional[str]]:
    """Return (slug, leading-year) for an opinion-number link label.

    e.g. "2018-002" -> ("2018-002", "2018"); "1999-001 thru 010" ->
    ("1999-001-thru-010", "1999").
    """
    label = label.strip()
    yr = None
    m = re.match(r"((?:18|19|20)\d{2})", label)
    if m:
        yr = m.group(1)
    slug = re.sub(r"[^A-Za-z0-9]+", "-", label).strip("-").lower() or "opinion"
    return slug, yr


def drive_download_url(file_id: str, resourcekey: Optional[str]) -> str:
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    if resourcekey:
        url += f"&resourcekey={resourcekey}"
    return url


class WYAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={"User-Agent": _UA,
                     "Accept": "text/html,application/pdf,*/*;q=0.8"},
            timeout=90,
        )
        self.delay = 1.0

    def _curl_bytes(self, url: str) -> Optional[bytes]:
        try:
            out = subprocess.run(
                ["curl", "-s", "-L", "--max-time", "120", "-A", _UA, url],
                capture_output=True, timeout=150,
            )
            if out.returncode == 0 and out.stdout:
                return out.stdout
        except Exception as e:
            logger.warning(f"curl fallback failed for {url}: {e}")
        return None

    def _fetch_bytes(self, url: str, retries: int = 3) -> Optional[bytes]:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    return resp.content
                if resp.status_code == 404:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if "SSL" in str(e) or "handshake" in str(e).lower():
                    break
            if attempt < retries:
                time.sleep(2 ** attempt)
        return self._curl_bytes(url)

    def _fetch_text(self, url: str) -> Optional[str]:
        b = self._fetch_bytes(url)
        if not b:
            return None
        return b.decode("utf-8", "replace")

    def _confirm_token(self, body: bytes) -> Optional[str]:
        """Drive sometimes serves an HTML interstitial with a confirm token."""
        m = re.search(rb'confirm=([0-9A-Za-z_\-]+)', body)
        if m:
            return m.group(1).decode()
        return None

    def _download_pdf(self, file_id: str, resourcekey: Optional[str]) -> Optional[bytes]:
        url = drive_download_url(file_id, resourcekey)
        b = self._fetch_bytes(url)
        if not b:
            return None
        # Direct PDF?
        if b[:5] == b"%PDF-":
            return b
        # HTML interstitial (virus-scan confirm) -> retry with confirm token.
        token = self._confirm_token(b)
        if token:
            url2 = url + f"&confirm={token}"
            b2 = self._fetch_bytes(url2)
            if b2 and b2[:5] == b"%PDF-":
                return b2
        logger.info(f"Drive id {file_id} did not return a PDF "
                    f"(got {b[:16]!r})")
        return None

    def discover(self) -> list:
        """Parse the index page into a list of opinion entries."""
        html = self._fetch_text(INDEX_URL)
        if not html:
            logger.error("Could not fetch index page")
            return []
        entries = []
        seen = set()
        for m in re.finditer(
            r'<a[^>]+href="([^"]*drive\.google\.com/file/d/([\w-]+)[^"]*)"[^>]*>(.*?)</a>',
            html, re.S,
        ):
            href, file_id, inner = m.group(1), m.group(2), m.group(3)
            if file_id in seen:
                continue
            seen.add(file_id)
            label = re.sub(r"<[^>]+>", "", inner).strip()
            rk = None
            rkm = re.search(r"resourcekey=([\w-]+)", href)
            if rkm:
                rk = rkm.group(1)
            entries.append({"label": label, "file_id": file_id, "resourcekey": rk})
        logger.info(f"Discovered {len(entries)} opinion links")
        return entries

    def _build_raw(self, entry: dict) -> Optional[dict]:
        file_id = entry["file_id"]
        pdf_bytes = self._download_pdf(file_id, entry.get("resourcekey"))
        if not pdf_bytes:
            logger.warning(f"Could not download Drive file {file_id} "
                           f"({entry['label']})")
            return None
        ref = f"https://drive.google.com/file/d/{file_id}"
        try:
            raw = extract_pdf_markdown(ref, SOURCE_ID,
                                       pdf_bytes=pdf_bytes, table="legislation")
        except Exception as e:
            logger.warning(f"PDF extract error {file_id}: {e}")
            return None
        text = clean_text(raw or "")
        if not text or len(text) < 300:
            logger.info(f"No usable text (scanned?) for {file_id} "
                        f"({len(text)} chars)")
            return None
        slug, year = parse_opinion_label(entry["label"])
        view_url = drive_download_url(file_id, entry.get("resourcekey")).replace(
            "uc?export=download&id=", "file/d/").split("&")[0]
        view_url = f"https://drive.google.com/file/d/{file_id}/view"
        return {
            "slug": slug,
            "title": f"Wyoming Attorney General Formal Opinion {entry['label']}",
            "text": text,
            "date": f"{year}-01-01" if year else None,
            "url": view_url,
            "year": year,
            "file_id": file_id,
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"{SOURCE_ID}/{raw['slug']}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for entry in self.discover():
            raw = self._build_raw(entry)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    def test_api(self) -> bool:
        logger.info("Testing Wyoming AG formal opinions archive...")
        try:
            entries = self.discover()
            if not entries:
                logger.error("  No opinion links discovered")
                return False
            for entry in entries:
                raw = self._build_raw(entry)
                if raw:
                    logger.info(f"  Extracted full text OK for {entry['label']} "
                                f"({len(raw['text'])} chars); title={raw['title']}")
                    logger.info("API test PASSED")
                    return True
            logger.error("  No PDF produced usable text")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WY-AGOpinions bootstrap")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WYAGOpinionsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()
    count = 0
    for raw in gen:
        record = scraper.normalize(raw)
        safe_id = record["_id"].replace("/", "_")
        with open(sample_dir / f"{safe_id}.json", "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
