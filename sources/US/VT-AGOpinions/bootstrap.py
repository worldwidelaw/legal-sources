#!/usr/bin/env python3
"""
US/VT-AGOpinions -- Vermont Attorney General Opinions

Fetches the full text of formal and informal legal opinions issued by the
Vermont Attorney General. Each opinion answers a legal question posed by a
public official or agency and constitutes an authoritative (advisory) state
legal interpretation (doctrine).

The opinions are published openly by the Office of the Vermont Attorney
General on a single Drupal listing page:
  https://ago.vermont.gov/about-attorney-generals-office/attorney-general-opinions
Each entry links a digitally-produced text PDF hosted under
ago.vermont.gov/sites/ago/files/ (real text layer, no OCR needed). A small
number of the older formal opinions are scanned images with no text layer;
those are skipped.

Strategy:
  1. Fetch the listing page HTML.
  2. Extract each opinion anchor: (link text, PDF URL). The link text carries
     the opinion type ("Formal Opinion" / "Informal Opinion") and issue date.
  3. Download each PDF and extract its text via the shared
     common.pdf_extract.extract_pdf_markdown helper (OOM-hardened).
  4. Normalize into the standard doctrine schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (data/records.jsonl)
  python bootstrap.py bootstrap --sample   # Fetch sample documents to sample/
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
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.VT-AGOpinions")

BASE_URL = "https://ago.vermont.gov"
LISTING_URL = (
    BASE_URL
    + "/about-attorney-generals-office/attorney-general-opinions"
)
FIRST_YEAR = 1980
CURRENT_YEAR = datetime.now(timezone.utc).year

# Anchor tags pointing at an opinion PDF, capturing href + inner text.
ANCHOR_RE = re.compile(
    r'<a\s[^>]*href="([^"]*\.pdf[^"]*)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)

# A formal opinion number embedded in a filename, e.g. 'FO 2001-1', 'AG Op. 2025-01'.
NUM_RE = re.compile(r"((?:19|20)\d{2})-(\d{1,3})")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


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


def strip_tags(html: str) -> str:
    return unescape(re.sub(r"<[^>]+>", "", html)).replace("\xa0", " ").strip()


def parse_anchor_date(anchor_text: str) -> str | None:
    """Extract 'Month D, YYYY' from an opinion's link text → ISO date."""
    m = re.search(
        r"\b(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
        anchor_text, re.IGNORECASE,
    )
    if m:
        mon = MONTHS[m.group(1).lower()]
        day = int(m.group(2))
        yr = int(m.group(3))
        if 1 <= day <= 31 and FIRST_YEAR <= yr <= CURRENT_YEAR + 1:
            return f"{yr:04d}-{mon:02d}-{day:02d}"
    return None


def opinion_type(anchor_text: str, filename: str) -> str:
    low = (anchor_text + " " + filename).lower()
    if "informal" in low or filename.lower().startswith("io"):
        return "Informal"
    return "Formal"


def opinion_number(filename: str) -> str | None:
    """Derive the official formal opinion number (YYYY-N) from the filename.

    Some newer files prefix a full issue date, e.g.
    '2025-11-20 AG Op. 2025-01.pdf' — strip a leading YYYY-MM-DD date so the
    real opinion number (after 'AG Op.') is matched rather than the date.
    """
    cleaned = re.sub(r"^\s*(?:19|20)\d{2}-\d{1,2}-\d{1,2}\b", "", filename)
    m = NUM_RE.search(cleaned)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}"
    return None


def slug_from_url(pdf_url: str) -> str:
    """Stable id slug from the PDF filename."""
    base = pdf_url.rsplit("/", 1)[-1]
    try:
        from urllib.parse import unquote
        base = unquote(base)
    except Exception:
        pass
    base = re.sub(r"\.pdf$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-")
    return base.lower() or "opinion"


class VTAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": self._ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;"
                          "q=0.9,application/pdf,*/*;q=0.8",
                # ago.vermont.gov 403s requests without a same-site Referer.
                "Referer": LISTING_URL,
            },
            timeout=60,
        )
        self.delay = 1.0

    def _curl_bytes(self, url: str) -> bytes | None:
        """Fetch raw bytes via the curl CLI (TLS/handshake fallback)."""
        try:
            out = subprocess.run(
                ["curl", "-s", "-L", "--max-time", "90",
                 "-A", self._ua, "-e", LISTING_URL, url],
                capture_output=True, timeout=120,
            )
            if out.returncode == 0 and out.stdout:
                return out.stdout
        except Exception as e:
            logger.warning(f"curl fallback failed for {url}: {e}")
        return None

    def _fetch_bytes(self, url: str, retries: int = 4) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
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

    def _get(self, url: str, retries: int = 4) -> str:
        data = self._fetch_bytes(url, retries=retries)
        if data is None:
            return ""
        return data.decode("utf-8", errors="replace")

    def discover_opinions(self) -> list:
        """Return ordered, de-duplicated list of (anchor_text, pdf_url)."""
        html = self._get(LISTING_URL)
        if not html:
            return []
        out = []
        seen = set()
        for m in ANCHOR_RE.finditer(html):
            href = unescape(m.group(1)).strip()
            text = strip_tags(m.group(2))
            url = urljoin(BASE_URL, href)
            key = url.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append((text, url))
        return out

    def _build_record(self, anchor_text: str, pdf_url: str) -> dict | None:
        slug = slug_from_url(pdf_url)
        pdf_bytes = self._fetch_bytes(pdf_url)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF {pdf_url}")
            return None
        try:
            raw = extract_pdf_markdown(
                source="US/VT-AGOpinions", source_id=slug,
                pdf_bytes=pdf_bytes, table="doctrine",
            )
        except Exception as e:
            logger.warning(f"PDF extract error {pdf_url}: {e}")
            return None
        text = clean_text(raw or "")
        if not text or len(text) < 200:
            # Scanned-image opinions (no text layer) are skipped.
            logger.warning(f"No usable text for {pdf_url} ({len(text)} chars)")
            return None
        filename = pdf_url.rsplit("/", 1)[-1]
        return {
            "slug": slug,
            "type": opinion_type(anchor_text, filename),
            "number": opinion_number(filename),
            "text": text,
            "date": parse_anchor_date(anchor_text),
            "url": pdf_url,
            "label": anchor_text,
        }

    def test_api(self) -> bool:
        logger.info("Testing Vermont AG opinions listing...")
        try:
            ops = self.discover_opinions()
            if not ops:
                logger.error("  No opinion PDF links found on listing page")
                return False
            logger.info(f"  Discovered {len(ops)} opinion PDF links")
            raw = self._build_record(*ops[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars)")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        otype = raw.get("type", "Formal")
        number = raw.get("number")
        if otype == "Formal" and number:
            title = f"Vermont Attorney General Formal Opinion No. {number}"
        else:
            label = raw.get("label") or ""
            if label:
                title = f"Vermont Attorney General {label}"
            else:
                title = f"Vermont Attorney General {otype} Opinion"
        return {
            "_id": f"US/VT-AGOpinions/{raw['slug']}",
            "_source": "US/VT-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "opinion_type": otype,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for anchor_text, pdf_url in self.discover_opinions():
            raw = self._build_record(anchor_text, pdf_url)
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


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/VT-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VTAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    if args.sample:
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for raw in scraper.fetch_sample():
            record = scraper.normalize(raw)
            safe_id = record["_id"].replace("/", "_")
            with open(sample_dir / f"{safe_id}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")
        logger.info(f"Bootstrap complete: {count} sample records saved to {sample_dir}")
    else:
        # Full run: stream normalized records to data/records.jsonl.
        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(exist_ok=True)
        out_path = data_dir / "records.jsonl"
        count = 0
        with open(out_path, "w", encoding="utf-8") as f:
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
                if count % 25 == 0:
                    logger.info(f"  {count} records written")
        logger.info(f"Bootstrap complete: {count} records written to {out_path}")


if __name__ == "__main__":
    main()
