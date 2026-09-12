#!/usr/bin/env python3
"""
US/TX-Legislation -- Texas Statutes (All Codes)

Fetches all 31 Texas Codes with full text from the official Texas Capitol
Statute Server (tcss.legis.texas.gov) via HTML ZIP bulk downloads.

Strategy:
  1. Fetch code listing from StatuteCodeDownloads.json
  2. For each code, download the HTML ZIP archive
  3. Extract chapter HTML files from the ZIP
  4. Parse each chapter into individual sections using <a name="X.XX"> anchors
  5. Normalize into standard schema

Data: Public domain (Texas government works). No auth required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all 31 codes)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import hashlib
import zipfile
import io
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-Legislation")

CODES_JSON_URL = "https://statutes.capitol.texas.gov/assets/StatuteCodeDownloads.json"
TCSS_BASE = "https://tcss.legis.texas.gov/resources"

# Sample: specific codes + chapters for quick testing
SAMPLE_CODES = ["PE", "GV", "FA"]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class TXLegislationScraper(BaseScraper):

    # `normalize()` stamps sections with the crawl date, not an enactment date, so
    # no `since` comparison is meaningful here; `fetch_updates` narrows on each
    # code ZIP's publication stamp and then a per-section hash. Declared so the
    # refresh classifier does not read the unused `since` as a no-op (#1502).
    incremental_comparator = "availability"

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "*/*",
            },
            timeout=120,
        )
        self.delay = 2.0
        self.force_walk = False

    def _get(self, url: str, binary: bool = False):
        """Fetch URL with rate limiting."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        if binary:
            return resp.content
        return resp.text

    # ---- refresh state -------------------------------------------------
    # A refresh has to answer "what became available since we last looked",
    # not "what is dated after `since`". Every section here is dated with the
    # crawl date rather than its enactment date, so a date comparator would
    # match either everything or nothing. The ZIPs carry the real signal.

    def _state_path(self, name: str) -> Path:
        path = Path(__file__).parent / "data" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _load_json(self, name: str, default):
        try:
            with open(self._state_path(name), encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return default

    def _save_json(self, name: str, payload) -> None:
        path = self._state_path(name)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        tmp.replace(path)  # atomic: a half-written state would skip codes

    def _zip_stamp(self, code: str) -> Optional[dict]:
        """HEAD a code's ZIP and return its publication stamp.

        tcss.legis.texas.gov serves every `{code}.htm.zip` with ETag,
        Last-Modified and Content-Length, and the codes move independently
        (Penal last changed 2026-04-10 while Government moved 2026-07-31).
        One HEAD per code is what lets an unchanged code cost nothing.
        """
        url = f"{TCSS_BASE}/Zips/{code}.htm.zip"
        try:
            time.sleep(self.delay)
            resp = self.http.session.head(url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Could not HEAD {code} ZIP ({e}) — will re-read it")
            return None
        stamp = {
            "etag": resp.headers.get("ETag"),
            "last_modified": resp.headers.get("Last-Modified"),
            "content_length": resp.headers.get("Content-Length"),
        }
        return stamp if any(stamp.values()) else None

    def _section_key(self, record: dict) -> str:
        """Refresh-state key. Must match `_id` so duplicate-numbered sections
        are tracked separately rather than overwriting each other."""
        return (f"{record['code']}-{record['section_num']}"
                f"{self._enacting_suffix(record.get('text', ''), record.get('chapter', ''))}")

    @staticmethod
    def _text_hash(text: str) -> str:
        return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:16]

    def test_api(self):
        """Test connectivity to Texas statute server."""
        logger.info("Testing Texas Capitol Statute Server...")
        try:
            text = self._get(CODES_JSON_URL)
            data = json.loads(text)
            codes = data.get("StatuteCode", [])
            logger.info(f"  Code listing: {len(codes)} codes found")

            # Test a single chapter download
            url = f"{TCSS_BASE}/PE/htm/PE.1.htm"
            html = self._get(url)
            if "Sec." in html and "PENAL CODE" in html:
                logger.info("  Chapter HTML: OK (Penal Code Ch 1)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("API test FAILED: unexpected chapter content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def get_code_list(self) -> list:
        """Fetch the list of all Texas codes."""
        text = self._get(CODES_JSON_URL)
        data = json.loads(text)
        return data.get("StatuteCode", [])

    def download_code_zip(self, code: str) -> Optional[zipfile.ZipFile]:
        """Download and return the HTML ZIP for a code."""
        url = f"{TCSS_BASE}/Zips/{code}.htm.zip"
        try:
            data = self._get(url, binary=True)
            return zipfile.ZipFile(io.BytesIO(data))
        except Exception as e:
            logger.warning(f"Failed to download ZIP for {code}: {e}")
            return None

    def parse_chapter_sections(self, html: str, code: str, code_name: str,
                                chapter: str) -> list:
        """Parse individual sections from a chapter HTML file.

        Structure: each section starts with a bold link containing "Sec. X.XX."
        and runs until the next such link or end of body.
        """
        sections = []

        title_match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE)
        chapter_title = strip_html(title_match.group(1)) if title_match else f"{code_name} Chapter {chapter}"

        # Find all section starts via the bold "Sec. X.XX." links
        # Pattern: <a ...font-weight:bold;">Sec. 1.01.  SHORT TITLE.</a>
        sec_starts = list(re.finditer(
            r'font-weight:\s*bold[^>]*>Sec\.\s+(\d+[A-Za-z]?\.\d+[a-z]?)\.',
            html
        ))

        if not sec_starts:
            return sections

        for i, m in enumerate(sec_starts):
            sec_num = m.group(1)
            # Find the beginning of this section's block — go back to find
            # the <p class="left"><a name= that precedes this Sec.
            block_start = html.rfind('<p class="left"><a name=', 0, m.start())
            if block_start == -1:
                block_start = m.start()

            # End is start of next section's block, or end of body
            if i + 1 < len(sec_starts):
                next_block = html.rfind('<p class="left"><a name=', 0, sec_starts[i + 1].start())
                block_end = next_block if next_block > block_start else sec_starts[i + 1].start()
            else:
                block_end = html.find('</body>', m.start())
                if block_end == -1:
                    block_end = len(html)

            raw_html = html[block_start:block_end]
            text = strip_html(raw_html)

            if text and len(text) > 20:
                sections.append({
                    "code": code,
                    "code_name": code_name,
                    "chapter": chapter,
                    "chapter_title": chapter_title,
                    "section_num": sec_num,
                    "text": text,
                })

        return sections

    # A Texas session can enact several *different* sections carrying the same
    # number: the 89th Legislature (2025) added four distinct Penal Code
    # § 32.56, one each from S.B. 1809, S.B. 1281, S.B. 1333 and S.B. 2373.
    # The statute server publishes all of them, each under its own
    # "Text of section as added by Acts ..., Ch. N" header. Keying on
    # code+section alone collapsed the four into one row and silently dropped
    # three real provisions, so the enacting chapter joins the key.
    _ENACTING_CH_RE = re.compile(
        r'Text of section as (?:added|amended) by\s+Acts\s+(\d{4})\b[^\n]*?\bCh\.\s*(\d+)',
        re.IGNORECASE,
    )

    # The other duplication mechanism: two wholly different chapters can share
    # a number, and the statute server separates them only by filename --
    # Civil Practice & Remedies ch. 100B is fraudulent crowdfunding in
    # `cp.100b.htm` and AI-related financial exploitation in `cp.100b.v2.htm`.
    _CHAPTER_VARIANT_RE = re.compile(r'\.v(\d+)$', re.IGNORECASE)

    # KNOWN RESIDUAL (~0.2% of sections): a third mechanism is not handled --
    # two different *subchapters* of one chapter can reuse the same numbers,
    # e.g. Family Code 264.191-264.195 exist once in the lead-entity
    # subchapter and again in the receivership subchapter. Nothing structural
    # separates them (same code, same chapter, no enacting header, no .vN
    # file), so only the catchline distinguishes them and those still collapse
    # pairwise. Measured on a 4-code sample: AG 2, CP 4, FA 6 of ~6,000
    # sections. Fixing it needs a catchline-derived key, which is a bigger
    # change than this refresh fix should carry.

    @classmethod
    def _enacting_suffix(cls, text: str, chapter: str = "") -> str:
        """Return a stable disambiguator, or "" for ordinary single sections.

        Derived from the enacting act / chapter variant rather than the
        position in the file, so it does not shift when another duplicate is
        codified ahead of it. Sections that need no disambiguation -- the vast
        majority -- keep their original `_id` untouched, so this does not
        churn the ~120K rows already ingested.
        """
        variant = cls._CHAPTER_VARIANT_RE.search(chapter or "")
        if variant:
            return f"~v{variant.group(1)}"
        match = cls._ENACTING_CH_RE.search(text or "")
        return f"~{match.group(1)}c{match.group(2)}" if match else ""

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        section_id = (f"{raw['code']}-{raw['section_num']}"
                      f"{self._enacting_suffix(raw.get('text', ''), raw.get('chapter', ''))}")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # The citation has to name the *state*, not just the code (#1619).
        # Texas, California, Louisiana and others all publish a "Penal Code",
        # a "Family Code" and an "Education Code", so a bare
        # "Family Code § 1.001" is ambiguous across jurisdictions and matches
        # no citation a reader would actually write. The form below is the one
        # a citation carries in the wild -- "Texas Estates Code § 55.001".
        citation = f"Texas {raw['code_name']} § {raw['section_num']}"

        # ... and it has to appear in the body as well. `text` opens with the
        # statute server's own catchline ("Sec. 55.001. OPPOSITION IN PROBATE
        # PROCEEDING."), which never names the code, so a keyword search for
        # "Texas Estates Code 55.001" had nothing lexical to hit and the
        # section was reachable only semantically.
        breadcrumb = citation
        chapter_title = raw.get("chapter_title", "")
        if chapter_title:
            breadcrumb = f"{breadcrumb} — {chapter_title}"

        return {
            "_id": section_id,
            "_source": "US/TX-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": citation,
            "text": f"{breadcrumb}\n\n{raw['text']}",
            "date": today,
            "url": f"https://statutes.capitol.texas.gov/Docs/{raw['code']}/htm/{raw['code']}.{raw['chapter']}.htm#{raw['section_num']}",
            "citation": citation,
            "subdivision": "US-TX",
            "code": raw["code"],
            "code_name": raw["code_name"],
            "chapter": raw["chapter"],
            "chapter_title": chapter_title,
            "section_num": raw["section_num"],
        }

    def process_code(self, code_info: dict, max_chapters: int = 0) -> Generator[dict, None, None]:
        """Process a single code: download ZIP, parse all chapters."""
        code = code_info["code"]
        code_name = code_info["CodeName"]
        logger.info(f"Processing {code} ({code_name})...")

        zf = self.download_code_zip(code)
        if not zf:
            return

        chapter_files = sorted(zf.namelist())
        if max_chapters > 0:
            chapter_files = chapter_files[:max_chapters]

        total_sections = 0
        for filename in chapter_files:
            # Extract chapter number from filename like "pe.1.htm"
            ch_match = re.match(r'[a-z]+\.(.+)\.htm$', filename, re.IGNORECASE)
            if not ch_match:
                continue

            chapter = ch_match.group(1)
            # Skip "_old" suffix files (superseded versions)
            if chapter.endswith("_old"):
                continue

            try:
                html = zf.read(filename).decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"  Failed to read {filename}: {e}")
                continue

            sections = self.parse_chapter_sections(html, code, code_name, chapter)
            for sec in sections:
                yield sec
                total_sections += 1

        logger.info(f"  {code}: {total_sections} sections extracted")
        zf.close()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all sections across all Texas Codes.

        Also records each code's ZIP stamp and each section's text hash, so
        the next refresh has a baseline to compare against instead of having
        to re-read all ~120K sections to discover that nothing moved.
        """
        codes = self.get_code_list()
        logger.info(f"Found {len(codes)} Texas codes")
        state = self._load_json("zip_state.json", {})
        hashes = self._load_json("section_hashes.json", {})
        total = 0
        for code_info in codes:
            code = code_info["code"]
            stamp = self._zip_stamp(code)
            for record in self.process_code(code_info):
                hashes[self._section_key(record)] = self._text_hash(record["text"])
                yield record
                total += 1
                if total % 500 == 0:
                    logger.info(f"  Progress: {total} sections fetched")
            if stamp:
                state[code] = stamp
            self._save_json("zip_state.json", state)
            self._save_json("section_hashes.json", hashes)
        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Yield only sections that are new or whose text actually changed.

        The old body was `yield from self.fetch_all()`, so every refresh slot
        re-downloaded all 31 code ZIPs and re-parsed ~120K sections purely to
        have the loader dedup ~all of them away (#1502).

        `since` is accepted but deliberately unused: it is a *crawl* time, and
        `normalize()` stamps every section with the crawl date rather than an
        enactment date, so no date comparison here can be meaningful. The
        comparator is availability instead — each code's ZIP publication stamp
        (ETag/Last-Modified/size), then a per-section text hash for the codes
        that did move. A quiet week costs 31 HEAD requests.
        """
        codes = self.get_code_list()
        state = self._load_json("zip_state.json", {})
        hashes = self._load_json("section_hashes.json", {})
        seeding = not hashes
        if seeding:
            logger.info("No section hashes recorded yet — this refresh seeds them")

        changed_codes = 0
        emitted = 0
        for code_info in codes:
            code = code_info["code"]
            stamp = self._zip_stamp(code)
            prior = state.get(code)
            if not self.force_walk and stamp and prior and stamp == prior and not seeding:
                logger.info(f"  {code}: ZIP unchanged since {prior.get('last_modified')} — skipped")
                continue

            changed_codes += 1
            for record in self.process_code(code_info):
                key = self._section_key(record)
                digest = self._text_hash(record["text"])
                if hashes.get(key) == digest:
                    continue
                hashes[key] = digest
                emitted += 1
                yield record

            if stamp:
                state[code] = stamp
            self._save_json("zip_state.json", state)
            self._save_json("section_hashes.json", hashes)

        logger.info(
            f"Refresh complete: {changed_codes} of {len(codes)} codes republished, "
            f"{emitted} sections new or changed"
        )

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample: 3 codes, first 2 chapters each."""
        codes = self.get_code_list()
        sample_codes = [c for c in codes if c["code"] in SAMPLE_CODES]
        logger.info(f"Fetching sample from {len(sample_codes)} codes...")
        count = 0
        for code_info in sample_codes:
            for record in self.process_code(code_info, max_chapters=2):
                yield record
                count += 1
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/TX-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TXLegislationScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "update":
        stats = scraper.update()

    else:
        # Drive BaseScraper rather than the generator directly. `fetch_all` /
        # `fetch_sample` yield RAW sections by contract (the double-normalize
        # sweep in fee7e120b made them raw everywhere), but this CLI kept
        # reading `record['_id']` off them -- so *every* invocation, sample or
        # full, died with `KeyError: '_id'` before writing a single record and
        # the source has been un-crawlable ever since. Going through
        # `bootstrap()` also streams the full corpus to data/records.jsonl,
        # which the hand-rolled loop never did (it wrote all ~120K sections
        # into sample/).
        if args.sample:
            # `bootstrap()` only ever reads fetch_all; bind the curated sample
            # walk onto it so a sample still spans PE/GV/FA rather than
            # whichever code the listing happens to put first.
            scraper.fetch_all = scraper.fetch_sample
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)

    if stats.get("records_fetched", 0) == 0:
        logger.error("No records fetched — failing loud rather than exiting 0")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
