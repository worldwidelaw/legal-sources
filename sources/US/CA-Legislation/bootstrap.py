#!/usr/bin/env python3
"""
US/CA-Legislation -- California Codes (Statutes)

Fetches every operative section of the California Codes (plus the California
Constitution) with full text, from the Legislature's own PUBINFO bulk export.

Primary strategy -- PUBINFO bulk dump (#1506):
  1. Pick the newest `pubinfo_YYYY.zip` from downloads.leginfo.legislature.ca.gov
  2. Read `LAW_SECTION_TBL.dat` (tab-delimited, one row per operative section)
  3. Join each row to its `LAW_SECTION_TBL_N.lob` sibling, which holds the CAML
     body, and to `LAW_TOC_TBL.dat` for the division/title/part/chapter heading
  4. Normalize into standard schema

The zip is a *consolidated* snapshot, not a session diff: 162,429 rows across 30
codes with effective dates from the 1800s to the current session. That was the
gating check of #1506 and it passes, so the dump can replace the walk outright.

Fallback strategy -- the original HTML walk, kept because it is the only path
that works if downloads.leginfo goes away:
  1. For each code, fetch the TOC from codedisplayexpand.xhtml
  2. Extract leaf-node links, list section numbers per leaf
  3. Fetch codes_displaySection.xhtml per section

The walk costs ~70h per refresh at 2s/section against a 100h fleet cap, which is
why it is no longer the default; the dump is one 1.2GB download and it carries
~40K sections the walk never reached.

Data: Public domain (California government works). No auth required.

Usage:
  python bootstrap.py bootstrap            # Full pull from the bulk dump
  python bootstrap.py bootstrap --sample   # ~15 sections, read over HTTP range
  python bootstrap.py bootstrap --walk     # Force the legacy HTML walk
  python bootstrap.py test-api             # Connectivity test
"""

import io
import os
import sys
import json
import hashlib
import logging
import re
import time
import zipfile
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote, unquote

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-Legislation")

BASE_URL = "https://leginfo.legislature.ca.gov/faces"
DUMP_BASE = "https://downloads.leginfo.legislature.ca.gov/"

SECTION_TBL = "LAW_SECTION_TBL.dat"
TOC_TBL = "LAW_TOC_TBL.dat"

# Column positions in LAW_SECTION_TBL.dat, per pubinfo_Readme.pdf. The file is
# tab-delimited with 18 columns; text columns are backtick-quoted and the body
# is spilled into the .lob named by column 14.
S_LAW_CODE, S_SECTION_NUM, S_EFFECTIVE = 1, 2, 6
S_DIVISION, S_TITLE, S_PART, S_CHAPTER, S_ARTICLE = 8, 9, 10, 11, 12
S_HISTORY, S_LOB, S_ACTIVE = 13, 14, 15
SECTION_COLS = 18

# Column positions in LAW_TOC_TBL.dat (19 columns).
T_LAW_CODE, T_DIVISION, T_TITLE, T_PART, T_CHAPTER, T_ARTICLE = 0, 1, 2, 3, 4, 5
T_HEADING, T_TREEPATH, T_HISTORY = 6, 13, 15
TOC_COLS = 19

# All 29 California Codes (from sitemap)
CA_CODES = {
    "BPC": "Business and Professions Code",
    "CIV": "Civil Code",
    "CCP": "Code of Civil Procedure",
    "COM": "Commercial Code",
    "CORP": "Corporations Code",
    "EDC": "Education Code",
    "ELEC": "Elections Code",
    "EVID": "Evidence Code",
    "FAM": "Family Code",
    "FIN": "Financial Code",
    "FGC": "Fish and Game Code",
    "FAC": "Food and Agricultural Code",
    "GOV": "Government Code",
    "HNC": "Harbors and Navigation Code",
    "HSC": "Health and Safety Code",
    "INS": "Insurance Code",
    "LAB": "Labor Code",
    "MVC": "Military and Veterans Code",
    "PEN": "Penal Code",
    "PROB": "Probate Code",
    "PCC": "Public Contract Code",
    "PRC": "Public Resources Code",
    "PUC": "Public Utilities Code",
    "RTC": "Revenue and Taxation Code",
    "SHC": "Streets and Highways Code",
    "UIC": "Unemployment Insurance Code",
    "VEH": "Vehicle Code",
    "WAT": "Water Code",
    "WIC": "Welfare and Institutions Code",
    # The dump carries the Constitution alongside the codes under `CONS`. The
    # HTML walk never reached it (codedisplayexpand has no CONS TOC), so these
    # ~372 sections are new corpus rather than a re-key of existing rows.
    "CONS": "California Constitution",
}

# Sample codes + sections for quick testing
SAMPLE_SECTIONS = [
    ("CIV", "1624"),
    ("CIV", "1550"),
    ("CIV", "3294"),
    ("PEN", "187"),
    ("PEN", "459"),
    ("PEN", "211"),
    # GOV 6250 (the old Public Records Act declaration) is gone — AB 473
    # recodified the CPRA into GOV 7920+ operative 2023, and the old section
    # now serves an empty body, so it sampled as a 0-char failure.
    ("GOV", "7921.000"),
    ("GOV", "11135"),
    ("FAM", "2310"),
    ("LAB", "201"),
    ("LAB", "510"),
    ("EVID", "352"),
    ("VEH", "23152"),
    ("HSC", "11350"),
    ("CCP", "340"),
]


def _text_hash(text: str) -> str:
    """Short digest of a section's text — the refresh comparator."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class _RangeFile(io.RawIOBase):
    """Seekable read-only view of a remote file, backed by HTTP Range requests.

    `zipfile` only needs seek/read, so handing it one of these lets us read a
    zip's central directory and a handful of members without pulling the whole
    1.2GB archive. Used for sample mode and for the pre-flight that decides
    whether a session zip is worth downloading at all; the full bootstrap
    downloads to disk instead, because 162K members served one range request
    each would be far slower than one sequential GET.
    """

    def __init__(self, url: str, session: requests.Session):
        self.url = url
        self.session = session
        resp = self.session.head(url, timeout=60, allow_redirects=True)
        resp.raise_for_status()
        if resp.headers.get("Accept-Ranges") != "bytes":
            raise OSError(f"{url} does not advertise byte ranges")
        self.size = int(resp.headers["Content-Length"])
        self.last_modified = resp.headers.get("Last-Modified")
        self._pos = 0

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            self._pos = offset
        elif whence == os.SEEK_CUR:
            self._pos += offset
        else:
            self._pos = self.size + offset
        return self._pos

    def tell(self) -> int:
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = self.size - self._pos
        if size <= 0 or self._pos >= self.size:
            return b""
        end = min(self._pos + size, self.size) - 1
        resp = self.session.get(
            self.url, headers={"Range": f"bytes={self._pos}-{end}"}, timeout=180
        )
        resp.raise_for_status()
        chunk = resp.content
        self._pos += len(chunk)
        return chunk

    def readinto(self, buf) -> int:
        chunk = self.read(len(buf))
        buf[: len(chunk)] = chunk
        return len(chunk)


def _unquote_field(value: str) -> Optional[str]:
    """Decode one PUBINFO column: backtick-quoted, bare, or the literal NULL."""
    value = value.strip()
    if value == "NULL" or not value:
        return None
    if len(value) >= 2 and value.startswith("`") and value.endswith("`"):
        value = value[1:-1]
    return value or None


def _iter_dat_rows(blob: bytes, expected_cols: int) -> Generator[list, None, None]:
    """Split a PUBINFO .dat into rows, tolerating newlines inside text columns.

    The current export happens to keep every row on one line, but a stray
    newline in a HISTORY string would otherwise shift every following row's
    columns and corrupt the corpus silently. Accumulating until the tab count
    matches makes that failure mode impossible rather than merely unobserved.
    """
    pending = ""
    for line in blob.decode("utf-8", "replace").split("\n"):
        pending = f"{pending}\n{line}" if pending else line
        fields = pending.split("\t")
        if len(fields) < expected_cols:
            continue
        if len(fields) > expected_cols:
            logger.warning(
                f"Dropping a {len(fields)}-column row (expected {expected_cols})"
            )
        else:
            yield fields
        pending = ""
    if pending.strip():
        logger.warning(f"Trailing {len(pending)} bytes did not form a complete row")


def caml_to_text(caml: str) -> str:
    """Render a CAML section body to plain text.

    CAML marks typographic spacing with empty elements (`<span class="EnSpace"/>`)
    rather than characters, so stripping tags first would run "(a)" straight into
    the following word. The HTML walk emitted these as U+00A0, and matching that
    keeps dump-sourced and walk-sourced text comparable.
    """
    if not caml:
        return ""
    text = re.sub(r'<span class="[^"]*Space"\s*/>', "\xa0", caml)
    return strip_html(text)


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    # Remove style and script blocks
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    # Replace <br>, <p>, <div> with newlines
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'<h[1-6][^>]*>', '\n## ', text)
    text = re.sub(r'</h[1-6]>', '\n', text)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = html_module.unescape(text)
    # Clean whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class CALegislationScraper(BaseScraper):

    # LegInfo publishes no modified-date facet, so `fetch_updates` narrows on the
    # bulk dump's Last-Modified and then a per-section content hash. Declared so
    # the refresh classifier does not read the unused `since` as a no-op (#1502).
    incremental_comparator = "availability"

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html",
            },
            timeout=60,
        )
        self.delay = 2.0  # seconds between requests (HTML fallback walk only)
        # Plain requests session for downloads.leginfo: the bulk host serves
        # byte ranges and multi-hundred-MB streams, neither of which the shared
        # HttpClient is set up for.
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
        })
        self._dump_last_modified = None
        # BaseScraper.bootstrap() calls fetch_all() with no arguments, so all
        # three flags have to ride on the instance.
        self.restart = False
        self.sample_mode = False
        self.force_walk = False

    def _get(self, url: str) -> str:
        """Fetch URL with rate limiting, return HTML string."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.text

    def test_api(self):
        """Test connectivity to leginfo website."""
        logger.info("Testing California LegInfo website...")
        try:
            url = f"{BASE_URL}/codes_displaySection.xhtml?lawCode=CIV&sectionNum=1624"
            html = self._get(url)
            if "single_law_section" in html:
                logger.info("  Connectivity: OK")
                logger.info("  Section content found: Yes")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("API test FAILED: section content not found in response")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def fetch_section_text(self, law_code: str, section_num: str) -> Optional[dict]:
        """Fetch full text of a single code section."""
        url = f"{BASE_URL}/codes_displaySection.xhtml?lawCode={law_code}&sectionNum={section_num}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch {law_code} § {section_num}: {e}")
            return None

        # Extract content from single_law_section div
        match = re.search(
            r'id="single_law_section"[^>]*>(.*?)(?=<div[^>]*id="(?:footer|j_id1)|</body>)',
            html,
            re.DOTALL,
        )
        if not match:
            logger.warning(f"No section content found for {law_code} § {section_num}")
            return None

        raw_html = match.group(1)
        text = strip_html(raw_html)

        if not text or len(text) < 20:
            logger.warning(f"Section text too short for {law_code} § {section_num}: {len(text)} chars")
            return None

        # Extract heading structure from the HTML
        heading_match = re.search(
            r'id="codeLawSectionNoHead"[^>]*>(.*?)<font',
            raw_html,
            re.DOTALL,
        )
        heading = ""
        if heading_match:
            heading = strip_html(heading_match.group(1))

        code_name = CA_CODES.get(law_code, law_code)
        return {
            "law_code": law_code,
            "code_name": code_name,
            "section_num": section_num,
            "heading": heading,
            "text": text,
            "url": url,
        }

    # ---- PUBINFO bulk dump (#1506) -------------------------------------

    def _dump_session(self) -> Optional[str]:
        """Name the newest `pubinfo_YYYY.zip` that actually carries the law tables.

        The daily archives (`pubinfo_daily_Wed.zip`, `pubinfo_Wed.zip`) are
        similarly named and similarly sized but hold only `BILL_*` tables, so
        picking by name alone would silently yield a corpus of zero sections.
        The candidate is confirmed by reading its central directory over HTTP
        ranges — a few MB — before committing to a 1.2GB download.
        """
        try:
            listing = self.session.get(DUMP_BASE, timeout=60).text
        except Exception as exc:
            logger.warning(f"Cannot list {DUMP_BASE}: {exc}")
            return None

        years = sorted({int(y) for y in re.findall(r"pubinfo_(\d{4})\.zip", listing)})
        if not years:
            logger.warning("No pubinfo_YYYY.zip entries in the download listing")
            return None

        for year in reversed(years):
            name = f"pubinfo_{year}.zip"
            try:
                handle = _RangeFile(DUMP_BASE + name, self.session)
                with zipfile.ZipFile(io.BufferedReader(handle, buffer_size=1 << 20)) as zf:
                    zf.getinfo(SECTION_TBL)
                    zf.getinfo(TOC_TBL)
            except Exception as exc:
                logger.info(f"{name} carries no law tables ({exc}) — trying older")
                continue
            logger.info(f"Using bulk dump {name} (modified {handle.last_modified})")
            self._dump_last_modified = handle.last_modified
            return name

        return None

    def _download_dump(self, name: str) -> Path:
        """Fetch the session zip to data/, reusing a complete previous copy."""
        target = self._state_path(name)
        url = DUMP_BASE + name
        head = self.session.head(url, timeout=60, allow_redirects=True)
        head.raise_for_status()
        expected = int(head.headers["Content-Length"])
        self._dump_last_modified = head.headers.get("Last-Modified")

        if target.exists() and target.stat().st_size == expected:
            logger.info(f"Reusing {target.name} ({expected / 1e9:.2f} GB) already on disk")
            return target

        logger.info(f"Downloading {url} ({expected / 1e9:.2f} GB)...")
        partial = target.with_suffix(".part")
        done = 0
        with self.session.get(url, stream=True, timeout=300) as resp:
            resp.raise_for_status()
            with open(partial, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
                    done += len(chunk)
                    if done % (100 << 20) < (1 << 20):
                        logger.info(f"  {done / 1e9:.2f} / {expected / 1e9:.2f} GB")

        if done != expected:
            partial.unlink(missing_ok=True)
            raise OSError(f"Truncated download: got {done} of {expected} bytes")
        partial.replace(target)
        logger.info(f"Downloaded {target.name}")
        return target

    def _load_toc_index(self, zf: zipfile.ZipFile) -> dict:
        """Map each code's TOC nodes by tree path, for breadcrumb reconstruction.

        A section's own node is found by its (division, title, part, chapter,
        article) tuple; its ancestors are the dotted prefixes of that node's
        tree path, which is how the heading block gets rebuilt in order.
        """
        by_tuple: dict = {}
        by_path: dict = {}
        for row in _iter_dat_rows(zf.read(TOC_TBL), TOC_COLS):
            code = _unquote_field(row[T_LAW_CODE])
            path = _unquote_field(row[T_TREEPATH])
            if not code or not path:
                continue
            heading = _unquote_field(row[T_HEADING]) or ""
            history = (_unquote_field(row[T_HISTORY]) or "").strip()
            by_path.setdefault(code, {})[path] = (heading, history)
            key = tuple(_unquote_field(row[c]) for c in
                        (T_DIVISION, T_TITLE, T_PART, T_CHAPTER, T_ARTICLE))
            by_tuple.setdefault(code, {})[key] = path
        logger.info(f"Indexed TOC nodes for {len(by_path)} codes")
        return {"by_path": by_path, "by_tuple": by_tuple}

    def _breadcrumb(self, toc: dict, code: str, key: tuple) -> str:
        """Rebuild the `## Civil Code - CIV` / `## DIVISION 3. ...` heading block."""
        parts = [f"## {CA_CODES.get(code, code)} - {code}", ""]
        path = toc["by_tuple"].get(code, {}).get(key)
        if path:
            nodes = path.split(".")
            for depth in range(1, len(nodes) + 1):
                entry = toc["by_path"][code].get(".".join(nodes[:depth]))
                if not entry:
                    continue
                heading, history = entry
                parts.append(f"## {heading}")
                if history:
                    parts.append(f"\xa0\xa0( {history} )")
                parts.append("\xa0\xa0")
        while parts and parts[-1] == "\xa0\xa0":
            parts.pop()
        return "\n".join(parts).strip()

    def _section_url(self, code: str, section_num: str, article: Optional[str]) -> str:
        """Build the public permalink for a dump row.

        Constitution sections are addressed by article rather than by a bare
        section number, so a CONS row needs the extra parameter or the link
        resolves to nothing.
        """
        url = (
            f"{BASE_URL}/codes_displaySection.xhtml"
            f"?lawCode={code}&sectionNum={quote(section_num)}"
        )
        if code == "CONS" and article:
            url += f"&article={quote(article)}"
        return url

    def _iter_dump_sections(
        self, zf: zipfile.ZipFile, wanted: Optional[set] = None
    ) -> Generator[dict, None, None]:
        """Yield one raw record per operative section in the dump.

        `wanted` restricts output to a set of `(law_code, section_num)` pairs and
        is applied *before* the lob is read. That ordering is what makes sample
        mode affordable: over HTTP ranges each lob costs a request, so filtering
        afterwards would turn a 15-record sample into 162K round trips.
        """
        toc = self._load_toc_index(zf)
        members = set(zf.namelist())
        total = skipped = 0

        for row in _iter_dat_rows(zf.read(SECTION_TBL), SECTION_COLS):
            if _unquote_field(row[S_ACTIVE]) != "Y":
                continue
            code = _unquote_field(row[S_LAW_CODE])
            raw_num = _unquote_field(row[S_SECTION_NUM])
            lob = _unquote_field(row[S_LOB])
            if not (code and raw_num and lob):
                skipped += 1
                continue

            # The dump stores section numbers with the trailing period LegInfo
            # prints ("1624."); the ~121K rows already in Neon are keyed without
            # it, so stripping here is what makes this an update rather than an
            # orphan-and-reinsert of the whole corpus.
            section_num = raw_num if code == "CONS" else raw_num.rstrip(".")
            if wanted is not None and (code, section_num) not in wanted:
                continue
            if lob not in members:
                logger.warning(f"{code} § {raw_num}: {lob} missing from the archive")
                skipped += 1
                continue

            body = caml_to_text(zf.read(lob).decode("utf-8", "replace"))
            if not body or len(body) < 20:
                logger.warning(f"{code} § {section_num}: {len(body)} chars — skipped")
                skipped += 1
                continue

            article = _unquote_field(row[S_ARTICLE])
            key = tuple(_unquote_field(row[c]) for c in
                        (S_DIVISION, S_TITLE, S_PART, S_CHAPTER, S_ARTICLE))
            heading = self._breadcrumb(toc, code, key)
            history = (_unquote_field(row[S_HISTORY]) or "").strip()
            text = f"{heading}\n\n## {raw_num}\xa0\xa0\n{body}"
            if history:
                text += f"\n({history})"

            effective = _unquote_field(row[S_EFFECTIVE])
            total += 1
            if total % 10000 == 0:
                logger.info(f"  Extracted {total:,} sections from the dump")

            yield {
                "law_code": code,
                "code_name": CA_CODES.get(code, code),
                "section_num": section_num,
                "heading": heading,
                "text": text,
                "url": self._section_url(code, raw_num, article),
                # Straight from the table, so it needs none of the provenance
                # guesswork the walk had to do on the trailing parenthetical.
                "effective_date": effective[:10] if effective else None,
                "history": history,
            }

        logger.info(f"Dump yielded {total:,} sections ({skipped:,} skipped)")
    def get_toc_leaves(self, law_code: str) -> list:
        """Get all leaf chapter/article links from the TOC for a code."""
        url = f"{BASE_URL}/codedisplayexpand.xhtml?tocCode={law_code}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch TOC for {law_code}: {e}")
            return []

        # Extract codes_displayText links with parameters
        pattern = (
            r'codes_displayText\.xhtml\?'
            r'lawCode=([A-Z]+)&amp;'
            r'division=([^&]*)&amp;'
            r'title=([^&]*)&amp;'
            r'part=([^&]*)&amp;'
            r'chapter=([^&]*)&amp;'
            r'article=([^&"]*)'
        )
        matches = re.findall(pattern, html)

        leaves = []
        seen = set()
        for m in matches:
            key = (m[0], m[1], m[2], m[3], m[4], m[5])
            if key not in seen:
                seen.add(key)
                leaves.append({
                    "law_code": m[0],
                    "division": m[1],
                    "title": m[2],
                    "part": m[3],
                    "chapter": m[4],
                    "article": m[5],
                })
        logger.info(f"  {law_code}: found {len(leaves)} TOC leaves")
        return leaves

    def get_sections_for_leaf(self, leaf: dict) -> list:
        """Get all section numbers from a TOC leaf page."""
        url = (
            f"{BASE_URL}/codes_displayText.xhtml?"
            f"lawCode={leaf['law_code']}"
            f"&division={leaf['division']}"
            f"&title={leaf['title']}"
            f"&part={leaf['part']}"
            f"&chapter={leaf['chapter']}"
            f"&article={leaf['article']}"
        )
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch section list for {leaf}: {e}")
            return []

        # Extract section numbers from submitCodesValues or h6 links
        sections = re.findall(r"submitCodesValues\('([\d\.]+[a-z]?)\.?'", html)
        if not sections:
            # Try alternate pattern: direct section links
            sections = re.findall(
                r'sectionNum=([\d\.]+[a-z]?)', html
            )
        return list(dict.fromkeys(sections))  # deduplicate preserving order

    def parse_provenance(self, text: str) -> tuple:
        """Pull the operative date out of a section's closing provenance line.

        Every section ends with one, e.g.
        `(Amended by Stats. 2023, Ch. 260, Sec. 14. (SB 345) Effective January 1, 2024.)`

        Returns `(date, precision, raw_line)`. See `date_from_provenance` for how
        the line itself is read.
        """
        match = re.search(r"\(([^()]*(?:\([^()]*\)[^()]*)*)\)\s*$", text.strip())
        if not match:
            return None, None, ""
        line = match.group(1).strip()
        date, precision = self.date_from_provenance(line)
        return date, precision, line

    def date_from_provenance(self, line: str) -> tuple:
        """Read `(date, precision)` out of an already-isolated provenance line.

        Split out of `parse_provenance` because the dump supplies the same text
        as its own HISTORY column, with no surrounding parenthetical to strip —
        and re-wrapping it in one to reuse the old entry point silently failed on
        the rows that nest parens themselves (`... (as added by Stats. 1965(2x),
        Ch. 12) ...`), which are exactly the rows with no EFFECTIVE_DATE.

        The explicit `Effective` date is preferred; sections that never carried
        one (`Enacted by Stats. 1872.`) fall back to January 1 of the chaptering
        year, flagged `precision="year"` so a consumer can tell a real date from
        a convention. `date` is None when neither is present rather than being
        back-filled with the crawl date — the previous code stamped every section
        with today, which made the field say nothing about the law.
        """
        if not line:
            return None, None

        effective = re.search(
            r"Effective\s+([A-Z][a-z]+)\s+(\d{1,2}),\s*(\d{4})", line
        )
        if effective:
            month, day, year = effective.groups()
            try:
                parsed = datetime.strptime(f"{month} {day} {year}", "%B %d %Y")
                return parsed.strftime("%Y-%m-%d"), "day"
            except ValueError:
                pass

        stats_year = re.search(r"Stats\.\s*(\d{4})", line)
        if stats_year:
            return f"{stats_year.group(1)}-01-01", "year"

        # The 1872 originals carry no session-law cite at all — they read
        # `(Enacted 1872.)`. Matching only `Stats. YYYY` left every one of them
        # dateless, which is not a missing date so much as an unparsed one.
        bare_year = re.search(r"\b(1[6-9]\d{2}|20\d{2})\b", line)
        if bare_year:
            return f"{bare_year.group(1)}-01-01", "year"

        return None, None

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        # Unchanged key: ~121K rows are already indexed under `{CODE}-{section}`,
        # and re-keying would orphan every one of them rather than update it.
        # The walk keeps the trailing period LegInfo prints ("6041.") while the
        # dump strips it, so the same section arrived under two different keys
        # depending on which path produced it (#1613). Strip in one place.
        section_num = raw["section_num"]
        if raw["law_code"] != "CONS":
            section_num = section_num.rstrip(".")
        section_id = f"{raw['law_code']}-{section_num}"
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Dump rows carry EFFECTIVE_DATE as its own column — but only for the
        # 58% of sections chaptered under the modern convention. The other
        # 67,652 leave it NULL and state the date in HISTORY instead, so without
        # this fallback 42% of the corpus normalizes to date=None and is dropped
        # by the required-temporal-key check rather than ingested (cf. #995).
        # Walk rows have no such column at all and are parsed the same way, from
        # the trailing provenance parenthetical.
        if "effective_date" in raw:
            provenance = raw.get("history", "")
            date = raw["effective_date"]
            precision = "day" if date else None
            if not date:
                date, precision = self.date_from_provenance(provenance)
        else:
            date, precision, provenance = self.parse_provenance(raw["text"])

        return {
            "_id": section_id,
            "_source": "US/CA-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": f"{raw['code_name']} § {raw['section_num']}",
            "text": raw["text"],
            "date": date,
            "date_precision": precision,
            "url": raw["url"],
            "law_code": raw["law_code"],
            "code_name": raw["code_name"],
            "section_num": raw["section_num"],
            "heading": raw.get("heading", ""),
            "history": provenance,
        }


    # ---- checkpointing -------------------------------------------------

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
        tmp.replace(path)  # atomic: a half-written checkpoint would skip sections

    def _walk_sections(self, resume: dict = None) -> Generator[dict, None, None]:
        """Walk every section of all 29 codes, checkpointing per TOC leaf.

        At ~2s per request and ~121K sections a cold walk costs on the order of
        the fleet's whole wall-clock budget, so a run that dies partway must not
        restart at the first code. The checkpoint records completed codes and the
        leaf index within the code in flight; resume replays at leaf granularity.
        """
        resume = resume if resume is not None else {}
        done_codes = set(resume.get("done_codes") or [])
        current_code = resume.get("current_code")
        done_leaves = resume.get("done_leaves") or 0
        total = 0

        for code, name in CA_CODES.items():
            if code in done_codes:
                logger.info(f"Skipping {code} ({name}) — completed by a previous run")
                continue

            logger.info(f"Processing {code} ({name})...")
            leaves = self.get_toc_leaves(code)

            skip = done_leaves if code == current_code else 0
            if skip:
                logger.info(f"  Resuming {code} at leaf {skip:,}/{len(leaves):,}")

            for index, leaf in enumerate(leaves):
                if index < skip:
                    continue
                for sec_num in self.get_sections_for_leaf(leaf):
                    raw = self.fetch_section_text(code, sec_num)
                    if raw:
                        yield raw
                        total += 1
                        if total % 100 == 0:
                            logger.info(f"  Progress: {total:,} sections fetched")

                self._save_json("checkpoint.json", {
                    "done_codes": sorted(done_codes),
                    "current_code": code,
                    "done_leaves": index + 1,
                })

            done_codes.add(code)
            self._save_json("checkpoint.json", {
                "done_codes": sorted(done_codes),
                "current_code": None,
                "done_leaves": 0,
            })

        logger.info(f"Total sections fetched: {total:,}")

    # ---- entry points ---------------------------------------------------

    def _walk_all(self, restart: bool) -> Generator[dict, None, None]:
        """Legacy path: crawl every section over HTTP at self.delay per request."""
        resume = {} if restart else self._load_json("checkpoint.json", {})
        if restart:
            logger.info("--restart given, discarding the resume checkpoint")
        elif resume.get("done_codes") or resume.get("current_code"):
            logger.info(
                f"Resuming: {len(resume.get('done_codes') or [])} codes already complete"
            )
        yield from self._walk_sections(resume)
        self._save_json("checkpoint.json", {})

    def fetch_all(self, restart: Optional[bool] = None) -> Generator[dict, None, None]:
        """Yield every operative California code section, preferring the bulk dump.

        The dump is one 1.2GB download against the ~70h the per-section walk
        costs, and it carries ~40K sections the walk never reached, so it is the
        default. The walk stays reachable behind `--walk` and as an automatic
        fallback: if downloads.leginfo is unreachable or stops shipping the law
        tables, a slow crawl still beats no corpus at all.
        """
        # BaseScraper drives sample mode by truncating fetch_all(); route it to
        # the curated sample so a `--sample` run neither downloads 1.2GB nor
        # leaves a checkpoint claiming a code was partly walked.
        if self.sample_mode:
            yield from self.fetch_sample()
            return

        restart = self.restart if restart is None else restart
        hashes = {} if restart else self._load_json("section_hashes.json", {})

        emitted = 0
        for raw in self._iter_sections(restart):
            hashes[f"{raw['law_code']}-{raw['section_num']}"] = _text_hash(raw["text"])
            emitted += 1
            yield raw

        self._save_json("section_hashes.json", hashes)
        self._save_json("dump_state.json", {"last_modified": self._dump_last_modified})
        logger.info(f"fetch_all emitted {emitted:,} sections")

    def _iter_sections(self, restart: bool) -> Generator[dict, None, None]:
        """Dispatch to the dump, falling back to the walk if the dump is unusable."""
        if not self.force_walk:
            try:
                name = self._dump_session()
                if name:
                    archive = self._download_dump(name)
                    with zipfile.ZipFile(archive) as zf:
                        yield from self._iter_dump_sections(zf)
                    return
                logger.warning("No usable PUBINFO session zip found")
            except Exception as exc:
                logger.warning(f"Bulk dump path failed ({exc}) — falling back to the walk")

        logger.info("Using the HTML section walk (~2s/section)")
        yield from self._walk_all(restart)

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Yield only sections that are new or whose text changed.

        LegInfo's HTML exposes no modified-date facet, which is why the old
        refresh had to re-fetch all ~121K sections just to hash them — a ~70h
        no-op most weeks. The dump does carry a comparator: the archive's own
        `Last-Modified`. When it has not moved, nothing in the corpus has, and
        the refresh costs one HEAD request instead of three days of crawling.

        When it has moved, the whole zip is re-read and each section's text is
        compared to its recorded hash, so only genuinely changed sections are
        emitted. Availability, not the section's chaptering date, is the
        comparator — a section republished without a new chaptering still shows
        up, and `since` stays unused precisely because it would not.
        """
        state = self._load_json("dump_state.json", {})
        hashes = self._load_json("section_hashes.json", {})
        if not hashes:
            logger.info("No section hashes recorded yet — this refresh seeds them")

        if not self.force_walk and state.get("last_modified"):
            try:
                name = self._dump_session()
            except Exception as exc:
                logger.warning(f"Cannot reach the bulk host ({exc})")
                name = None
            if name and self._dump_last_modified == state["last_modified"]:
                logger.info(
                    f"{name} unchanged since {state['last_modified']} — "
                    "no sections to refresh"
                )
                return

        emitted = seen = 0
        for raw in self._iter_sections(restart=False):
            seen += 1
            key = f"{raw['law_code']}-{raw['section_num']}"
            digest = _text_hash(raw["text"])
            if hashes.get(key) == digest:
                continue
            hashes[key] = digest
            emitted += 1
            yield raw

        self._save_json("section_hashes.json", hashes)
        self._save_json("dump_state.json", {"last_modified": self._dump_last_modified})
        logger.info(f"Refresh: {emitted:,} new or changed of {seen:,} sections")

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample, reading the dump over HTTP ranges.

        Sampling from the same code path the full bootstrap uses is the point:
        a sample drawn from the HTML walk would pass while the dump parser was
        broken. Range requests keep the cost to a few MB instead of 1.2GB.
        """
        wanted = {(code, num) for code, num in SAMPLE_SECTIONS}
        try:
            name = self._dump_session()
            if not name:
                raise OSError("no usable PUBINFO session zip")
            handle = _RangeFile(DUMP_BASE + name, self.session)
            with zipfile.ZipFile(io.BufferedReader(handle, buffer_size=1 << 20)) as zf:
                count = 0
                for raw in self._iter_dump_sections(zf, wanted=wanted):
                    yield raw
                    count += 1
                    if count >= len(wanted):
                        break
            logger.info(f"Sample complete: {count} sections from {name}")
            return
        except Exception as exc:
            logger.warning(f"Dump sample failed ({exc}) — falling back to the walk")

        count = 0
        for law_code, section_num in SAMPLE_SECTIONS:
            raw = self.fetch_section_text(law_code, section_num)
            if raw:
                yield raw
                count += 1
        logger.info(f"Sample complete: {count} sections fetched over HTTP")

def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CA-Legislation bootstrap")
    parser.add_argument(
        "command",
        # `bootstrap-fast` is what the fleet wrapper invokes; rejecting it exited 1
        # and fell back to re-ingesting sample/, which reads downstream as a
        # sample-only run rather than as a broken CLI (#1502).
        choices=["bootstrap", "bootstrap-fast", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument(
        "--restart", action="store_true",
        help="Ignore the resume checkpoint and re-walk from the first code",
    )
    parser.add_argument(
        "--walk", action="store_true",
        help="Force the legacy per-section HTML walk instead of the bulk dump",
    )
    args = parser.parse_args()

    scraper = CALegislationScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    if args.restart:
        scraper.restart = True
    if args.walk:
        scraper.force_walk = True

    # Route through BaseScraper so records stream to data/records.jsonl. The
    # previous code wrote one file per record into sample/ and indexed the raw
    # dicts fetch_all() yields with normalized keys, so `bootstrap` raised
    # KeyError: '_id' before it could write anything at all.
    if args.command == "update":
        stats = scraper.update()
    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
    elif args.sample:
        scraper.sample_mode = True
        stats = scraper.bootstrap(sample_mode=True, sample_size=15)
    else:
        stats = scraper.bootstrap()

    logger.info(f"{args.command} complete: {stats}")
    if not stats.get("records_fetched"):
        logger.error("No records fetched")
        sys.exit(1)


if __name__ == "__main__":
    main()
