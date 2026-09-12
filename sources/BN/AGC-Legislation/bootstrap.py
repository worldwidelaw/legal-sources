#!/usr/bin/env python3
"""
BN/AGC-Legislation -- Brunei Attorney General's Chambers Legislation (archive tree)

The AGC "Laws of Brunei" index page (Laws of Brunei.aspx) used to carry a column of
PDF links; in mid-2026 the AGC stripped that column and the page is now a plain
chapter-number/title table with no anchors at all. The PDFs themselves are still
served from /AGC Images/ — only the index disappeared (issue #1354).

BN/AGCLaws already rebuilds the *index-page* corpus (top-level ACT_PDF/ plus
LOB/PDF/, LOB/pdf/, LOB/PDF (EN)/) from the newest Wayback capture that still has
the link column. This source covers the rest of the AGC legislation tree, which the
index page never linked:

  /AGC Images/LAWS/ACT_PDF/{A..Z}/   alphabetical revised editions (incl. historical
                                     1984/2001 revisions and replacements)
  /AGC Images/LAWS/BLUV/             Orders in the Brunei Laws Updating Volume
  /AGC Images/LAWS/ENACTMENT/{year}/ historical enactments 1908-1975
  /AGC Images/LOB/Order/**           subsidiary orders, filed by title
  /AGC Images/LOB/Order PDF (EN)/    subsidiary orders (English)
  /AGC Images/LOB/chapter 157 (...)/ UBD statutes and regulations under Cap. 157
  /AGC Images/LOB/cons_doc/          consolidated volumes

Strategy:
  1. Enumerate the PDF inventory of those directories from the Internet Archive CDX
     index (the directories are not browsable and no live page links them).
  2. Download each PDF LIVE from agc.gov.bn; fall back to the newest Wayback replay
     only when the live host 404s a path that used to exist.
  3. Extract full text with the shared extractor.

Malay-language directories (Peng_PDF, "(BM)") and the gazette tree (Gazette_PDF,
GAZETTE NOTIFICATION -- covered by BN/AGC-GazetteII) are excluded.

Usage:
  python bootstrap.py bootstrap            # sample-size pull into sample/
  python bootstrap.py bootstrap --sample   # same, explicit
  python bootstrap.py bootstrap --full     # full pull to data/records.jsonl
  python bootstrap.py bootstrap-fast       # full pull, concurrent extraction
  python bootstrap.py test                 # connectivity test
"""

import re
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from urllib.parse import unquote, urlsplit

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BN.AGC-Legislation")

BASE_URL = "https://www.agc.gov.bn"
LISTING_URL = f"{BASE_URL}/AGC%20Site%20Pages/Laws%20of%20Brunei.aspx"

CDX_URL = "http://web.archive.org/cdx/search/cdx"
# The two trees that hold every AGC legislation PDF.
CDX_PREFIXES = [
    "agc.gov.bn/AGC%20Images/LAWS*",
    "agc.gov.bn/AGC%20Images/LOB*",
]
WAYBACK_REPLAY = "https://web.archive.org/web/{ts}id_/{url}"

# Directories owned by this source. Matched against the decoded path, case-sensitively
# where the site is (ACT_PDF/A vs LOB/PDF), so we do not re-collect BN/AGCLaws' set.
INCLUDE_RE = re.compile(
    r"^/AGC Images/(?:"
    r"LAWS/ACT_PDF/[A-Z]/"          # alphabetical revised editions
    r"|LAWS/BLUV/"                  # Brunei Laws Updating Volume orders
    r"|LAWS/ENACTMENT/"             # historical enactments
    r"|LOB/Order/"                  # subsidiary orders by title
    r"|LOB/Order PDF \(EN\)/"       # subsidiary orders (English)
    r"|LOB/chapter 157 \("          # UBD statutes/regulations
    r"|LOB/cons_doc/"               # consolidated volumes
    r")"
)

# Never collect: gazettes (BN/AGC-GazetteII), Malay-only trees, and the index-page
# corpus that BN/AGCLaws owns (top-level ACT_PDF/, LOB/PDF/, LOB/pdf/).
EXCLUDE_RE = re.compile(
    r"Gazette_PDF|Ga-zette_PDF|GAZETTE NOTIFICATION|/Peng_PDF/|\(BM\)",
    re.IGNORECASE,
)

# "CHAPTER 002(1984).pdf" / "CAP 58 ARMS AND EXPLOSIVES ACT.pdf" / "Cap182subRg1.pdf"
CHAPTER_RE = re.compile(r"\b(?:CHAPTER|CHAP|CAP)\.?\s*0*(\d{1,3})\b", re.IGNORECASE)
# A parenthesised or trailing 4-digit year, e.g. "CHAPTER 002(1984)", "Order, 2006"
YEAR_RE = re.compile(r"(1[89]\d{2}|20\d{2})")

MIN_TEXT_CHARS = 200

# Running head of every revised-edition PDF: "Arms and Explosives CAP. 58 3"
RUNNING_HEAD_RE = re.compile(r"^(.{3,80}?)\s+CAP\.?\s*\d{1,3}\b", re.MULTILINE)
# Cover-page block: "CHAPTER 31" on its own line, act name in caps on the next line(s).
CHAPTER_HEADING_RE = re.compile(r"^CHAPTER\s*\d{1,3}$", re.IGNORECASE)
CAPS_LINE_RE = re.compile(r"^[A-Z][A-Z ,'()\-/&.]{2,79}$")
# Boilerplate that follows the act name on the cover/contents page.
STOP_LINES = {
    "LAWS OF BRUNEI",
    "ARRANGEMENT OF SECTIONS",
    "ARRANGEMENT OF RULES",
    "ARRANGEMENT OF REGULATIONS",
    "ARRANGEMENT OF ORDERS",
    "ARRANGEMENT OF PARAGRAPHS",
    "REVISED EDITION",
}


def _decoded_path(url: str) -> str:
    """Path component of a CDX row, percent-decoded, e.g. '/AGC Images/LAWS/...'."""
    return unquote(urlsplit(url).path)


def _title_from_path(path: str) -> str:
    """Human-readable title from the decoded PDF path."""
    fname = path.rsplit("/", 1)[-1]
    fname = re.sub(r"\.pdf$", "", fname, flags=re.IGNORECASE)
    title = fname.replace("_", " ").strip()
    title = re.sub(r"\s+", " ", title)
    # "CHAPTER 002(1984)" -> "Chapter 2 (1984)"
    m = re.match(r"^(?:CHAPTER|CHAP|CAP)\.?\s*0*(\d{1,3})\s*(.*)$", title, re.IGNORECASE)
    if m:
        rest = m.group(2).strip()
        title = f"Chapter {m.group(1)}" + (f" {rest}" if rest else "")
    if title.isupper() and len(title) > 12:
        title = title.title()
    return title


def _act_name_from_text(text: str) -> Optional[str]:
    """Recover the act's short name from the PDF cover block or running head.

    Filenames in the alphabetical tree are bare ("CHAPTER 002(1984).pdf"). The cover
    page carries "LAWS OF BRUNEI / CHAPTER 31 / ANTIQUITIES AND TREASURE TROVE ACT"
    (the name may wrap over two lines), and every page repeats "<name> CAP. <n>" as a
    running head — try the cover block first, then the head.
    """
    lines = [ln.strip() for ln in text[:8000].splitlines()]
    for i, ln in enumerate(lines):
        if not CHAPTER_HEADING_RE.match(ln):
            continue
        name_parts = []
        for nxt in lines[i + 1:i + 4]:
            if not nxt or not CAPS_LINE_RE.match(nxt) or nxt.upper() in STOP_LINES:
                break
            name_parts.append(nxt)
        if name_parts:
            name = re.sub(r"\s+", " ", " ".join(name_parts)).strip(" .-—")
            if len(name) >= 3:
                return name.title() if name.isupper() else name

    for m in RUNNING_HEAD_RE.finditer(text[:8000]):
        name = re.sub(r"\s+", " ", m.group(1)).strip(" .-—")
        if name.upper() in ("LAWS OF BRUNEI", "BRUNEI") or len(name) < 3:
            continue
        # Drop a "LAWS OF BRUNEI" prefix left on the same line.
        name = re.sub(r"^LAWS OF BRUNEI\s*", "", name, flags=re.IGNORECASE).strip()
        if len(name) >= 3 and not name.isdigit():
            return name
    return None


def _doc_id(path: str) -> str:
    """Stable ID from the decoded path below /AGC Images/."""
    rel = path.split("/AGC Images/", 1)[-1]
    rel = re.sub(r"\.pdf$", "", rel, flags=re.IGNORECASE)
    return re.sub(r"[^A-Za-z0-9]+", "_", rel).strip("_")


class BNLegislationScraper(BaseScraper):
    """Scraper for BN/AGC-Legislation -- the AGC legislation archive tree."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
        })

    # ------------------------------------------------------------------ index

    def _cdx_rows(self, prefix: str) -> List[str]:
        """Fetch 'timestamp original' rows for one prefix from the CDX index."""
        params = {
            "url": prefix,
            "output": "text",
            "fl": "timestamp,original",
            "collapse": "urlkey",
            "filter": "!statuscode:404",
        }
        resp = self.session.get(CDX_URL, params=params, timeout=300)
        resp.raise_for_status()
        return [ln for ln in resp.text.splitlines() if ln.strip()]

    def _build_index(self) -> List[Dict[str, str]]:
        """Enumerate every in-scope PDF from the Wayback CDX index."""
        by_path: Dict[str, str] = {}  # decoded path -> newest wayback timestamp
        raw_url: Dict[str, str] = {}  # decoded path -> a percent-encoded original

        for prefix in CDX_PREFIXES:
            rows = self._cdx_rows(prefix)
            logger.info(f"CDX {prefix}: {len(rows)} captures")
            for line in rows:
                parts = line.split(" ", 1)
                if len(parts) != 2:
                    continue
                ts, original = parts[0].strip(), parts[1].strip()
                if not original.lower().endswith(".pdf"):
                    continue
                path = _decoded_path(original)
                if EXCLUDE_RE.search(path) or not INCLUDE_RE.match(path):
                    continue
                if ts > by_path.get(path, ""):
                    by_path[path] = ts
                    raw_url[path] = original
                by_path.setdefault(path, ts)
                raw_url.setdefault(path, original)

        if not by_path:
            raise RuntimeError(
                "CDX index returned no in-scope AGC legislation PDFs — the archive "
                "query or the include patterns are broken, refusing to report success."
            )

        index = []
        for path in sorted(by_path):
            encoded = urlsplit(raw_url[path]).path  # keep the site's own escaping
            index.append({
                "path": path,
                "pdf_url": BASE_URL + encoded,
                "wayback_ts": by_path[path],
                "wayback_original": raw_url[path],
            })
        logger.info(f"Index built: {len(index)} legislation PDFs")
        return index

    # --------------------------------------------------------------- fetching

    def _live_ok(self, url: str) -> bool:
        try:
            resp = self.session.head(url, timeout=60, allow_redirects=True)
            ctype = resp.headers.get("Content-Type", "")
            return resp.status_code == 200 and "pdf" in ctype.lower()
        except Exception:
            return False

    def _extract(self, doc_id: str, url: str) -> Optional[str]:
        try:
            return extract_pdf_markdown(
                source="BN/AGC-Legislation",
                source_id=doc_id,
                pdf_url=url,
                table="legislation",
            )
        except Exception as e:
            logger.debug(f"    extraction failed for {doc_id} via {url}: {e}")
            return None

    # ---------------------------------------------------------------- scraper

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Yield RAW index entries; normalize() downloads and extracts the text."""
        for entry in self._build_index():
            yield entry

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        path = raw["path"]
        doc_id = _doc_id(path)
        pdf_url = raw["pdf_url"]

        text = self._extract(doc_id, pdf_url)
        source_url = pdf_url
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            # The live host dropped this file — replay the newest archived copy.
            replay = WAYBACK_REPLAY.format(
                ts=raw["wayback_ts"], url=raw["wayback_original"]
            )
            archived = self._extract(doc_id, replay)
            if archived and len(archived.strip()) >= MIN_TEXT_CHARS:
                text, source_url = archived, replay
            else:
                logger.warning(f"  skip {doc_id}: no extractable text")
                return None

        fname = path.rsplit("/", 1)[-1]
        chap_m = CHAPTER_RE.search(fname) or CHAPTER_RE.search(path)
        year_m = YEAR_RE.search(fname)

        title = _title_from_path(path)
        act_name = _act_name_from_text(text)
        if act_name and act_name.lower() not in title.lower():
            title = f"{act_name} — {title}"

        return {
            "_id": f"BN/AGC-Legislation/{doc_id}",
            "_source": "BN/AGC-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": f"{year_m.group(1)}-01-01" if year_m else None,
            "url": source_url,
            "doc_id": doc_id,
            "chapter": chap_m.group(1) if chap_m else "",
            "pdf_url": pdf_url,
            "collection": path.split("/AGC Images/", 1)[-1].rsplit("/", 1)[0],
        }

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """No incremental feed — the archive tree is static; re-run the full pull."""
        logger.info("No incremental update support; use full refresh.")
        return
        yield


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample|--full]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv or (
        command == "bootstrap" and "--full" not in sys.argv
    )

    if command == "test":
        scraper = BNLegislationScraper()
        idx = scraper._build_index()
        print(f"Index OK: {len(idx)} PDFs")
        for e in idx[:5]:
            print("  ", e["pdf_url"], "live" if scraper._live_ok(e["pdf_url"]) else "ARCHIVE-ONLY")
        sys.exit(0)

    scraper = BNLegislationScraper()
    if command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command in ("bootstrap-fast", "bootstrap_fast"):
        scraper.bootstrap_fast()
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
