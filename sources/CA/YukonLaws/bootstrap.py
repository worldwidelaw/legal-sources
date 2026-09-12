#!/usr/bin/env python3
"""
CA/YukonLaws -- Yukon Consolidated Acts, Regulations & Gazettes

Strategy (Internet Archive / Wayback Machine):

  The whole yukon.ca legislation estate (laws.yukon.ca, legislation.yukon.ca)
  now sits behind a Cloudflare managed challenge. Verified 2026-08-18: the
  by-title index POST, a plain GET and the /cms/images/LEGISLATION/ PDF assets
  all return 403 with ``server: cloudflare`` / ``cf-mitigated: challenge`` and
  the "Just a moment..." Turnstile interstitial, under both our own UA and a
  Chrome/126 UA, from a residential vantage. So it is neither a datacenter-IP
  block nor a UA filter -- the live host needs browser automation (issue
  #1441-D).

  The corpus is therefore read from the Internet Archive, which holds a deep
  crawl of the PDF assets under ``/cms/images/LEGISLATION/``:

  1. ``_load_index()`` enumerates every archived legislation PDF via the
     Wayback CDX API and folds the two URL schemes the site has used into one
     document namespace:

         current   /LEGISLATION/{PRINCIPAL|SUBORDINATE|AMENDING|GAZETTES}
                       /{year}/{doc_id}/{doc_id}[_{version}].pdf
         legacy    /LEGISLATION/{acts|regs|app}/{doc_id}.pdf

     Documents are keyed on (namespace, doc_id); when both schemes archived the
     same document the current-scheme capture wins, then the highest ``_N``
     point-in-time version, then the newest snapshot.

  2. ``fetch_all()`` replays each PDF raw (``/web/{ts}id_/{url}``), falling back
     through older captures of the same URL when a snapshot is itself an error
     body, and extracts the full text with fitz/pdfplumber.

  3. Titles come from the archived HTML index pages (acts-before-2003,
     acts-from-2003-onwards, index-of-regulations, the annual-acts articles,
     ...), which link each PDF under its proper title. The by-title index
     itself is POST-driven and unarchived, so anything those pages miss falls
     back to a title parsed out of the PDF's own first page.

  Excluded: ``/LEGISLATION/historical_statutes/`` (216 bound statute volumes,
  1902-2002) and ``/LEGISLATION/ncnr/`` (45 not-consolidated-not-repealed
  acts) are scanned page images with no text layer -- they need OCR, which the
  fleet image does not have, so they are recorded as a coverage gap rather
  than ingested as empty records.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap-fast       # Alias used by the fleet runner
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
from itertools import zip_longest
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.YukonLaws")

BASE_URL = "https://laws.yukon.ca"
CDX_URL = "http://web.archive.org/cdx/search/cdx"
WB_RAW = "https://web.archive.org/web/{ts}id_/{url}"

USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Current scheme: /LEGISLATION/{COLLECTION}/{year}/{doc_id}/{file}.pdf
CURRENT_RE = re.compile(
    r"^https?://laws\.yukon\.ca/cms/images/LEGISLATION/"
    r"(PRINCIPAL|SUBORDINATE|AMENDING|GAZETTES)/(\d{4})/([0-9A-Za-z_-]+)/"
    r"([0-9A-Za-z_.-]+)\.pdf$",
    re.IGNORECASE,
)
# Legacy scheme: /LEGISLATION/{acts|regs|app}/{doc_id}.pdf
LEGACY_RE = re.compile(
    r"^https?://laws\.yukon\.ca/cms/images/LEGISLATION/"
    r"(acts|regs|app)/([0-9A-Za-z_.-]+)\.pdf$",
    re.IGNORECASE,
)
# Scanned-image collections: no text layer, need OCR.
IMAGE_ONLY = ("historical_statutes", "ncnr")

# Folded document namespaces, in the order that claims a bare doc id.
NAMESPACES = ("PRINCIPAL", "SUBORDINATE", "AMENDING", "GAZETTES", "APPOINTMENT")
COLLECTION_OF = {
    "PRINCIPAL": "PRINCIPAL",
    "ACTS": "PRINCIPAL",
    "SUBORDINATE": "SUBORDINATE",
    "REGS": "SUBORDINATE",
    "AMENDING": "AMENDING",
    "GAZETTES": "GAZETTES",
    "APP": "APPOINTMENT",
}
DOC_TYPE_OF = {
    "PRINCIPAL": "act",
    "SUBORDINATE": "regulation",
    "AMENDING": "amending_act",
    "GAZETTES": "gazette",
    "APPOINTMENT": "appointment_order",
}

# Archived HTML index pages that link each PDF under its proper title.
INDEX_CDX_PARAMS = {
    "url": "laws.yukon.ca/cms/*",
    "output": "text",
    "fl": "timestamp,original",
    "filter": ["statuscode:200", "mimetype:text/html"],
    "collapse": "urlkey",
    "limit": "3000",
}
INDEX_ANCHOR_RE = re.compile(
    r'<a[^>]*?href="([^"]*?/LEGISLATION/[^"]+\.pdf)"[^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)
ASSET_RE = re.compile(r"\.(js|css|ico|png|gif|jpe?g|pdf)$", re.IGNORECASE)

# Stub PDFs that stand in for a repealed/federal/regulation-only entry.
PLACEHOLDER_RE = re.compile(
    r"placeholder file|there is no act|^\s*see\s+\S+.*act",
    re.IGNORECASE,
)

# Header lines that precede the real title on a first page.
TITLE_NOISE_RE = re.compile(
    r"^(?:YUKON|CANADA|STATUTES OF YUKON.*|CHAPTER\s+\d+|CHAPITRE\s+\d+|"
    r"(?:C\.?O\.?|O\.?C\.?|O\.?I\.?C\.?|D\.?C\.?)[\s.]*\d{4}\s*/\s*\d+|"
    r"ORDER[- ]IN[- ]COUNCIL.*|D[EÉ]CRET.*|CONSOLIDATED TO.*|"
    r"REVISED STATUTES.*|LOIS? DU YUKON.*)$",
    re.IGNORECASE,
)
# A title line must look like a title, not prose.
TITLE_KEYWORD_RE = re.compile(
    r"\b(ACT|ACTS|REGULATION|REGULATIONS|ORDER|ORDERS|RULES?|CODE|BYLAW|"
    r"BY-LAW|GAZETTE|LOI|LOIS|R[EÈ]GLEMENT|ARR[EÊ]T[EÉ]|D[EÉ]CRET)\b",
    re.IGNORECASE,
)
FRENCH_STOP_RE = re.compile(
    r"^(LOI|LOIS|R[EÈ]GLEMENT|ARR[EÊ]T[EÉ]|D[EÉ]CRET|CHAPITRE|PARTIE|SECTION)\b",
    re.IGNORECASE,
)


def _clean_label(html_fragment: str) -> str:
    """Strip tags/entities from an anchor's inner HTML."""
    text = re.sub(r"<[^>]+>", " ", html_fragment)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&#39;", "'")
        .replace("&rsquo;", "'")
        .replace("&quot;", '"')
    )
    text = re.sub(r"\s+", " ", text).strip()
    # A handful of index rows carry an unescaped ">" inside a tooltip attribute,
    # so the anchor's inner HTML starts with the tail of that attribute
    # (`Commenced: Monday, May 31, 2021">Child Care Act`). Keep what follows.
    if '">' in text:
        text = text.rsplit('">', 1)[-1].strip()
    # Amending-act rows prefix the title with a bold "amended by..." marker.
    text = re.sub(r"^amended by\s*(?:\.{2,}|…)\s*", "", text, flags=re.IGNORECASE).strip()
    # Trailing "[2]" marks how many point-in-time versions exist, not the title.
    return re.sub(r"\s*\[\d+\]\s*$", "", text).strip()


def _version_of(filename: str) -> int:
    m = re.search(r"_(\d+)$", filename)
    return int(m.group(1)) if m else 0


def _title_from_text(text: str, collection: str) -> str:
    """Parse a document title out of the PDF's own first page.

    Yukon PDFs are bilingual two-column, so the extracted head interleaves an
    English block with its French counterpart. Take the leading run of
    title-shaped lines and stop at the first French one.
    """
    if collection == "GAZETTES":
        return ""
    lines = [l.strip() for l in text.split("\n")[:40]]
    # An amending OIC prints its parent act's name once per language column,
    # so the leading run is the same line twice.
    lines = [l for i, l in enumerate(lines) if i == 0 or l != lines[i - 1]]
    parts: List[str] = []
    for line in lines:
        if not line:
            if parts:
                break
            continue
        if not parts and TITLE_NOISE_RE.match(line):
            continue
        # Real titles are set in caps on the cover; prose is not.
        letters = [c for c in line if c.isalpha()]
        if not letters or sum(c.isupper() for c in letters) / len(letters) < 0.8:
            break
        if len(line) > 90:
            break
        if parts and (FRENCH_STOP_RE.match(line) or TITLE_NOISE_RE.match(line)):
            break
        parts.append(line)
        if len(parts) >= 4:
            break

    title = re.sub(r"\s+", " ", " ".join(parts)).strip(" .,-")
    if not title or not TITLE_KEYWORD_RE.search(title):
        return ""
    return title.title() if title.isupper() else title


def _year_from_doc_id(doc_id: str) -> str:
    """Legacy-scheme ids carry no /{year}/ path segment, but most embed one
    (``oic2017_076`` -> 2017). Fall back to a leading 4-digit run."""
    for candidate in re.findall(r"(?<!\d)(1[89]\d{2}|20\d{2})(?!\d)", doc_id):
        return candidate
    return doc_id[:4] if doc_id[:4].isdigit() else ""


class YukonLawsScraper(BaseScraper):
    """Scraper for CA/YukonLaws -- Yukon Consolidated Laws (Wayback rebuild)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": USER_AGENT, "Accept": "text/html,application/pdf,*/*"}
        )
        self._index_cache: Optional[List[Dict[str, Any]]] = None
        self._titles: Optional[Dict[str, str]] = None
        self.cache_dir = source_dir / "data"

    # ---------------------------------------------------------------- http

    def _get(self, url: str, tries: int = 4, timeout: int = 120):
        """GET with backoff. web.archive.org 429/503s freely under load."""
        last = None
        for attempt in range(tries):
            self.rate_limiter.wait()
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    last = f"HTTP {resp.status_code}"
                    time.sleep(min(60, 5 * (2 ** attempt)))
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                last = str(e)
                time.sleep(min(60, 5 * (2 ** attempt)))
        raise RuntimeError(f"GET failed after {tries} tries ({last}): {url}")

    # --------------------------------------------------------------- index

    def _cdx_rows(self) -> List[Tuple[str, str, str]]:
        """(timestamp, original, statuscode) for every archived LEGISLATION PDF."""
        params = {
            "url": "laws.yukon.ca/cms/images/LEGISLATION/*",
            "output": "json",
            "fl": "timestamp,original,statuscode",
            "filter": r"original:.*\.[pP][dD][fF]$",
        }
        url = requests.Request("GET", CDX_URL, params=params).prepare().url
        data = self._get(url, timeout=300).json()
        if not data or len(data) < 2:
            raise RuntimeError(
                "Wayback CDX returned no rows for laws.yukon.ca LEGISLATION PDFs — "
                "the archive is unreachable or the path scheme changed."
            )
        return [tuple(r[:3]) for r in data[1:]]

    def _load_titles(self) -> Dict[str, str]:
        """Map ``{pdf filename}`` -> title, harvested from archived index pages.

        Normally served straight out of the committed ``titles.json`` (rebuilt
        by ``build_titles.py``), so a fresh clone does not spend ~400 throttled
        Wayback fetches before the first document. Falls back to sweeping the
        index pages live when that file is missing.

        Best effort either way: a failure here costs titles, not documents, so
        it never aborts the crawl.
        """
        if self._titles is not None:
            return self._titles

        for cache in (Path(__file__).parent / "titles.json",
                      self.cache_dir / "wayback_titles.json"):
            if not cache.exists():
                continue
            try:
                self._titles = json.loads(cache.read_text())
                logger.info(f"Loaded {len(self._titles)} titles from {cache.name}")
                return self._titles
            except Exception as e:
                logger.warning(f"Could not read {cache.name}: {e}")

        logger.info("No titles.json — sweeping the archived index pages live")
        titles: Dict[str, str] = {}
        try:
            url = requests.Request("GET", CDX_URL, params=INDEX_CDX_PARAMS).prepare().url
            rows = [l.split() for l in self._get(url, timeout=300).text.splitlines() if l.strip()]
        except Exception as e:
            logger.warning(f"Index-page CDX sweep failed ({e}); titles fall back to PDF bodies")
            self._titles = {}
            return self._titles

        pages: Dict[str, str] = {}
        for row in rows:
            if len(row) != 2:
                continue
            ts, original = row
            if "%2F" in original or ASSET_RE.search(original):
                continue
            pages.setdefault(original, ts)

        failed = 0
        for original, ts in pages.items():
            try:
                html = self._get(WB_RAW.format(ts=ts, url=original), tries=2, timeout=60).text
            except Exception:
                failed += 1
                continue
            for href, label in INDEX_ANCHOR_RE.findall(html):
                filename = href.rsplit("/", 1)[-1]
                label = _clean_label(label)
                if label and len(label) > 3 and filename not in titles:
                    titles[filename] = label

        logger.info(
            f"Index-page title sweep: {len(pages) - failed}/{len(pages)} pages, "
            f"{len(titles)} titles"
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(titles, indent=0, sort_keys=True))
        self._titles = titles
        return titles

    def _load_index(self) -> List[Dict[str, Any]]:
        """Enumerate every archived legislation PDF, one entry per document."""
        if self._index_cache is not None:
            return self._index_cache

        rows = self._cdx_rows()
        logger.info(f"CDX: {len(rows)} PDF snapshot rows")

        docs: Dict[Tuple[str, str], Dict[str, Any]] = {}
        skipped_image_only = 0

        for ts, original, statuscode in rows:
            # Keep 200s and revisit records ("-"); drop archived error bodies.
            if statuscode not in ("200", "-"):
                continue
            if any(f"/LEGISLATION/{c}/" in original for c in IMAGE_ONLY):
                skipped_image_only += 1
                continue

            m = CURRENT_RE.match(original)
            if m:
                raw_coll, year, doc_id, filename = m.groups()
                scheme = 1
            else:
                m = LEGACY_RE.match(original)
                if not m:
                    continue
                raw_coll, doc_id = m.groups()
                filename, year, scheme = doc_id, "", 0

            collection = COLLECTION_OF[raw_coll.upper()]
            # Year 0000 holds stub PDFs ("This is a placeholder file only", or
            # a pointer to a federal act) with no legislative text.
            if year == "0000":
                continue

            key = (collection, doc_id)
            rank = (scheme, _version_of(filename), ts)
            entry = docs.get(key)
            if entry is None:
                docs[key] = {
                    "doc_id": doc_id,
                    "collection": collection,
                    "year": year or _year_from_doc_id(doc_id),
                    "pdf_filename": filename + ".pdf",
                    "url": original,
                    "captures": [ts],
                    "version": _version_of(filename),
                    "_rank": rank,
                }
            else:
                entry["captures"].append(ts)
                if rank > entry["_rank"]:
                    entry.update(
                        year=year or entry["year"],
                        pdf_filename=filename + ".pdf",
                        url=original,
                        version=_version_of(filename),
                        _rank=rank,
                    )

        if not docs:
            raise RuntimeError(
                "No Yukon legislation PDFs survived the CDX filter — the archived "
                "URL scheme has changed or every snapshot is an error body."
            )

        if skipped_image_only:
            self.record_coverage_gap(
                "historical_statutes+ncnr",
                "scanned page images with no text layer — need OCR",
                snapshots=skipped_image_only,
            )

        titles = self._load_titles()
        result = sorted(docs.values(), key=lambda d: (d["collection"], d["doc_id"]))

        # Doc ids are only unique within a collection. Keep the historical bare
        # id where it is unambiguous so previously-ingested rows are not
        # duplicated, and qualify only the losers of a collision.
        owner: Dict[str, str] = {}
        for collection in NAMESPACES:
            for d in result:
                if d["collection"] == collection:
                    owner.setdefault(d["doc_id"], collection)
        for d in result:
            d.pop("_rank", None)
            d["uid"] = (
                d["doc_id"]
                if owner[d["doc_id"]] == d["collection"]
                else f"{d['collection']}/{d['doc_id']}"
            )
            # Newest capture first; older ones are the fallback when a snapshot
            # turns out to be an error body rather than the PDF.
            d["captures"] = sorted(set(d["captures"]), reverse=True)[:4]
            d["title"] = titles.get(d["pdf_filename"], "")

        by_collection = {
            c: sum(1 for d in result if d["collection"] == c) for c in NAMESPACES
        }
        titled = sum(1 for d in result if d["title"])
        logger.info(
            f"Index: {len(result)} documents {by_collection}; "
            f"{titled} titled from index pages"
        )

        # Round-robin the collections so any prefix of the crawl (in particular
        # the sample cut, which the base scraper takes off the front) mixes
        # acts, regulations and orders rather than 15 zoning orders.
        buckets = [[d for d in result if d["collection"] == c] for c in NAMESPACES]
        interleaved = [d for row in zip_longest(*buckets) for d in row if d is not None]

        self._index_cache = interleaved
        return interleaved

    # ------------------------------------------------------------ fetching

    def _fetch_pdf(self, doc: Dict[str, Any]) -> Optional[bytes]:
        """Replay the archived PDF, walking back through older captures."""
        for ts in doc["captures"]:
            try:
                resp = self._get(WB_RAW.format(ts=ts, url=doc["url"]), tries=2)
            except Exception as e:
                logger.debug(f"  {doc['doc_id']} @{ts}: {e}")
                continue
            body = resp.content
            if len(body) > 100 and body[:5] == b"%PDF-":
                return body
            logger.debug(f"  {doc['doc_id']} @{ts}: not a PDF ({len(body)} bytes)")
        return None

    def normalize(self, raw: dict) -> dict:
        collection = raw.get("collection", "PRINCIPAL")
        year = raw.get("year") or ""
        return {
            "_id": f"CA/YukonLaws/{raw.get('uid') or raw['doc_id']}",
            "_source": "CA/YukonLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or raw["doc_id"],
            "collection": collection,
            "text": raw.get("_prefetched_text", ""),
            "date": f"{year}-01-01" if re.fullmatch(r"\d{4}", year) else None,
            "url": raw["url"],
            "archive_url": WB_RAW.format(ts=raw["captures"][0], url=raw["url"]),
            "doc_id": raw["doc_id"],
            "doc_type": DOC_TYPE_OF.get(collection, "regulation"),
            "version": raw.get("version", 0),
            "jurisdiction": "CA-YT",
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        all_docs = self._load_index()
        logger.info(f"Total documents to process: {len(all_docs)}")

        limit = 15 if sample else None
        count = 0
        skipped = 0
        consecutive_failures = 0

        for doc in all_docs:
            if limit and count >= limit:
                break

            doc_id = doc["doc_id"]
            pdf_bytes = self._fetch_pdf(doc)
            if pdf_bytes is None:
                logger.warning(f"  No usable capture for {doc_id}")
                skipped += 1
                consecutive_failures += 1
                if consecutive_failures >= 150:
                    raise RuntimeError(
                        f"{consecutive_failures} consecutive replay failures — "
                        "the Internet Archive is refusing this vantage or is down. "
                        "Aborting rather than reporting a truncated corpus."
                    )
                continue
            consecutive_failures = 0

            text = extract_pdf_markdown(
                source="CA/YukonLaws",
                source_id=doc_id,
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if len(text) < 200:
                logger.warning(f"  Skipping {doc_id} - no/short text ({len(text)} chars)")
                skipped += 1
                continue

            if len(text) < 400 and PLACEHOLDER_RE.search(text):
                logger.warning(f"  Skipping {doc_id} - placeholder stub, not legislative text")
                skipped += 1
                continue

            if not doc["title"]:
                doc["title"] = (
                    _title_from_text(text, doc["collection"])
                    or (f"Yukon Gazette {doc_id}" if doc["collection"] == "GAZETTES" else "")
                    or doc_id
                )

            doc["_prefetched_text"] = text
            yield doc
            count += 1
            logger.info(f"  [{count}] {doc['title'][:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count} ({skipped} skipped)")

        if count == 0:
            raise RuntimeError(
                "Crawl produced 0 records from a non-empty index — treat as a "
                "failure, not an empty corpus."
            )

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|test] [--sample] [--full]")
        sys.exit(1)

    scraper = YukonLawsScraper()
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing Yukon Laws access via the Internet Archive...")
        rows = scraper._cdx_rows()
        print(f"CDX rows: {len(rows)}")
        ts, url, _ = next(r for r in rows if r[2] == "200")
        body = scraper._get(WB_RAW.format(ts=ts, url=url)).content
        print(f"Sample replay {url.rsplit('/', 1)[-1]}: {len(body)} bytes, "
              f"magic={body[:5]!r}")
        ok = bool(rows) and body[:5] == b"%PDF-"
        print("Test PASSED" if ok else "Test FAILED")
        sys.exit(0 if ok else 1)
    elif command in ("bootstrap", "bootstrap-fast"):
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
