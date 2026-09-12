#!/usr/bin/env python3
"""
INTL/CaribbeanCourt -- Caribbean Court of Justice (CCJ) Judgments

Fetches full-text judgments of the Caribbean Court of Justice (ccj.org):
  - Appellate Jurisdiction (AJ): final court of appeal for CARICOM states that
    have acceded to it (Barbados, Belize, Guyana, Dominica, ...).
  - Original Jurisdiction (OJ): interprets the Revised Treaty of Chaguaramas.

Strategy:
  - Enumerate every PDF in the site's WordPress media library via the REST API
    (/wp-json/wp/v2/media?mime_type=application/pdf). This yields *live* PDF
    URLs, bypassing the stale hrefs (old www.ccj.org /
    caribbeancourtofjustice.org hostnames) that the on-page Ninja Tables still
    point at for older judgments.
  - Keep only files whose name matches the neutral-citation pattern
    `YYYY_CCJ_N_(AJ|OJ)` (case-insensitive; `_` or `-` separators), dedup
    re-uploaded variants (`-1`, `v1`, translations), and skip non-English
    translations (`-Dutch`, `-French`).
  - Enrich party names / delivery dates from the two on-page Ninja Tables
    (appellate table_id=8856, original table_id=18497) by matching citation.
  - Download each judgment PDF and extract full text (opendataloader / fitz).

Data Coverage:
  - ~500 judgments, 2007-present, born-digital PDFs with clean text layer.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Concurrent full pull (fleet)
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity / enumeration check
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.CaribbeanCourt")

BASE_URL = "https://ccj.org"
MEDIA_API = f"{BASE_URL}/wp-json/wp/v2/media"
NINJA_AJAX = f"{BASE_URL}/wp-admin/admin-ajax.php"
APPELLATE_TABLE_ID = 8856
ORIGINAL_TABLE_ID = 18497
CDX_API = "http://web.archive.org/cdx/search/cdx"
WAYBACK_REPLAY = "https://web.archive.org/web/{ts}id_/{url}"
# Both hostnames the court has published judgments under. The WordPress media
# `guid` keeps whichever host was live at upload time, and the pre-2019
# caribbeancourtofjustice.org uploads were NOT carried over to ccj.org (they
# 404 today), so those judgments are only recoverable from the Internet Archive.
CDX_DOMAINS = ("caribbeancourtofjustice.org", "ccj.org")
PER_PAGE = 100
MAX_PDF_BYTES = 60 * 1024 * 1024  # 60 MB
MIN_TEXT_CHARS = 200

# Judgment filename: 2025_CCJ_16_AJ_BZ.pdf / 2021-CCJ-8-AJ-BB.pdf / 2026_CCJ_3_OJ_RF.pdf
JUDG_RE = re.compile(
    r"((?:19|20)\d{2})[_\-]?CCJ[_\-]?(\d+)[_\-]?(AJ|OJ)", re.IGNORECASE
)
# Non-English translation markers to skip (English is the authoritative version)
TRANSLATION_RE = re.compile(r"(dutch|french|spanish|portuguese|traduction)", re.IGNORECASE)

# ISO-3166-2 style state suffixes used in CCJ citations -> jurisdiction hint
STATE_SUFFIX = {
    "BB": "BB", "BZ": "BZ", "GY": "GY", "DM": "DM", "TT": "TT",
    "JM": "JM", "SR": "SR", "LC": "LC", "SU": "SR", "RF": None,
}


class CaribbeanCourtScraper(BaseScraper):
    """Scraper for Caribbean Court of Justice judgments."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "application/json,text/html,*/*",
            "Accept-Language": "en",
        })
        self._ninja_meta: Optional[dict] = None
        self._wayback: Optional[dict] = None

    # ── HTTP helper with retry/backoff ───────────────────────────────

    def _get(self, url: str, params: dict = None, timeout: int = 40, tries: int = 4):
        last = None
        for attempt in range(tries):
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"HTTP {resp.status_code}")
                return resp
            except Exception as e:  # noqa: BLE001
                last = e
                time.sleep(min(2 ** attempt, 20))
        raise last

    # ── Ninja-table metadata (party names, delivery dates) ───────────

    def _load_ninja_meta(self) -> dict:
        """Build {normalized_citation: {parties, date, keywords}} from both tables."""
        if self._ninja_meta is not None:
            return self._ninja_meta
        meta: dict = {}
        for table_id in (APPELLATE_TABLE_ID, ORIGINAL_TABLE_ID):
            try:
                resp = self._get(NINJA_AJAX, params={
                    "action": "wp_ajax_ninja_tables_public_action",
                    "table_id": table_id,
                    "target_action": "get-all-data",
                    "default_sorting": "new_first",
                    "skip_rows": 0,
                    "limit_rows": 0,
                }, timeout=40)
                rows = resp.json()
            except Exception as e:  # noqa: BLE001
                logger.warning(f"  Ninja table {table_id} fetch failed: {e}")
                continue
            for row in rows:
                v = row.get("value", {})
                cite = unescape(re.sub(r"<[^>]+>", "", v.get("neutralcitationnumber", ""))).strip()
                cite = re.sub(r"\s+", " ", cite)
                key = self._cite_key(cite)
                if not key:
                    continue
                parties = self._row_parties(v)
                date = self._normalize_ninja_date(v.get("deliverydate", ""))
                kw = re.sub(r"<[^>]+>", " ", v.get("keywords", "") or "").strip()
                meta.setdefault(key, {
                    "citation": cite,
                    "parties": parties,
                    "date": date,
                    "keywords": kw,
                })
        logger.info(f"Loaded Ninja-table metadata for {len(meta)} citations")
        self._ninja_meta = meta
        return meta

    @staticmethod
    def _row_parties(v: dict) -> str:
        def clean(s):
            return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s or "")).strip()
        a = clean(v.get("appellant") or v.get("claimant") or "")
        b = clean(v.get("respondent") or v.get("defendant") or "")
        if a and b:
            return f"{a} v {b}"
        return a or b or clean(v.get("partiesdescription", ""))

    @staticmethod
    def _normalize_ninja_date(raw: str) -> Optional[str]:
        raw = (raw or "").strip()
        if not raw:
            return None
        raw = raw.replace("/", "-")
        for fmt in ("%Y-%m-%d", "%Y-%m-%d ", "%B %d, %Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(raw.strip(), fmt.strip()).strftime("%Y-%m-%d")
            except ValueError:
                continue
        m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", raw)
        if m:
            y, mo, d = (int(x) for x in m.groups())
            try:
                return datetime(y, mo, d).strftime("%Y-%m-%d")
            except ValueError:
                return None
        return None

    @staticmethod
    def _cite_key(cite: str) -> Optional[str]:
        """Normalize a citation / filename stem to a dedup key: (year, num, aj/oj)."""
        if not cite:
            return None
        m = re.search(r"((?:19|20)\d{2}).{0,4}CCJ.{0,4}?(\d+).{0,6}(AJ|OJ)", cite, re.I)
        if not m:
            return None
        return f"{m.group(1)}-{int(m.group(2))}-{m.group(3).upper()}"

    # ── Internet Archive index (for judgments dropped in the migration) ──

    def _load_wayback(self) -> dict:
        """Build {cite_key: [replay_url, ...]} from CDX captures of both hosts.

        One CDX query per host (not per document), so the whole fallback costs
        two requests regardless of how many judgments need it.
        """
        if self._wayback is not None:
            return self._wayback
        index: dict = {}
        for domain in CDX_DOMAINS:
            try:
                resp = self._get(CDX_API, params={
                    "url": domain,
                    "matchType": "domain",
                    "output": "json",
                    "fl": "original,timestamp,statuscode",
                    "filter": "mimetype:application/pdf",
                    "collapse": "urlkey",
                    "limit": 50000,
                }, timeout=120)
                rows = resp.json()
            except Exception as e:  # noqa: BLE001
                logger.warning(f"  CDX query for {domain} failed: {e}")
                continue
            for row in rows[1:]:  # row[0] is the header
                original, ts, status = row[0], row[1], row[2]
                # Keep 200 and revisit records ("-"); drop archived errors.
                if status not in ("200", "-"):
                    continue
                fn = original.rsplit("/", 1)[-1]
                if TRANSLATION_RE.search(fn):
                    continue
                key = self._cite_key(fn)
                if not key:
                    continue
                index.setdefault(key, []).append(
                    WAYBACK_REPLAY.format(ts=ts, url=original))
        logger.info(f"Wayback index: {len(index)} citations with archived PDFs")
        self._wayback = index
        return index

    @staticmethod
    def _url_rank(url: str) -> tuple:
        """Order candidates: live-host first, then base upload over re-uploads."""
        host_penalty = 0 if "//ccj.org" in url or "//www.ccj.org" in url else 1
        stem = url.rsplit("/", 1)[-1]
        return (host_penalty, len(stem), url)

    # ── Media enumeration (live PDF URLs) ────────────────────────────

    def _media_query(self, after_iso: str, before_iso: str) -> tuple[int, list]:
        """Single page-1 media query for an upload-date window. Returns (total, items)."""
        resp = self._get(MEDIA_API, params={
            "mime_type": "application/pdf",
            "per_page": PER_PAGE,
            "page": 1,
            "_fields": "guid,date",
            "after": after_iso,
            "before": before_iso,
        }, timeout=40)
        try:
            total = int(resp.headers.get("X-WP-Total", "0"))
        except (TypeError, ValueError):
            total = 0
        try:
            items = resp.json()
        except Exception:  # noqa: BLE001
            items = []
        if not isinstance(items, list):
            items = []
        return total, items

    def _iter_media_pdfs(self) -> Generator[dict, None, None]:
        """Yield every PDF in the media library via recursive date-window split.

        The WP media REST `page` pagination is unreliable past ~130 items (the
        endpoint reports 4000+ total but only serves the first ~1-2 pages), so
        instead we recurse on the upload-date range, halving any window whose
        X-WP-Total exceeds one page. Every leaf query is therefore page-1 only.
        """
        seen: set[str] = set()
        start = datetime(2004, 1, 1)
        end = datetime.now(timezone.utc).replace(tzinfo=None).replace(hour=23) \
            + timedelta(days=1)
        stack = [(start, end, 0)]
        windows = 0
        while stack:
            a, b, depth = stack.pop()
            if a >= b:
                continue
            a_iso = a.strftime("%Y-%m-%dT%H:%M:%S")
            b_iso = b.strftime("%Y-%m-%dT%H:%M:%S")
            try:
                total, items = self._media_query(a_iso, b_iso)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"  media window {a_iso[:10]}..{b_iso[:10]} failed: {e}")
                continue
            windows += 1
            # Split if over one page AND the window is still divisible.
            if total > PER_PAGE and depth < 16 and (b - a).total_seconds() > 86400:
                mid = a + (b - a) / 2
                stack.append((mid, b, depth + 1))
                stack.append((a, mid, depth + 1))
                continue
            if total > PER_PAGE:
                logger.warning(
                    f"  window {a_iso[:10]}..{b_iso[:10]} has {total} PDFs but "
                    f"cannot split further; only 100 captured")
            for it in items:
                url = it.get("guid", {}).get("rendered", "")
                if not url.lower().endswith(".pdf") or url in seen:
                    continue
                seen.add(url)
                yield {
                    "url": url,
                    "filename": url.rsplit("/", 1)[-1],
                    "wp_date": it.get("date", ""),
                }
            time.sleep(0.2)
        logger.info(f"Media enumeration: {windows} windows, {len(seen)} unique PDFs")

    def _collect_judgments(self) -> list[dict]:
        """Enumerate media, keep judgment PDFs, dedup variants, enrich metadata.

        Every URL variant of a judgment is kept as a download candidate: the
        media library advertises re-uploads and dead pre-migration hostnames
        side by side, and only trying the PDF tells you which one still serves.
        """
        ninja = self._load_ninja_meta()
        best: dict = {}  # cite_key -> chosen record
        for m in self._iter_media_pdfs():
            fn = m["filename"]
            jm = JUDG_RE.search(fn)
            if not jm:
                continue
            if TRANSLATION_RE.search(fn):
                continue
            year, num, jt = jm.group(1), int(jm.group(2)), jm.group(3).upper()
            key = f"{year}-{num}-{jt}"
            stem = fn[:-4]
            # state suffix (chars after the AJ/OJ token)
            tail = fn[jm.end():]
            state_m = re.search(r"[_\-]?([A-Z]{2})", tail)
            state = state_m.group(1) if state_m else None
            record = {
                "cite_key": key,
                "citation": f"[{year}] CCJ {num} ({jt})" + (f" {state}" if state else ""),
                "year": year,
                "number": num,
                "jurisdiction_type": jt,
                "state": state,
                "url": m["url"],
                "filename": fn,
                "wp_date": m["wp_date"],
            }
            enr = ninja.get(key)
            if enr:
                record["parties"] = enr.get("parties", "")
                record["ninja_date"] = enr.get("date")
                record["keywords"] = enr.get("keywords", "")
                if enr.get("citation"):
                    record["citation"] = enr["citation"].strip()
            prev = best.get(key)
            if prev is None:
                record["urls"] = [m["url"]]
                best[key] = record
                continue
            prev["urls"].append(m["url"])
            # Keep the richest metadata across variants.
            if not prev.get("parties") and record.get("parties"):
                prev.update({k: record[k] for k in
                             ("parties", "ninja_date", "keywords", "citation")
                             if k in record})
            if len(stem) < len(prev["filename"][:-4]):
                prev["filename"] = fn

        archived = self._load_wayback()
        for key, record in best.items():
            record["urls"].sort(key=self._url_rank)
            record["archive_urls"] = archived.get(key, [])
            record["url"] = record["urls"][0]
        judgments = sorted(best.values(), key=lambda r: (r["year"], r["number"]))
        with_archive = sum(1 for r in judgments if r["archive_urls"])
        logger.info(f"Collected {len(judgments)} unique CCJ judgments "
                    f"({with_archive} with an Internet Archive fallback)")
        return judgments

    # ── PDF download + text extraction ───────────────────────────────

    def _download_pdf(self, url: str) -> Optional[bytes]:
        try:
            resp = self._get(url, timeout=90)
            if resp.status_code != 200:
                return None
            content = resp.content
            if content[:5] != b"%PDF-":
                logger.warning(f"  not a PDF (magic bytes) {url}")
                return None
            if len(content) > MAX_PDF_BYTES:
                logger.warning(f"  PDF too large ({len(content)} bytes) {url}")
                return None
            return content
        except Exception as e:  # noqa: BLE001
            logger.error(f"  download failed {url}: {e}")
            return None

    def _extract_fallback(self, pdf_bytes: bytes) -> Optional[str]:
        try:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            txt = "\n".join(p.get_text() for p in doc)
            doc.close()
            if txt.strip():
                return txt
        except Exception:  # noqa: BLE001
            pass
        for mod in ("pdfplumber", "pypdf"):
            try:
                if mod == "pdfplumber":
                    import pdfplumber
                    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                        t = "\n\n".join((p.extract_text() or "") for p in pdf.pages)
                else:
                    from pypdf import PdfReader
                    r = PdfReader(io.BytesIO(pdf_bytes))
                    t = "\n\n".join((p.extract_text() or "") for p in r.pages)
                if t.strip():
                    return t
            except Exception:  # noqa: BLE001
                continue
        return None

    # ── Main fetch methods ───────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        for rec in self._collect_judgments():
            yield rec

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        for rec in self._collect_judgments():
            wp = rec.get("wp_date", "")
            if wp:
                try:
                    dt = datetime.fromisoformat(wp.replace("Z", "+00:00")).replace(tzinfo=None)
                    if dt < since.replace(tzinfo=None):
                        continue
                except (ValueError, TypeError):
                    pass
            yield rec

    def normalize(self, raw: dict) -> Optional[dict]:
        """Download the judgment PDF and build the normalized record.

        Heavy work lives here so bootstrap_fast parallelizes downloads.
        """
        candidates = raw.get("urls") or ([raw["url"]] if raw.get("url") else [])
        candidates = candidates + list(raw.get("archive_urls") or [])
        if not candidates:
            return None
        source_id = raw["cite_key"].lower().replace("-", "_")
        for url in candidates:
            pdf_bytes = self._download_pdf(url)
            if pdf_bytes:
                break
        else:
            logger.warning(
                f"  no live or archived PDF for {raw.get('citation')} "
                f"({len(candidates)} URLs tried), skipping")
            return None
        text = extract_pdf_markdown(
            source="INTL/CaribbeanCourt",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        )
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            text = self._extract_fallback(pdf_bytes)
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            logger.warning(f"  insufficient text for {raw.get('citation')}, skipping")
            return None
        text = text.strip()

        # date: Ninja delivery date > first-page date in PDF > wp_date > Jan 1.
        # A CCJ neutral citation carries the delivery year, so any candidate
        # from a different year is a mis-read (a cited case's date, or the
        # re-upload timestamp of a migrated file) and must be discarded.
        wp = raw.get("wp_date", "")
        date = None
        for candidate in (raw.get("ninja_date"),
                          self._date_from_text(text[:2000]),
                          wp[:10] if wp and not wp.startswith("1970") else None):
            if candidate and candidate[:4] == raw["year"]:
                date = candidate
                break
        if not date:
            date = f"{raw['year']}-01-01"

        citation = raw.get("citation", "").strip()
        parties = (raw.get("parties") or "").strip()
        jt_full = "Appellate Jurisdiction" if raw["jurisdiction_type"] == "AJ" else "Original Jurisdiction"
        if parties:
            title = f"{citation} — {parties}"
        else:
            title = f"{citation} ({jt_full})"

        return {
            "_id": f"ccj-{source_id}",
            "_source": "INTL/CaribbeanCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": text,
            "date": date,
            "url": url,
            "citation": citation,
            "jurisdiction_type": jt_full,
            "parties": parties,
            "state": raw.get("state") or "",
            "keywords": raw.get("keywords", ""),
            "pdf_url": url,
            "canonical_url": raw.get("url", url),
        }

    @staticmethod
    def _date_from_text(head: str) -> Optional[str]:
        # e.g. "Delivered on the 20th day of November 2025" or "20 November 2025"
        months = ("January February March April May June July August "
                  "September October November December").split()
        mmap = {m.lower(): i + 1 for i, m in enumerate(months)}
        m = re.search(r"(\d{1,2})(?:st|nd|rd|th)?\s+(?:day\s+of\s+)?([A-Za-z]+)[,\s]+((?:19|20)\d{2})", head)
        if m and m.group(2).lower() in mmap:
            d, mo, y = int(m.group(1)), mmap[m.group(2).lower()], int(m.group(3))
            try:
                return datetime(y, mo, d).strftime("%Y-%m-%d")
            except ValueError:
                return None
        return None


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = CaribbeanCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        judgments = scraper._collect_judgments()
        print(f"Enumerated {len(judgments)} unique CCJ judgments")
        for r in judgments[:3] + judgments[-3:]:
            print(f"  {r['citation']:<28} {r['filename']}")
        sys.exit(0)

    if command in ("bootstrap", "bootstrap-fast"):
        if command == "bootstrap-fast" and not sample:
            result = scraper.bootstrap_fast()
        else:
            result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
