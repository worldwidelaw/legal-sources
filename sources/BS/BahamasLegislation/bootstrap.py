#!/usr/bin/env python3
"""
BS/BahamasLegislation -- Bahamas Legislation Online (iLAWS)

Fetches full-text legislation from The Bahamas official legislation portal.

Data access (live-first, Internet-Archive fallback):
  1. LIVE: alphabetical index at
     laws.bahamas.gov.bs/cms/legislation/acts_only/by-alphabetical-order.html
     navigated letter-by-letter with a POST (submit4=<LETTER>).
  2. ARCHIVE: since 2026-08 laws.bahamas.gov.bs answers foreign/datacenter
     clients with HTTP 403 ("Access to this page is forbidden") or an HTTP 202
     interstitial, so the corpus is read from the Wayback Machine instead:
       - CDX-enumerate /cms/images/LEGISLATION/{CATEGORY}/{year}/{year-NNNN}/*.pdf
       - recover titles from archived captures of the alphabetical index pages
         (they carry <a class="npWrap" href="...pdf">Title</a>), falling back to
         the CamelCase PDF filename
       - replay each PDF through /web/{ts}id_/ and extract full text

  PDF files live at /cms/images/LEGISLATION/<CATEGORY>/<year>/<year-number>/<file>.pdf
  Full text is extracted from the PDFs via common/pdf_extract.

Usage:
  python bootstrap.py bootstrap          # Full pull -> data/records.jsonl
  python bootstrap.py bootstrap --sample # Fetch 15 sample records -> sample/
  python bootstrap.py bootstrap-fast     # Alias of the full pull (fleet entrypoint)
  python bootstrap.py update             # Incremental (not supported, runs full)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BS.BahamasLegislation")

BASE_URL = "https://laws.bahamas.gov.bs"
ALPHA_URL = f"{BASE_URL}/cms/legislation/acts_only/by-alphabetical-order.html"
DELAY = 2.0
LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

CDX_URL = "http://web.archive.org/cdx/search/cdx"
WAYBACK_URL = "https://web.archive.org/web/{ts}id_/{url}"
WAYBACK_LATEST_TS = "2026"
# Archived index captures parsed for act titles (each capture holds ~90 titles).
MAX_INDEX_CAPTURES = 150

# Regex to extract act title and PDF URL from the HTML index
# Pattern: <a class="npWrap" href="/cms/images/LEGISLATION/PRINCIPAL/..." target="_blank">Title&nbsp;...
ACT_PATTERN = re.compile(
    r'<a\s+class="npWrap"\s+href="(/cms\d?/images/LEGISLATION/[^"]+\.pdf)"'
    r'[^>]*>([^<]+?)(?:&nbsp;|\s*<)',
    re.IGNORECASE,
)

# Extract category, year and act number from the PDF path
# e.g., /cms/images/LEGISLATION/PRINCIPAL/2014/2014-0047/2014-0047_2.pdf
PATH_PATTERN = re.compile(
    r'/cms\d?/images/LEGISLATION/(PRINCIPAL|AMENDING|SUBORDINATE|BILLS)'
    r'/(\d{4})/(\d{4}-\d{4})/([^/?]+\.pdf)$',
    re.IGNORECASE,
)
# Legacy pattern kept for the live index (principal acts only).
PRINCIPAL_PATH_PATTERN = re.compile(r'/(PRINCIPAL|AMENDING|SUBORDINATE|BILLS)/(\d{4})/(\d{4}-\d{4})',
                                    re.IGNORECASE)

# Categories in the order we prefer when the same act number appears twice.
CATEGORY_PRIORITY = {"PRINCIPAL": 0, "AMENDING": 1, "BILLS": 2, "SUBORDINATE": 3}

BLOCK_MARKERS = ("403 - Forbidden", "Access to this page is forbidden")


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
        "Accept": "text/html,application/xhtml+xml",
    })
    return session


def camel_to_title(stem: str) -> str:
    """'AbolitionofMandatoryMinimumSentencesAct2014_1' -> 'Abolition of Mandatory ...'."""
    stem = re.sub(r'_\d+$', '', stem)
    spaced = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', ' ', stem)
    spaced = re.sub(r'(?<=[A-Za-z])(?=\d{4}$)', ' ', spaced)
    spaced = spaced.replace('_', ' ').replace('-', ' ')
    return re.sub(r'\s+', ' ', spaced).strip()


class BahamasLegislationFetcher:
    SOURCE_ID = "BS/BahamasLegislation"

    def __init__(self):
        self.session = get_session()
        self.archive_mode = False
        self.max_index_captures = MAX_INDEX_CAPTURES

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, *, timeout: int = 90, tries: int = 4) -> Optional[requests.Response]:
        """GET with backoff. Returns None on 404 or exhausted retries."""
        delay = 4
        for attempt in range(1, tries + 1):
            try:
                resp = self.session.get(url, timeout=timeout)
            except requests.RequestException as exc:
                logger.warning("Request error (%d/%d) %s: %s", attempt, tries, url, exc)
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if (retry_after or "").isdigit() else delay
                logger.warning("HTTP %d (%d/%d) %s; retrying in %ds",
                               resp.status_code, attempt, tries, url, wait)
                time.sleep(min(wait, 120))
                delay = min(delay * 2, 60)
                continue
            logger.warning("HTTP %d for %s", resp.status_code, url)
            return None
        return None

    def _wayback_get(self, url: str, ts: str = WAYBACK_LATEST_TS) -> Optional[requests.Response]:
        return self._get(WAYBACK_URL.format(ts=ts, url=url))

    def _cdx(self, url: str, match_type: str = "prefix", limit: int = 60000) -> List[List[str]]:
        query = (f"{CDX_URL}?url={url}&matchType={match_type}"
                 f"&fl=original,timestamp,statuscode&limit={limit}")
        resp = self._get(query, timeout=180, tries=5)
        if resp is None:
            return []
        return [p for p in (line.split() for line in resp.text.splitlines()) if len(p) == 3]

    # ------------------------------------------------------------------
    # Live index
    # ------------------------------------------------------------------

    def _fetch_letter_page(self, letter: str) -> str:
        """Fetch the HTML for a given letter page via POST."""
        data = {
            "submit4": letter,
            "pointintime_post": datetime.now().strftime("%Y-%m-%d 00:00:00"),
            "pointintime_post_alpha": datetime.now().strftime("%Y-%m-%d 00:00:00"),
        }
        for attempt in range(3):
            try:
                resp = self.session.post(ALPHA_URL, data=data, timeout=60)
                if resp.status_code == 200 and not self._looks_blocked(resp.text):
                    return resp.text
                if resp.status_code in (202, 403) or self._looks_blocked(resp.text):
                    logger.warning("laws.bahamas.gov.bs returned HTTP %d / block page for letter %s "
                                   "— switching to Internet Archive", resp.status_code, letter)
                    self.archive_mode = True
                    return ""
                logger.warning("HTTP %d for letter %s (attempt %d)",
                               resp.status_code, letter, attempt + 1)
            except requests.RequestException as e:
                logger.warning("Request error for letter %s (attempt %d): %s", letter, attempt + 1, e)
            time.sleep(5 * (attempt + 1))
        return ""

    @staticmethod
    def _looks_blocked(html: str) -> bool:
        head = html[:4000]
        return any(marker in head for marker in BLOCK_MARKERS)

    def _parse_acts_from_html(self, html: str) -> List[Dict[str, str]]:
        """Extract act titles and PDF URLs from index HTML."""
        acts = []
        seen_paths = set()
        for match in ACT_PATTERN.finditer(html):
            pdf_path = match.group(1)
            title = match.group(2).strip()
            if pdf_path in seen_paths:
                continue
            seen_paths.add(pdf_path)

            path_match = PRINCIPAL_PATH_PATTERN.search(pdf_path)
            category = path_match.group(1).upper() if path_match else "PRINCIPAL"
            year = path_match.group(2) if path_match else None
            act_number = path_match.group(3) if path_match else None

            acts.append({
                "title": title,
                "pdf_path": pdf_path,
                "pdf_url": f"{BASE_URL}{pdf_path}",
                "year": year,
                "act_number": act_number,
                "category": category,
                "archived": False,
            })
        return acts

    # ------------------------------------------------------------------
    # Internet Archive
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_archived_url(original: str) -> Optional[str]:
        """Strip scheme/host/port/query from an archived URL; keep the /cms path."""
        path = re.sub(r'^https?://laws\.bahamas\.gov\.bs(:80)?', '', original)
        path = path.split('?')[0].split('#')[0]
        if not path.startswith('/cms'):
            return None
        return path

    def _archive_title_map(self) -> Dict[str, str]:
        """Harvest act titles from archived captures of the alphabetical indexes."""
        rows = self._cdx("laws.bahamas.gov.bs/cms")
        wanted = re.compile(
            r'(all-legislation|by-alphabetical|acts_only|acts_alpha|acts-by-year|'
            r'statutes|by-title|laws\.html|index_all)', re.IGNORECASE)
        captures = []
        for original, timestamp, status in rows:
            if status not in ("200", "-"):
                continue
            if original.lower().endswith(('.pdf', '.css', '.js', '.png', '.jpg', '.svg', '.gif')):
                continue
            if not wanted.search(original):
                continue
            captures.append((original, timestamp))

        # Newest captures first — the later the capture, the fuller the index.
        captures.sort(key=lambda c: c[1], reverse=True)
        captures = captures[:self.max_index_captures]
        logger.info("Internet Archive: parsing %d archived index captures for titles",
                    len(captures))

        titles: Dict[str, str] = {}
        for i, (original, timestamp) in enumerate(captures, 1):
            resp = self._wayback_get(original, ts=timestamp)
            if resp is None:
                continue
            found = 0
            for act in self._parse_acts_from_html(resp.text):
                key = self._normalize_archived_url(BASE_URL + act["pdf_path"])
                if key and key not in titles:
                    titles[key] = act["title"]
                    found += 1
                if act.get("act_number"):
                    akey = f"{act['category']}:{act['act_number']}"
                    titles.setdefault(akey, act["title"])
            if i % 25 == 0:
                logger.info("  ...%d/%d captures, %d titles so far", i, len(captures), len(titles))
        logger.info("Internet Archive: recovered %d titles", len(titles))
        return titles

    def _archive_documents(self) -> List[Dict[str, Any]]:
        """Enumerate every archived legislation PDF, one entry per act number."""
        rows = self._cdx("laws.bahamas.gov.bs")
        best: Dict[tuple, Dict[str, Any]] = {}
        for original, timestamp, status in rows:
            if status not in ("200", "-"):
                continue
            path = self._normalize_archived_url(original)
            if not path:
                continue
            match = PATH_PATTERN.search(path)
            if not match:
                continue
            category, year, act_number, filename = match.groups()
            category = category.upper()
            key = (category, act_number)
            # Prefer a filename that carries the act title over the bare number,
            # then the most recent capture.
            has_words = bool(re.search(r'[A-Za-z]{4}', filename.rsplit('.', 1)[0]))
            candidate = {
                "path": path,
                "pdf_url": BASE_URL + path,
                "capture_ts": timestamp,
                "category": category,
                "year": year,
                "act_number": act_number,
                "filename": filename,
                "archived": True,
                "_rank": (1 if has_words else 0, timestamp),
            }
            current = best.get(key)
            if current is None or candidate["_rank"] > current["_rank"]:
                best[key] = candidate

        docs = sorted(best.values(), key=lambda d: (d["year"], d["act_number"], d["category"]))
        logger.info("Internet Archive: %d distinct legislation documents", len(docs))
        return docs

    def _fetch_archive_records(self, limit: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        docs = self._archive_documents()
        if not docs:
            raise RuntimeError(
                "Wayback CDX returned no legislation PDFs for laws.bahamas.gov.bs — "
                "cannot enumerate the corpus"
            )
        titles = self._archive_title_map() if limit is None or limit > 0 else {}

        emitted = 0
        for doc in docs:
            if limit is not None and emitted >= limit:
                return
            title = (titles.get(doc["path"])
                     or titles.get(f"{doc['category']}:{doc['act_number']}"))
            if not title:
                stem = doc["filename"].rsplit('.', 1)[0]
                if re.fullmatch(r'\d{4}-\d{4}(_\d+)?', stem):
                    label = "Statutory Instrument" if doc["category"] == "SUBORDINATE" else "Act"
                    number = doc["act_number"].split('-')[1].lstrip('0') or '0'
                    title = f"{label} No. {number} of {doc['year']}"
                else:
                    title = camel_to_title(stem)
            doc["title"] = title
            emitted += 1
            yield doc

    def _fetch_pdf_bytes(self, doc: Dict[str, Any]) -> Optional[bytes]:
        """Download a PDF live, or replay it from the archive."""
        if not doc.get("archived") and not self.archive_mode:
            resp = self._get(doc["pdf_url"], tries=2)
            if resp is not None and resp.content[:4] == b"%PDF":
                return resp.content
            self.archive_mode = True

        for ts in [t for t in (doc.get("capture_ts"), WAYBACK_LATEST_TS) if t]:
            resp = self._wayback_get(doc["pdf_url"], ts=ts)
            if resp is not None and resp.content[:4] == b"%PDF":
                return resp.content
        return None

    # ------------------------------------------------------------------
    # Records
    # ------------------------------------------------------------------

    def _doc_id(self, raw: Dict[str, Any]) -> str:
        act_number = raw.get("act_number") or ""
        if not act_number:
            return f"BS-{raw['title'][:80]}"
        if raw.get("category", "PRINCIPAL").upper() == "SUBORDINATE":
            return f"BS-SI-{act_number}"
        return f"BS-{act_number}"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw act data into the standard schema."""
        return {
            "_id": self._doc_id(raw),
            "_source": self.SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": f"{raw['year']}-01-01" if raw.get("year") else None,
            "year": int(raw["year"]) if raw.get("year") else None,
            "act_number": raw.get("act_number", ""),
            "category": raw.get("category", "PRINCIPAL").title(),
            "url": raw["pdf_url"],
            "country": "BS",
            "jurisdiction": "Bahamas",
            "language": "en",
        }

    def _iter_raw(self, limit: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        """Yield raw documents: live index first, Internet Archive as fallback."""
        emitted = 0
        seen = set()

        if not self.archive_mode:
            for letter in LETTERS:
                if limit is not None and emitted >= limit:
                    return
                logger.info("Fetching acts for letter %s...", letter)
                html = self._fetch_letter_page(letter)
                if self.archive_mode:
                    break
                if not html:
                    logger.warning("No HTML returned for letter %s, skipping", letter)
                    continue
                time.sleep(DELAY)
                acts = self._parse_acts_from_html(html)
                logger.info("  Found %d acts for letter %s", len(acts), letter)
                for act in acts:
                    if limit is not None and emitted >= limit:
                        return
                    if act["pdf_url"] in seen:
                        continue
                    seen.add(act["pdf_url"])
                    emitted += 1
                    yield act

        if emitted:
            return

        logger.info("Falling back to the Internet Archive for enumeration")
        for doc in self._fetch_archive_records(limit=limit):
            emitted += 1
            yield doc

        if emitted == 0:
            raise RuntimeError(
                "Enumerated 0 Bahamian acts from both laws.bahamas.gov.bs and the "
                "Internet Archive — refusing to report a silent success"
            )

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Yield normalized records with full text."""
        total = 0
        skipped = 0
        # Over-fetch in sample mode: some PDFs are scanned images with no text.
        limit = 45 if sample else None
        if sample:
            self.max_index_captures = 20
        for raw in self._iter_raw(limit=limit):
            doc_id = self._doc_id(raw)
            pdf_bytes = self._fetch_pdf_bytes(raw)
            if not pdf_bytes:
                skipped += 1
                logger.warning("  No PDF bytes for %s (%s)", doc_id, raw["title"][:70])
                continue
            text = extract_pdf_markdown(
                source=self.SOURCE_ID,
                source_id=doc_id,
                pdf_bytes=pdf_bytes,
                table="legislation",
                force=True,
            )
            if not text or len(text) < 50:
                skipped += 1
                logger.warning("  Skipping '%s' (no text extracted from PDF)", raw["title"][:70])
                continue

            raw["text"] = text
            yield self.normalize(raw)
            total += 1

            if sample and total >= 15:
                logger.info("Sample mode: reached %d records, stopping", total)
                return
            if total % 50 == 0:
                logger.info("Progress: %d records (%d skipped)", total, skipped)

        logger.info("Total acts fetched: %d (%d skipped)", total, skipped)
        if total == 0:
            raise RuntimeError("Fetched 0 Bahamian acts with full text")

    def fetch_updates(self, since: Optional[datetime] = None) -> Generator[Dict[str, Any], None, None]:
        """No incremental endpoint available — runs full fetch."""
        logger.info("No incremental endpoint; running full fetch")
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            html = self._fetch_letter_page("A")
            acts = self._parse_acts_from_html(html) if html else []
            if acts:
                logger.info("Live test passed: found %d acts for letter A", len(acts))
                return True
            docs = self._archive_documents()
            logger.info("Live index unavailable; Internet Archive holds %d legislation PDFs",
                        len(docs))
            return len(docs) > 0
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    parser = argparse.ArgumentParser(description="BS/BahamasLegislation bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    fetcher = BahamasLegislationFetcher()
    source_dir = Path(__file__).parent

    if args.command == "test":
        success = fetcher.test()
        sys.exit(0 if success else 1)

    if args.command in ("bootstrap", "bootstrap-fast"):
        sample = args.sample and args.command == "bootstrap"
        if sample:
            sample_dir = source_dir / "sample"
            sample_dir.mkdir(exist_ok=True)
            count = 0
            for record in fetcher.fetch_all(sample=True):
                fname = re.sub(r'[^\w\-]', '_', record["_id"])[:100] + ".json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info("[%d] Saved: %s (%d chars)", count, record["title"], len(record["text"]))
            logger.info("Sample bootstrap complete: %d records saved to %s", count, sample_dir)
            if count == 0:
                raise RuntimeError("Sample bootstrap produced 0 records")
            return

        data_dir = source_dir / "data"
        data_dir.mkdir(exist_ok=True)
        out_path = data_dir / "records.jsonl"
        count = 0
        with open(out_path, "w", encoding="utf-8") as fh:
            for record in fetcher.fetch_all():
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                fh.flush()
                count += 1
        logger.info("Bootstrap complete: %d records written to %s", count, out_path)

    elif args.command == "update":
        data_dir = source_dir / "data"
        data_dir.mkdir(exist_ok=True)
        out_path = data_dir / "records.jsonl"
        count = 0
        with open(out_path, "w", encoding="utf-8") as fh:
            for record in fetcher.fetch_updates():
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
        logger.info("Update complete: %d records written to %s", count, out_path)


if __name__ == "__main__":
    main()
