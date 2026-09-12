#!/usr/bin/env python3
"""
DK/Retsinformation -- Denmark Official Law Database Fetcher

Fetches Danish legislation via sitemap discovery + ELI XML full text.

Strategy:
  - Parse sitemap index (21 pages, ~200K document URLs, newest-first)
  - Each sitemap entry carries a <lastmod>, which is what makes an
    incremental refresh possible: --since only fetches what changed.
  - Download XML for each via {eli_url}/xml, in parallel
  - Parse XML body (non-<Meta> children of <Dokument>) for full text
  - Fall back to the site's own document API when the ELI XML carries no
    body. Roughly half the corpus (the older half of the sitemap) serves a
    Meta-only XML stub, or 404s on /xml entirely, while the full text is
    there under POST /api/document/eli/{path} -> documentHtml. Trusting
    /xml alone silently dropped ~109K documents (#1549).
  - Completed ELI paths are checkpointed, so a re-launched run resumes
    instead of re-walking the whole space.
  - For recent updates: harvest API (last 10 days only, 1 req/10s)

Two hosts with very different limits:
  - api.retsinformation.dk (harvest API): hard 1 req / 10 s
  - retsinformation.dk (sitemap + ELI /xml): serves ~20 req/s happily

Usage:
  python bootstrap.py bootstrap                 # Full bootstrap via sitemap
  python bootstrap.py bootstrap --sample        # Fetch 15 sample records
  python bootstrap.py bootstrap-fast            # Alias used by the fleet runner
  python bootstrap.py bootstrap --since 2026-07-26   # Only docs changed since
  python bootstrap.py update                    # Harvest API, last 10 days
  python bootstrap.py test                      # Quick connectivity test
"""

import sys
import json
import logging
import threading
import time
import re
import xml.etree.ElementTree as ET
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DK.Retsinformation")

SITEMAP_INDEX = "https://www.retsinformation.dk/sitemap.xml"
HARVEST_API = "https://api.retsinformation.dk/v1/Documents"
# The site's own document endpoint, used by the retsinformation.dk SPA.
# POST {"isRawHtml": false} -> [{accessionNumber, title, documentHtml, ...}]
DOCUMENT_API = "https://www.retsinformation.dk/api/document/eli/"

MIN_TEXT_CHARS = 100

# retsinformation.dk (the crawl host) is comfortable well above this; the
# 1 req/10s figure in the docs applies to api.retsinformation.dk only.
CRAWL_REQUESTS_PER_SECOND = 10.0
HARVEST_MIN_GAP = 10.0

CHECKPOINT_FLUSH_EVERY = 250

# Tags that end a line in the documentHtml the document API returns.
_HTML_BLOCK_TAGS = {
    "div", "p", "br", "tr", "li", "table", "h1", "h2", "h3", "h4", "h5", "h6",
}


class _HtmlToText(HTMLParser):
    """Minimal HTML -> plain text, keeping block-level line breaks."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._parts: List[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip += 1
        elif tag in _HTML_BLOCK_TAGS:
            self._parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = max(0, self._skip - 1)
        elif tag in _HTML_BLOCK_TAGS:
            self._parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._parts.append(data)

    def text(self) -> str:
        out = "".join(self._parts)
        out = out.replace("\xa0", " ")
        out = re.sub(r"[ \t\r]+", " ", out)
        out = re.sub(r" *\n *", "\n", out)
        # Retsinformation wraps every physical line in its own block element,
        # so open+close tags would otherwise double-space the whole document.
        return re.sub(r"\n{2,}", "\n", out).strip()


def html_to_text(html: str) -> str:
    parser = _HtmlToText()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        pass
    return parser.text()


def _dk_date_to_iso(value: str) -> str:
    """'14/06/1995' -> '1995-06-14'. Returns '' when unparseable."""
    m = re.match(r"\s*(\d{2})/(\d{2})/(\d{4})", value or "")
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else ""

SITEMAP_ENTRY_RE = re.compile(
    r"<loc>\s*(https?://(?:www\.)?retsinformation\.dk/eli/[^<]+?)\s*</loc>"
    r"(?:\s*<lastmod>\s*([\dT:+-]+)\s*</lastmod>)?",
    re.IGNORECASE,
)


class RetsinformationScraper(BaseScraper):
    """Scraper for DK/Retsinformation -- Danish legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._last_harvest_request = 0.0
        self._crawl_lock = threading.Lock()
        self._crawl_next_slot = 0.0
        # v2: a pre-#1549 checkpoint marks the ~109K API-only documents as
        # processed, so reusing it would permanently bake in the coverage
        # hole. The version bump makes those runs restart instead of resume.
        self._checkpoint_path = self.source_dir / "data" / "eli_checkpoint_v2.txt"
        self._checkpoint_fh = None
        # Set when discovery succeeded but the checkpoint already covers
        # everything — a no-op resume, not a failed run.
        self.nothing_left_to_fetch = False

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _crawl_throttle(self):
        """Global token spacing across all worker threads for the crawl host."""
        gap = 1.0 / CRAWL_REQUESTS_PER_SECOND
        with self._crawl_lock:
            now = time.monotonic()
            slot = max(now, self._crawl_next_slot)
            self._crawl_next_slot = slot + gap
        delay = slot - now
        if delay > 0:
            time.sleep(delay)

    def _http_get(self, url: str, min_gap: Optional[float] = None) -> Optional[str]:
        """HTTP GET with rate limiting and retries. Returns None on 404."""
        import urllib.request

        if min_gap is None:
            self._crawl_throttle()
        else:
            elapsed = time.time() - self._last_harvest_request
            if elapsed < min_gap:
                time.sleep(min_gap - elapsed)
            self._last_harvest_request = time.time()

        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={
                    "User-Agent": "LegalDataHunter/1.0 (open-data-research)",
                    "Accept": "application/xml, text/xml, application/json, */*",
                })
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                msg = str(e)
                if "404" in msg:
                    return None
                if "429" in msg or "503" in msg:
                    logger.warning("Throttled on %s, backing off", url[:80])
                    time.sleep(15 * (attempt + 1))
                else:
                    logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                    time.sleep(2 * (attempt + 1))
        return None

    def _http_post_json(self, url: str, payload: Dict[str, Any]) -> Optional[Any]:
        """POST JSON to the crawl host. Returns parsed JSON, or None on 404."""
        import urllib.request

        self._crawl_throttle()
        body = json.dumps(payload).encode("utf-8")

        for attempt in range(3):
            try:
                req = urllib.request.Request(url, data=body, method="POST", headers={
                    "User-Agent": "LegalDataHunter/1.0 (open-data-research)",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                })
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read().decode("utf-8", errors="replace"))
            except json.JSONDecodeError as e:
                logger.warning(f"Non-JSON from {url[:80]}: {e}")
                return None
            except Exception as e:
                msg = str(e)
                # 400/404/405 mean "no document here" — retrying cannot help.
                if any(code in msg for code in ("400", "404", "405")):
                    return None
                if "429" in msg or "503" in msg:
                    logger.warning("Throttled on %s, backing off", url[:80])
                    time.sleep(15 * (attempt + 1))
                else:
                    logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                    time.sleep(2 * (attempt + 1))
        return None

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def _get_sitemap_entries(self) -> List[Tuple[str, str]]:
        """Parse the sitemap index and all pages -> [(eli_url, lastmod)].

        Order is preserved: the sitemap is newest-first, which is what we
        want when a run is cut short by the fleet's wall-clock cap.
        """
        index_xml = self._http_get(SITEMAP_INDEX)
        if not index_xml:
            raise RuntimeError(f"Cannot fetch sitemap index {SITEMAP_INDEX}")

        sitemap_pages = re.findall(
            r"<loc>(https?://[^<]+sitemap\.xml\?page=\d+)</loc>", index_xml
        )
        if not sitemap_pages:
            raise RuntimeError("Sitemap index contained no paged sitemap URLs")

        logger.info(f"Found {len(sitemap_pages)} sitemap pages")

        entries: List[Tuple[str, str]] = []
        seen = set()
        for page_url in sitemap_pages:
            page_xml = self._http_get(page_url)
            if not page_xml:
                logger.warning(f"Sitemap page unreachable: {page_url}")
                continue

            page_entries = [
                (loc, (lastmod or "")[:10])
                for loc, lastmod in SITEMAP_ENTRY_RE.findall(page_xml)
            ]
            new = 0
            for loc, lastmod in page_entries:
                key = self._eli_key(loc)
                # /eli/about is the ELI documentation page, not a document.
                if key in seen or key == "about":
                    continue
                seen.add(key)
                entries.append((loc, lastmod))
                new += 1
            logger.info(f"  {page_url}: {new} ELI URLs")

        logger.info(f"Total ELI URLs from sitemap: {len(entries)}")
        if not entries:
            raise RuntimeError("Sitemap yielded 0 ELI URLs — discovery is broken")
        return entries

    @staticmethod
    def _eli_key(eli_url: str) -> str:
        """Compact stable key for checkpointing, e.g. 'lta/1999/963'."""
        return eli_url.rstrip("/").split("/eli/", 1)[-1]

    # ------------------------------------------------------------------
    # Checkpoint
    # ------------------------------------------------------------------

    def _load_checkpoint(self) -> set:
        if not self._checkpoint_path.exists():
            return set()
        try:
            with open(self._checkpoint_path, encoding="utf-8") as fh:
                done = {line.strip() for line in fh if line.strip()}
            logger.info(f"Checkpoint: {len(done)} ELI URLs already processed")
            return done
        except Exception as e:
            logger.warning(f"Could not read checkpoint ({e}) — starting fresh")
            return set()

    def _checkpoint_append(self, keys: List[str]):
        if not keys:
            return
        if self._checkpoint_fh is None:
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            self._checkpoint_fh = open(self._checkpoint_path, "a", encoding="utf-8")
        self._checkpoint_fh.write("".join(k + "\n" for k in keys))
        self._checkpoint_fh.flush()

    def _close_checkpoint(self):
        if self._checkpoint_fh is not None:
            self._checkpoint_fh.close()
            self._checkpoint_fh = None

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _fetch_eli_xml(self, eli_url: str) -> Optional[str]:
        """Fetch XML document via ELI URL + /xml suffix."""
        return self._http_get(eli_url.rstrip("/") + "/xml")

    def _parse_xml(self, xml_content: str) -> Dict[str, str]:
        """Parse XML for metadata plus the document body.

        `text` is built from the non-<Meta> children of <Dokument> only.
        Historic documents (mostly pre-2008) ship a Meta-only stub with no
        body at all; keying off Meta text would let those through as
        metadata-only records.
        """
        result = {"title": "", "text": "", "year": "", "number": "", "date": "",
                  "status": "", "document_type": "", "accession_number": ""}

        try:
            xml_clean = re.sub(r'\sxmlns[^"]*"[^"]*"', '', xml_content, count=5)
            root = ET.fromstring(xml_clean)
        except ET.ParseError:
            return self._regex_parse_xml(xml_content)

        meta = root.find(".//Meta")
        if meta is not None:
            for tag, key in (
                ("DocumentTitle", "title"),
                ("Year", "year"),
                ("Number", "number"),
                ("Status", "status"),
                ("DocumentType", "document_type"),
                ("AccessionNumber", "accession_number"),
            ):
                elem = meta.find(tag)
                if elem is not None and elem.text:
                    result[key] = elem.text.strip()

        for date_tag in ["StartDate", "DiesSigni", "SignatureDate", "DiesEdicti", "EndDate"]:
            date_elem = root.find(f".//{date_tag}")
            if date_elem is not None and date_elem.text and date_elem.text.strip():
                result["date"] = date_elem.text.strip()[:10]
                break

        parts = []
        for child in root:
            if child.tag == "Meta":
                continue
            chunk = " ".join(t.strip() for t in child.itertext() if t and t.strip())
            if chunk:
                parts.append(chunk)

        result["text"] = re.sub(r"[ \t]+", " ", "\n".join(parts)).strip()

        if not result["title"]:
            titel = root.find(".//TitelGruppe")
            if titel is not None:
                result["title"] = " ".join(
                    t.strip() for t in titel.itertext() if t and t.strip()
                )[:500]

        return result

    def _regex_parse_xml(self, xml_content: str) -> Dict[str, str]:
        """Fallback parsing when the XML does not parse."""
        def grab(tag):
            m = re.search(rf'<{tag}[^>]*>(.*?)</{tag}>', xml_content, re.DOTALL)
            return m.group(1).strip() if m else ""

        body = re.split(r'</Meta>', xml_content, maxsplit=1)
        body = body[1] if len(body) > 1 else ""
        text = re.sub(r'<[^>]+>', ' ', body)
        text = re.sub(r'\s+', ' ', text).strip()

        return {"title": grab("DocumentTitle"), "text": text, "year": grab("Year"),
                "number": grab("Number"), "date": "", "status": grab("Status"),
                "document_type": grab("DocumentType"),
                "accession_number": grab("AccessionNumber")}

    def _parse_document_api(self, key: str) -> Optional[Dict[str, str]]:
        """Fetch full text from the document API for one ELI key.

        Same shape as _parse_xml() so the two paths are interchangeable.
        Returns None when the API has no record for this key.
        """
        docs = self._http_post_json(DOCUMENT_API + key, {"isRawHtml": False})
        if not isinstance(docs, list) or not docs:
            return None

        text = "\n\n".join(
            t for t in (html_to_text(d.get("documentHtml") or "") for d in docs) if t
        )

        head = docs[0]
        meta = {m.get("displayName", ""): (m.get("displayValue") or "").strip()
                for m in (head.get("metadata") or [])
                if isinstance(m, dict)}

        # Signature date is the document's own date; publication is the
        # fallback, matching the XML path's DiesSigni -> DiesEdicti order.
        date = (_dk_date_to_iso(meta.get("Dato for underskrift", ""))
                or _dk_date_to_iso(meta.get("Offentliggørelsesdato", "")))

        return {
            "title": (head.get("title") or "").strip(),
            "text": text,
            "year": meta.get("År for udstedelse", ""),
            "number": meta.get("Forskriftens nummer", ""),
            "date": date,
            "status": "Historic" if str(head.get("isHistorical")).lower() == "true" else "Valid",
            "document_type": re.sub(r"\s+", " ", meta.get("Dokumenttype", "")).strip(),
            "accession_number": (head.get("accessionNumber") or "").strip(),
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        accession = raw.get("accession_number", "")
        eli_url = raw.get("eli_url", "")
        date = raw.get("date", "") or raw.get("change_date", "")

        url = eli_url if eli_url else (
            f"https://www.retsinformation.dk/eli/accn/{accession}" if accession else ""
        )

        doc_id = accession or (self._eli_key(eli_url).replace("/", "-") if eli_url else "")

        return {
            "_id": f"DK-RI-{doc_id}",
            "_source": "DK/Retsinformation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": url,
            "accession_number": accession,
            "document_type": raw.get("document_type", ""),
            "year": raw.get("year", ""),
            "number": raw.get("number", ""),
            "status": raw.get("status", ""),
        }

    # ------------------------------------------------------------------
    # Fetching
    # ------------------------------------------------------------------

    def _fetch_one(self, entry: Tuple[str, str]
                   ) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """Worker: fetch + parse a single ELI document. Never raises.

        Returns (checkpoint_key, record_or_None, skip_reason). skip_reason is
        "" when a record was produced; otherwise it names exactly why the
        document was dropped, so a coverage cliff cannot hide behind a bare
        counter (#1549).
        """
        eli_url, lastmod = entry
        key = self._eli_key(eli_url)
        try:
            parsed = None
            xml_content = self._fetch_eli_xml(eli_url)
            if xml_content:
                parsed = self._parse_xml(xml_content)

            # Fallback below the working XML path: about half the corpus
            # serves a Meta-only stub (or 404s on /xml) but carries full text
            # under the document API. Documents the XML path already handles
            # never reach here, so their text stays byte-identical.
            if parsed is None or len(parsed.get("text") or "") < MIN_TEXT_CHARS:
                xml_reason = "xml_unavailable" if parsed is None else "xml_meta_only"
                api_parsed = self._parse_document_api(key)
                if api_parsed is None:
                    return key, None, f"{xml_reason}+api_404"
                if len(api_parsed.get("text") or "") < MIN_TEXT_CHARS:
                    return key, None, (
                        f"{xml_reason}+api_text_{len(api_parsed.get('text') or '')}"
                        f"chars type={api_parsed.get('document_type') or '?'}"
                    )
                parsed = api_parsed

            return key, {
                "accession_number": parsed.get("accession_number", ""),
                "eli_url": eli_url,
                "date": parsed.get("date", "") or lastmod,
                "title": parsed["title"],
                "text": parsed["text"],
                "document_type": parsed["document_type"],
                "year": parsed["year"],
                "number": parsed["number"],
                "status": parsed["status"],
            }, ""
        except Exception as e:
            logger.warning(f"Failed {eli_url[:80]}: {e}")
            return key, None, f"exception:{type(e).__name__}"

    def fetch_all(self, since: Optional[str] = None,
                  max_workers: int = 8) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents via sitemap discovery + ELI XML, in parallel.

        Args:
            since: ISO date (YYYY-MM-DD). Only documents whose sitemap
                   <lastmod> is on/after this date are fetched.
            max_workers: concurrent XML downloads.
        """
        entries = self._get_sitemap_entries()
        total_discovered = len(entries)

        if since:
            since_day = since[:10]
            entries = [e for e in entries if e[1] and e[1] >= since_day]
            logger.info(
                f"Incremental: {len(entries)}/{total_discovered} entries "
                f"with lastmod >= {since_day}"
            )

        # The checkpoint exists to resume an interrupted *full* walk. An
        # incremental run must ignore it: a document crawled last month and
        # amended since is exactly what --since is meant to re-fetch.
        if not since:
            done = self._load_checkpoint()
            if done:
                entries = [e for e in entries if self._eli_key(e[0]) not in done]
                logger.info(f"Resuming: {len(entries)} entries left to process")

        if not entries:
            logger.info("Nothing to fetch — everything already processed")
            self.nothing_left_to_fetch = True
            return

        count = 0
        skipped = 0
        processed = 0
        pending_keys: List[str] = []
        # Every skip is logged with a reason; this aggregates them so the
        # shape of a coverage cliff is visible in the progress line itself.
        skip_reasons: Counter = Counter()
        started = time.time()
        chunk_size = max_workers * 8

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                for offset in range(0, len(entries), chunk_size):
                    chunk = entries[offset:offset + chunk_size]
                    for key, raw, reason in pool.map(self._fetch_one, chunk):
                        processed += 1
                        pending_keys.append(key)
                        if raw is None:
                            skipped += 1
                            skip_reasons[reason.split(" ")[0]] += 1
                            logger.warning(f"SKIP {key}: {reason}")
                        else:
                            count += 1
                            yield raw

                    if len(pending_keys) >= CHECKPOINT_FLUSH_EVERY:
                        self._checkpoint_append(pending_keys)
                        pending_keys = []

                    elapsed = max(time.time() - started, 1e-6)
                    logger.info(
                        f"Progress: {processed}/{len(entries)} processed, "
                        f"{count} yielded, {skipped} skipped "
                        f"({processed / elapsed:.1f} docs/s) "
                        f"skip_reasons={dict(skip_reasons.most_common(5))}"
                    )
        finally:
            self._checkpoint_append(pending_keys)
            self._close_checkpoint()

        logger.info(
            f"Completed: {count} documents fetched, {skipped} skipped "
            f"— skip reasons: {dict(skip_reasons.most_common())}"
        )

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Incremental fetch.

        The harvest API only accepts dates within the last 10 days, so
        anything older falls back to the sitemap's <lastmod> filter.
        """
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        today = datetime.now()

        if since:
            since_day = since[:10]
            try:
                age = (today - datetime.strptime(since_day, "%Y-%m-%d")).days
            except ValueError:
                age = 999
            if age > 9:
                logger.info(
                    f"since={since_day} is {age} days back — harvest API only "
                    f"covers 10 days, using sitemap lastmod filter instead"
                )
                yield from self.fetch_all(since=since_day)
                return
            days = range(0, min(age + 1, 10))
        else:
            days = range(1, 11)

        for days_back in days:
            date_str = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")

            text = self._http_get(f"{HARVEST_API}?date={date_str}", min_gap=HARVEST_MIN_GAP)
            if not text:
                continue

            try:
                data = json.loads(text, strict=False)
                docs = data if isinstance(data, list) else []
            except json.JSONDecodeError:
                continue

            logger.info(f"{date_str}: {len(docs)} documents")

            for doc in docs:
                accession = doc.get("accessionsnummer", "")
                if not accession:
                    continue

                eli_url = f"https://www.retsinformation.dk/eli/accn/{accession}"
                key, raw, reason = self._fetch_one((eli_url, date_str))
                if raw is None:
                    logger.warning(f"SKIP {key}: {reason}")
                    continue
                raw["change_date"] = doc.get("changeDate", date_str)
                yield raw

    def test(self) -> bool:
        """Quick connectivity test."""
        entries = self._get_sitemap_entries()
        logger.info(f"Sitemap OK: {len(entries)} ELI URLs")

        checked = 0
        for eli_url, lastmod in entries[:20]:
            _, raw, _reason = self._fetch_one((eli_url, lastmod))
            if raw:
                logger.info(
                    f"XML OK: {raw['title'][:60]} ({len(raw['text'])} chars, {raw['date']})"
                )
                return True
            checked += 1
        logger.error(f"No full-text document found in first {checked} sitemap entries")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="DK/Retsinformation data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch a small sample (for validation)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--since", default=None,
                        help="Only fetch documents changed on/after this date (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=8,
                        help="Concurrent XML downloads (default 8)")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="Discard the resume checkpoint and re-crawl from scratch")
    args = parser.parse_args()

    scraper = RetsinformationScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    if args.command in ("bootstrap", "bootstrap-fast"):
        if args.reset_checkpoint:
            scraper._checkpoint_path.unlink(missing_ok=True)
            logger.info("Checkpoint reset")
        if args.since or args.workers != 8:
            _fetch_all = scraper.fetch_all
            scraper.fetch_all = lambda: _fetch_all(since=args.since,
                                                   max_workers=args.workers)
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0 and not scraper.nothing_left_to_fetch:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
