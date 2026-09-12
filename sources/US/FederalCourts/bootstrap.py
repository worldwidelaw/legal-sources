#!/usr/bin/env python3
"""
US/FederalCourts -- Federal Court Opinions (SCOTUS + Circuit Courts)

Fetches case law via CourtListener's public search API, then resolves full text
through a fallback ladder (Opinions API when a token is configured, otherwise
the stored PDF/HTML on storage.courtlistener.com).

Courts covered:
  - Supreme Court of the United States (scotus)
  - 1st through 11th Circuit Courts of Appeals (ca1..ca11)
  - DC Circuit (cadc) and Federal Circuit (cafc)

Enumeration (issue #1494)
------------------------
The corpus is swept **per court, per year, newest year first**, with a
checkpoint written after every completed (court, year) window. This replaces
the old single flat `dateFiled desc` cursor walk, which restarted from the
newest opinion on every run and therefore never advanced past the most recent
couple of years (8,583 rows indexed, 2024-2026 only, out of ~1.4M available).

Because each window is checkpointed, successive runs resume where the previous
one stopped instead of re-crawling the same recent opinions.

Full text availability
----------------------
Opinions filed from roughly 1950 onward carry a PDF/HTML file on
storage.courtlistener.com and are retrievable anonymously. Older opinions
(largely the Harvard CAP import) store their text only in the CourtListener
database, which is served by the authenticated Opinions API. Set
COURTLISTENER_API_TOKEN to include them; without it those windows are reported
as skipped rather than silently dropped.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap --full     # Full checkpointed backfill
  python bootstrap.py bootstrap-fast --full
  python bootstrap.py update --since YYYY-MM-DD
  python bootstrap.py coverage             # Expected-vs-indexed audit per court
  python bootstrap.py test
"""

import sys
import os
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FederalCourts")

API_BASE = "https://www.courtlistener.com/api/rest/v4"
SEARCH_URL = f"{API_BASE}/search/"
OPINION_URL = f"{API_BASE}/opinions/"
STORAGE_BASE = "https://storage.courtlistener.com/"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# Courts are swept in this order. CA2 and CA9 lead because issue #1494 tracks
# them explicitly; the remaining circuits follow, SCOTUS last (it is by far the
# largest single court at ~498K opinions).
COURT_ORDER = [
    "ca2", "ca9", "ca1", "ca3", "ca4", "ca5", "ca6",
    "ca7", "ca8", "ca10", "ca11", "cadc", "cafc", "scotus",
]

FEDERAL_COURTS = ",".join(
    ["scotus", "ca1", "ca2", "ca3", "ca4", "ca5", "ca6", "ca7",
     "ca8", "ca9", "ca10", "ca11", "cadc", "cafc"]
)

COURT_NAMES = {
    "scotus": "Supreme Court of the United States",
    "ca1": "U.S. Court of Appeals for the First Circuit",
    "ca2": "U.S. Court of Appeals for the Second Circuit",
    "ca3": "U.S. Court of Appeals for the Third Circuit",
    "ca4": "U.S. Court of Appeals for the Fourth Circuit",
    "ca5": "U.S. Court of Appeals for the Fifth Circuit",
    "ca6": "U.S. Court of Appeals for the Sixth Circuit",
    "ca7": "U.S. Court of Appeals for the Seventh Circuit",
    "ca8": "U.S. Court of Appeals for the Eighth Circuit",
    "ca9": "U.S. Court of Appeals for the Ninth Circuit",
    "ca10": "U.S. Court of Appeals for the Tenth Circuit",
    "ca11": "U.S. Court of Appeals for the Eleventh Circuit",
    "cadc": "U.S. Court of Appeals for the District of Columbia Circuit",
    "cafc": "U.S. Court of Appeals for the Federal Circuit",
}

COURT_ABBRS = {
    "scotus": "SCOTUS",
    "ca1": "1CIR",
    "ca2": "2CIR",
    "ca3": "3CIR",
    "ca4": "4CIR",
    "ca5": "5CIR",
    "ca6": "6CIR",
    "ca7": "7CIR",
    "ca8": "8CIR",
    "ca9": "9CIR",
    "ca10": "10CIR",
    "ca11": "11CIR",
    "cadc": "DCCIR",
    "cafc": "FEDCIR",
}

# Ordered preference for the Opinions-API text fields.
TEXT_FIELDS = [
    "html_with_citations",
    "html",
    "html_lawbox",
    "html_columbia",
    "html_anon_2020",
    "xml_harvard",
    "plain_text",
]

MIN_TEXT_CHARS = 100

# Hosts CourtListener still references in download_url but which no longer serve.
DEAD_FILE_HOSTS = ("bulk.resource.org", "public.resource.org")


class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self._pieces = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip += 1
        elif tag in ("p", "br", "div", "blockquote"):
            self._pieces.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self._skip:
            self._skip -= 1

    def handle_data(self, data):
        if not self._skip:
            self._pieces.append(data)

    def get_text(self):
        return "".join(self._pieces)


def strip_html(html: str) -> str:
    extractor = _HTMLTextExtractor()
    try:
        extractor.feed(html)
    except Exception:
        pass
    text = extractor.get_text()
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class FederalCourtsScraper(BaseScraper):
    """Scraper for US/FederalCourts via the CourtListener API."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        }
        self.token = os.environ.get("COURTLISTENER_API_TOKEN", "").strip()
        if self.token:
            headers["Authorization"] = f"Token {self.token}"
            logger.info("COURTLISTENER_API_TOKEN found - Opinions API text enabled "
                        "(covers pre-1950 opinions that have no stored PDF)")
        else:
            logger.warning(
                "No COURTLISTENER_API_TOKEN - falling back to stored PDF/HTML only. "
                "Opinions without a stored file (mostly pre-1950) will be reported "
                "as skipped rather than fetched."
            )
        self.session.headers.update(headers)

        self.data_dir = self.source_dir / "data"
        self.checkpoint_path = self.data_dir / "checkpoint.json"
        self.coverage_path = self.data_dir / "coverage_report.json"
        self._checkpoint = self._load_checkpoint()
        # Per-court tallies for the coverage report.
        self._tally: Dict[str, Dict[str, int]] = {}
        self._sample_mode = False
        self._search_delay = float(self.config.get("fetch", {}).get("search_delay", 2.0))

    # ── checkpoint ────────────────────────────────────────────────────

    def _load_checkpoint(self) -> Dict[str, Any]:
        try:
            with open(self.checkpoint_path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("done"), list):
                logger.info(f"Checkpoint: {len(data['done'])} (court, year) windows already complete")
                return {"done": set(data["done"])}
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Could not read checkpoint ({e}); starting fresh")
        return {"done": set()}

    def _save_checkpoint(self) -> None:
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            tmp = self.checkpoint_path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"done": sorted(self._checkpoint["done"])}, f)
            tmp.replace(self.checkpoint_path)
        except Exception as e:
            logger.warning(f"Could not write checkpoint: {e}")

    # ── HTTP ──────────────────────────────────────────────────────────

    def _get_json(self, url: str, params: Dict[str, Any] = None,
                  max_attempts: int = 6) -> Optional[Dict[str, Any]]:
        """GET returning parsed JSON, with real 429/5xx backoff.

        The anonymous search API throttles aggressively, so backoff has to be
        long enough to actually clear the window rather than burning the three
        quick retries the previous implementation used.
        """
        for attempt in range(max_attempts):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                if resp.status_code == 429:
                    wait = min(60, 5 * (2 ** attempt))
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait = max(wait, min(300, int(float(retry_after))))
                        except ValueError:
                            pass
                    logger.warning(f"429 rate limited; sleeping {wait}s "
                                   f"(attempt {attempt + 1}/{max_attempts})")
                    time.sleep(wait)
                    continue
                if resp.status_code in (401, 403):
                    # Auth-only endpoint; caller decides how to fall back.
                    return None
                if resp.status_code >= 500:
                    wait = min(60, 3 * (2 ** attempt))
                    logger.warning(f"HTTP {resp.status_code}; retrying in {wait}s")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError) as e:
                wait = min(60, 3 * (2 ** attempt))
                logger.warning(f"{type(e).__name__}; retrying in {wait}s")
                time.sleep(wait)
            except Exception as e:
                logger.error(f"Request error for {url}: {e}")
                if attempt >= 2:
                    return None
                time.sleep(3)
        logger.error(f"Giving up on {url} after {max_attempts} attempts")
        return None

    def _search(self, court: str, filed_after: str = None, filed_before: str = None,
                page_size: int = 20, order: str = "dateFiled desc",
                cursor_url: str = None) -> Optional[Dict[str, Any]]:
        if cursor_url:
            return self._get_json(cursor_url)
        params = {
            "format": "json",
            "type": "o",
            "court": court,
            "page_size": min(page_size, 20),
            "order_by": order,
        }
        if filed_after:
            params["filed_after"] = filed_after
        if filed_before:
            params["filed_before"] = filed_before
        return self._get_json(SEARCH_URL, params=params)

    def court_total(self, court: str) -> int:
        data = self._search(court, page_size=1)
        return int(data.get("count", 0)) if data else 0

    def _earliest_year(self, court: str) -> int:
        data = self._search(court, page_size=1, order="dateFiled asc")
        if data:
            for r in data.get("results", []):
                d = r.get("dateFiled") or ""
                if len(d) >= 4 and d[:4].isdigit():
                    return int(d[:4])
        return 1789

    # ── text resolution ───────────────────────────────────────────────

    def _download_file(self, url: str) -> Optional[bytes]:
        try:
            resp = self.session.get(url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
            if len(resp.content) > 100:
                return resp.content
            return None
        except Exception as e:
            logger.debug(f"Failed to download {url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_data: bytes, doc_id: str = "") -> str:
        try:
            # doc_id must be the exact string normalize() emits as _id, otherwise
            # the skip-if-already-in-Neon guard is disabled and every refresh
            # re-extracts the whole corpus (issue #1480).
            return extract_pdf_markdown(
                source="US/FederalCourts",
                source_id=doc_id,
                pdf_bytes=pdf_data,
                table="case_law",
            ) or ""
        except Exception as e:
            logger.debug(f"PDF extraction failed: {e}")
            return ""

    def _get_file_url(self, opinion: Dict) -> Optional[str]:
        local_path = opinion.get("local_path")
        if local_path:
            return STORAGE_BASE + local_path
        download_url = opinion.get("download_url") or ""
        # Opinions imported from the Public.Resource.Org bulk dump carry a
        # download_url on bulk.resource.org, which has been offline for years
        # (DNS does not resolve). Treating those as a live file misreports them
        # as extraction failures and burns a request each; they are in fact
        # API-only, i.e. token-required.
        if any(host in download_url for host in DEAD_FILE_HOSTS):
            return None
        return download_url or None

    def _extract_text_from_url(self, url: str, doc_id: str = "") -> str:
        data = self._download_file(url)
        if not data:
            return ""
        header = data[:200].lower()
        if (url.lower().endswith((".html", ".htm"))
                or b"<!doctype html" in header or b"<html" in header):
            return strip_html(data.decode("utf-8", errors="replace"))
        return self._extract_pdf_text(data, doc_id=doc_id)

    def _text_from_opinions_api(self, opinion_id: Any) -> str:
        """Fetch stored text for one opinion. Requires an API token."""
        if not self.token or not opinion_id:
            return ""
        data = self._get_json(f"{OPINION_URL}{opinion_id}/", params={"format": "json"})
        if not data:
            return ""
        for field in TEXT_FIELDS:
            value = data.get(field)
            if value and len(value) >= MIN_TEXT_CHARS:
                return value.strip() if field == "plain_text" else strip_html(value)
        return ""

    def _opinion_text(self, opinion: Dict, doc_id: str = "") -> str:
        """Resolve one opinion's full text: Opinions API first, then stored file."""
        text = self._text_from_opinions_api(opinion.get("id"))
        if len(text) >= MIN_TEXT_CHARS:
            return text
        file_url = self._get_file_url(opinion)
        if file_url:
            return self._extract_text_from_url(file_url, doc_id=doc_id)
        return ""

    # ── record assembly ───────────────────────────────────────────────

    def _bump(self, court: str, key: str, n: int = 1) -> None:
        self._tally.setdefault(court, {})
        self._tally[court][key] = self._tally[court].get(key, 0) + n

    def _process_search_result(self, result: Dict) -> Optional[Dict[str, Any]]:
        court_id = result.get("court_id", "") or ""
        opinions = result.get("opinions", []) or []
        if not opinions:
            self._bump(court_id, "skipped_no_opinions")
            return None

        doc_id = (f"US-FED-{COURT_ABBRS.get(court_id, court_id.upper())}-"
                  f"{result.get('cluster_id')}")

        # Concatenate every sub-opinion in the cluster (lead opinion,
        # concurrences, dissents) so the record carries the whole decision.
        parts: List[str] = []
        for opinion in opinions:
            text = self._opinion_text(opinion, doc_id=doc_id)
            if len(text) < MIN_TEXT_CHARS:
                continue
            label = (opinion.get("type") or "").replace("-", " ").strip()
            if label and len(opinions) > 1:
                parts.append(f"## {label.title()}\n\n{text}")
            else:
                parts.append(text)
            time.sleep(self.config.get("fetch", {}).get("delay", 1.0))

        full_text = "\n\n".join(parts).strip()
        if len(full_text) < MIN_TEXT_CHARS:
            # Report, do not silently drop (issue #1494 acceptance criterion).
            if any(self._get_file_url(o) for o in opinions):
                self._bump(court_id, "skipped_extraction_failed")
                logger.warning(
                    f"[{court_id}] extraction produced {len(full_text)} chars for "
                    f"cluster {result.get('cluster_id')} "
                    f"({result.get('caseName', '')[:60]})"
                )
            else:
                self._bump(court_id, "skipped_no_stored_text")
                logger.debug(
                    f"[{court_id}] no stored file and no API text for cluster "
                    f"{result.get('cluster_id')} (dateFiled={result.get('dateFiled')})"
                )
            return None

        citations = result.get("citation") or []
        lead = opinions[0]
        self._bump(court_id, "fetched")
        return {
            "cluster_id": result.get("cluster_id"),
            "case_name": result.get("caseName", ""),
            "case_name_full": result.get("caseNameFull", ""),
            "docket_number": result.get("docketNumber", ""),
            "court_id": court_id,
            "court": result.get("court", ""),
            "date_filed": result.get("dateFiled"),
            "citation": citations[0] if citations else "",
            "status": result.get("status", ""),
            "judge": result.get("judge", ""),
            "file_url": self._get_file_url(lead) or "",
            "cl_url": f"https://www.courtlistener.com{result.get('absolute_url', '')}",
            "opinion_count": len(opinions),
            "text": full_text,
        }

    # ── enumeration ───────────────────────────────────────────────────

    def _sweep_window(self, court: str, year: int, max_yield: int = None,
                      max_scan: int = None) -> Generator[Dict[str, Any], None, None]:
        """Yield every processed opinion filed by `court` during `year`.

        `max_yield`/`max_scan` bound the window for sample mode. Without them a
        barren window (e.g. an early year whose PDFs are un-OCR'd scans) would
        be scanned in full before producing anything.
        """
        filed_after = f"{year}-01-01"
        filed_before = f"{year}-12-31"
        cursor_url = None
        seen = 0
        produced = 0
        while True:
            data = self._search(court, filed_after=filed_after,
                                filed_before=filed_before,
                                order="dateFiled asc", cursor_url=cursor_url)
            time.sleep(self._search_delay)
            if not data:
                self._bump(court, "search_failures")
                logger.error(f"[{court} {year}] search failed; window left unfinished")
                return
            results = data.get("results", []) or []
            if cursor_url is None:
                self._bump(court, "available", int(data.get("count", 0)))
            if not results:
                break
            for result in results:
                seen += 1
                raw = self._process_search_result(result)
                if raw:
                    produced += 1
                    yield raw
                    if max_yield and produced >= max_yield:
                        return
                if max_scan and seen >= max_scan:
                    logger.info(f"[{court} {year}] scan cap reached "
                                f"({seen} clusters, {produced} usable)")
                    return
            cursor_url = data.get("next")
            if not cursor_url:
                break
        if seen:
            logger.info(f"[{court} {year}] window complete: {seen} clusters seen")

    def _sample_stream(self) -> Generator[Dict[str, Any], None, None]:
        """Spread samples across courts and eras.

        Round-robins one record at a time from several (court, year) windows so
        a 15-record sample proves the backfill works for both the modern
        PDF-backed era and the older scanned era, rather than filling up from
        whichever window happens to come first.
        """
        current_year = datetime.now(timezone.utc).year
        # Restricted to 2005+ because that is where `local_path` (the only
        # token-free file) starts; older windows are API-only. See README.
        windows = [
            ("ca2", current_year), ("ca9", current_year), ("scotus", current_year),
            ("ca2", 2012), ("ca9", 2012), ("scotus", 2015),
            ("cadc", 2018), ("cafc", 2015), ("ca5", 2020), ("ca1", 2010),
        ]
        streams = [self._sweep_window(c, y, max_yield=2, max_scan=25)
                   for c, y in windows]
        while streams:
            for stream in list(streams):
                try:
                    yield next(stream)
                except StopIteration:
                    streams.remove(stream)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        if self._sample_mode:
            yield from self._sample_stream()
            return

        current_year = datetime.now(timezone.utc).year
        total = 0
        for court in COURT_ORDER:
            floor_year = self._earliest_year(court)
            time.sleep(self._search_delay)
            logger.info(f"=== {court} ({COURT_NAMES.get(court, court)}): "
                        f"sweeping {current_year} back to {floor_year} ===")
            for year in range(current_year, floor_year - 1, -1):
                key = f"{court}:{year}"
                if key in self._checkpoint["done"]:
                    continue
                for raw in self._sweep_window(court, year):
                    total += 1
                    yield raw
                self._checkpoint["done"].add(key)
                self._save_checkpoint()
            self._write_coverage_report()
            logger.info(f"=== {court} done. Running total: {total} ===")
        logger.info(f"Total fetched: {total}")
        self._write_coverage_report()

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        if not since:
            from datetime import timedelta
            since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        cursor_url = None
        while True:
            data = self._search(FEDERAL_COURTS, filed_after=since, cursor_url=cursor_url)
            time.sleep(self._search_delay)
            if not data:
                break
            results = data.get("results", []) or []
            if not results:
                break
            for result in results:
                raw = self._process_search_result(result)
                if raw:
                    yield raw
            cursor_url = data.get("next")
            if not cursor_url:
                break

    # ── coverage reporting ────────────────────────────────────────────

    def _write_coverage_report(self) -> None:
        """Per-court expected-vs-fetched audit trail (issue #1494)."""
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "token_configured": bool(self.token),
            "courts": {},
        }
        for court in COURT_ORDER:
            t = self._tally.get(court)
            if not t:
                continue
            report["courts"][court] = {
                "name": COURT_NAMES.get(court, court),
                "available_in_windows_swept": t.get("available", 0),
                "fetched_with_full_text": t.get("fetched", 0),
                "skipped_no_stored_text": t.get("skipped_no_stored_text", 0),
                "skipped_extraction_failed": t.get("skipped_extraction_failed", 0),
                "skipped_no_opinions": t.get("skipped_no_opinions", 0),
                "search_failures": t.get("search_failures", 0),
                "windows_complete": sum(
                    1 for k in self._checkpoint["done"] if k.startswith(f"{court}:")
                ),
            }
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            with open(self.coverage_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not write coverage report: {e}")

    def coverage_audit(self) -> Dict[str, Any]:
        """Query the live per-court totals so expected counts are documented."""
        out = {}
        grand = 0
        for court in COURT_ORDER:
            n = self.court_total(court)
            grand += n
            out[court] = {"name": COURT_NAMES.get(court, court), "available": n}
            logger.info(f"{court:>7} {COURT_NAMES.get(court, court):<62} {n:>8,}")
            time.sleep(self._search_delay)
        logger.info(f"{'TOTAL':>7} {'':<62} {grand:>8,}")
        out["_total"] = grand
        return out

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        court_id = raw.get("court_id", "") or ""
        cluster_id = raw.get("cluster_id", "")
        court_abbr = COURT_ABBRS.get(court_id, court_id.upper())
        # Unchanged from the original scheme so already-indexed rows keep their key.
        doc_id = f"US-FED-{court_abbr}-{cluster_id}"
        court_name = COURT_NAMES.get(court_id, raw.get("court", "US Federal Court"))
        return {
            "_id": doc_id,
            "_source": "US/FederalCourts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("case_name", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date_filed"),
            "url": raw.get("cl_url", ""),
            "case_number": raw.get("docket_number", ""),
            "court": court_name,
            "court_id": court_id,
            "citation": raw.get("citation", ""),
            "status": raw.get("status", ""),
            "judge": raw.get("judge", ""),
            "jurisdiction": "US",
            "file_url": raw.get("file_url", ""),
        }

    def test_connection(self) -> bool:
        try:
            data = self._search("ca9", page_size=5)
            if not data:
                logger.error("Connection test failed: no response")
                return False
            count = data.get("count", 0)
            results = data.get("results", [])
            logger.info(f"Connection test: ca9 has {count:,} opinions, got {len(results)} results")
            if results:
                first = results[0]
                logger.info(f"  First: {first.get('caseName', '')[:60]}, date={first.get('dateFiled')}")
            return count > 0 and len(results) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/FederalCourts case law fetcher")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "update", "test", "coverage"])
    parser.add_argument("--sample", action="store_true", help="Sample mode")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    parser.add_argument("--count", type=int, default=15, help="Sample count")
    parser.add_argument("--since", help="YYYY-MM-DD (update mode)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Accepted for VPS wrapper compatibility")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Accepted for VPS wrapper compatibility")

    args = parser.parse_args()

    scraper = FederalCourtsScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test_connection() else 1)

    elif args.command == "coverage":
        result = scraper.coverage_audit()
        print(json.dumps(result, indent=2))

    elif args.command in ("bootstrap", "bootstrap-fast"):
        if args.sample:
            scraper._sample_mode = True
            stats = scraper.bootstrap(sample_mode=True, sample_size=args.count)
        else:
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")
        if not args.sample and stats.get("records_fetched", 0) == 0:
            sys.exit(1)

    elif args.command == "update":
        since = args.since
        count = 0
        data_dir = scraper.source_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        with open(data_dir / "updates.jsonl", "w", encoding="utf-8") as f:
            for raw in scraper.fetch_updates(since=since):
                record = scraper.normalize(raw)
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                count += 1
        logger.info(f"Fetched {count} updates since {since}")


if __name__ == "__main__":
    main()
