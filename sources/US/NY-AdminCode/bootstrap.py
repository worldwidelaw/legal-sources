#!/usr/bin/env python3
"""
US/NY-AdminCode -- New York Codes Rules and Regulations (NYCRR)

Fetches all NYCRR sections with full text from Cornell LII
(law.cornell.edu/regulations/new-york).

Strategy:
  1. Fetch title list to discover all 24 title URLs
  2. Per title, breadth-first walk the chapter -> subchapter -> part -> subpart
     hierarchy, collecting section links at *every* level (a part page can list
     its own sections and still have deeper named subdivisions, e.g.
     title-6/chapter-I/subchapter-A/part-1 plus .../part-1/bear)
  3. For each section, fetch the page and extract full regulation text

Resumability (issue #1370): enumeration per title and the set of already
written sections are checkpointed under ``data/``, and the whole run is bounded
by a wall-clock deadline, so a run that is cut short resumes where it stopped
instead of restarting the 30K-page walk from Title 1.

Data: Public domain (New York government regulations). No auth required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all sections)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py bootstrap-fast       # Full pull, streams to data/
  python bootstrap.py test-api             # Connectivity test
"""

import os
import sys
import json
import logging
import re
import threading
import time
import html as html_module
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from email.utils import format_datetime, parsedate_to_datetime

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient, request_with_deadline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NY-AdminCode")

BASE_URL = "https://www.law.cornell.edu"

# Seconds of wall clock a single HTTP request may consume before it is
# abandoned. Cornell answers a hierarchy page in ~0.5s; anything past this is a
# stalled socket, which is what wedged the Title 6 walk in issue #1370.
REQUEST_DEADLINE = int(os.environ.get("NY_ADMINCODE_REQUEST_DEADLINE", "90"))

# Wall clock for the whole crawl. The walk is ~34K pages, so a run can be cut
# short; the checkpoint under data/ lets the next one pick up where this
# stopped. 0 disables the deadline.
MAX_RUNTIME = int(os.environ.get("NY_ADMINCODE_MAX_RUNTIME", str(20 * 3600)))

# Returned by ``_get`` when a conditional request was answered 304. A distinct
# sentinel object, not "" — an empty body and an unchanged page mean opposite
# things to a refresh, and conflating them turns "nothing changed" into "the
# page is gone".
NOT_MODIFIED = "__NOT_MODIFIED__"

# Minimum gap between requests, shared across the section-fetch pool.
CRAWL_GAP = float(os.environ.get("NY_ADMINCODE_CRAWL_GAP", "1.0"))
SECTION_WORKERS = int(os.environ.get("NY_ADMINCODE_WORKERS", "4"))

# Sample sections from different titles for --sample mode
SAMPLE_SECTIONS = [
    "/regulations/new-york/1-NYCRR-1.1",
    "/regulations/new-york/1-NYCRR-1.2",
    "/regulations/new-york/3-NYCRR-2.1",
    "/regulations/new-york/6-NYCRR-200.1",
    "/regulations/new-york/8-NYCRR-100.1",
    "/regulations/new-york/8-NYCRR-100.2",
    "/regulations/new-york/9-NYCRR-2200.1",
    "/regulations/new-york/10-NYCRR-2.1",
    "/regulations/new-york/11-NYCRR-60-2.1",
    "/regulations/new-york/12-NYCRR-800.1",
    "/regulations/new-york/14-NYCRR-633.1",
    "/regulations/new-york/18-NYCRR-347.1",
    "/regulations/new-york/20-NYCRR-1.1",
    "/regulations/new-york/22-NYCRR-202.8",
    "/regulations/new-york/23-NYCRR-500.1",
]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'</div>', '\n\n', text)
    text = re.sub(r'</li>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class NYAdminCodeScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )
        self.delay = CRAWL_GAP
        self._pace_lock = threading.Lock()
        self._next_slot = 0.0
        self._deadline = time.monotonic() + MAX_RUNTIME if MAX_RUNTIME > 0 else None
        self.data_dir = Path(self.source_dir) / "data"
        self.data_dir.mkdir(exist_ok=True)
        self._enum_cache_path = self.data_dir / "enumeration.json"
        self._done_path = self.data_dir / "done_sections.txt"
        self._done_lock = threading.Lock()
        self._stamps_path = self.data_dir / "section_stamps.json"
        self._stamps_lock = threading.Lock()
        self._stamps = self._load_stamps()
        self._stamps_dirty = False

    # -- pacing / deadline -------------------------------------------------

    def _pace(self):
        """Space requests ``self.delay`` apart across every worker thread."""
        with self._pace_lock:
            now = time.monotonic()
            wait = self._next_slot - now
            self._next_slot = max(now, self._next_slot) + self.delay
        if wait > 0:
            time.sleep(wait)

    def out_of_time(self) -> bool:
        return self._deadline is not None and time.monotonic() >= self._deadline

    def _get(self, url: str, retries: int = 2, if_modified_since: str = None):
        """Fetch URL with rate limiting, a wall-clock deadline, and retries.

        Returns the page body, or ``NOT_MODIFIED`` when the server answers 304 to
        a conditional request. Callers that pass no ``if_modified_since`` can
        never see the sentinel and keep treating the result as a plain string.
        """
        headers = {"If-Modified-Since": if_modified_since} if if_modified_since else None
        for attempt in range(retries + 1):
            self._pace()
            try:
                resp = request_with_deadline(
                    self.http.session, "GET", url,
                    wall_timeout=REQUEST_DEADLINE, timeout=(15, 45),
                    headers=headers,
                )
                if resp.status_code == 304:
                    return NOT_MODIFIED
                if resp.status_code == 200:
                    self._note_stamp(url, resp.headers.get("Last-Modified"))
                    return resp.text
                if resp.status_code == 429:
                    wait = min(60, max(self.delay, 5) * (attempt + 2))
                    logger.warning(f"Rate limited on {url}, waiting {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code == 404:
                    logger.debug(f"404: {url}")
                    return ""
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url}: {e}")
                if attempt < retries:
                    time.sleep(5)
        return ""

    # -- checkpoint --------------------------------------------------------

    def _load_enumeration(self) -> dict:
        if self._enum_cache_path.exists():
            try:
                return json.loads(self._enum_cache_path.read_text())
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Ignoring unreadable enumeration cache: {e}")
        return {}

    def _save_enumeration(self, cache: dict):
        tmp = self._enum_cache_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(cache))
        tmp.replace(self._enum_cache_path)

    def _load_done(self) -> set:
        if self._done_path.exists():
            return {
                line.strip()
                for line in self._done_path.read_text().splitlines()
                if line.strip()
            }
        return set()

    def _mark_done(self, section_path: str):
        with self._done_lock:
            with self._done_path.open("a") as fh:
                fh.write(section_path + "\n")

    # -- upstream Last-Modified stamps -------------------------------------

    def _load_stamps(self) -> dict:
        if self._stamps_path.exists():
            try:
                return json.loads(self._stamps_path.read_text())
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Ignoring unreadable stamp cache: {e}")
        return {}

    def _note_stamp(self, url: str, last_modified: str):
        """Record the upstream Last-Modified of a page we just fetched in full.

        This is what a later refresh compares against. Storing it per section —
        rather than relying on the run's `since` — makes the comparison exact:
        the question asked of Cornell is "has *this* page moved since the copy I
        hold", which stays right even if the fleet's last_run bookkeeping drifts.
        """
        if not last_modified:
            return
        path = url[len(BASE_URL):] if url.startswith(BASE_URL) else url
        with self._stamps_lock:
            if self._stamps.get(path) != last_modified:
                self._stamps[path] = last_modified
                self._stamps_dirty = True

    def _save_stamps(self):
        with self._stamps_lock:
            if not self._stamps_dirty:
                return
            snapshot = dict(self._stamps)
            self._stamps_dirty = False
        tmp = self._stamps_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snapshot))
        tmp.replace(self._stamps_path)

    def test_api(self):
        """Test connectivity to Cornell LII."""
        logger.info("Testing Cornell LII New York regulations...")
        try:
            html = self._get(f"{BASE_URL}/regulations/new-york")
            if "title-1" in html or "title-2" in html:
                logger.info("  Title list: OK")
            else:
                logger.error("  Title list: unexpected content")
                return False

            html = self._get(f"{BASE_URL}/regulations/new-york/22-NYCRR-202.8")
            if html and len(html) > 500:
                logger.info("  Section page: OK (22 NYCRR § 202.8)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("  Section page: unexpected content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def _extract_links(self, html: str, pattern: str) -> list:
        """Extract unique links matching a regex pattern from HTML."""
        links = []
        seen = set()
        for m in re.finditer(pattern, html):
            path = m.group(1)
            if path not in seen:
                seen.add(path)
                links.append(path)
        return links

    def discover_titles(self) -> list:
        """Discover all title URLs from the main page."""
        html = self._get(f"{BASE_URL}/regulations/new-york")
        title_paths = []
        seen = set()
        for m in re.finditer(r'href="(/regulations/new-york/title-(\d+))"', html):
            path = m.group(1)
            num = m.group(2)
            if num not in seen:
                seen.add(num)
                title_paths.append({"path": path, "num": num})
        logger.info(f"Discovered {len(title_paths)} titles")
        return title_paths

    def discover_hierarchy(self, page_path: str) -> tuple:
        """Read one hierarchy page, returning ``(sections, child_pages)``.

        Both lists matter at every level: a part page routinely lists its own
        sections *and* links deeper named subdivisions carrying more sections
        (``.../part-1`` has §§1.10-1.13 while ``.../part-1/bear`` has §§1.31-1.32).
        The old code returned as soon as it saw any section link, which dropped
        every subdivision below it — the ~3,900-of-30K shortfall in issue #1370.
        """
        html = self._get(f"{BASE_URL}{page_path}")
        if not html:
            return [], []

        sections = self._extract_links(
            html, r'href="(/regulations/new-york/\d+-NYCRR-[^"#]+)"'
        )
        # Any link strictly below the current page is a further subdivision.
        children = self._extract_links(
            html, r'href="(' + re.escape(page_path) + r'/[^"#?]+)"'
        )
        return sections, children

    def crawl_to_sections(self, page_path: str) -> list:
        """Breadth-first walk of one subtree, returning every section path.

        Sets ``self.enum_complete`` False if the deadline cut the walk short, so
        the caller knows not to checkpoint a partial enumeration as final.
        """
        self.enum_complete = True
        queue = [page_path]
        visited = {page_path}
        sections = []
        seen_sections = set()
        pages = 0

        while queue:
            if self.out_of_time():
                logger.warning(
                    f"Deadline hit while enumerating {page_path} "
                    f"({pages} pages, {len(sections)} sections so far)"
                )
                self.enum_complete = False
                return sections
            current = queue.pop(0)
            found, children = self.discover_hierarchy(current)
            pages += 1
            for s in found:
                if s not in seen_sections:
                    seen_sections.add(s)
                    sections.append(s)
            for child in children:
                # Depth is bounded by the URL itself, and `visited` keeps a page
                # that links a sibling subtree from being walked twice.
                if child not in visited and child.count("/") <= 10:
                    visited.add(child)
                    queue.append(child)

        logger.info(f"  Walked {pages} hierarchy pages under {page_path}")
        return sections

    def fetch_section(self, section_path: str, if_modified_since: str = None):
        """Fetch a single section page and extract full text.

        Returns ``NOT_MODIFIED`` when a conditional request was answered 304, so
        a refresh can tell "unchanged upstream" apart from "no text found".
        """
        url = f"{BASE_URL}{section_path}"
        html = self._get(url, if_modified_since=if_modified_since)
        if html is NOT_MODIFIED:
            return NOT_MODIFIED
        if not html:
            return None

        path_match = re.match(r'/regulations/new-york/(\d+)-NYCRR-(.+)', section_path)
        if not path_match:
            return None
        title_num = path_match.group(1)
        section_num = path_match.group(2)

        # Extract the page title
        heading = ""
        title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if title_match:
            heading = strip_html(title_match.group(1))
        if not heading:
            title_match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
            if title_match:
                heading = strip_html(title_match.group(1)).split("|")[0].strip()

        # Detect redirected/index pages (not actual section content)
        if 'State Regulations Toolbox' in html and 'Title 1 -' in html:
            logger.debug(f"Redirected to index page for {section_path}")
            return None

        # Extract the main regulation text
        text = self._extract_regulation_text(html)
        if not text:
            return None

        # Extract authority and reference
        authority = ""
        reference = ""
        auth_match = re.search(
            r'(?:Note:\s*)?Authority\s+cited?:?\s*(.*?)(?:(?:Reference|$))',
            html, re.DOTALL | re.IGNORECASE
        )
        if auth_match:
            authority = strip_html(auth_match.group(1)).strip().rstrip('.')
        ref_match = re.search(
            r'Reference:?\s*(.*?)(?:</p>|</div>|<br|$)',
            html, re.DOTALL | re.IGNORECASE
        )
        if ref_match:
            reference = strip_html(ref_match.group(1)).strip().rstrip('.')

        return {
            "section_id": f"NYCRR-{title_num}-{section_num}",
            "title_num": title_num,
            "section_number": section_num,
            "title": heading,
            "text": text,
            "authority": authority,
            "reference": reference,
            "url": url,
        }

    def _extract_regulation_text(self, html: str) -> str:
        """Extract the main regulation text from a section page."""
        text = ""

        # Strategy 1: Look for the main content div
        for marker in [
            'id="block-system-main"',
            'class="field-name-body"',
            'class="field--name-body"',
            'property="content:encoded"',
            'class="pane-node-body"',
            'id="content"',
        ]:
            idx = html.find(marker)
            if idx > 0:
                content = html[idx:]
                for end_marker in ['Note: Authority', 'NOTE: Authority',
                                   'AUTHORITY:', 'HISTORY:', 'History:',
                                   'Statutory authority:',
                                   'class="field-name-field-notes"',
                                   'id="footer"', '</article>']:
                    end_idx = content.find(end_marker)
                    if end_idx > 0:
                        content = content[:end_idx]
                        break
                text = strip_html(content)
                if len(text) > 50:
                    break

        # Strategy 2: Extract everything between first heading and Note/Authority
        if len(text) < 50:
            h1_end = html.find('</h1>')
            if h1_end > 0:
                content = html[h1_end + 5:]
                for end_marker in ['Note: Authority', 'NOTE: Authority',
                                   'AUTHORITY:', 'Statutory authority:',
                                   'class="footnote"']:
                    end_idx = content.find(end_marker)
                    if end_idx > 0:
                        content = content[:end_idx]
                        break
                text = strip_html(content)

        # Clean up common artifacts
        if text:
            text = re.sub(r'^id="[^"]*"[^>]*>\s*', '', text)
            # Remove repeated section heading (NYCRR citation format)
            text = re.sub(r'^N\.Y\.\s*Comp\.\s*Codes\s*R\.\s*&\s*Regs\.\s*[Tt]it\.\s*\d+,?\s*§§?\s*\S+\s*-?\s*[^\n]*\n*', '', text)
            # Remove navigation text that bleeds into content
            text = re.sub(r'\s*State Regulations\s*\n*', '\n', text)
            text = re.sub(r'\s*Compare\s*\n', '\n', text)
            text = re.sub(r'(?:Previous|Next)\s*(?:§|Section)\s*', '', text)
            text = re.sub(r'(?:Table of Contents|Browse)\s*', '', text)
            # Remove LII boilerplate
            text = re.sub(r'Cornell Law School.*?Legal Information Institute', '', text, flags=re.DOTALL)
            text = re.sub(r'About LII.*$', '', text, flags=re.DOTALL)
            text = re.sub(r'State Regulations Toolbox.*$', '', text, flags=re.DOTALL)
            # LII's quarterly version-comparison widget and the page footer trail
            # every section body; the amendment history above them is real and
            # must survive, so anchor on the widget's own opening sentence.
            text = re.sub(r'State regulations are updated quarterly.*$', '',
                          text, flags=re.DOTALL)
            text = re.sub(r'\n\s*Toolbox\s*(\n\s*about\s*)?$', '', text)
            text = re.sub(r'Accessibility\s*$', '', text)
            text = re.sub(r'\n{3,}', '\n\n', text).strip()

        return text

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["section_id"],
            "_source": "US/NY-AdminCode",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "section_id": raw["section_id"],
            "title_num": raw.get("title_num", ""),
            "section_number": raw.get("section_number", ""),
            "title": raw["title"],
            "text": raw["text"],
            "authority": raw.get("authority", ""),
            "reference": raw.get("reference", ""),
            "url": raw.get("url", ""),
            # Cornell publishes no amendment date, so the closest thing to a
            # document date is when it last rebuilt the page. That beats the
            # crawl timestamp this used to carry, which said nothing at all.
            "date": self._stamp_date(raw.get("url", "")),
        }

    def _stamp_date(self, url: str) -> str:
        """ISO date of the upstream Last-Modified for a section, else today."""
        path = url[len(BASE_URL):] if url.startswith(BASE_URL) else url
        stamp = self._stamps.get(path)
        if stamp:
            try:
                return parsedate_to_datetime(stamp).date().isoformat()
            except (TypeError, ValueError):
                pass
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _title_sections(self, title: dict, enum_cache: dict) -> list:
        """Section paths for one title, reusing a checkpointed enumeration."""
        num = title["num"]
        cached = enum_cache.get(num)
        if cached:
            logger.info(f"  Reusing checkpointed enumeration: {len(cached)} sections")
            return cached

        sections = self.crawl_to_sections(title["path"])
        if self.enum_complete:
            enum_cache[num] = sections
            self._save_enumeration(enum_cache)
        return sections

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all NYCRR sections with full text, resuming from checkpoint."""
        if getattr(self, "_sample", False):
            for sec_path in SAMPLE_SECTIONS:
                raw = self.fetch_section(sec_path)
                if raw and raw.get("text"):
                    yield raw
            return

        titles = self.discover_titles()
        enum_cache = self._load_enumeration()
        done = self._load_done()
        if done:
            logger.info(f"Resuming: {len(done)} sections already written")

        total = 0
        for title in titles:
            if self.out_of_time():
                logger.warning("Runtime deadline reached — stopping cleanly; "
                               "rerun to resume from the checkpoint")
                break
            logger.info(f"Processing Title {title['num']}...")
            sections = self._title_sections(title, enum_cache)
            pending = [s for s in sections if s not in done]
            logger.info(
                f"  Title {title['num']}: {len(sections)} sections, "
                f"{len(pending)} pending"
            )

            for raw in self._fetch_sections(pending):
                yield raw
                total += 1
                if total % 100 == 0:
                    logger.info(f"  Progress: {total} sections fetched this run")

        logger.info(f"Total sections fetched this run: {total}")

    def _fetch_sections(self, paths: list) -> Generator[dict, None, None]:
        """Fetch section pages through a paced worker pool, checkpointing each."""
        if not paths:
            return
        with ThreadPoolExecutor(max_workers=SECTION_WORKERS) as pool:
            # Chunked so the deadline is checked regularly and an aborted run
            # does not leave a large window of in-flight, unrecorded work.
            for start in range(0, len(paths), 200):
                if self.out_of_time():
                    logger.warning("Runtime deadline reached mid-title — stopping")
                    return
                chunk = paths[start:start + 200]
                for sec_path, raw in zip(chunk, pool.map(self.fetch_section, chunk)):
                    if raw and raw is not NOT_MODIFIED and raw.get("text"):
                        yield raw
                        self._mark_done(sec_path)
                    else:
                        logger.debug(f"No text for {sec_path}")
                # Seeds the comparator a later refresh reads. Flushed per chunk
                # so a run cut short by the deadline still narrows the next one.
                self._save_stamps()

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        """Yield only the sections Cornell has rebuilt since we last read them.

        NYCRR carries no amendment date we could filter on, and Cornell publishes
        no sitemap, no feed and no "recently updated" listing — so there is no
        index that would let a refresh skip pages without asking about them. What
        Cornell *does* serve is a per-page ``Last-Modified`` and honest support
        for conditional GET, which is the availability comparator this uses: each
        section is requested with ``If-Modified-Since`` set to the stamp of the
        copy we already hold, and an unchanged page comes back 304 with an empty
        body instead of 27KB of HTML to re-parse and re-upsert.

        The comparison is per section rather than per run. A section we have
        never stamped falls back to `since` if it is in the done-set (we demonstrably
        read it before that run) and is fetched unconditionally otherwise, so a
        section this scraper has not actually got can never be skipped.

        Deliberately *not* pruned at the hierarchy level: a part page's
        Last-Modified lags its own sections' by seconds (measured 2026-08-30:
        part-500 at 20:54:28 vs its newest section at 20:54:37), so skipping a
        subtree whose index looks untouched would silently drop real updates.
        The hierarchy is re-walked in full each refresh, which is also what
        surfaces sections added since the last crawl.
        """
        since_http = None
        since_date = as_date_str(since)
        if since_date:
            try:
                since_http = format_datetime(
                    datetime.fromisoformat(since_date).replace(tzinfo=timezone.utc),
                    usegmt=True,
                )
            except ValueError:
                logger.warning(f"Unparseable since value {since!r}; ignoring")

        titles = self.discover_titles()
        enum_cache = self._load_enumeration()
        done = self._load_done()
        logger.info(
            f"Incremental refresh: {len(self._stamps)} stamped sections, "
            f"{len(done)} previously written, cutoff {since_http or '(none)'}"
        )

        changed = unchanged = added = 0
        for title in titles:
            if self.out_of_time():
                logger.warning("Runtime deadline reached — stopping cleanly; "
                               "rerun to resume")
                break
            # Re-enumerate rather than reuse the cache: a cached list cannot
            # contain a section Cornell added after it was written.
            sections = self.crawl_to_sections(title["path"])
            if self.enum_complete and sections:
                enum_cache[title["num"]] = sections
                self._save_enumeration(enum_cache)
            logger.info(f"  Title {title['num']}: checking {len(sections)} sections")

            for sec_path in sections:
                if self.out_of_time():
                    break
                stamp = self._stamps.get(sec_path)
                if stamp is None and sec_path in done:
                    stamp = since_http
                is_new = sec_path not in done
                raw = self.fetch_section(sec_path, if_modified_since=stamp)
                if raw is NOT_MODIFIED:
                    unchanged += 1
                    continue
                if not (raw and raw.get("text")):
                    continue
                if is_new:
                    added += 1
                else:
                    changed += 1
                yield raw
                self._mark_done(sec_path)
            self._save_stamps()

        self._save_stamps()
        logger.info(
            f"Incremental refresh complete: {changed} rebuilt upstream, "
            f"{added} newly added, {unchanged} unchanged (304)"
        )


def main():
    scraper = NYAdminCodeScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py "
              "[test-api|bootstrap|bootstrap-fast|update] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)
    elif cmd == "bootstrap":
        # Route through BaseScraper so a full run streams to data/records.jsonl;
        # the old override wrote every record of a full run into sample/, so the
        # pipeline had nothing to ingest.
        scraper._sample = sample
        scraper.bootstrap(sample_mode=sample, sample_size=len(SAMPLE_SECTIONS))
    elif cmd == "update":
        # `update` used to be an alias for `bootstrap`, which re-crawled all
        # ~50K sections; it now runs the conditional-GET refresh (#1502).
        stats = scraper.update()
        logger.info(
            f"update complete: {stats.get('records_fetched', 0)} fetched, "
            f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
        )
    elif cmd == "bootstrap-fast":
        scraper.bootstrap_fast()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
