"""
Legal Data Hunter — Bulgarian State Gazette Scraper

Fetches legislative materials from the Bulgarian State Gazette (Държавен вестник)
published by the Bulgarian National Assembly.

Data source: https://dv.parliament.bg
Method: RSS feed + HTML scraping
Coverage: 2003 onwards
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings since the site has certificate issues
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("BG/StateGazette")

# Earliest known document ID (2005).
FIRST_ID = 1000

# Durable resume point, committed to git so a fresh VPS clone starts where the
# previous fleet worker stopped instead of re-crawling from FIRST_ID (issue #1433).
# Deliberately NOT named "*checkpoint*.json" — .gitignore excludes those.
RESUME_POINT_FILE = Path(__file__).parent / "resume_point.json"

# Per-run progress, under data/ (gitignored). Survives restarts on the same box.
RUNTIME_PROGRESS_FILE = Path(__file__).parent / "data" / "fetch_all_progress.json"

# Legacy plain-integer checkpoint kept for backwards compatibility.
LEGACY_CHECKPOINT_FILE = Path(__file__).parent / ".checkpoint_fetch_all"

# Write progress at least this often (in scanned IDs), so long runs of missing
# IDs still advance the resume point — doc-count-only checkpointing stalled
# across the large gaps in the ID space.
PROGRESS_EVERY_IDS = 250


class BulgarianStateGazetteScraper(BaseScraper):
    """
    Scraper for: Bulgarian State Gazette (Държавен вестник)
    Country: BG
    URL: https://dv.parliament.bg

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Set up HTTP client - must disable SSL verification due to cert issues
        self.client = HttpClient(
            base_url=self.config.get("api", {}).get("base_url", ""),
            headers=self._auth_headers,
        )
        # Disable SSL verification for this site due to certificate issues
        self.client.session.verify = False

        # Set by --restart; BaseScraper calls fetch_all() with no arguments.
        self._restart = False

    # ── Resume state ──────────────────────────────────────────────

    @staticmethod
    def _read_resume_id(path: Path) -> int:
        """Return the last completed idMat recorded in a JSON state file, or 0."""
        if not path.exists():
            return 0
        try:
            state = json.loads(path.read_text())
            return int(state.get("last_id", 0))
        except Exception as e:
            logger.warning(f"Ignoring unreadable resume state {path.name}: {e}")
            return 0

    def _resume_from(self) -> int:
        """
        Decide the first idMat to scan.

        Takes the furthest of the committed resume point, this box's runtime
        progress and the legacy plain-integer checkpoint, so a fresh clone still
        skips everything a previous fleet worker already ingested.
        """
        candidates = {
            "resume_point.json": self._read_resume_id(RESUME_POINT_FILE),
            "data/fetch_all_progress.json": self._read_resume_id(RUNTIME_PROGRESS_FILE),
        }
        if LEGACY_CHECKPOINT_FILE.exists():
            try:
                candidates[".checkpoint_fetch_all"] = int(
                    LEGACY_CHECKPOINT_FILE.read_text().strip()
                )
            except Exception:
                pass

        origin, last_id = max(candidates.items(), key=lambda kv: kv[1])
        if last_id <= FIRST_ID:
            logger.info(f"No usable resume point — starting from idMat={FIRST_ID}")
            return FIRST_ID

        logger.info(f"Resuming from {origin}: last completed idMat={last_id}")
        return last_id + 1

    def _save_progress(
        self, last_id: int, docs_found: int, scanned_to: int, complete: bool = False
    ) -> None:
        """
        Persist the crawl position so the next run resumes here.

        `last_id` is the highest idMat that actually yielded a document, not the
        highest scanned: the tail past the last document is where newly published
        materials land, so it must be re-scanned next run. Recording the scanned
        ceiling instead would push the frontier up by the ceiling margin on every
        run and silently skip everything published into that window.
        """
        RUNTIME_PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        RUNTIME_PROGRESS_FILE.write_text(
            json.dumps(
                {
                    "source_id": "BG/StateGazette",
                    "last_id": last_id,
                    "scanned_to": scanned_to,
                    "docs_found": docs_found,
                    "complete": complete,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                indent=2,
            )
        )
        # Keep the legacy file in step for any tooling still reading it.
        LEGACY_CHECKPOINT_FILE.write_text(str(last_id))

    # Scan this far past the newest published idMat, to cover materials issued
    # while a long run is still in flight.
    CEILING_MARGIN = 2000

    # Used only if the RSS ceiling lookup fails; above the live max as of 2026-08.
    FALLBACK_MAX_ID = 250000

    def _discover_max_id(self, floor_id: int) -> int:
        """
        Return the idMat to scan up to, derived from the gazette's own index.

        The ceiling used to be hardcoded (250000), which wasted a long tail of
        requests and would silently truncate the corpus once the gazette passed
        it. Probing the ID space directly does not work either: only about a
        third of IDs are live, in clusters separated by dead runs of 10-40 (and
        wider bands around e.g. 231300-231500), so any single-point stride probe
        reports a false end. Instead read the newest issue from the official RSS
        feed and take the highest idMat it links to.
        """
        newest = self._newest_published_id()
        if newest is None:
            max_id = max(self.FALLBACK_MAX_ID, floor_id + self.CEILING_MARGIN)
            logger.warning(
                f"Could not read the newest idMat from RSS — falling back to idMat={max_id}"
            )
            return max_id

        max_id = max(newest, floor_id) + self.CEILING_MARGIN
        logger.info(f"Newest published idMat = {newest}; scanning to {max_id}")
        return max_id

    def _newest_published_id(self):
        """
        Highest idMat linked from the most recent issue in the official RSS feed.

        RSS items link to issues (`materiali.faces?idObj=...`), not to materials,
        so this is a two-hop lookup: newest idObj, then that issue's contents.
        """
        try:
            self.rate_limiter.wait()
            rss = self.client.get("/rss_newspaper.jsp")
            issue_ids = [int(m) for m in re.findall(r"idObj=(\d+)", rss.text)]
            if not issue_ids:
                logger.warning("RSS feed listed no issues")
                return None

            self.rate_limiter.wait()
            issue = self.client.get(f"/materiali.faces?idObj={max(issue_ids)}")
            mat_ids = [int(m) for m in re.findall(r"idMat=(\d+)", issue.text)]
            if not mat_ids:
                logger.warning(f"Issue idObj={max(issue_ids)} listed no materials")
                return None

            return max(mat_ids)
        except Exception as e:
            logger.warning(f"Failed to read newest idMat from RSS: {e}")
            return None

    def fetch_all(self, restart: bool = False) -> Generator[dict, None, None]:
        """
        Yield all documents by iterating through document IDs (idMat).

        The Bulgarian State Gazette uses sequential document IDs from ~1000
        (2005) to ~245000+ (current). The ID space has large gaps, so a run of
        misses is not the end of the corpus — only `max_consecutive_failures`
        in a row past the discovered ceiling stops the scan.

        The crawl is resumable: the last scanned ID is persisted continuously and
        `resume_point.json` is committed to git, so a fleet worker that hits the
        100h wall can be relaunched and pick up where it left off (issue #1433).
        """
        restart = restart or self._restart
        start_id = FIRST_ID if restart else self._resume_from()
        if restart:
            logger.info("--restart requested: ignoring saved resume state")

        end_id = self._discover_max_id(max(start_id, FIRST_ID))
        if start_id > end_id:
            logger.info(
                f"Resume point idMat={start_id - 1} is at the live ceiling — nothing to fetch"
            )
            self._save_progress(start_id - 1, 0, start_id - 1, complete=True)
            return

        logger.info(f"Fetching documents from idMat={start_id} to {end_id}")

        consecutive_failures = 0
        # Safety net only — `end_id` is the real terminator now. Kept well above
        # the widest measured internal gap (~400 IDs) so a dead run mid-corpus
        # cannot cut the crawl short the way a 500 threshold nearly would.
        max_consecutive_failures = 2500
        docs_found = 0
        last_saved_id = start_id
        doc_id = start_id
        # Highest idMat that yielded a document; the resume point.
        last_found_id = start_id - 1

        for doc_id in range(start_id, end_id + 1):
            try:
                doc_data = self._fetch_document_by_id(doc_id)
                if doc_data:
                    consecutive_failures = 0
                    docs_found += 1
                    yield doc_data
                    last_found_id = doc_id
                else:
                    consecutive_failures += 1
                    # Log every 100 consecutive failures for debugging
                    if consecutive_failures % 100 == 0:
                        logger.debug(f"Consecutive failures: {consecutive_failures} at idMat={doc_id}")

            except Exception as e:
                logger.warning(f"Error fetching idMat={doc_id}: {e}")
                consecutive_failures += 1

            # Checkpoint on scanned IDs, not on documents found: the gaps in the
            # ID space are long enough that doc-count checkpointing left the
            # resume point thousands of IDs behind the real position.
            if doc_id - last_saved_id >= PROGRESS_EVERY_IDS:
                self._save_progress(last_found_id, docs_found, doc_id)
                last_saved_id = doc_id
                logger.info(
                    f"Progress saved: resume at idMat={last_found_id + 1} "
                    f"(scanned to {doc_id}), docs this run={docs_found}"
                )

            # Stop if too many consecutive failures (we've likely reached the end)
            if consecutive_failures >= max_consecutive_failures:
                logger.info(f"Stopping after {max_consecutive_failures} consecutive failures at idMat={doc_id}")
                break

        complete = doc_id >= end_id or consecutive_failures >= max_consecutive_failures
        self._save_progress(last_found_id, docs_found, doc_id, complete=complete)
        logger.info(
            f"fetch_all finished at idMat={doc_id}: {docs_found} documents found, "
            f"last document at idMat={last_found_id} "
            f"({'reached the ceiling' if complete else 'interrupted — rerun to resume'})"
        )

    def _fetch_document_by_id(self, doc_id: int):
        """
        Fetch a document directly by its idMat value.
        Returns None if document doesn't exist.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/showMaterialDV.jsp?idMat={doc_id}")

            # Check for valid response
            if resp.status_code != 200:
                logger.debug(f"HTTP {resp.status_code} for idMat={doc_id}")
                return None

            # Check if it's a valid document page (contains content markers)
            html = resp.text
            if len(html) < 500:  # Too short to be a real document
                return None

            if "titleHead" not in html and "tdHead1" not in html:
                # Not a document page
                return None

            soup = BeautifulSoup(html, "html.parser")

            # Extract title from titleHead div
            title_elem = soup.find("div", class_="titleHead")
            title = title_elem.get_text(strip=True) if title_elem else ""

            # Extract issue info (брой: XX, от дата DD.MM.YYYY г.)
            issue_number = None
            issue_date = None
            category = ""

            # Find all mark spans for metadata extraction
            mark_spans = soup.find_all("span", class_="mark")
            for span in mark_spans:
                text = span.get_text()
                # Extract issue number
                if "брой:" in text:
                    parts = text.split(",")
                    if parts:
                        issue_number = parts[0].replace("брой:", "").strip()
                # Extract date
                date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', text)
                if date_match and not issue_date:
                    issue_date = date_match.group(1)
                # Extract category
                if "Официален раздел" in text or "Неофициален раздел" in text:
                    if "/" in text:
                        category = text.split("/")[-1].strip()

            # Extract description from tdHead1 span
            desc_elem = soup.find("span", class_="tdHead1")
            description = desc_elem.get_text(strip=True) if desc_elem else ""

            # Extract full text content - try multiple approaches
            full_text = ""

            # Method 1: Look for div with width: 100% style
            content_div = soup.find("div", style=lambda x: x and "width: 100%" in x)
            if content_div:
                full_text = content_div.get_text(separator='\n', strip=True)

            # Method 2: If that fails, try to get text from main table
            if not full_text or len(full_text) < 20:
                # Find the table containing the document content
                tables = soup.find_all("table", {"cellpadding": "0", "width": "840px"})
                for table in tables:
                    text = table.get_text(separator='\n', strip=True)
                    if len(text) > len(full_text):
                        full_text = text

            # Method 3: Fall back to body text minus navigation
            if not full_text or len(full_text) < 20:
                body = soup.find("body")
                if body:
                    # Remove navigation elements
                    for nav in body.find_all(["script", "noscript", "style", "nav"]):
                        nav.decompose()
                    full_text = body.get_text(separator='\n', strip=True)

            # Clean up excessive whitespace
            if full_text:
                full_text = re.sub(r'\n\s*\n', '\n\n', full_text)
                full_text = re.sub(r' +', ' ', full_text)
                full_text = full_text.strip()

            if not full_text or len(full_text) < 50:
                # Document exists but has no meaningful content
                logger.debug(f"Document {doc_id} has insufficient content ({len(full_text) if full_text else 0} chars)")
                return None

            return {
                "doc_id": str(doc_id),
                "title": title,
                "description": description,
                "category": category,
                "issue_number": issue_number,
                "issue_date": issue_date,
                "full_text": full_text,
            }

        except Exception as e:
            logger.debug(f"Failed to fetch document {doc_id}: {e}")
            return None

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given datetime.

        For updates, we start from a recent document ID and work backwards
        until we find documents older than 'since'.
        """
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        logger.info(f"Fetching updates since {since}")

        # Start from a high ID (current max) and work backwards
        # until we find documents older than 'since'
        start_id = 250000
        found_old_doc = False
        consecutive_failures = 0

        for doc_id in range(start_id, 1000, -1):
            try:
                doc_data = self._fetch_document_by_id(doc_id)
                if doc_data:
                    consecutive_failures = 0
                    # Parse issue date
                    issue_date_str = doc_data.get("issue_date", "")
                    if issue_date_str:
                        try:
                            doc_date = datetime.strptime(issue_date_str, "%d.%m.%Y")
                            doc_date = doc_date.replace(tzinfo=timezone.utc)

                            if doc_date < since:
                                # Found a document older than 'since', stop
                                found_old_doc = True
                                logger.info(f"Found document from {issue_date_str}, stopping")
                                break
                            else:
                                yield doc_data
                        except:
                            # Can't parse date, include it
                            yield doc_data
                    else:
                        yield doc_data
                else:
                    consecutive_failures += 1
            except Exception as e:
                logger.warning(f"Error fetching idMat={doc_id}: {e}")
                consecutive_failures += 1

            if consecutive_failures >= 50:
                logger.info(f"Too many consecutive failures, moving to earlier IDs")
                consecutive_failures = 0

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into the standard schema.

        CRITICAL: Full text is now fetched during fetch_all/fetch_updates,
        stored in 'full_text' field.
        """
        # Get document ID
        doc_id = raw.get("doc_id", "")
        if not doc_id:
            # Generate from content hash if no ID available
            import hashlib
            content = f"{raw.get('category', '')}{raw.get('description', '')}{raw.get('issue_date', '')}"
            doc_id = hashlib.md5(content.encode()).hexdigest()[:16]

        # Parse publication date
        pub_date_str = raw.get("issue_date", "")
        pub_date_iso = None
        if pub_date_str:
            try:
                # Format: "8.2.2026"
                pub_date = datetime.strptime(pub_date_str, "%d.%m.%Y")
                pub_date_iso = pub_date.replace(tzinfo=timezone.utc).isoformat()
            except Exception as e:
                logger.warning(f"Failed to parse date '{pub_date_str}': {e}")

        # Build source URL
        source_url = f"https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat={doc_id}"

        # Use title or category for title field
        title = raw.get("title") or raw.get("category", "")

        # Full text was already fetched in fetch_all/fetch_updates
        full_text = raw.get("full_text", "")

        return {
            "_id": f"BG/StateGazette/{doc_id}",
            "_source": "BG/StateGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": pub_date_iso,
            "url": source_url,

            # Additional fields
            "doc_id": doc_id,
            "description": raw.get("description", ""),
            "category": raw.get("category", ""),
            "publication_date": pub_date_iso,

            # Issue metadata
            "issue_number": raw.get("issue_number"),
            "issue_date": raw.get("issue_date"),

            # Links
            "source_url": source_url,

            # Keep all raw fields
            "_raw": raw,
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = BulgarianStateGazetteScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update] "
            "[--sample] [--sample-size N] [--full] [--restart]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    # --restart discards the saved resume point and re-crawls from idMat=1000.
    scraper._restart = "--restart" in sys.argv

    # The fleet wrapper invokes `bootstrap-fast`; without the alias argparse-style
    # dispatch fell through and the pipeline re-ingested sample/ instead.
    if command == "bootstrap-fast":
        command = "bootstrap"
        sample_mode = sample_mode and "--full" not in sys.argv

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
