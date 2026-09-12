#!/usr/bin/env python3
"""
US/WA-Legislation -- Washington State Legislative Web Services

Fetches full text of Washington state legislation:
  - Revised Code of Washington (RCW): codified statutes from lawfilesext.leg.wa.gov
  - Bills: enrolled legislation via SOAP web services + HTML full text

Strategy:
  1. RCW: Crawl directory listing at lawfilesext.leg.wa.gov/law/RCW/ →
     title dirs → chapter dirs → section .htm files → extract text
  2. Bills: Use LegislativeDocumentService SOAP/REST to enumerate bill
     document URLs, then fetch HTML full text from lawfilesext.leg.wa.gov

Incremental refresh (see issue #1502):
  Both collections carry a real *availability* timestamp, so `fetch_updates`
  never has to walk the whole corpus.
    - RCW: the IIS directory listings print a modification timestamp on every
      entry. Section files are filtered on their own listed mtime; chapter
      directories are skipped wholesale when the directory's mtime predates the
      cutoff (a chapter dir's mtime is always >= the newest file inside it,
      because WA republishes a chapter by rewriting its files). Title
      directories are NOT used for pruning — their mtimes are demonstrably
      staler than their children's.
    - Bills: GetAllDocumentsByClass returns HtmLastModifiedDate per document,
      so one request per biennium yields the full change set.

Data: Public domain. No authentication required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all collections)
  python bootstrap.py bootstrap-fast       # Full pull, concurrent normalize
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py update [--since D]   # Incremental refresh
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import re
import time
import json
import logging
import html as html_module
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, date as date_cls
from typing import Generator, Optional
from urllib.parse import unquote, urljoin, quote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WA-Legislation")

RCW_BASE = "https://lawfilesext.leg.wa.gov/law/RCW"
SOAP_BASE = "https://wslwebservices.leg.wa.gov"
FILE_BASE = "https://lawfilesext.leg.wa.gov"
DELAY = 1.0  # seconds between requests

# The first biennium the LegislativeDocumentService exposes.
FIRST_BIENNIUM_YEAR = 1991


def biennium_for(year: int) -> str:
    """Return the WA biennium string ("2025-26") containing a calendar year.

    Bienniums start in odd years, so 2025 and 2026 both belong to "2025-26".
    """
    start = year if year % 2 == 1 else year - 1
    return f"{start}-{(start + 1) % 100:02d}"


def current_biennium() -> str:
    return biennium_for(datetime.now(timezone.utc).year)


def coerce_since(since) -> Optional[datetime]:
    """Reduce whatever `update()` hands us to a naive datetime, or None.

    `BaseScraper.update()` passes a `datetime`, the CLI passes a string, and a
    caller may pass a `date`. Comparing the wrong pair raises TypeError inside
    a generator, which surfaces as a silent zero-record refresh (#1512), so
    normalise here instead of at each comparison site. The listing timestamps
    are naive local times, so the result is made naive too.
    """
    if since is None:
        return None
    if isinstance(since, datetime):
        return since.replace(tzinfo=None) if since.tzinfo else since
    if isinstance(since, date_cls):
        return datetime(since.year, since.month, since.day)
    text = str(since).strip()
    if not text:
        return None
    text = text.replace("Z", "").replace("T", " ").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    # A cutoff we cannot parse would silently become "fetch everything" or
    # "fetch nothing"; both are worse than saying so.
    raise ValueError(f"Cannot parse `since` value: {since!r}")


def _soft_date(value) -> Optional[datetime]:
    """coerce_since for per-document stamps: an odd one is skipped, not fatal."""
    try:
        return coerce_since(value)
    except ValueError:
        logger.debug("Unparseable document timestamp: %r", value)
        return None


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'</tr>', '\n', text)
    text = re.sub(r'</li>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    # Remove BOM (both UTF-8 BOM character and raw bytes) and stray "PDF" link text
    text = text.replace('\ufeff', '').replace('\xef\xbb\xbf', '')
    text = re.sub(r'^ï»¿', '', text)
    text = re.sub(r'^PDF', '', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


# An IIS directory-listing row, e.g.
#   " 7/16/2026  6:26 AM        &lt;dir&gt; <A HREF="/law/RCW/...">RCW   1  TITLE</A>"
#   " 7/12/2024  8:03 AM         1685 <A HREF="/law/RCW/...">RCW   1 . 04 .010.htm</A>"
_LISTING_ROW = re.compile(
    r'(?P<d>\d{1,2}/\d{1,2}/\d{4})\s+(?P<t>\d{1,2}:\d{2}\s*[AP]M)\s+'
    r'(?P<size>&lt;dir&gt;|<dir>|[\d,]+)\s*'
    r'<a\s+href="(?P<href>[^"]+)"[^>]*>(?P<label>[^<]*)</a>',
    re.IGNORECASE,
)


class Entry:
    """One row of an IIS directory listing."""

    __slots__ = ("name", "url", "modified", "is_dir")

    def __init__(self, name: str, url: str, modified, is_dir: bool):
        self.name = name
        self.url = url
        self.modified = modified  # datetime or None
        self.is_dir = is_dir

    def __repr__(self):
        return f"<Entry {self.name!r} dir={self.is_dir} mtime={self.modified}>"


def _absolute(href: str, base_url: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{href}"
    return urljoin(base_url.rstrip("/") + "/", href)


def parse_directory_listing(html: str, base_url: str) -> list:
    """Parse an IIS directory listing into Entry objects carrying mtimes.

    The mtime is the whole point: it is the server's own record of when the
    file became available to us, which is the only comparator an incremental
    refresh can trust here (the documents themselves carry no reliable date).
    """
    entries = []
    for m in _LISTING_ROW.finditer(html):
        href = m.group("href")
        if href.startswith("?") or href.startswith(".."):
            continue
        label = html_module.unescape(m.group("label")).strip()
        if label.lower().startswith("[to parent directory]"):
            continue
        try:
            stamp = datetime.strptime(
                f"{m.group('d')} {m.group('t').replace(' ', '')}", "%m/%d/%Y %I:%M%p"
            )
        except ValueError:
            stamp = None
        name = label or unquote(href.rstrip("/").split("/")[-1])
        is_dir = m.group("size").lower() in ("&lt;dir&gt;", "<dir>")
        entries.append(Entry(name, _absolute(href, base_url), stamp, is_dir))
    return entries


class WALegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html, application/xml, text/xml, */*",
            },
            timeout=60,
        )

    def _get(self, url: str) -> str:
        """Fetch URL and return text, with rate limiting."""
        time.sleep(DELAY)
        resp = self.http.get(url)
        return resp.text

    def _get_xml(self, url: str) -> ET.Element:
        """Fetch URL and parse XML."""
        text = self._get(url)
        return ET.fromstring(text)

    # ── RCW (Revised Code of Washington) ──────────────────────────────

    def fetch_rcw_titles(self) -> list:
        """Get RCW title directories from the file server."""
        html = self._get(f"{RCW_BASE}/")
        entries = parse_directory_listing(html, f"{RCW_BASE}/")
        return [
            e for e in entries
            if e.is_dir and ("TITLE" in e.name.upper() or "TITLE" in e.url.upper())
        ]

    def fetch_rcw_chapters(self, title_url: str) -> list:
        """Get chapter directories within an RCW title."""
        html = self._get(title_url)
        entries = parse_directory_listing(html, title_url)
        return [
            e for e in entries
            if e.is_dir and ("CHAPTER" in e.name.upper() or "CHAPTER" in e.url.upper())
        ]

    def fetch_rcw_sections(self, chapter_url: str) -> list:
        """Get section .htm files within an RCW chapter."""
        html = self._get(chapter_url)
        entries = parse_directory_listing(html, chapter_url)
        sections = []
        for e in entries:
            if e.is_dir:
                continue
            if not e.url.lower().endswith((".htm", ".html")):
                continue
            # Skip chapter index pages (end with "CHAPTER.htm")
            if re.search(r'CHAPTER\.htm', e.url, re.IGNORECASE):
                continue
            sections.append(e)
        return sections

    def parse_rcw_section(self, html: str, section_name: str) -> dict:
        """Parse an RCW section HTML file to extract metadata and text."""
        # Extract section number from filename (e.g., "RCW  1 .04 .010.htm")
        # Clean the filename to get section number
        sec_num = section_name.replace(".htm", "").replace(".html", "")
        sec_num = re.sub(r'\s+', ' ', sec_num).strip()
        # Normalize: "RCW  1 .04 .010" -> "1.04.010"
        clean_num = sec_num.replace("RCW", "").strip()
        clean_num = re.sub(r'\s*\.\s*', '.', clean_num)
        clean_num = re.sub(r'\s+', '', clean_num)

        # Extract title from <title> or <h1> tags
        title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL | re.IGNORECASE)
        title = strip_html(title_match.group(1)) if title_match else sec_num

        # Extract the main body text
        text = strip_html(html)

        return {
            "section_number": clean_num,
            "title": title,
            "text": text,
        }

    def iter_rcw(self, max_sections: int = 0, since=None) -> Generator[dict, None, None]:
        """Iterate RCW sections with full text, newest-modified-first when filtered.

        With `since` set, only sections whose listed mtime is at or after the
        cutoff are downloaded. Chapter directories whose own mtime predates the
        cutoff are skipped without a request: WA republishes a chapter by
        rewriting the files in it, so the directory stamp is never older than
        the newest file it holds. Title directories get no such treatment —
        their stamps lag their chapters', so pruning there would drop real
        updates.
        """
        cutoff = coerce_since(since)
        titles = self.fetch_rcw_titles()
        logger.info(
            "RCW: %d titles found%s",
            len(titles),
            f" (incremental, modified since {cutoff})" if cutoff else "",
        )
        count = 0
        chapters_seen = 0
        chapters_scanned = 0
        sections_seen = 0

        for title in titles:
            # Extract title number from dir name
            t_match = re.search(r'(\d+[A-Z]?)', title.name)
            t_num = t_match.group(1) if t_match else title.name

            chapters = self.fetch_rcw_chapters(title.url)
            chapters_seen += len(chapters)
            logger.info(f"  Title {t_num}: {len(chapters)} chapters")

            for chapter in chapters:
                if cutoff and chapter.modified and chapter.modified < cutoff:
                    continue
                chapters_scanned += 1

                # Extract chapter number
                ch_match = re.search(r'(\d+[A-Z]?\s*\.\s*\d+[A-Z]?)', chapter.name)
                ch_num = (re.sub(r'\s+', '', ch_match.group(1))
                          if ch_match else chapter.name)

                sections = self.fetch_rcw_sections(chapter.url)
                sections_seen += len(sections)

                for section in sections:
                    if cutoff and section.modified and section.modified < cutoff:
                        continue
                    try:
                        html = self._get(section.url)
                        parsed = self.parse_rcw_section(html, section.name)

                        if not parsed["text"] or len(parsed["text"]) < 20:
                            continue

                        yield {
                            "collection": "RCW",
                            "section_id": f"RCW-{parsed['section_number']}",
                            "title_num": t_num,
                            "chapter_num": ch_num,
                            "section_number": parsed["section_number"],
                            "title": parsed["title"],
                            "text": parsed["text"],
                            "url": section.url,
                            "doc_date": section.modified,
                            "last_modified": section.modified,
                        }
                        count += 1
                        if count % 100 == 0:
                            logger.info(f"    RCW progress: {count} sections")
                        if max_sections and count >= max_sections:
                            return
                    except Exception as e:
                        logger.warning(f"Failed to fetch {section.url}: {e}")
                        continue

        if cutoff:
            logger.info(
                "RCW incremental: %d/%d chapters carried changes, %d sections "
                "listed, %d downloaded",
                chapters_scanned, chapters_seen, sections_seen, count,
            )

    # ── Bills (via SOAP/REST web services) ────────────────────────────

    def fetch_bill_documents(self, biennium: str, doc_class: str = "Bills") -> list:
        """Get bill document list via LegislativeDocumentService."""
        url = (
            f"{SOAP_BASE}/LegislativeDocumentService.asmx"
            f"/GetAllDocumentsByClass?biennium={biennium}&documentClass={doc_class}"
        )
        root = self._get_xml(url)
        ns = {"d": "http://WSLWebServices.leg.wa.gov/"}
        docs = []
        for doc in root.findall(".//d:LegislativeDocument", ns):
            def field(tag):
                el = doc.find(f"d:{tag}", ns)
                return (el.text or "") if el is not None else ""

            name = field("Name")
            htm_url = field("HtmUrl")
            if not name or not htm_url:
                continue
            docs.append({
                "name": name,
                "htm_url": htm_url,
                "pdf_url": field("PdfUrl"),
                "bill_id": field("BillId"),
                "long_name": field("LongFriendlyName"),
                "doc_type": field("Type"),
                # HtmCreateDate is when the document first became available;
                # HtmLastModifiedDate is when it last changed. The latter is the
                # incremental comparator, the former is the document's date.
                "htm_created": _soft_date(field("HtmCreateDate")),
                "htm_modified": _soft_date(field("HtmLastModifiedDate")),
            })
        return docs

    def fetch_bill_text(self, htm_url: str) -> str:
        """Fetch and clean full text from a bill HTML file."""
        try:
            html = self._get(htm_url)
            return strip_html(html)
        except Exception as e:
            logger.warning(f"Failed to fetch bill text {htm_url}: {e}")
            return ""

    def iter_bills(self, biennium: str = None, max_bills: int = 0,
                   since=None) -> Generator[dict, None, None]:
        """Iterate bills with full text for a given biennium.

        `since` filters on HtmLastModifiedDate, which the document service
        returns for every entry — so the whole change set for a biennium costs
        exactly one request.
        """
        biennium = biennium or current_biennium()
        cutoff = coerce_since(since)
        docs = self.fetch_bill_documents(biennium)
        if cutoff:
            fresh = [d for d in docs
                     if d["htm_modified"] is None or d["htm_modified"] >= cutoff]
            logger.info(
                "Bills (%s): %d of %d documents modified since %s",
                biennium, len(fresh), len(docs), cutoff,
            )
            docs = fresh
        else:
            logger.info(f"Bills ({biennium}): {len(docs)} documents found")
        count = 0

        for doc in docs:
            htm_url = doc["htm_url"]
            if not htm_url:
                continue

            text = self.fetch_bill_text(htm_url)
            if not text or len(text) < 50:
                continue

            bill_name = doc["name"]
            yield {
                "collection": "Bill",
                "section_id": f"BILL-{biennium}-{bill_name}",
                "biennium": biennium,
                "bill_name": bill_name,
                "bill_id": doc.get("bill_id", ""),
                "title": (doc.get("long_name")
                          or f"Washington State Bill {bill_name} ({biennium})"),
                "text": text,
                "url": htm_url,
                "doc_date": doc.get("htm_created"),
                "last_modified": doc.get("htm_modified"),
            }
            count += 1
            if count % 50 == 0:
                logger.info(f"    Bills progress: {count}")
            if max_bills and count >= max_bills:
                return

    # ── Standard interface ────────────────────────────────────────────

    def normalize(self, raw: dict) -> dict:
        """Transform raw record into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        section_id = raw.get("section_id", "")
        collection = raw.get("collection", "")

        # `date` is the document's publication date on the file server, not the
        # time we happened to crawl it — a crawl clock here would make every
        # record look brand new and defeat any date-filtered query downstream.
        doc_date = raw.get("doc_date") or raw.get("last_modified")
        last_modified = raw.get("last_modified")
        if doc_date is None:
            # `date` is a required temporal key, so a missing stamp would fail
            # validation and drop the record entirely (#995). Keep the document
            # and say which ones fell back.
            logger.warning("No upstream timestamp for %s; dating it to the crawl",
                           section_id)
            doc_date = datetime.now(timezone.utc)

        return {
            "_id": section_id,
            "_source": "US/WA-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": doc_date.date().isoformat() if doc_date else None,
            "last_modified": last_modified.isoformat() if last_modified else None,
            "url": raw.get("url", ""),
            "collection": collection,
            "section_number": raw.get("section_number", ""),
            "title_num": raw.get("title_num", ""),
            "chapter_num": raw.get("chapter_num", ""),
            "biennium": raw.get("biennium", ""),
            "bill_id": raw.get("bill_id", ""),
        }

    def _bill_bienniums(self, since=None) -> list:
        """Bienniums to walk. Full runs cover the current one; a refresh also
        revisits the previous one, since bills are amended across the biennium
        boundary."""
        current = current_biennium()
        if since is None:
            return [current]
        start = int(current.split("-")[0])
        return [biennium_for(start - 2), current]

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents (RCW + Bills)."""
        logger.info("Starting full fetch: RCW + Bills")
        yield from self.iter_rcw()
        for biennium in self._bill_bienniums():
            yield from self.iter_bills(biennium=biennium)

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        """Yield only documents that became available since `since`.

        Both halves compare against an upstream availability stamp — the IIS
        listing mtime for RCW sections, HtmLastModifiedDate for bills — rather
        than against anything in the document body, so a section whose text
        predates the cutoff still comes through when WA republishes it.
        """
        cutoff = coerce_since(since)
        if cutoff is None:
            logger.info("No cutoff supplied; falling back to a full fetch")
            yield from self.fetch_all()
            return

        logger.info("Incremental refresh: documents modified since %s", cutoff)
        yield from self.iter_rcw(since=cutoff)
        for biennium in self._bill_bienniums(since=cutoff):
            yield from self.iter_bills(biennium=biennium, since=cutoff)

    def run_sample(self) -> list:
        """Fetch sample records for testing."""
        records = []
        # 10 RCW sections
        for raw in self.iter_rcw(max_sections=10):
            records.append(self.normalize(raw))
        # 5 bills
        for raw in self.iter_bills(max_bills=5):
            records.append(self.normalize(raw))
        return records

    def test_api(self):
        """Test connectivity to the web services and file server."""
        logger.info("Testing RCW file server...")
        try:
            titles = self.fetch_rcw_titles()
            logger.info(f"  RCW: {len(titles)} titles found")
        except Exception as e:
            logger.error(f"  RCW file server failed: {e}")

        logger.info("Testing SOAP web service...")
        biennium = current_biennium()
        try:
            docs = self.fetch_bill_documents(biennium)
            dated = sum(1 for d in docs if d["htm_modified"])
            logger.info(
                "  Bills (%s): %d documents, %d with HtmLastModifiedDate",
                biennium, len(docs), dated,
            )
        except Exception as e:
            logger.error(f"  SOAP service failed: {e}")


def main():
    scraper = WALegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py "
              "[bootstrap|bootstrap-fast|update|test-api] [--sample] [--since YYYY-MM-DD]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test-api":
        scraper.test_api()

    elif cmd in ("bootstrap", "bootstrap-fast"):
        if "--sample" in sys.argv:
            records = scraper.run_sample()
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            for i, rec in enumerate(records):
                out_path = sample_dir / f"sample_{i:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(records)} sample records to {sample_dir}")

            total = len(records)
            with_text = sum(1 for r in records if r.get("text") and len(r["text"]) > 50)
            logger.info(f"Validation: {with_text}/{total} records have substantial text")
        elif cmd == "bootstrap-fast":
            stats = scraper.bootstrap_fast()
            logger.info(f"bootstrap_fast complete: {stats}")
        else:
            stats = scraper.bootstrap()
            logger.info(f"bootstrap complete: {stats}")

    elif cmd == "update":
        if "--since" in sys.argv:
            since = sys.argv[sys.argv.index("--since") + 1]
            count = 0
            for raw in scraper.fetch_updates(since):
                count += 1
            logger.info(f"update --since {since}: {count} changed documents")
        else:
            stats = scraper.update()
            logger.info(f"update complete: {stats}")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
