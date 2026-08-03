#!/usr/bin/env python3
"""
US/NC-IC -- North Carolina Industrial Commission (workers'-compensation
decisions: Full Commission & Deputy Commissioner Opinions and Awards).

The N.C. Industrial Commission is the state tribunal that adjudicates
workers'-compensation claims (and State tort claims). It publishes its
decisions in public, full-text-searchable OpenText(TM) Content Server
databases (ic.nc.gov/livelink). Each decision ("Opinion and Award")
resolves a specific contested case = case_law. These are official North
Carolina state-government works in the public domain (government edicts).

Access (no JavaScript, no CAPTCHA):
  Content Server exposes a guest account -- username/password both the
  literal word "public". The flow, reverse-engineered from the live
  advanced-search UI:

    1. POST func=ll.login (Username=public&Password=public) -> LLCookie
       session (persisted in a cookie jar).
    2. GET the searchprompt page and extract the byte-exact `template`
       hidden field (an A<...> structured argument).
    3. POST func=NewSearch with a MINIMAL field set (the full form is
       rejected as "[Pattern of argument was not recognized.]"). A
       Referer/Origin header of ic.nc.gov is mandatory. The result page
       links each decision as func=doc.ViewDoc&nodeid=<N>&vernum=1.
    4. The decision full text is the "View as Web Page" HTML render at
       /livelink/llview.exe/<name>.html?func=doc.View&nodeId=<N>&vernum=1.

  Content Server denies the guest user "browse" access, so the corpus is
  enumerated by full-text searching each workers'-comp slice for a set of
  ubiquitous decision terms and de-duplicating by nodeid.

Usage:
  python bootstrap.py bootstrap            # full pull
  python bootstrap.py bootstrap --sample   # ~12 samples
  python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # connectivity test
"""

from __future__ import annotations

import html
import json
import logging
import re
import subprocess
import sys
import tempfile
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NC-IC")

BASE = "https://ic.nc.gov/livelink/livelink.exe"
ORIGIN = "https://ic.nc.gov"
SEARCHPROMPT = f"{BASE}?func=ll&objType=258&objAction=searchprompt"

# Content Server slice (database) IDs -> readable database name.
SLICES = {
    "148754": "Full Commission",
    "268779": "Deputy Commissioner",
}

# Ubiquitous terms present in virtually every WC opinion-and-award; the
# union of their result sets (deduped by nodeid) approximates the whole
# corpus, since Content Server denies the guest user folder browsing.
QUERY_TERMS = [
    "injury", "employee", "compensation", "plaintiff", "defendant",
    "hearing", "award", "commission", "medical", "disability",
    "benefits", "workers", "opinion", "order", "employer",
]

HOWMANY = 50

VIEWDOC_RE = re.compile(r"func=doc\.ViewDoc&nodeid=(\d+)&vernum=(\d+)", re.I)
LLVIEW_RE = re.compile(r'["\'](/livelink/llview\.exe/[^"\']*func=doc\.View[^"\']*)["\']', re.I)
CASE_NO_RE = re.compile(r"I\.?\s*C\.?\s*N[Oo][Ss]?\.?\s*[:#]?\s*([0-9][0-9A-Za-z\-]{3,})")
_MONTH = ("January|February|March|April|May|June|July|August|"
         "September|October|November|December")
# NC IC decisions carry the filing date in two forms:
#   "Filed 31 January 2008"      (day-first, British style — the common one)
#   "Filed: January 31, 2008"    (month-first)
FILED_DMY_RE = re.compile(rf"Filed:?\s*(\d{{1,2}})\s+({_MONTH})\s+(\d{{4}})", re.I)
FILED_MDY_RE = re.compile(rf"Filed:?\s*({_MONTH})\s+(\d{{1,2}}),?\s+(\d{{4}})", re.I)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


class NCICScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        self._jar = tempfile.NamedTemporaryFile(
            prefix="ncic_cookies_", suffix=".txt", delete=False
        ).name
        self._template = None
        self._logged_in = False

    # ---------------------------------------------------------------- http
    def _curl(self, url: str, post_file: str = None, referer: str = None) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            cmd = ["curl", "-s", "-L", "--max-time", "90",
                   "-c", self._jar, "-b", self._jar,
                   "-A", self._ua, "-H", "Accept: */*"]
            if referer:
                cmd += ["-e", referer, "-H", f"Origin: {ORIGIN}"]
            if post_file:
                cmd += ["--data", f"@{post_file}"]
            cmd.append(url)
            try:
                out = subprocess.run(cmd, capture_output=True, timeout=120)
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _post(self, fields: list, referer: str = None) -> str | None:
        body = urllib.parse.urlencode(fields)
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            f.write(body)
            path = f.name
        try:
            return self._curl(BASE, post_file=path, referer=referer)
        finally:
            try:
                Path(path).unlink()
            except OSError:
                pass

    # ------------------------------------------------------------- session
    def _login(self) -> bool:
        if self._logged_in:
            return True
        self._post([
            ("func", "ll.login"),
            ("CurrentClientTime", ""),
            ("NextURL", "/livelink/livelink.exe?func=ll&objType=258&objAction=searchprompt"),
            ("Username", "public"),
            ("Password", "public"),
        ], referer=SEARCHPROMPT)
        page = self._curl(SEARCHPROMPT, referer=f"{BASE}?func=ll.index")
        if not page or "Advanced Search" not in page:
            logger.error("Login/searchprompt failed (no Advanced Search form).")
            return False
        self._template = self._extract_template(page)
        if not self._template:
            logger.error("Could not extract search template.")
            return False
        self._logged_in = True
        return True

    @staticmethod
    def _extract_template(page: str) -> str | None:
        i = page.find('NAME="template" VALUE="')
        if i < 0:
            m = re.search(r'name="template"\s+value="', page, re.I)
            if not m:
                return None
            i = m.start()
        start = page.find('VALUE="', i)
        if start < 0:
            start = page.lower().find('value="', i)
        start += len('VALUE="')
        nxt = page.find("<INPUT", start)
        if nxt < 0:
            nxt = start + 4000
        chunk = page[start:nxt]
        end = chunk.rfind('">')
        if end < 0:
            return None
        return html.unescape(chunk[:end])

    # ------------------------------------------------------------ discovery
    def _search_page(self, slice_id: str, term: str, start_at: int) -> str | None:
        fields = [
            ("func", "NewSearch"),
            ("objType", "258"),
            ("template", self._template),
            ("FullText_value1", term),
            ("FullText_mode1", "And"),
            ("FullText_content1", "All"),
            ("FullText_RowNum", "1"),
            ("FullText_SubRowNum", "1"),
            ("cbox_FullText", "on"),
            ("collections_search_selected", slice_id),
            ("cbox_Collections", "on"),
            ("StartAt", str(start_at)),
            ("Howmany", str(HOWMANY)),
            ("dspOptsForm",
             "A<1,?,'DisplayRegionList'={'Score','OTName','OTLocation'},"
             "'Howmany'=%d,'NarrativeType'='SO','ShowSummary'='on'>" % HOWMANY),
        ]
        return self._post(fields, referer=SEARCHPROMPT)

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield {nodeid, vernum, database} for every distinct decision."""
        if not self._login():
            return
        seen = set()
        for slice_id, db_name in SLICES.items():
            for term in QUERY_TERMS:
                start_at = 0
                while True:
                    page = self._search_page(slice_id, term, start_at)
                    if not page or "Result Page" not in page:
                        break
                    hits = VIEWDOC_RE.findall(page)
                    new_here = 0
                    for nodeid, vernum in hits:
                        if nodeid in seen:
                            continue
                        seen.add(nodeid)
                        new_here += 1
                        yield {"nodeid": nodeid, "vernum": vernum, "database": db_name}
                        if sample and len(seen) >= 12:
                            return
                    # advance pages only while the term keeps producing hits
                    if len(hits) < HOWMANY:
                        break
                    if new_here == 0:
                        break
                    start_at += HOWMANY
                if sample and len(seen) >= 12:
                    return

    # --------------------------------------------------------------- fetch
    def _fetch_text(self, nodeid: str, vernum: str) -> tuple[str, str] | None:
        viewdoc = f"{BASE}?func=doc.ViewDoc&nodeid={nodeid}&vernum={vernum}"
        page = self._curl(viewdoc, referer=BASE)
        if not page:
            return None
        m = LLVIEW_RE.search(page)
        if not m:
            return None
        llview = urllib.parse.urljoin("https://ic.nc.gov", html.unescape(m.group(1)))
        rendered = self._curl(llview, referer=viewdoc)
        if not rendered:
            return None
        text = self._clean_html(rendered)
        if len(text) < 400:
            return None
        return text, llview

    @staticmethod
    def _clean_html(page: str) -> str:
        page = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", page, flags=re.S | re.I)
        page = re.sub(r"<[^>]+>", " ", page)
        page = html.unescape(page)
        # drop the Content Server chrome fingerprint line if present
        page = re.sub(r"^\s*dcs[A-Za-z0-9_]+\s*", "", page)
        page = re.sub(r"[ \t\xa0]+", " ", page)
        page = re.sub(r"\s*\n\s*\n\s*", "\n\n", page)
        return page.strip()

    def _build_raw(self, doc: dict) -> dict | None:
        res = self._fetch_text(doc["nodeid"], doc["vernum"])
        if not res:
            return None
        text, url = res
        # trim leading UI residue before the first substantive caption
        head = text[:4000]
        cm = CASE_NO_RE.search(head)
        case_no = cm.group(1).strip(" .,") if cm else None
        date = None
        dm = FILED_DMY_RE.search(text)
        if dm:
            mo = MONTHS.get(dm.group(2).lower())
            try:
                date = datetime(int(dm.group(3)), mo, int(dm.group(1))).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date = None
        if date is None:
            mm = FILED_MDY_RE.search(text)
            if mm:
                mo = MONTHS.get(mm.group(1).lower())
                try:
                    date = datetime(int(mm.group(3)), mo, int(mm.group(2))).strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    date = None
        # caption/parties: text right after the NC INDUSTRIAL COMMISSION header
        parties = None
        pm = re.search(
            r"NORTH CAROLINA INDUSTRIAL COMMISSION\s+(.{20,400}?)(?:OPINION AND AWARD|Filed:)",
            text, re.I | re.S,
        )
        if pm:
            parties = re.sub(r"\s+", " ", pm.group(1)).strip(" .,")[:350]
        return {
            "nodeid": doc["nodeid"],
            "database": doc["database"],
            "case_number": case_no,
            "parties": parties,
            "text": text,
            "url": url,
            "date": date,
        }

    def normalize(self, raw: dict) -> dict:
        cn = raw.get("case_number")
        parties = raw.get("parties")
        title = f"NC Industrial Commission I.C. No. {cn}" if cn else "NC Industrial Commission Decision"
        if parties:
            title = f"{title}: {parties}"
        return {
            "_id": f"US/NC-IC/{raw['nodeid']}",
            "_source": "US/NC-IC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "nodeid": raw["nodeid"],
            "case_number": cn,
            "database": raw.get("database"),
            "parties": parties,
            "issuer": "North Carolina Industrial Commission",
            "title": title[:300],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NC",
        }

    # ------------------------------------------------------------- drivers
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    def test_api(self) -> bool:
        if not self._login():
            return False
        page = self._search_page("148754", "injury", 0)
        ok = bool(page and "Result Page" in page and VIEWDOC_RE.search(page))
        logger.info(f"test-api: login OK, search returns results = {ok}")
        return ok


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NC-IC bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NCICScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
