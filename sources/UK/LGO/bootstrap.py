#!/usr/bin/env python3
"""
Legal Data Hunter — UK Local Government & Social Care Ombudsman (LGSCO/LGO)
Decisions Scraper

Fetches the published investigation decisions of the Local Government and Social
Care Ombudsman — the statutory body (Commission for Local Administration in
England, under the Local Government Act 1974) that investigates complaints about
councils and adult social care providers. Each decision records the Ombudsman's
findings (fault / no fault / maladministration), any injustice caused and the
remedy agreed = case_law. Decisions cover adult social care, children's care,
housing, education, benefits & tax, environment, planning, transport & highways,
and health (joint working with the Parliamentary & Health Service Ombudsman).

Source: https://www.lgo.org.uk/decisions
  - The public decisions listing is a server-rendered search:
    GET /Decisions/SearchResults?fd=YYYY-MM-DD&td=YYYY-MM-DD&page=N
    (10 results/page; total shown as "Your search has N results"). The site's
    front-end posts a form to /decisionsnew/newsearchpost which 302-redirects to
    this GET results URL — we call the GET directly (no reCAPTCHA on results;
    the captcha only guards the site-wide /searchpost and complaint forms).
  - Each result links to an individual decision page
    /decisions/{category}/{subcategory}/{ref}  (ref = "YY NNN NNN") whose
    <article id="article"> holds the FULL TEXT: an articleHeader (authority +
    reference, Category, Decision outcome, Decision date) followed by the
    narrative (Summary / The complaint / The Ombudsman's role and powers / How I
    considered this complaint / My assessment / Final decision).

Coverage: ~57,500 decisions (bulk 2016–present; the search endpoint answers
whole-corpus date windows). Born-digital HTML, no OCR.

Enumeration is date-windowed (one calendar month at a time) so each window stays
well under any pagination ceiling; completed windows are checkpointed to
data/lgo_checkpoint.json so fleet re-runs resume without re-fetching.

License: Commission for Local Administration in England custom re-use terms —
free re-use in any format (copying, publishing, broadcasting, translating) with
attribution; only bars use "for the principal purpose of advertising or
promoting a particular product or service". OGL-like; commercial use permitted
with attribution. https://www.lgo.org.uk/copyright

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
  python bootstrap.py test               # Connectivity check
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/LGO")

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

MIN_TEXT_CHARS = 150
START_YEAR = 2010  # empty windows before ~2016 return 0 fast

# Individual decision permalink: /decisions/{cat}/{subcat}/{ref}
DECISION_RE = re.compile(r"^/decisions/[a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*/[0-9][0-9a-z-]*$")
COUNT_RE = re.compile(r"has\s+([0-9,]+)\s+results", re.I)


class UKLGOScraper(BaseScraper):
    """Scraper for Local Government & Social Care Ombudsman decisions."""

    BASE_URL = "https://www.lgo.org.uk"
    SEARCH_URL = BASE_URL + "/Decisions/SearchResults"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; legal research)",
            "Accept": "text/html,application/xhtml+xml",
        })
        self._ckpt_path = Path(__file__).parent / "data" / "lgo_checkpoint.json"

    # ------------------------------------------------------------------- fetch
    def _get(self, url: str, params: dict = None) -> Optional[str]:
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, params=params, timeout=45)
            if resp.status_code == 200:
                return resp.text
            logger.warning(f"{resp.status_code} for {url} {params or ''}")
            return None
        except Exception as e:
            logger.warning(f"Request failed for {url}: {e}")
            return None

    # ------------------------------------------------------------ checkpoint
    def _load_ckpt(self) -> set:
        try:
            return set(json.loads(self._ckpt_path.read_text()))
        except Exception:
            return set()

    def _save_ckpt(self, done: set):
        try:
            self._ckpt_path.parent.mkdir(parents=True, exist_ok=True)
            self._ckpt_path.write_text(json.dumps(sorted(done)))
        except Exception as e:
            logger.warning(f"Could not write checkpoint: {e}")

    # --------------------------------------------------------------- discovery
    @staticmethod
    def _month_windows() -> list:
        """Yield (fd, td, key) monthly windows from START_YEAR to current month."""
        now = datetime.now(timezone.utc)
        wins = []
        for year in range(START_YEAR, now.year + 1):
            last_month = 12 if year < now.year else now.month
            for month in range(1, last_month + 1):
                fd = f"{year:04d}-{month:02d}-01"
                if month == 12:
                    td = f"{year:04d}-12-31"
                else:
                    # first day of next month minus one day, computed simply
                    nm = datetime(year, month + 1, 1)
                    from datetime import timedelta
                    ld = nm - timedelta(days=1)
                    td = ld.strftime("%Y-%m-%d")
                wins.append((fd, td, f"{year:04d}-{month:02d}"))
        return wins

    def _result_links(self, html: str) -> list:
        soup = BeautifulSoup(html, "html.parser")
        out, seen = [], set()
        for a in soup.find_all("a", href=True):
            href = a["href"].split("?")[0].split("#")[0]
            if DECISION_RE.match(href) and href not in seen:
                seen.add(href)
                out.append(self.BASE_URL + href)
        return out

    def _window_urls(self, fd: str, td: str) -> Generator[str, None, None]:
        """Paginate one date window, yielding every decision URL."""
        first = self._get(self.SEARCH_URL, {"fd": fd, "td": td, "page": 1})
        if first is None:
            return
        m = COUNT_RE.search(first)
        total = int(m.group(1).replace(",", "")) if m else 0
        if total == 0:
            return
        pages = (total + 9) // 10
        seen = set()
        for link in self._result_links(first):
            if link not in seen:
                seen.add(link)
                yield link
        for page in range(2, pages + 1):
            html = self._get(self.SEARCH_URL, {"fd": fd, "td": td, "page": page})
            if not html:
                continue
            links = self._result_links(html)
            if not links:
                break
            for link in links:
                if link not in seen:
                    seen.add(link)
                    yield link

    # --------------------------------------------------------------- extraction
    @staticmethod
    def _parse_decision(html: str, url: str) -> Optional[dict]:
        soup = BeautifulSoup(html, "html.parser")
        article = soup.find(id="article") or soup.find("article") or soup
        for tag in article(["script", "style", "nav", "header", "footer", "form"]):
            tag.decompose()

        h1 = article.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else ""

        # Metadata label/value pairs in the article header.
        meta = {}
        for lab in article.find_all(class_="label"):
            val = lab.find_next(class_="value")
            if val:
                key = lab.get_text(" ", strip=True).rstrip(":").strip()
                v = re.sub(r"\s+", " ", val.get_text(" ", strip=True)).strip()
                if key and key not in meta:
                    meta[key] = v

        # Full text: article content, whitespace-normalised.
        raw_text = article.get_text("\n", strip=True)
        lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
        text = "\n".join(lines).strip()

        # Reference from URL last segment (already dash-formatted, e.g. 24-011-791).
        ref = url.rstrip("/").split("/")[-1]

        # Authority = title up to the "(...)" reference.
        authority = re.sub(r"\s*\([0-9 ]+\)\s*$", "", title).strip()

        category = meta.get("Category", "")
        cat_main, _, cat_sub = category.partition(">")
        return {
            "url": url,
            "ref": ref,
            "title": title,
            "authority": authority,
            "text": text,
            "category": cat_main.strip() or None,
            "subcategory": cat_sub.strip() or None,
            "outcome": meta.get("Decision", "").strip() or None,
            "date_text": meta.get("Decision date", "").strip(),
        }

    # ---------------------------------------------------------------- iteration
    def fetch_all(self) -> Generator[dict, None, None]:
        # Fail loud if the results endpoint is blocked, rather than emit nothing.
        probe = self._get(self.SEARCH_URL, {"fd": "2025-01-01", "td": "2025-01-31", "page": 1})
        if probe is None or COUNT_RE.search(probe) is None:
            raise RuntimeError(
                "LGO SearchResults endpoint unreachable / returned no result count "
                "— possible datacenter-IP block (fail loud rather than emit an empty corpus)"
            )

        done = self._load_ckpt()
        windows = self._month_windows()
        count, skipped = 0, 0
        seen_refs = set()
        for fd, td, key in windows:
            if key in done:
                continue
            for url in self._window_urls(fd, td):
                ref = url.rstrip("/").split("/")[-1]
                if ref in seen_refs:
                    continue
                seen_refs.add(ref)
                dhtml = self._get(url)
                if not dhtml:
                    skipped += 1
                    continue
                parsed = self._parse_decision(dhtml, url)
                if not parsed or len(parsed["text"]) < MIN_TEXT_CHARS:
                    skipped += 1
                    continue
                count += 1
                yield parsed
            done.add(key)
            self._save_ckpt(done)
            logger.info(f"  window {key} done — {count} decisions ({skipped} skipped)")
        logger.info(f"Total: {count} decisions ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions dated on/after `since` (walk recent monthly windows)."""
        since_str = since.strftime("%Y-%m-%d")
        now = datetime.now(timezone.utc)
        # Walk from `since`'s month to the current month.
        from datetime import timedelta
        cursor = datetime(since.year, since.month, 1)
        while cursor <= now:
            fd = cursor.strftime("%Y-%m-01")
            if cursor.month == 12:
                td = cursor.strftime("%Y-12-31")
                nxt = datetime(cursor.year + 1, 1, 1)
            else:
                nxt = datetime(cursor.year, cursor.month + 1, 1)
                td = (nxt - timedelta(days=1)).strftime("%Y-%m-%d")
            for url in self._window_urls(fd, td):
                dhtml = self._get(url)
                if not dhtml:
                    continue
                parsed = self._parse_decision(dhtml, url)
                if not parsed or len(parsed["text"]) < MIN_TEXT_CHARS:
                    continue
                d = self._parse_date(parsed.get("date_text", ""))
                if d and d >= since_str:
                    yield parsed
            cursor = nxt

    # ---------------------------------------------------------------- normalize
    @staticmethod
    def _parse_date(text: str) -> Optional[str]:
        if not text:
            return None
        m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text)
        if not m:
            return None
        day, mon, year = m.group(1), m.group(2)[:3].lower(), m.group(3)
        month = MONTHS.get(mon)
        if not month:
            return None
        try:
            return f"{int(year):04d}-{month:02d}-{int(day):02d}"
        except ValueError:
            return None

    def normalize(self, raw: dict) -> dict:
        text = (raw.get("text", "") or "").strip()
        if not text:
            return None
        ref = (raw.get("ref", "") or "").strip() or raw.get("url", "").rstrip("/").split("/")[-1]
        case_id = ref.replace("-", " ")
        # LGSCO decision pages have no <h1>, so the scraped title is usually empty.
        # Build a descriptive one from the reference, category and outcome.
        title = (raw.get("title", "") or "").strip()
        if not title:
            cat = raw.get("category") or ""
            sub = raw.get("subcategory") or ""
            cat_part = " — ".join(p for p in (cat, sub) if p)
            outcome = raw.get("outcome") or ""
            bits = [f"LGSCO decision {case_id}"]
            if cat_part:
                bits.append(cat_part)
            if outcome:
                bits.append(f"({outcome})")
            title = " · ".join(bits[:2]) + (f" {bits[2]}" if len(bits) > 2 else "")
        return {
            "_id": f"UK/LGO/{ref}",
            "_source": "UK/LGO",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_id": case_id,
            "title": title,
            "text": text,
            "date": self._parse_date(raw.get("date_text", "")),
            "authority": raw.get("authority") or None,
            "category": raw.get("category") or None,
            "subcategory": raw.get("subcategory") or None,
            "outcome": raw.get("outcome") or None,
            "url": raw.get("url", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKLGOScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "test":
        html = scraper._get(
            scraper.SEARCH_URL, {"fd": "2025-01-01", "td": "2025-01-31", "page": 1}
        )
        m = COUNT_RE.search(html or "")
        links = scraper._result_links(html or "")
        print(f"Connection OK: {m.group(0) if m else 'no count'}; {len(links)} links on page 1")
        print("OK" if (m and links) else "FAILED")
    elif cmd in ("bootstrap", "bootstrap-fast"):
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    elif cmd == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
