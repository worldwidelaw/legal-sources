import sys
import json
import hashlib
import logging
import re
import threading
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter")

BASE = "https://www.ccr.ro"

# Content Views paginates with `_page`, NOT `page`. A plain `?page=N` is
# silently ignored: every page returns the identical first slice, so the old
# "stop when a page has no links" loop never terminated (issue #1534).
SECTIONS = [
    "/jurisprudenta/jurisprudenta-decizii-de-admitere/",
    "/jurisprudenta/decizii-relevante/",
    "/jurisprudenta/hotarari-de-admitere/",
    "/jurisprudenta/hotarari-relevante/",
]

# Observed last pages 2026-09-01: 100 / 38 / 4 / 23. The cap is a runaway
# guard only — the walk normally stops on the first empty or all-seen page.
MAX_PAGES = 400

RO_MONTHS = {
    "ianuarie": 1, "februarie": 2, "martie": 3, "aprilie": 4,
    "mai": 5, "iunie": 6, "iulie": 7, "august": 8,
    "septembrie": 9, "octombrie": 10, "noiembrie": 11, "decembrie": 12,
}

MORE_LINK_TEXT = {"citește mai mult", "citeste mai mult", ""}


def solve_waf_challenge(html: str) -> Optional[str]:
    """Solve the ccr.ro SHA1 proof-of-work WAF challenge.
    Returns the cookie value 'res=<token><nonce>' or None if not a challenge page."""
    m = re.search(r"const a0_0x2a54=\['([^']+)','([^']+)','([^']+)'\]", html)
    if not m:
        return None
    arr = [m.group(1), m.group(2), m.group(3)]
    # Rotation: ++0x178 = 377, then while(--n) runs 376 times = 376 mod 3 shifts
    rot = 376 % len(arr)
    for _ in range(rot):
        arr.append(arr.pop(0))
    # arr[2] is the challenge token, arr[1] is 'array', arr[0] is 'res='
    challenge = arr[2]
    n1 = int(challenge[0], 16)
    for i in range(500000):
        h = hashlib.sha1((challenge + str(i)).encode()).digest()
        if h[n1] == 0xb0 and h[n1 + 1] == 0x0b:
            return f"res={challenge}{i}"
    return None


def parse_ro_date(title: str) -> Optional[str]:
    """'DECIZIA nr.883 din 17 august 2026' -> '2026-08-17'.

    The decision date only exists in the listing title; the upload path
    (/YYYY/MM/) is when the PDF was published to the CMS, which lags the
    decision by weeks and is wrong for a temporal key.
    """
    m = re.search(
        r"\b(\d{1,2})\s+(" + "|".join(RO_MONTHS) + r")\s+(\d{4})",
        title.lower(),
    )
    if not m:
        return None
    day, month, year = int(m.group(1)), RO_MONTHS[m.group(2)], int(m.group(3))
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


class SourceScraper(BaseScraper):
    """Scraper for Romanian Constitutional Court (ccr.ro)"""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        import requests
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
        })
        self._waf_solved = False
        # normalize() downloads PDFs and bootstrap_fast runs it on worker
        # threads, so the WAF re-solve must not race.
        self._waf_lock = threading.Lock()

    def _ensure_waf(self):
        """Solve WAF challenge if not already done."""
        with self._waf_lock:
            if self._waf_solved:
                return
            resp = self.session.get(BASE + SECTIONS[0], timeout=30)
            if resp.status_code == 503:
                cookie_val = solve_waf_challenge(resp.text)
                if cookie_val:
                    name, val = cookie_val.split("=", 1)
                    self.session.cookies.set(name, val, domain="www.ccr.ro", path="/")
                    logger.info("WAF challenge solved")
                    time.sleep(1)
                else:
                    raise RuntimeError("Could not solve WAF challenge")
            self._waf_solved = True

    def _get(self, url: str, pause: float = 1.0):
        """GET with WAF handling."""
        self._ensure_waf()
        resp = self.session.get(url, timeout=60)
        if resp.status_code == 503:
            self._waf_solved = False
            self._ensure_waf()
            resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        if pause:
            time.sleep(pause)
        return resp

    def _walk_listing(self, stop_before: Optional[datetime] = None):
        """Yield listing entries newest-first across every section.

        stop_before: when set, a section is abandoned as soon as a whole page
        is older than this date (listings are newest-first), which is what
        makes fetch_updates a real incremental pass rather than a full
        re-crawl.
        """
        from bs4 import BeautifulSoup

        seen_urls = set()

        for section in SECTIONS:
            logger.info(f"Scraping section: {section}")
            for page in range(1, MAX_PAGES + 1):
                url = f"{BASE}{section}?_page={page}"
                try:
                    resp = self._get(url)
                except Exception as e:
                    logger.error(f"Failed to fetch {url}: {e}")
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                # hrefs carry a '#new_tab' fragment, so an [href$='.pdf']
                # match finds almost nothing — that is why the fleet run
                # reported "1 fetched" (issue #1534).
                anchors = soup.select("a[href*='wp-content/uploads'][href*='.pdf']")
                if not anchors:
                    logger.info(f"No more PDFs in {section} after page {page - 1}")
                    break

                # Each item is linked twice: once by its title, once by a
                # "Citește mai mult" teaser. Keep the informative text.
                titles = {}
                for a in anchors:
                    href = a["href"].split("#")[0]
                    if not href.startswith("http"):
                        href = BASE + href
                    text = a.get_text(strip=True)
                    if text.lower() in MORE_LINK_TEXT:
                        titles.setdefault(href, "")
                    elif len(text) > len(titles.get(href, "")):
                        titles[href] = text

                fresh = [(u, t) for u, t in titles.items() if u not in seen_urls]
                if not fresh:
                    # Every URL repeated — the pagination parameter stopped
                    # working. Bail instead of looping forever.
                    logger.warning(
                        f"{section} page {page} returned only already-seen PDFs; "
                        "stopping this section"
                    )
                    break

                page_dates = []
                for pdf_url, title in fresh:
                    seen_urls.add(pdf_url)
                    date = parse_ro_date(title)
                    page_dates.append(date)
                    if stop_before and date and date < stop_before.strftime("%Y-%m-%d"):
                        continue
                    yield {"pdf_url": pdf_url, "title": title, "date": date}

                if stop_before and page_dates and all(
                    d and d < stop_before.strftime("%Y-%m-%d") for d in page_dates
                ):
                    logger.info(
                        f"{section}: page {page} entirely older than "
                        f"{stop_before:%Y-%m-%d}, stopping section"
                    )
                    break

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._walk_listing()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        if isinstance(since, str):
            since = datetime.fromisoformat(since.replace("Z", "+00:00"))
        yield from self._walk_listing(stop_before=since)

    def normalize(self, raw: dict) -> Optional[dict]:
        # The PDF download and text extraction live here rather than in
        # fetch_all so bootstrap_fast's worker pool actually overlaps them —
        # base_scraper only parallelises normalize().
        from common.pdf_extract import extract_pdf_markdown

        pdf_url = raw["pdf_url"]
        filename = pdf_url.split("/")[-1].replace(".pdf", "")

        text = raw.get("text")
        if text is None:
            try:
                pdf_resp = self._get(pdf_url, pause=0.2)
                text = extract_pdf_markdown(
                    "RO/ConstitutionalCourt",
                    filename,
                    pdf_bytes=pdf_resp.content,
                )
            except Exception as e:
                logger.warning(f"Failed to extract PDF {pdf_url}: {e}")
                return None

        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text extracted from {pdf_url}")
            return None

        date = raw.get("date")
        if not date:
            m = re.search(r"/(\d{4})/(\d{2})/", pdf_url)
            date = f"{m.group(1)}-{m.group(2)}-01" if m else None

        return {
            "_id": filename,
            "_source": "RO/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "id": filename,
            "title": raw.get("title", ""),
            "text": text,
            "date": date,
            "url": pdf_url,
        }


def main():
    scraper = SourceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "bootstrap-fast":
        # The fleet runner's entry point. Route it to the concurrent path so
        # the 1,300-odd PDF downloads overlap instead of running serially.
        stats = scraper.bootstrap_fast()
        print(f"\nBootstrap complete: {stats.get('records_new', 0)} new, "
              f"{stats.get('records_updated', 0)} updated, {stats.get('records_skipped', 0)} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
