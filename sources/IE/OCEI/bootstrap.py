#!/usr/bin/env python3
"""
IE/OCEI -- Commissioner for Environmental Information (Ireland) — Appeal Decisions

Fetches the full text of formal, binding review decisions issued by the Office
the Commissioner for Environmental Information (OCEI, ocei.ie), the independent
statutory office that reviews decisions made by public authorities under the
European Communities (Access to Information on the Environment) Regulations
2007-2018 (the "AIE Regulations"). Each published decision determines whether a
public authority was justified in its handling of a request for environmental
information and is a formal, binding adjudication on a specific appeal = case_law.

Strategy:
  1. Enumerate every decision by paging the server-rendered decisions listing:
         https://ocei.ie/en/decisions/?page=N
     Each page lists 10 decisions as /en/ombudsman-decision/{slug}/ links.
     (The listing runs ~77 pages -> ~770 decisions.)
  2. Fetch each decision's canonical page and extract the born-digital full
     text from the gov.ie-style main content region (<div id="main" role="main">
     up to the site footer / feedback form) — no OCR/PDF extraction required.
  3. Parse the case number (OCE-NNNNNN-XXXXXX), publication date
     (<time datetime="...">) and the decision title (which encodes the
     applicant and the respondent FOI body).

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py bootstrap-fast       # Full pull (runner alias)
  python bootstrap.py update               # Incremental (recent decisions)
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.OCEI")

BASE_URL = "https://ocei.ie"
LIST_PATH = "/en/decisions/"
DETAIL_PREFIX = "/en/ombudsman-decision/"
MAX_PAGES = 200  # safety ceiling; real corpus is ~77 pages

# The decision body is inside the gov.ie-style main content region. It ends at
# the site footer, or (rendered just before it) the "Help us improve" feedback
# form — whichever comes first.
_BODY_END_MARKERS = ["Help us improve our site", "reboot-footer", "<footer"]


def _flatten(fragment: str) -> str:
    """Strip tags, decode entities, collapse whitespace -> clean text."""
    text = re.sub(r"<script.*?</script>", " ", fragment, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    # Preserve paragraph breaks lightly while collapsing runs of whitespace.
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_body(detail_html: str) -> str:
    """Isolate the decision body from the gov.ie-style main content region."""
    start = detail_html.find('id="main"')
    if start == -1:
        return ""
    # Move to the end of the opening tag.
    gt = detail_html.find(">", start)
    region = detail_html[gt + 1:] if gt != -1 else detail_html[start:]
    end = len(region)
    for marker in _BODY_END_MARKERS:
        idx = region.find(marker)
        if idx != -1:
            end = min(end, idx)
    return _flatten(region[:end])


def _extract_meta(detail_html: str) -> Dict[str, Optional[str]]:
    """Case number, ISO date and title from a decision page."""
    case_no = None
    m = re.search(r"Case number:\s*([A-Za-z0-9\-]+)", detail_html)
    if m:
        case_no = m.group(1).strip()

    date_iso = None
    m = re.search(r'<time[^>]*datetime="(\d{4}-\d{2}-\d{2})', detail_html)
    if m:
        date_iso = m.group(1)

    title = None
    m = re.search(r"<h1[^>]*>(.*?)</h1>", detail_html, re.DOTALL | re.IGNORECASE)
    if m:
        title = html.unescape(re.sub(r"<[^>]+>", "", m.group(1))).strip()
        title = re.sub(r"\s+", " ", title)
    return {"case_no": case_no, "date": date_iso, "title": title}


def _split_parties(title: str) -> Dict[str, str]:
    """Split 'Applicant & Respondent FOI body' -> (applicant, respondent)."""
    if not title:
        return {"applicant": "", "respondent": ""}
    # Titles use '&' (rendered from '&amp;') as the separator.
    parts = re.split(r"\s+&\s+", title, maxsplit=1)
    if len(parts) == 2:
        return {"applicant": parts[0].strip(), "respondent": parts[1].strip()}
    return {"applicant": title.strip(), "respondent": ""}


class OCEIScraper(BaseScraper):
    """Scraper for Irish Commissioner for Environmental Information (AIE) appeal decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "en-IE,en;q=0.9",
            },
            timeout=90,
        )

    def _list_page(self, page: int) -> Optional[List[str]]:
        """Return the decision links on a listing page (None on transient error)."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(LIST_PATH, params={"page": page})
            if resp.status_code != 200:
                logger.warning(f"list page {page}: HTTP {resp.status_code}")
                return None
            body = resp.content.decode("utf-8", errors="replace")
            links = re.findall(
                r'href="(' + re.escape(DETAIL_PREFIX) + r'[^"#?]+?/)"', body)
            # De-dupe while preserving order.
            seen, out = set(), []
            for l in links:
                if l not in seen:
                    seen.add(l)
                    out.append(l)
            return out
        except Exception as e:
            logger.warning(f"Error listing page {page}: {e}")
            return None

    def _detail(self, link: str) -> Optional[str]:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(link)
            if resp.status_code != 200:
                logger.warning(f"detail {link}: HTTP {resp.status_code}")
                return None
            return resp.content.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"Error fetching detail {link}: {e}")
            return None

    def _build(self, link: str) -> Optional[Dict[str, Any]]:
        detail_html = self._detail(link)
        if not detail_html:
            return None
        body = _extract_body(detail_html)
        if len(body) < 200:
            return None
        meta = _extract_meta(detail_html)
        return {"link": link, "body": body, "meta": meta}

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        any_item = False
        for page in range(1, MAX_PAGES + 1):
            links = self._list_page(page)
            if links is None:
                # Transient error — stop rather than silently truncate.
                break
            if not links:
                break
            for link in links:
                raw = self._build(link)
                if raw:
                    any_item = True
                    yield raw
            logger.info(f"page {page}: {len(links)} decisions")
        if not any_item:
            raise RuntimeError(
                "OCEI decisions listing returned 0 usable decisions — "
                "site blocked, listing changed, or markup changed"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_iso = since.strftime("%Y-%m-%d")
        for page in range(1, MAX_PAGES + 1):
            links = self._list_page(page)
            if not links:
                break
            stop = False
            for link in links:
                raw = self._build(link)
                if not raw:
                    continue
                d = raw["meta"].get("date")
                # Listing is newest-first; stop once we pass the cutoff.
                if d and d < since_iso:
                    stop = True
                    break
                yield raw
            if stop:
                break

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        body = raw.get("body", "")
        if len(body) < 200:
            return None
        link = raw.get("link", "")
        meta = raw.get("meta", {})
        case_no = meta.get("case_no")
        date_iso = meta.get("date")
        title_raw = meta.get("title") or ""
        parties = _split_parties(title_raw)

        url = link if link.startswith("http") else (BASE_URL + link)
        slug = link.strip("/").split("/")[-1]
        uid = case_no or slug
        title = title_raw or f"OCEI Decision {case_no or slug}"

        return {
            "_id": f"IE-OCEI-{uid}",
            "_source": "IE/OCEI",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": body,
            "date": date_iso,
            "url": url,
            "case_reference": case_no,
            "applicant": parties["applicant"],
            "respondent": parties["respondent"],
            "authority": "Commissioner for Environmental Information (Ireland)",
            "jurisdiction": "IE",
            "language": "en",
        }

    def test_connection(self):
        print("Testing OCEI decisions listing...")
        links = self._list_page(1)
        if links is None:
            print("  listing page 1 FETCH FAILED")
            return
        print(f"  listing page 1: {len(links)} decision links")
        if not links:
            return
        raw = self._build(links[0])
        if not raw:
            print("  detail build FAILED")
            return
        rec = self.normalize(raw)
        if rec:
            print(f"    title:     {rec['title']}")
            print(f"    reference: {rec['case_reference']}")
            print(f"    date:      {rec['date']}")
            print(f"    respondent:{rec['respondent']}")
            print(f"    url:       {rec['url']}")
            print(f"    text:      {len(rec['text'])} chars")
            print("    preview:   " + rec["text"][:200].replace("\n", " "))


def main():
    scraper = OCEIScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
