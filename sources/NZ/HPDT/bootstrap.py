#!/usr/bin/env python3
"""
NZ/HPDT -- Health Practitioners Disciplinary Tribunal decisions

Fetches the Tribunal's decisions on disciplinary charges laid against New
Zealand health practitioners under the Health Practitioners Competence
Assurance Act 2003.

Strategy:
  - /Search-Decisions is a DNN/XModPro grid. An empty search is addressable by
    query string, but the pager is ASP.NET postback-only: paging forward means
    POSTing __EVENTTARGET=...lnkNext together with the __VIEWSTATE returned by
    the previous page, so the index walk is necessarily sequential.
  - Each result row carries the file number, decision date, profession, scope
    of practice and practitioner name; the file number links to
    /Charge-Details?file={ref}.
  - The detail page names the decision PDFs in `.DecF1`..`.DecF6` anchors whose
    href is empty in the HTML — jQuery fills it in as
    https://www.hpdt.org.nz/portals/0/{filename}. That path is reconstructed
    here rather than executing the page's JavaScript.
  - Decision PDFs are born-digital; text comes from the PDF.

Source: https://www.hpdt.org.nz/Search-Decisions
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap-fast       # Same, concurrent normalize (fleet entry point)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import re
import json
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NZ.HPDT")

BASE_URL = "https://www.hpdt.org.nz"
SEARCH_TMPL = (
    f"{BASE_URL}/Search-Decisions"
    "?Keyword=&PID=&CCID=&CCID2=&FID=&AOID=&YEAR={year}&CYEAR=&DA=&return=1"
)
SEARCH_URL = SEARCH_TMPL.format(year="")
PORTAL_FILES = f"{BASE_URL}/portals/0/"

REQUEST_TIMEOUT = (10, 60)
REQUEST_WALL_TIMEOUT = 150

# A host that starts dropping packets looks exactly like the end of the grid,
# so transport failures abort loudly instead of reporting a truncated corpus.
MAX_CONSECUTIVE_TRANSPORT_ERRORS = 8
MAX_PAGES = 400

MIN_TEXT = 500
PROGRESS_EVERY = 50

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

ROW_RE = re.compile(
    r"<tr[^>]*>\s*"
    r"<td><strong>(.*?)</strong></td>\s*"   # file number
    r"<td><strong>(.*?)</strong></td>\s*"   # decision date
    r"<td><strong>(.*?)</strong></td>\s*"   # profession
    r"<td><strong>(.*?)</strong></td>\s*"   # scope of practice
    r"<td><strong>(.*?)</strong></td>",     # practitioner
    re.S,
)


def _strip_html(fragment: str) -> str:
    fragment = re.sub(r"<script.*?</script>|<style.*?</style>", "", fragment, flags=re.S | re.I)
    fragment = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)
    fragment = re.sub(r"</(p|li|h\d|div|tr|blockquote)>", "\n", fragment, flags=re.I)
    text = html_mod.unescape(re.sub(r"<[^>]+>", "", fragment)).replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _parse_date(raw: str) -> Optional[str]:
    """'28 May 2026' -> '2026-05-28'."""
    m = re.search(r"(\d{1,2})\s+([A-Za-z]{3})[a-z]*\s+(\d{4})", raw or "")
    if not m:
        return None
    month = MONTHS.get(m.group(2).lower())
    if not month:
        return None
    return f"{int(m.group(3)):04d}-{month:02d}-{int(m.group(1)):02d}"


def _hidden(page: str, name: str) -> str:
    m = re.search(r'name="%s"[^>]*value="([^"]*)"' % re.escape(name), page)
    return html_mod.unescape(m.group(1)) if m else ""


def _slug(ref: str) -> str:
    """'Nur 22/565P' -> 'nur22-565p' (file numbers carry stray spaces)."""
    return re.sub(r"[^a-z0-9]+", "-", ref.lower().replace(" ", "")).strip("-")


class HPDTScraper(BaseScraper):
    """
    Scraper for NZ/HPDT -- Health Practitioners Disciplinary Tribunal.
    Country: NZ
    URL: https://www.hpdt.org.nz/Search-Decisions
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-NZ,en;q=0.9",
            },
            timeout=REQUEST_TIMEOUT,
            wall_timeout=REQUEST_WALL_TIMEOUT,
        )
        self.max_records: Optional[int] = None

    def _request(self, method: str, url: str, **kwargs):
        self.rate_limiter.wait()
        try:
            resp = self.client.post(url, **kwargs) if method == "POST" else self.client.get(url, **kwargs)
        except Exception as e:
            logger.debug(f"Transport error on {url}: {e}")
            return None
        if resp is None or resp.status_code in (429, 500, 502, 503, 504):
            return None
        return resp

    # ── Discovery ─────────────────────────────────────────────────────

    @staticmethod
    def _parse_rows(page: str) -> list:
        """Return [(ref, date_iso, profession, scope, practitioner)] for one grid page."""
        rows = []
        for ref, date_raw, prof, scope, name in ROW_RE.findall(page):
            ref = _strip_html(ref)
            if not ref:
                continue
            rows.append((
                ref,
                _parse_date(_strip_html(date_raw)),
                _strip_html(prof),
                _strip_html(scope),
                _strip_html(name),
            ))
        return rows

    def _years(self) -> list:
        """Read the file-year filter's own option values (e.g. '2025' … '2004/2005').

        The years are scraped rather than generated: the oldest option is the
        combined '2004/2005', and a hardcoded range would silently drop next
        year's decisions once the Tribunal adds the option.
        """
        resp = self._request("GET", SEARCH_URL)
        if resp is None or resp.status_code != 200:
            raise RuntimeError(
                "Could not load /Search-Decisions — hpdt.org.nz is refusing this vantage"
            )
        m = re.search(r'<select[^>]*\$Year"[^>]*>(.*?)</select>', resp.text, re.S)
        years = [v for v, _ in re.findall(r'value="([^"]*)"[^>]*>([^<]*)<', m.group(1))] if m else []
        years = [y for y in years if y]
        if not years:
            raise RuntimeError(
                "Could not read the year filter on /Search-Decisions — the XModPro "
                "layout changed or the host is serving a challenge page"
            )
        return years

    def _iter_index(self) -> Generator[tuple, None, None]:
        """Walk the results grid one file-year at a time, newest year first.

        The whole-corpus grid is one ~75-page postback chain, where a single
        dropped viewstate costs the rest of the crawl; per-year queries are
        addressable by URL and only run 1-5 pages each.
        """
        seen = set()
        for year in self._years():
            n_before = len(seen)
            for row in self._iter_year(year, seen):
                yield row
            logger.info(f"File-year {year}: {len(seen) - n_before} decisions")
            if self.max_records and len(seen) >= self.max_records:
                break

        logger.info(f"Discovered {len(seen)} decisions")
        if len(seen) < 500:
            self.record_coverage_gap(
                "search-decisions", "index walk returned far fewer decisions than published",
                found=len(seen), expected=1500,
            )
        else:
            self.clear_coverage_gap("search-decisions")

    def _iter_year(self, year: str, seen: set) -> Generator[tuple, None, None]:
        """Page through one file-year of the grid via its Next postback."""
        url = SEARCH_TMPL.format(year=quote(year, safe=""))
        resp = self._request("GET", url)
        if resp is None or resp.status_code != 200:
            raise RuntimeError(f"Could not load the {year} results grid at {url}")
        page = resp.text

        page_no = 1
        transport_errors = 0
        first_year = year

        while True:
            rows = self._parse_rows(page)
            if not rows:
                if page_no == 1:
                    raise RuntimeError(
                        f"Results grid for file-year {first_year} yielded 0 rows — the "
                        f"XModPro layout changed or the host is serving a challenge page"
                    )
                break

            new = 0
            for row in rows:
                if row[0] in seen:
                    continue
                seen.add(row[0])
                new += 1
                yield row

            if self.max_records and len(seen) >= self.max_records:
                return
            # A pager that re-serves the same rows would otherwise spin to MAX_PAGES.
            if new == 0:
                logger.debug(f"{first_year} page {page_no} repeated known rows — end of year")
                break

            m = re.search(r"__doPostBack\(&#39;([^&]*lnkNext)&#39;", page)
            if not m:
                break

            data = {
                "__EVENTTARGET": html_mod.unescape(m.group(1)),
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": _hidden(page, "__VIEWSTATE"),
                "__VIEWSTATEGENERATOR": _hidden(page, "__VIEWSTATEGENERATOR"),
                "__EVENTVALIDATION": _hidden(page, "__EVENTVALIDATION"),
            }
            resp = self._request("POST", url, data=data)
            if resp is None or resp.status_code != 200:
                transport_errors += 1
                if transport_errors >= MAX_CONSECUTIVE_TRANSPORT_ERRORS:
                    raise RuntimeError(
                        f"{MAX_CONSECUTIVE_TRANSPORT_ERRORS} consecutive failures paging past "
                        f"page {page_no} of file-year {first_year} — refusing to report a "
                        f"truncated corpus of {len(seen)} decisions"
                    )
                # The viewstate is still valid; retry the same Next postback.
                continue
            transport_errors = 0
            page = resp.text

            page_no += 1
            if page_no > MAX_PAGES:
                logger.warning(f"Stopping file-year {first_year} at the {MAX_PAGES}-page guard")
                break

    # ── Parsing ───────────────────────────────────────────────────────

    def _detail(self, ref: str) -> Optional[dict]:
        """Fetch /Charge-Details and pull the decision PDF names + charge metadata."""
        url = f"{BASE_URL}/Charge-Details?file={quote(ref, safe='/')}"
        resp = self._request("GET", url)
        if resp is None:
            return None
        if resp.status_code != 200:
            return {}
        page = resp.text

        # jQuery rewrites these empty hrefs to /portals/0/{filename}.
        pdfs, seen = [], set()
        for cls in ("DecF", "AppDecF"):
            for m in re.finditer(
                r'class="[^"]*\b%s\d\b[^"]*"[^>]*>([^<]*)</a>' % cls, page
            ):
                name = html_mod.unescape(m.group(1)).strip()
                if name.lower().endswith(".pdf") and name not in seen:
                    seen.add(name)
                    pdfs.append(name)

        def section(anchor_id: str) -> str:
            i = page.find(f'id="{anchor_id}"')
            if i < 0:
                return ""
            end = page.find("</span>", i)
            return _strip_html(page[i:end if end > 0 else i + 4000])

        return {
            "url": url,
            "pdf_names": pdfs,
            "charge_characteristics": section("secCC"),
            "outcome": section("secOutcome"),
        }

    def _raw(self, row: tuple) -> Optional[dict]:
        ref, date_iso, profession, scope, practitioner = row
        detail = self._detail(ref)
        if detail is None:
            return None
        if not detail:
            return {}
        return {
            "ref": ref,
            "date": date_iso,
            "profession": profession,
            "scope_of_practice": scope,
            "practitioner": practitioner,
            **detail,
        }

    # ── BaseScraper interface ─────────────────────────────────────────

    def iter_sample_raw(self, n: int = 15) -> Generator[dict, None, None]:
        """Yield ~n raw decisions spread across the grid.

        The grid is ordered newest first, so sampling the first n rows would
        only validate 2025-2026 decisions and never touch the 2004 layout.
        """
        years = self._years()
        step = max(1, len(years) // max(1, n))
        yielded = 0
        for year in years[::step]:
            for row in self._iter_year(year, set()):
                raw = self._raw(row)
                if raw:
                    yielded += 1
                    yield raw
                break  # one decision per year is enough to validate the layout
            if yielded >= n:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        fetched = 0
        for row in self._iter_index():
            raw = self._raw(row)
            if raw is None:
                logger.warning(f"Transport failure on {row[0]} — skipping")
                continue
            if not raw:
                continue
            yield raw
            fetched += 1
            if fetched % PROGRESS_EVERY == 0:
                logger.info(f"Fetched {fetched} decisions")
            if self.max_records and fetched >= self.max_records:
                break
        logger.info(f"fetch_all complete: {fetched} decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions dated on/after `since` (the grid is newest first)."""
        cutoff = since.date().isoformat()
        stale_streak = 0
        for raw in self.fetch_all():
            if raw.get("date") and raw["date"] < cutoff:
                stale_streak += 1
                if stale_streak >= 20:
                    logger.info(f"Reached decisions older than {cutoff} — stopping update walk")
                    return
                continue
            stale_streak = 0
            yield raw

    def normalize(self, raw: dict) -> dict:
        ref = raw["ref"]
        parts = []
        for name in raw.get("pdf_names", []):
            try:
                text = extract_pdf_markdown(
                    "NZ/HPDT", f"{_slug(ref)}-{name}",
                    pdf_url=PORTAL_FILES + quote(name), table="case_law",
                )
            except Exception as e:
                logger.debug(f"PDF extraction failed for {name}: {e}")
                continue
            if text:
                parts.append(text)

        text = "\n\n".join(parts)
        practitioner = raw.get("practitioner") or ""
        profession = raw.get("profession") or ""
        title = f"HPDT {ref}" + (f" — {practitioner}" if practitioner else "")
        if profession:
            title += f" ({profession})"

        return {
            "_id": f"hpdt-{_slug(ref)}",
            "_source": "NZ/HPDT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "case_reference": ref,
            "court": "Health Practitioners Disciplinary Tribunal",
            "jurisdiction": "New Zealand",
            "practitioner": practitioner or None,
            "profession": profession or None,
            "scope_of_practice": raw.get("scope_of_practice") or None,
            "charge_characteristics": raw.get("charge_characteristics") or None,
            "pdf_url": (PORTAL_FILES + quote(raw["pdf_names"][0])) if raw.get("pdf_names") else None,
            "language": "en",
        }

    def _dedup_key(self, record: dict) -> str:
        return record.get("_id", "")

    def test_api(self) -> bool:
        resp = self._request("GET", SEARCH_URL)
        if resp is None or resp.status_code != 200:
            logger.error("Could not load the HPDT decision search")
            return False
        rows = self._parse_rows(resp.text)
        if not rows:
            logger.error("Results grid yielded no rows")
            return False
        logger.info(f"OK — grid page 1 carries {len(rows)} decisions, newest {rows[0][0]}")
        raw = self._raw(rows[0])
        if not raw:
            logger.error(f"Could not read the detail page for {rows[0][0]}")
            return False
        rec = self.normalize(raw)
        logger.info(f"OK — {rec['_id']} ({rec['date']}): {len(rec['text'])} chars")
        return len(rec["text"]) >= MIN_TEXT


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NZ/HPDT bootstrap")
    # The fleet wrapper invokes `bootstrap-fast`; without it argparse exits 2
    # and the wrapper falls back to re-ingesting sample/.
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Full fetch (all records)")
    args = parser.parse_args()

    scraper = HPDTScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.sample:
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for raw in scraper.iter_sample_raw(15):
            record = scraper.normalize(raw)
            if len(record.get("text", "")) < MIN_TEXT:
                logger.warning(f"Skipping {record['_id']} — only {len(record.get('text',''))} chars")
                continue
            count += 1
            (sample_dir / f"{count:04d}.json").write_text(
                json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            logger.info(
                f"[{count}] {record['_id']} — {record['title'][:60]} ({len(record['text'])} chars)"
            )
            if count >= 15:
                break
        logger.info(f"Done: {count} sample records fetched")
        return

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
    else:
        stats = scraper.bootstrap()

    logger.info(
        f"Done: {stats.get('records_fetched', 0)} fetched, "
        f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
    )
    if stats.get("error_message"):
        logger.error(f"Bootstrap failed: {stats['error_message']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
