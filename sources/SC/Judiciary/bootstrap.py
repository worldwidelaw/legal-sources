#!/usr/bin/env python3
"""
Legal Data Hunter - Seychelles Judiciary Court Decisions Scraper

Fetches full-text judgments from the official Judiciary of Seychelles website
(judiciary.sc). The "Decisions from the Courts" page publishes judgments of the
Court of Appeal, Constitutional Court, and Supreme Court (criminal and civil)
as downloadable PDFs with rich structured metadata (case name, case number,
media-neutral citation, decision date, presiding judges, decision type, and a
keyword/summary blurb).

This is the official judiciary source and is distinct from SeyLII (blocked).

Strategy:
  - Enumerate every published judgment PDF via the site's WordPress REST media
    API (/wp-json/wp/v2/media) — a stable superset of the JS-filtered
    court-decisions table. Judgment PDFs are named with a leading hearing date
    (DDMMYYYY) followed by the case number and parties, e.g.
    "25062026-cp12_2022-mukesh-valabhji-v-the-republic-ors.pdf". Cause lists,
    court rolls, legislation and articles are filtered out.
  - Enrich with the richer metadata (neutral citation, keywords, court heading)
    from the court-decisions HTML table where the PDF URL matches.
  - Download each PDF and extract full text via common/pdf_extract

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Sample records
  python bootstrap.py bootstrap-fast --full
  python bootstrap.py test
"""

import re
import sys
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

requests.packages.urllib3.disable_warnings()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("SC/Judiciary")

BASE_URL = "https://www.judiciary.sc"
DECISIONS_URL = f"{BASE_URL}/resources/court-decisions/"
MEDIA_API = f"{BASE_URL}/wp-json/wp/v2/media"
MAX_PDF_SIZE = 30 * 1024 * 1024  # 30MB

# A judgment PDF's slug/filename starts with a hearing date (DDMMYYYY or DDMMYY)
# followed by "-" or "_", then the case ref + parties.
SLUG_DATE_RE = re.compile(r"^(\d{2})(\d{2})(\d{2,4})[-_](.+)$")
# Case number token right after the date, e.g. "cp12_2022", "cr79-of-2020", "mc28_2023"
CASE_TOKEN_RE = re.compile(r"^([a-z]{2,4})[_-]?(\d+)[_-](?:of[_-])?(\d{4})", re.I)
# Slugs to always exclude even if they somehow carry a leading date.
NON_JUDGMENT_RE = re.compile(
    r"causelist|cause-list|calendar|holiday|newsletter|circular|"
    r"practice-direction|vacan|tender|recruit",
    re.I,
)

# Case-type prefix → court (best-effort; text extraction is authoritative)
CASE_TYPE_COURT = {
    "ca": "Court of Appeal", "coa": "Court of Appeal", "sca": "Court of Appeal",
    "cp": "Constitutional Court", "cc": "Constitutional Court", "con": "Constitutional Court",
    "cs": "Supreme Court", "co": "Supreme Court", "cr": "Supreme Court",
    "cn": "Supreme Court", "cm": "Supreme Court", "ma": "Supreme Court",
    "xp": "Supreme Court", "dc": "Supreme Court",
    "mc": "Magistrates' Court", "mag": "Magistrates' Court",
    "ce": "Employment Tribunal", "et": "Employment Tribunal",
}

HEADERS = {"User-Agent": "LegalDataHunter/1.0 (legal research project)"}

# Court headings as they appear on the page → normalized court name
COURT_HEADINGS = {
    "court of appeal": "Court of Appeal",
    "constitutional court": "Constitutional Court",
    "supreme court (criminal)": "Supreme Court (Criminal)",
    "supreme court (civil)": "Supreme Court (Civil)",
    "supreme court": "Supreme Court",
}

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

CITATION_RE = re.compile(r"\[\d{4}\]\s+[A-Z]{2,6}(?:\s*\([A-Za-z]+\))?\s*\d+", re.I)
DATE_RE = re.compile(r"\b(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\b")
DECISION_TYPES = {"judgment", "ruling", "order", "decision", "sentence"}


class SCJudiciaryScraper(BaseScraper):
    """Scraper for the Judiciary of Seychelles court decisions."""

    def __init__(self):
        super().__init__()

    # ── discovery ──────────────────────────────────────────────────
    def _fetch_page(self) -> str:
        self.rate_limiter.wait()
        resp = requests.get(DECISIONS_URL, headers=HEADERS, timeout=60, verify=False)
        resp.raise_for_status()
        return resp.text

    def _court_for_table(self, table) -> str:
        """Find the nearest preceding heading naming a court."""
        node = table
        for _ in range(40):
            node = node.find_previous(["h1", "h2", "h3", "h4", "h5"])
            if node is None:
                break
            txt = node.get_text(" ", strip=True).lower()
            for key, name in COURT_HEADINGS.items():
                if key in txt:
                    return name
        return "Seychelles Courts"

    def _parse_date(self, text: str) -> Optional[str]:
        # Iterate all "<day> <word> <year>" matches and return the first one
        # whose middle token is an actual month name. This skips false hits
        # like "CP 2 of 2023" (matches "2 of 2023" but "of" is not a month).
        for m in DATE_RE.finditer(text or ""):
            day, mon, year = m.group(1), m.group(2).lower(), m.group(3)
            if mon not in MONTHS:
                continue
            try:
                return datetime(int(year), MONTHS[mon], int(day)).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _collect_decisions(self) -> list[dict]:
        html = self._fetch_page()
        soup = BeautifulSoup(html, "html.parser")
        out = []
        seen = set()

        for table in soup.find_all("table"):
            court = self._court_for_table(table)
            for tr in table.find_all("tr"):
                anchor = tr.find("a", href=re.compile(r"\.pdf$", re.I))
                if not anchor:
                    continue
                href = anchor["href"]
                if "wp-content/uploads" not in href:
                    continue
                if href.startswith("/"):
                    href = BASE_URL + href
                if href in seen:
                    continue
                seen.add(href)

                cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                row_text = " | ".join(cells)

                case_name = anchor.get_text(" ", strip=True) or (cells[0] if cells else "")

                cit_m = CITATION_RE.search(row_text)
                citation = cit_m.group(0).strip() if cit_m else None

                date_iso = self._parse_date(row_text)

                # case number: a cell that looks like "CP 2 of 2023" / "CO34/2024"
                case_number = None
                for c in cells:
                    if re.search(r"\b[A-Z]{1,3}\s*\d+.*\bof\b\s*\d{4}", c) or re.search(r"\b[A-Z]{1,3}\d+/\d{4}", c):
                        case_number = c
                        break

                decision_type = None
                for c in cells:
                    if c.strip().lower() in DECISION_TYPES:
                        decision_type = c.strip()
                        break

                # keywords: usually the longest cell that is not the case name
                keywords = ""
                longest = sorted((c for c in cells if c != case_name), key=len, reverse=True)
                if longest and len(longest[0]) > 60:
                    keywords = longest[0]

                out.append({
                    "court": court,
                    "case_name": case_name,
                    "case_number": case_number,
                    "citation": citation,
                    "date": date_iso,
                    "decision_type": decision_type,
                    "keywords": keywords,
                    "url": href,
                })

        logger.info(f"Collected {len(out)} judgment links from court-decisions page")
        return out

    # ── media-API discovery (primary) ──────────────────────────────
    def _prettify_parties(self, rest: str) -> str:
        """Turn the slug tail into a readable case name."""
        # Drop trailing duplicate-upload counters like "-2" / "-3"
        rest = re.sub(r"-\d{1,2}$", "", rest)
        # Drop upload-processing tokens that aren't part of the case name.
        rest = re.sub(r"(?:^|[-_])scanned(?=[-_])", "", rest, flags=re.I)
        name = rest.replace("_", " ").replace("-", " ")
        name = re.sub(r"\s+", " ", name).strip()
        # Common substitutions for readability
        name = re.sub(r"\bv\b", "v", name)
        return name.title() if name else ""

    def _parse_slug(self, slug: str, url: str) -> Optional[dict]:
        """Parse a judgment PDF slug into a metadata dict, or None if not a judgment."""
        if NON_JUDGMENT_RE.search(slug):
            return None
        m = SLUG_DATE_RE.match(slug)
        if not m:
            return None
        dd, mm, yy, rest = m.groups()
        year = yy if len(yy) == 4 else "20" + yy
        try:
            date_iso = datetime(int(year), int(mm), int(dd)).strftime("%Y-%m-%d")
        except ValueError:
            return None

        court = "Seychelles Courts"
        case_number = None
        ct = CASE_TOKEN_RE.match(rest)
        if ct:
            ctype, num, cyear = ct.group(1), ct.group(2), ct.group(3)
            court = CASE_TYPE_COURT.get(ctype.lower(), "Seychelles Courts")
            case_number = f"{ctype.upper()} {num}/{cyear}"

        return {
            "court": court,
            "case_name": self._prettify_parties(rest),
            "case_number": case_number,
            "citation": None,
            "date": date_iso,
            "decision_type": None,
            "keywords": "",
            "url": url,
            "slug": slug,
        }

    def _collect_from_media_api(self) -> list[dict]:
        """Enumerate every judgment PDF via the WordPress media REST API."""
        out, seen = [], set()
        page = 1
        while True:
            self.rate_limiter.wait()
            params = {"mime_type": "application/pdf", "per_page": 100, "page": page}
            try:
                resp = requests.get(MEDIA_API, params=params, headers=HEADERS,
                                    timeout=60, verify=False)
            except Exception as e:
                logger.warning(f"media API page {page} failed: {e}")
                break
            if resp.status_code != 200:
                # 400 = past the last page
                break
            items = resp.json()
            if not items:
                break
            for it in items:
                url = it.get("source_url") or ""
                if not url.lower().endswith(".pdf") or url in seen:
                    continue
                slug = it.get("slug") or url.rsplit("/", 1)[-1][:-4]
                info = self._parse_slug(slug, url)
                if info is None:
                    continue
                seen.add(url)
                out.append(info)
            total_pages = int(resp.headers.get("X-WP-TotalPages") or 0)
            if total_pages and page >= total_pages:
                break
            page += 1
        logger.info(f"Collected {len(out)} judgment PDFs from media API")
        return out

    def _collect_all(self) -> list[dict]:
        """Merge media-API judgments (primary) with richer HTML-table metadata."""
        media = self._collect_from_media_api()
        # Index HTML-table rows by URL for metadata enrichment.
        html_by_url = {}
        try:
            for row in self._collect_decisions():
                html_by_url[row["url"]] = row
        except Exception as e:
            logger.warning(f"HTML-table enrichment unavailable: {e}")

        # Generic anchor labels that are not real case names.
        junk_names = {"download", "pdf", "view", "click here", "read more", "here", "link"}

        by_url = {}
        for info in media:
            html = html_by_url.get(info["url"])
            if html:
                # Prefer HTML values where present (citation, keywords, court, type).
                for k in ("citation", "keywords", "decision_type"):
                    if html.get(k):
                        info[k] = html[k]
                if html.get("court") and html["court"] != "Seychelles Courts":
                    info["court"] = html["court"]
                hname = (html.get("case_name") or "").strip()
                if hname and hname.lower() not in junk_names and len(hname) > 4:
                    info["case_name"] = hname
                if html.get("case_number") and not info.get("case_number"):
                    info["case_number"] = html["case_number"]
            by_url[info["url"]] = info

        # Include any HTML-only rows the media API somehow missed (skip junk names).
        for url, row in html_by_url.items():
            if url not in by_url:
                by_url[url] = row

        # Collapse re-uploaded duplicates: the site sometimes publishes the same
        # judgment under "-1"/"-2" suffixes. Key by (case_number|citation, date)
        # and keep the record carrying a neutral citation (HTML-enriched).
        deduped = {}
        for info in by_url.values():
            ref = info.get("case_number") or info.get("citation")
            if not ref:
                deduped[info["url"]] = info
                continue
            key = (re.sub(r"\s+", "", ref).lower(), info.get("date"))
            prev = deduped.get(key)
            if prev is None or (not prev.get("citation") and info.get("citation")):
                deduped[key] = info

        result = list(deduped.values())
        logger.info(f"Total {len(result)} unique judgments after media+HTML merge")
        return result

    def _download_pdf(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = requests.get(url, headers=HEADERS, timeout=90, verify=False)
            if resp.status_code != 200:
                logger.warning(f"PDF download returned {resp.status_code}: {url}")
                return None
            if len(resp.content) > MAX_PDF_SIZE:
                logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"PDF download failed: {url}: {e}")
            return None

    def _make_doc_id(self, info: dict) -> str:
        if info.get("citation"):
            return re.sub(r"[^A-Za-z0-9]+", "-", info["citation"]).strip("-")
        slug = info["url"].rsplit("/", 1)[-1]
        if slug.lower().endswith(".pdf"):
            slug = slug[:-4]
        return slug[:100] + "-" + hashlib.md5(info["url"].encode()).hexdigest()[:8]

    # ── BaseScraper interface ──────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        decisions = self._collect_all()
        count = 0
        skipped = 0
        for info in decisions:
            pdf_bytes = self._download_pdf(info["url"])
            if not pdf_bytes:
                skipped += 1
                continue

            doc_id = self._make_doc_id(info)
            text = extract_pdf_markdown(
                source="SC/Judiciary",
                source_id=doc_id,
                pdf_bytes=pdf_bytes,
                table="case_law",
            ) or ""

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text for {info['case_name']}: {len(text)} chars")
                skipped += 1
                continue

            count += 1
            raw = dict(info)
            raw["doc_id"] = doc_id
            raw["text"] = text
            yield raw

            if count % 10 == 0:
                logger.info(f"  {count} judgments fetched ({skipped} skipped)")

        logger.info(f"Total: {count} judgments with text ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        text = (raw.get("text") or "").strip()
        if not text:
            return None

        doc_id = raw.get("doc_id") or self._make_doc_id(raw)
        title = raw.get("case_name") or raw.get("citation") or doc_id

        return {
            "_id": f"SC/Judiciary/{doc_id}",
            "_source": "SC/Judiciary",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "court": raw.get("court", ""),
            "neutral_citation": raw.get("citation", ""),
            "case_number": raw.get("case_number", ""),
            "decision_type": raw.get("decision_type", ""),
            "keywords": raw.get("keywords", ""),
            "url": raw.get("url", ""),
        }

    def health_check(self) -> dict:
        try:
            decisions = self._collect_from_media_api()
            ok = len(decisions) > 0
            return {"status": "ok" if ok else "empty", "judgments_found": len(decisions)}
        except Exception as e:
            return {"status": "error", "error": str(e)}


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|bootstrap-fast|test] [--sample] [--full]")
        sys.exit(1)

    cmd = sys.argv[1]
    scraper = SCJudiciaryScraper()

    if cmd == "bootstrap-fast":
        result = scraper.bootstrap_fast()
        print(result)
    elif cmd == "bootstrap":
        sample = "--sample" in sys.argv
        if sample:
            result = scraper.bootstrap(sample_mode=True, sample_size=12)
        else:
            result = scraper.bootstrap(sample_mode=False)
        print(result)
    elif cmd == "test":
        print(scraper.health_check())
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
