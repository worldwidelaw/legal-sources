#!/usr/bin/env python3
"""
US/CA-WCAB -- California Workers' Compensation Appeals Board decisions.

The WCAB publishes three decision series as born-digital PDFs on the DIR site,
each backed by an HTML table that carries the authoritative metadata (decision
date, official WCAB citation, ADJ case numbers and an official summary):

  * En banc decisions     /wcab/wcab_enbanc.htm   -> EnBancdecisions{YEAR}/*.pdf
  * Significant panel     /wcab/wcab_panel.htm    -> SignificantPanelDecisions{YEAR}/*.pdf
  * Panel decisions       /wcab/wcab-Decisions.htm-> Panel-Decisions-{YEAR}/*.pdf

En banc decisions bind all workers' compensation judges and the Board itself
(Cal. Code Regs. tit. 8 s.10325); significant panel decisions are persuasive
precedent; ordinary panel decisions (~5,600, 2021-present) are the Board's
routine appellate output. All three adjudicate specific contested claims, so
they are case_law.

Plain GET, no auth/JS/CAPTCHA. www.dir.ca.gov serves the listings from the
same host as the PDFs.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap          # sequential full pull
  python bootstrap.py bootstrap-fast     # concurrent full pull (VPS wrapper)
"""

import sys
import re
import html
import hashlib
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, urlsplit

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-WCAB")

BASE_URL = "https://www.dir.ca.gov/wcab/"
LISTINGS = [
    ("wcab_enbanc.htm", "en_banc", "En Banc Decision"),
    ("wcab_panel.htm", "significant_panel", "Significant Panel Decision"),
    ("wcab-Decisions.htm", "panel", "Panel Decision"),
]

# Hundreds of rows on wcab-Decisions.htm are missing their opening <tr> and a
# handful are missing the closing one, so rows are recovered by splitting on
# either tag rather than matching a pair.
ROW_SPLIT_RE = re.compile(r"</tr\s*>|<tr\b[^>]*>", re.I)
TD_RE = re.compile(r"<t[dh]\b([^>]*)>(.*?)</t[dh]>", re.I | re.S)
PDF_HREF_RE = re.compile(r'href="([^"]+\.pdf)"', re.I)
# A few anchors leak the DIR staging host (http://oak01web/...) alongside the
# real link; those duplicates are unresolvable from the public internet.
INTERNAL_HOST_RE = re.compile(r"://oak\d*web\b", re.I)
YEAR_IN_PATH_RE = re.compile(
    r"(?:EnBancdecisions|SignificantPanelDecisions|Panel-Decisions-)(\d{4})", re.I
)
CITATION_RE = re.compile(r"\b(\d{4}-(?:EB|SPD?)-?\d+)\b", re.I)
REPORTER_RE = re.compile(r"\b\d+\s*Cal\.?\s*Comp\.?\s*Cases?\b", re.I)
# WCAB case numbers: the EAMS "ADJ" series and the legacy district-office series.
CASE_NO_RE = re.compile(r"\b(ADJ\d{5,9})\b", re.I)
LONG_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+(\d{1,2}),?\s+(\d{4})\b"
)
NUM_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"], start=1)}


def strip_tags(fragment: str) -> str:
    """Turn an HTML table cell into clean single-spaced text."""
    text = re.sub(r"<br\s*/?>", " ", fragment, flags=re.I)
    text = re.sub(r"</?(p|div|li|tr)\b[^>]*>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip(" ,; ")


def slugify(value: str) -> str:
    """Uppercase slug that keeps `_` distinct from `-`.

    The WCAB republishes some decisions under near-identical filenames that
    differ only by separator (Steve_Hoddinott.pdf / Steve-Hoddinott.pdf /
    SteveHoddinott.pdf are three different documents), so collapsing both
    separators would silently merge distinct decisions.
    """
    return re.sub(r"[^A-Za-z0-9_]+", "-", value).strip("-").upper()


def url_key(url: str) -> str:
    """Case-insensitive path key used to dedup the listings.

    The pages link the same file as `2000-eb2.pdf`, `/wcab/2000-eb2.pdf` and
    `/WCAB/2000-eb2.pdf`; IIS serves all three, so they must collapse to one.
    """
    return urlsplit(url).path.lower()


def parse_date(text: str) -> Optional[str]:
    m = LONG_DATE_RE.search(text)
    if m:
        month, day, year = m.groups()
        return f"{year}-{MONTHS[month]:02d}-{int(day):02d}"
    m = NUM_DATE_RE.search(text)
    if m:
        month, day, year = (int(x) for x in m.groups())
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year:04d}-{month:02d}-{day:02d}"
    return None


class CAWCABScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
        })

    def _get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp
            except Exception as e:
                logger.warning(f"GET {url} failed (attempt {attempt + 1}/3): {e}")
        return None

    def _parse_listing(self, page: str, series: str, label: str) -> list:
        """Parse one WCAB listing page into decision dicts, in page order.

        Each decision occupies a linked row (caption + date + citation) that may
        be followed by full-width `colspan` rows carrying the ADJ case numbers,
        the Cal. Comp. Cases reporter citation and an official summary.
        """
        resp = self._get(urljoin(BASE_URL, page))
        if resp is None:
            raise RuntimeError(
                f"Could not fetch the WCAB listing {page}. www.dir.ca.gov is "
                f"reachable from residential vantages — a hard failure here usually "
                f"means the egress IP is blocked."
            )

        decisions, seen = [], set()
        current = None
        for row in ROW_SPLIT_RE.split(resp.text):
            cells = TD_RE.findall(row)
            hrefs = [h for h in PDF_HREF_RE.findall(row)
                     if not INTERNAL_HOST_RE.search(h)]

            if hrefs:
                url = urljoin(urljoin(BASE_URL, page), hrefs[0])
                key = url_key(url)
                current = None
                if key in seen:
                    continue
                seen.add(key)

                caption, date, citation = "", None, None
                for attrs, body in cells:
                    text = strip_tags(body)
                    if PDF_HREF_RE.search(body):
                        caption = text
                        continue
                    if date is None and parse_date(text):
                        date = parse_date(text)
                        continue
                    cite_match = CITATION_RE.search(text)
                    if cite_match:
                        citation = cite_match.group(1).upper()

                year_match = YEAR_IN_PATH_RE.search(url)
                current = {
                    "pdf_url": url,
                    "series": series,
                    "series_label": label,
                    "caption": caption,
                    "date": date,
                    "citation": citation,
                    "year": int(year_match.group(1)) if year_match
                            else (int(date[:4]) if date else None),
                    "case_numbers": sorted(set(
                        m.upper() for m in CASE_NO_RE.findall(url + " " + caption)
                    )),
                    "reporter_citation": None,
                    "summary": None,
                }
                decisions.append(current)
                continue

            # Splitting on both <tr> and </tr> yields empty segments between rows;
            # they carry no cells and must not detach the decision above them.
            if not cells:
                continue

            # Full-width continuation rows belong to the decision above them.
            if current is None or len(cells) != 1 or "colspan" not in cells[0][0].lower():
                current = None
                continue

            text = strip_tags(cells[0][1])
            if not text:
                continue
            if text.lower().startswith("case no"):
                current["case_numbers"] = sorted(set(
                    current["case_numbers"]
                    + [m.upper() for m in CASE_NO_RE.findall(text)]
                ))
            elif REPORTER_RE.search(text) and len(text) < 120:
                current["reporter_citation"] = text
            elif len(text) > 120:
                current["summary"] = text

        if not decisions:
            raise RuntimeError(
                f"WCAB listing {page} returned 200 but held no decision rows — the "
                f"page layout changed or an interstitial was served."
            )
        return decisions

    def test_api(self) -> bool:
        logger.info("Testing WCAB decision listings...")
        try:
            for page, series, label in LISTINGS:
                decisions = self._parse_listing(page, series, label)
                dated = sum(1 for d in decisions if d["date"])
                logger.info(f"  {series}: {len(decisions)} decisions ({dated} dated)")
            logger.info("Connectivity test PASSED")
            return True
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            return False

    def fetch_all(self) -> Generator[dict, None, None]:
        for page, series, label in LISTINGS:
            decisions = self._parse_listing(page, series, label)
            logger.info(f"{series}: {len(decisions)} decisions listed")
            for i, decision in enumerate(decisions, 1):
                yield decision
                if i % 500 == 0:
                    logger.info(f"  {series}: {i}/{len(decisions)} yielded")

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        """The listings are small and fully re-scraped; the loader dedups on _id.

        `since` (ISO date) drops decisions the listing already dates earlier,
        so an incremental run only downloads the new PDFs.
        """
        for decision in self.fetch_all():
            if since and decision["date"] and decision["date"] < str(since)[:10]:
                continue
            yield decision

    def _doc_id(self, raw: dict) -> str:
        """Stable id per published file.

        The WCAB stores separate decisions in the same case under filenames that
        differ only by a separator ("...ADJ11036278 ADJ15515237.pdf" and
        "...ADJ11036278-ADJ15515237.pdf" are two different documents), so the
        slug alone is not unique. A short digest of the case-folded path keeps
        those apart while still collapsing the /wcab/ vs /WCAB/ link variants.
        """
        stem = Path(urlsplit(raw["pdf_url"]).path).name
        stem = re.sub(r"(\.pdf)+$", "", stem, flags=re.I)
        year = raw.get("year") or "NA"
        digest = hashlib.sha1(url_key(raw["pdf_url"]).encode()).hexdigest()[:6]
        return f"US-CA-WCAB-{raw['series'].upper()}-{year}-{slugify(stem)}-{digest}"

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw["pdf_url"]
        doc_id = self._doc_id(raw)

        text = extract_pdf_markdown(
            "US/CA-WCAB", doc_id, pdf_url=url, table="case_law"
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text extracted from {url}")
            return None
        text = text.strip()

        # The listing date is authoritative; fall back to the PDF body.
        date = raw.get("date") or parse_date(text)
        if not date and raw.get("year"):
            date = f"{raw['year']}-01-01"

        case_numbers = sorted(set(
            raw.get("case_numbers", [])
            + [m.upper() for m in CASE_NO_RE.findall(text)]
        ))

        caption = raw.get("caption") or Path(url).stem.replace("_", " ").replace("-", " ").title()
        title = f"{caption} — WCAB {raw['series_label']}"
        if raw.get("citation"):
            title = f"{title} ({raw['citation']})"
        elif raw.get("year"):
            title = f"{title} ({raw['year']})"

        return {
            "_id": doc_id,
            "_source": "US/CA-WCAB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "court": "California Workers' Compensation Appeals Board",
            "jurisdiction": "US-CA",
            "decision_series": raw["series"],
            "citation": raw.get("citation"),
            "reporter_citation": raw.get("reporter_citation"),
            "case_numbers": case_numbers,
            "parties": caption,
            "summary": raw.get("summary"),
            "year": raw.get("year"),
            "language": "en",
        }

    def run_curated_sample(self, size: int = 15) -> int:
        logger.info(f"=== SAMPLE MODE: {size} decisions ===")
        per_series = max(1, size // len(LISTINGS))
        records = []
        for page, series, label in LISTINGS:
            decisions = self._parse_listing(page, series, label)
            taken = 0
            for decision in decisions:
                if taken >= per_series or len(records) >= size:
                    break
                record = self.normalize(decision)
                if record:
                    records.append(record)
                    taken += 1
                    logger.info(f"  {record['_id']}: {len(record['text'])} chars")

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        for record in records:
            path = sample_dir / f"{record['_id']}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"=== Sample complete: {len(records)} records ===")
        return len(records)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/CA-WCAB bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = CAWCABScraper()
    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)
    if args.sample:
        sys.exit(0 if scraper.run_curated_sample() > 0 else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
    else:
        stats = scraper.bootstrap()
    logger.info(
        f"{args.command} complete: {stats.get('records_fetched', 0)} fetched, "
        f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
    )
    sys.exit(0 if stats.get("records_fetched", 0) > 0 else 1)


if __name__ == "__main__":
    main()
