#!/usr/bin/env python3
"""
US/PA-AGOpinions -- Pennsylvania Attorney General Official Opinions

Fetches the FULL TEXT of the official opinions of the Pennsylvania Attorney
General. These are authoritative (advisory) interpretations of Pennsylvania
law issued by the Commonwealth's chief legal officer — a doctrine corpus.

Source / access
---------------
The Pennsylvania Office of Attorney General publishes its opinions as
born-digital, text-layer PDFs in its WordPress media store at
``https://www.attorneygeneral.gov/wp-content/uploads/...``. The opinions
fall into two groups:

  * Biennial bound VOLUMES, 1895-1992 — each PDF is the full "Opinions of
    the Attorney General of Pennsylvania" report for a two-year (occasionally
    one- or multi-year) span, containing every formal opinion of that period.
    Filenames look like ``1949_1950_AG_Chidsey_Margiotti_opinions.pdf``.
  * Individual modern opinions, 1994-2019 (Preate, Fisher, Corbett, Kelly,
    Kane and later) issued as standalone PDFs, e.g.
    ``AG_CORBETT_OPINION_FEB2010.pdf`` or
    ``Abortion-Medicaid-Opinion-FINAL-2.19.19.pdf``.

The HTML index page (/resources/official-ag-opinions/) is JS/WAF-fronted and
its WordPress REST API is disabled, so the canonical list of PDF URLs is
embedded below (harvested from the official page; every URL is a direct,
publicly downloadable attorneygeneral.gov file — verified HTTP 200,
application/pdf). The direct PDF endpoints are NOT blocked.

Each PDF is downloaded and its text extracted via the shared, OOM-hardened
``common.pdf_extract.extract_pdf_markdown`` helper. All volumes carry a real
text layer (no OCR needed); the rare scanned/empty PDF is skipped.

Usage:
  python bootstrap.py bootstrap            # Full pull (all PDFs)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity / extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.PA-AGOpinions")

SOURCE_ID = "US/PA-AGOpinions"

# Canonical list of official PA AG opinion PDFs (attorneygeneral.gov media
# store). Biennial volumes 1895-1992 + individual modern opinions 1994-2019.
OPINION_PDFS = [
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1895_1896_AG__Cassidy_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1897_1898_AG__Kirkpatrick_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1899_1900_AG__Kirkpatric_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1901_1902_AG_Elkin_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1903_1904_AG_Carson_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1905_1906_AG_Carson_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1907_1908_AG_Todd_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1909_1910_AG_Todd_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1911_1912_AG_Bell_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1913_1914_AG_Bell_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1915_1916_AG_Brown_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1917_1918_AG_Brown_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1919_1920_AG_Schaffer_Alter_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1921_1922_AG_Alter_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1923_1924_AG_Woodruff_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1925_1926_AG_Woodruff_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1927_1928_AG_Baldrige_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1929_1930_AG_Woods_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1931_1932_AG_Schnader_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1933_1934_AG_Schnader_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1935_1936_AG_Margiotti_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1937_1938_AG_Margiotti_Bard_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1939_1940_AG_Reno_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1941_1942_AG_Reno_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1943_1944_AG_Duff_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1945_1946_AG_Duff_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1947_1948_AG_Chidsey_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1949_1950_AG_Chidsey_Margiotti_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1951_1952_AG_Woodside_opinions1.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1953_1954_AG_Woodside_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1955_1956_AG_Cohen_McBride_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1957_AG_McBride_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1958_AG_McBride_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1959_1960_AG_Alpern_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1961_1962_AG_Stahl_Alpern_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1963_1966_AG_Alessandroni_Friendman_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1967_1969_AG_Sennett_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1971_AG_Creamer_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1972_AG_Creamer_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1973_AG_Packel_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1974_AG_Packel_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1975_AG_Kane_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1976_AG_Kane_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1977_AG_Kane_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1978_AG_Kane_Gornish_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1979_1981_AG_Blewitt_Biester_Bartle_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1981_1984_Zimmerman_opinions2.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1984_1988_Zimmerman_opinions1.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/1989_1992_Preate_opinions.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/AG_CORBETT_OPINION_DEC1996.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/AG_CORBETT_OPINION_DEC20061.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/AG_CORBETT_OPINION_DEC_30_19961.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/AG_CORBETT_OPINION_FEB2010.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/AG_CORBETT_OPINION_MARCH2006.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/AG_FISHER_OPINION_FEB1998.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/AG_KANE_OPINION_JULY2013.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/AG_KELLY_OPINION_JULY2011.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/AG_PREATE_OPINION_FEB1994.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/February-25-2016-Attorney-General-Opinion-2016-1.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2018/01/PA_Gaming_Control_Advice_Guns_Casino-signed.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2019/01/20190129100159.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2019/02/Abortion-Medicaid-Opinion-FINAL-2.19.19.pdf",
    "https://www.attorneygeneral.gov/wp-content/uploads/2019/12/19.12.16-Receivers-Legal-Opinion.pdf",
]

MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

# Tokens that are not part of an Attorney General's surname.
_NAME_STOP = {"AG", "OPINION", "OPINIONS", "OPINIONS1", "OPINIONS2", "FINAL",
              "ATTORNEY", "GENERAL", "PA", "SIGNED", "ADVICE", "LEGAL",
              "CONTROL", "GAMING", "GUNS", "CASINO", "NO"}


def clean_text(text: str) -> str:
    """Normalize whitespace; strip pdfplumber (cid:N) artefacts."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\(cid:\d+\)", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _filename(url: str) -> str:
    return url.split("?", 1)[0].rsplit("/", 1)[-1]


def slug_from_url(url: str) -> str:
    base = re.sub(r"\.pdf$", "", _filename(url), flags=re.IGNORECASE)
    base = re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-")
    return base or "opinion"


def years_in_filename(fname: str) -> list:
    """All plausible 4-digit publication years in the leading filename."""
    yrs = [int(y) for y in re.findall(r"(1[89]\d{2}|20[0-2]\d)", fname)]
    return [y for y in yrs if 1850 <= y <= 2030]


def surnames_in_filename(fname: str) -> list:
    """Attorney-General surnames embedded in the filename (best effort)."""
    stem = re.sub(r"\.pdf$", "", fname, flags=re.IGNORECASE)
    parts = re.split(r"[ _\-.]+", stem)
    out = []
    for p in parts:
        if not p or p.isdigit():
            continue
        up = p.upper()
        if up in _NAME_STOP or up.startswith("OPINION"):
            continue
        if any(ch.isdigit() for ch in p):
            continue
        # Title-case the surname.
        out.append(p[:1].upper() + p[1:].lower())
    # De-duplicate while preserving order.
    seen = set()
    uniq = []
    for n in out:
        if n.lower() not in seen:
            seen.add(n.lower())
            uniq.append(n)
    return uniq


def parse_date(fname: str, url: str, text: str, year: int | None) -> str | None:
    """Best-effort ISO date for an opinion PDF.

    Priority: explicit date embedded in the filename -> leading filename year
    (Jan 1, for the bound volumes) -> Month D, YYYY in the document head (for
    modern undated filenames) -> upload-path year.

    Note: the filename year is preferred over a date parsed from the document
    text, because a bound volume's body contains many opinion dates and the
    first one is not the volume's date.
    """
    stem = re.sub(r"\.pdf$", "", fname, flags=re.IGNORECASE)

    # February-25-2016 style.
    m = re.search(
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)[ _\-]?(\d{1,2})[ _\-](\d{4})",
        stem, re.IGNORECASE,
    )
    if m:
        return f"{int(m.group(3)):04d}-{MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"

    # DEC1996 / FEB2010 / MARCH2006 (month abbrev + 4-digit year, no day).
    m = re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)"
                  r"[a-z]*[ _\-]?((?:18|19|20)\d{2})", stem, re.IGNORECASE)
    if m:
        return f"{int(m.group(2)):04d}-{MONTHS[m.group(1).lower()]:02d}-01"

    # DEC_30_1996 style (month _ day _ year).
    m = re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)"
                  r"[a-z]*[ _\-](\d{1,2})[ _\-]((?:18|19|20)\d{2})",
                  stem, re.IGNORECASE)
    if m:
        return f"{int(m.group(3)):04d}-{MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"

    # 2.19.19 / 19.12.16 numeric dates.
    m = re.search(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2,4})\b", stem)
    if m:
        a, b, c = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if c < 100:
            c += 2000
        # Disambiguate using the upload-path year if available.
        path_yr = None
        pm = re.search(r"/uploads/((?:18|19|20)\d{2})/", url)
        if pm:
            path_yr = int(pm.group(1))
        if path_yr and c == path_yr and 1 <= a <= 12 and 1 <= b <= 31:
            return f"{c:04d}-{a:02d}-{b:02d}"      # m.d.yy
        if path_yr and a == path_yr % 100 + 0 and False:
            pass
        # Generic m.d.y if it validates.
        if 1 <= a <= 12 and 1 <= b <= 31:
            return f"{c:04d}-{a:02d}-{b:02d}"
        if 1 <= b <= 12 and 1 <= a <= 31:           # y.m.d-ish fallback
            return f"{c:04d}-{b:02d}-{a:02d}"

    # Bound volumes / any filename carrying a leading year: use it (Jan 1).
    if year:
        return f"{year:04d}-01-01"

    # Modern undated filenames: fall back to a date in the document head.
    head = (text or "")[:2000]
    m = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{1,2}),?\s+((?:18|19|20)\d{2})",
        head, re.IGNORECASE,
    )
    if m:
        mon = MONTHS[m.group(1).lower()]
        day = int(m.group(2))
        yr = int(m.group(3))
        if 1 <= day <= 31 and 1850 <= yr <= 2030:
            return f"{yr:04d}-{mon:02d}-{day:02d}"

    pm = re.search(r"/uploads/((?:18|19|20)\d{2})/", url)
    if pm:
        return f"{int(pm.group(1)):04d}-01-01"
    return None


def build_title(fname: str, years: list, names: list) -> str:
    """Human-readable title for an opinion / volume PDF."""
    name_str = ", ".join(names) if names else ""
    if len(years) >= 2 and (max(years) - min(years)) >= 1:
        span = f"{min(years)}–{max(years)}"
        base = f"Opinions of the Attorney General of Pennsylvania, {span}"
    elif years:
        base = f"Opinions of the Attorney General of Pennsylvania, {years[0]}"
    else:
        base = "Pennsylvania Attorney General Opinion"
    if name_str:
        return f"{base} (Attorney General {name_str})"
    return base


class PAAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        self.http = HttpClient(
            base_url="",
            headers={"User-Agent": self._ua,
                     "Accept": "application/pdf,*/*;q=0.8"},
            timeout=90,
        )
        self.delay = 1.0

    def _curl_bytes(self, url: str) -> bytes | None:
        try:
            out = subprocess.run(
                ["curl", "-s", "-L", "--max-time", "120", "-A", self._ua, url],
                capture_output=True, timeout=150,
            )
            if out.returncode == 0 and out.stdout:
                return out.stdout
        except Exception as e:
            logger.warning(f"curl fallback failed for {url}: {e}")
        return None

    def _fetch_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    return resp.content
                if resp.status_code == 404:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if "SSL" in str(e) or "handshake" in str(e).lower():
                    break
            if attempt < retries:
                time.sleep(2 ** attempt)
        return self._curl_bytes(url)

    def _build_raw(self, url: str) -> dict | None:
        fname = _filename(url)
        pdf_bytes = self._fetch_bytes(url)
        if not pdf_bytes:
            logger.warning(f"Could not download {url}")
            return None
        try:
            raw = extract_pdf_markdown(url, SOURCE_ID,
                                       pdf_bytes=pdf_bytes, table="legislation")
        except Exception as e:
            logger.warning(f"PDF extract error {url}: {e}")
            return None
        text = clean_text(raw or "")
        if not text or len(text) < 300:
            logger.info(f"No usable text (scanned?) for {url} ({len(text)} chars)")
            return None
        years = years_in_filename(fname)
        names = surnames_in_filename(fname)
        year = years[0] if years else None
        return {
            "slug": slug_from_url(url),
            "title": build_title(fname, years, names),
            "text": text,
            "date": parse_date(fname, url, text, year),
            "url": url,
            "year": year,
        }

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        return {
            "_id": f"{SOURCE_ID}/{raw['slug']}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        # Newest-first: modern standalone opinions, then volumes descending.
        for url in reversed(OPINION_PDFS):
            raw = self._build_raw(url)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    def test_api(self) -> bool:
        logger.info("Testing Pennsylvania AG opinions archive...")
        try:
            for url in reversed(OPINION_PDFS):
                raw = self._build_raw(url)
                if raw:
                    logger.info(f"  Extracted full text OK for {url} "
                                f"({len(raw['text'])} chars); title={raw['title']}")
                    logger.info("API test PASSED")
                    return True
            logger.error("  No PDF produced usable text")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/PA-AGOpinions bootstrap")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PAAGOpinionsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()
    count = 0
    for raw in gen:
        record = scraper.normalize(raw)
        safe_id = record["_id"].replace("/", "_")
        with open(sample_dir / f"{safe_id}.json", "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
