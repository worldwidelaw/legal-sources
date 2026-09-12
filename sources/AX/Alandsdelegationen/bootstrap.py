#!/usr/bin/env python3
"""
AX/Alandsdelegationen -- Åland Delegation opinions (utlåtanden) fetcher.

Ålandsdelegationen (the Åland Delegation) is the expert body that, under § 56 of
the Act on the Autonomy of Åland, issues formal opinions (utlåtanden) to the
Finnish Council of State, ministries, the Government of Åland and the courts on
the application of the Autonomy Act -- chiefly on the division of legislative and
administrative competence between the Republic of Finland and the autonomous
province of Åland. Its opinions are also the basis for the President of the
Republic's review of every Åland provincial law (lex Åland). This is a unique,
authoritative Åland constitutional-law doctrine corpus published nowhere else.

Strategy:
  - Iterate the per-year case ("ärenden") pages at /alandsdelegationen/arenden/{year}
  - Newer years (2025+) attach the full opinion as a PDF under /sites/default/files/
    -> download and extract text via the shared pdf_extract backend.
  - Older years embed each opinion's full text inline as a <li> under
    div.view-content -> extract the cleaned text directly from the HTML.
  - Normalize into the standard schema (type: doctrine).

Usage:
  python bootstrap.py bootstrap          # Fetch all opinions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap (VPS runner)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AX.Alandsdelegationen")

SOURCE_ID = "AX/Alandsdelegationen"
BASE_URL = "https://www.ambetsverket.ax"
YEAR_PATH = "/alandsdelegationen/arenden"

# The delegation's online case archive currently spans 2007 onward.
FIRST_YEAR = 2007
# Iterate a little past the current year so newly published years are picked up
# without a code change (empty year pages are simply skipped).
LAST_YEAR = 2027

DATE_RE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b")
HEADER_DATE_RE = re.compile(r"^\s*(\d{1,2})\.(\d{1,2})\.(\d{4})\s*$")
SIGN_DATE_RE = re.compile(r"(?:Helsingfors|Mariehamn)(?:\s+den)?\s+(\d{1,2})\.(\d{1,2})\.(\d{4})")
NR_RE = re.compile(r"\bNr\s+(\d+\s*/\s*\d+)")
# Only the PDF opinions carry a "Till {authority}" letterhead line. Restrict the
# match to lines that actually name an authority so we don't catch prose like
# "Till denna del ..." in the inline-HTML opinions.
TILL_RE = re.compile(
    r"^Till\s+([^\n]{3,80}?(?:ministeriet|ministeriern|regeringen|landskapsregeringen|"
    r"domstol\w*|nämnd\w*|rådet|riksdagen|statsrådet|verket|byrån|delegationen|kansli\w*))",
    re.MULTILINE,
)


def _iso_date(d: str, m: str, y: str) -> str:
    try:
        return datetime(int(y), int(m), int(d)).date().isoformat()
    except ValueError:
        return ""


class AlandsdelegationenScraper(BaseScraper):
    """Scraper for AX/Alandsdelegationen -- Åland Delegation opinions."""

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "sv,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 15s")
                    time.sleep(15)
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:90]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    # ---- discovery -------------------------------------------------------

    def _iter_years(self) -> Generator[Dict[str, Any], None, None]:
        for year in range(FIRST_YEAR, LAST_YEAR + 1):
            url = f"{BASE_URL}{YEAR_PATH}/{year}"
            resp = self._request(url)
            if resp is None:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            pdfs = []
            seen = set()
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.lower().endswith(".pdf") and href not in seen:
                    seen.add(href)
                    pdfs.append(href if href.startswith("http") else BASE_URL + href)

            if pdfs:
                logger.info(f"Year {year}: {len(pdfs)} PDF opinions")
                for pdf_url in pdfs:
                    yield {"kind": "pdf", "year": year, "pdf_url": pdf_url, "page_url": url}
            else:
                blocks = self._extract_html_opinions(soup)
                if blocks:
                    logger.info(f"Year {year}: {len(blocks)} embedded HTML opinions")
                for b in blocks:
                    b.update({"kind": "html", "year": year, "page_url": url})
                    yield b

    def _extract_html_opinions(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        vc = soup.find("div", class_="view-content")
        if not vc:
            return []
        out = []
        idx = 0
        for li in vc.find_all("li"):
            text = li.get_text("\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text)
            if len(text) < 800:
                continue
            low = text.lower()
            if "utlåtande" not in low and "närvarande" not in low and "rekommendation" not in low:
                continue
            # Each opinion's decision date lives in the accordion header
            # (an <h3> like "18.12.2024") immediately preceding the <li>, not
            # inside the body text — capture it for a reliable date.
            header_date = ""
            prev = li.find_previous(["h2", "h3", "h4"])
            if prev:
                m = HEADER_DATE_RE.match(prev.get_text(strip=True))
                if m:
                    header_date = _iso_date(*m.groups())
            out.append({"text": text, "index": idx, "header_date": header_date})
            idx += 1
        return out

    # ---- hydration -------------------------------------------------------

    def _hydrate_pdf(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=Path(item["pdf_url"]).stem,
            pdf_url=item["pdf_url"],
            table="doctrine",
            force=True,
        )
        if not text or len(text) < 200:
            logger.warning(f"Insufficient PDF text: {item['pdf_url'][:90]}")
            return None
        item["text"] = text
        return item

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        count = 0
        for item in self._iter_years():
            if item["kind"] == "pdf":
                hydrated = self._hydrate_pdf(item)
                if hydrated is None:
                    continue
                item = hydrated
            count += 1
            yield item
        logger.info(f"Completed: {count} opinions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        # Re-scan only the two most recent years for incremental updates.
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        global FIRST_YEAR
        saved = FIRST_YEAR
        try:
            FIRST_YEAR = LAST_YEAR - 2
            for item in self.fetch_all():
                if since:
                    rec = self.normalize(item)
                    if rec and rec.get("date") and rec["date"] < since:
                        continue
                yield item
        finally:
            FIRST_YEAR = saved

    # ---- normalization ---------------------------------------------------

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        text = (raw.get("text") or "").strip()
        if not text:
            return None
        year = raw.get("year")

        nr_m = NR_RE.search(text)
        nr = re.sub(r"\s+", "", nr_m.group(1)) if nr_m else ""

        # Date resolution, in order of reliability:
        #   1. accordion header date captured from the page (HTML opinions),
        #   2. the "Helsingfors/Mariehamn den DD.MM.YYYY" signing line (PDFs),
        #   3. a date in the signature block right before "Närvarande".
        # We deliberately do NOT fall back to an arbitrary in-body date — those
        # are request deadlines / cited dates and are frequently wrong.
        date = raw.get("header_date") or ""
        if not date:
            sign_m = SIGN_DATE_RE.search(text)
            if sign_m:
                date = _iso_date(*sign_m.groups())
        if not date:
            i = text.rfind("Närvarande")
            if i != -1:
                m = DATE_RE.search(text[max(0, i - 250):i])
                if m:
                    date = _iso_date(*m.groups())

        till_m = TILL_RE.search(text)
        recipient = till_m.group(1).strip() if till_m else ""

        if raw["kind"] == "pdf":
            doc_id = Path(raw["pdf_url"]).stem
            url = raw["pdf_url"]
        else:
            h = hashlib.sha1(text[:500].encode("utf-8")).hexdigest()[:10]
            doc_id = f"AD-{year}-{raw.get('index', 0):03d}-{h}"
            url = raw.get("page_url", "")

        # Build a human-readable title: "Ålandsdelegationens utlåtande {nr|year}
        # – {recipient or first-line subject excerpt}".
        title_bits = ["Ålandsdelegationens utlåtande"]
        if nr:
            title_bits.append(f"nr {nr}")
        elif year:
            title_bits.append(f"({year})")
        subject = recipient
        if not subject:
            for line in text.splitlines():
                line = re.sub(r"^\s*\d+[.)]\s*", "", line.strip())
                if len(line) >= 15:
                    subject = (line[:90] + "…") if len(line) > 90 else line
                    break
        if subject:
            title_bits.append(f"– {subject}")
        title = " ".join(title_bits)

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "case_number": nr,
            "recipient": recipient,
            "year": year,
            "publication_type": "utlåtande",
        }

    # ---- connectivity test ----------------------------------------------

    def test(self) -> bool:
        resp = self._request(f"{BASE_URL}{YEAR_PATH}/2025")
        if resp is None:
            logger.error("Cannot reach Ålandsdelegationen year page")
            return False
        soup = BeautifulSoup(resp.text, "html.parser")
        pdfs = [a["href"] for a in soup.find_all("a", href=True)
                if a["href"].lower().endswith(".pdf")]
        if not pdfs:
            logger.error("No PDF opinions found on 2025 page")
            return False
        url = pdfs[0] if pdfs[0].startswith("http") else BASE_URL + pdfs[0]
        item = self._hydrate_pdf({"kind": "pdf", "year": 2025, "pdf_url": url})
        if item:
            rec = self.normalize(item)
            logger.info(f"OK: {rec['title']} ({len(rec['text'])} chars, date={rec['date']})")
            return True
        logger.error("Could not hydrate first opinion")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="AX/Alandsdelegationen data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AlandsdelegationenScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
