#!/usr/bin/env python3
"""
CU/ParlamentoCubano -- Cuba National Assembly Laws & Decree-Laws

Fetches enacted legislation from the Asamblea Nacional del Poder Popular's
"Labor legislativa" section.

Strategy:
  - Paginate through /labor-legislativa?page=N (pages 0-5, ~9 laws/page)
  - Extract PDF links from the approved-laws block (view-display-id-block_3)
  - Also grab the Constitution from block_8
  - Download PDFs and extract full text with pdfplumber
  - ~55 enacted laws + Constitution

Usage:
  python bootstrap.py bootstrap          # Full pull -> data/records.jsonl
  python bootstrap.py bootstrap --sample # Fetch 15 sample records -> sample/
  python bootstrap.py bootstrap-fast     # Alias for the full bootstrap (fleet entry point)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import unicodedata
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List, Tuple
from urllib.parse import unquote, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CU.ParlamentoCubano")

BASE_URL = "https://www.parlamentocubano.gob.cu"
LISTING_URL = BASE_URL + "/labor-legislativa"
CRAWL_DELAY = 2

MONTHS = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}

# Every document in a Gaceta issue carries a code like GOC-2026-290-O39. It is
# printed inside the summary caption of the front matter and again, alone on
# its own line, where the document's body starts.
GOC_CODE = r'GOC-\d{4}-\d+-[A-Z]{1,3}\d+'
CODE_RE = re.compile(GOC_CODE)
SECTION_START_RE = re.compile(rf'^[ \t]*({GOC_CODE})[ \t]*$', re.M)

# Masthead of a regular issue: "EDICIÓN ORDINARIA LA HABANA, MARTES 5 DE MAYO
# DE 2026". Some issues drop the "EDICIÓN" and some write the first of the
# month as "1ro.".
MASTHEAD_RE = re.compile(
    r'(?:EDICI[OÓ]N\s+)?(?:ORDINARIA|EXTRAORDINARIA)\s+LA\s+HABANA,\s*\w+\s+'
    r'(\d{1,2})(?:ro)?\.?\s+DE\s+(\w+)\s+DE\s+(\d{4})',
    re.IGNORECASE,
)
# Cover sheet of an issue that has one: "Gaceta Oficial No. 87 Ordinaria de 13
# de septiembre de 2023".
COVER_DATE_RE = re.compile(
    r'Gaceta\s+Oficial\s+No\.\s*\d+\s+\w+\s+de\s+(\d{1,2})(?:ro)?\.?\s+de\s+(\w+)\s+de\s+(\d{4})',
    re.IGNORECASE,
)
# Running footer on every page: "1453 GOC-2026-O39 05/05/2026".
FOOTER_DATE_RE = re.compile(rf'GOC-\d{{4}}-[A-Z]{{1,3}}\d+\s+(\d{{2}})/(\d{{2}})/(\d{{4}})')

# A law's caption in the front matter is at most this many characters long;
# captions wrap over several lines and are followed by dotted page leaders.
CAPTION_WINDOW = 260
# Below this a slice is too short to be a law and we keep the whole issue.
MIN_SECTION_CHARS = 500

# Leading "Ley", "Ley no. 165", "Decreto-Ley 86/2024" etc. The listing and the
# gazette caption number the same law differently (the listing often omits the
# number), so the distinctive part of the name has to be matched on its own.
LEADING_LAW_NUMBER_RE = re.compile(
    r'^(?:ley|codigo|decreto ley|decreto|constitucion)\b(?:\s+no)?(?:\s+\d+(?:/\d+)?)?\s*'
)


def _canonical_pdf_url(href: str) -> str:
    """Undo the site's over-encoding of a PDF href.

    Every law is linked twice: once under its filename, correctly encoded, and
    once under its title with the percent signs escaped again — the title link
    for "del proceso penal.pdf" reads ``del%2520proceso%2520penal.pdf``. Only
    the title link carries a usable title, so the href it gives has to be
    decoded back down to the real path or the file 404s. Filenames with no
    space or accent are unaffected, which is why this only ever broke the
    older entries.
    """
    previous = None
    while previous != href:
        previous, href = href, unquote(href)
    return quote(href, safe="/:")


def _normalize(text: str) -> str:
    """Fold accents, quotes and punctuation so two spellings of a law title match."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-z0-9/]+", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def _title_keys(title: str) -> List[str]:
    """Match keys for a listing title, longest first."""
    normalized = _normalize(title)
    keys = [normalized]
    without_number = LEADING_LAW_NUMBER_RE.sub("", normalized)
    if without_number != normalized:
        keys.append(without_number)
    return [k for k in keys if len(k) >= 8]


class CubaParlamentoScraper(BaseScraper):
    """
    Scraper for CU/ParlamentoCubano — Cuba National Assembly legislation.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "es-CU,es;q=0.9,en;q=0.5",
            },
            timeout=60,
        )

    def _fetch_html(self, url: str) -> Optional[str]:
        try:
            time.sleep(CRAWL_DELAY)
            resp = self.client.session.get(url, timeout=60)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _fetch_pdf_bytes(self, url: str) -> Optional[bytes]:
        try:
            time.sleep(CRAWL_DELAY)
            resp = self.client.session.get(url, timeout=120)
            resp.raise_for_status()
            if len(resp.content) < 500:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF {url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        try:
            import pdfplumber
            import io
            pages_text = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        pages_text.append(t)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
            return "\n\n".join(pages_text)
        except Exception as e:
            logger.warning(f"pdfplumber failed: {e}, trying pypdf")
            try:
                from pypdf import PdfReader
                import io
                reader = PdfReader(io.BytesIO(pdf_bytes))
                pages_text = []
                for page in reader.pages:
                    t = page.extract_text()
                    if t:
                        pages_text.append(t)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                return "\n\n".join(pages_text)
            except Exception as e2:
                logger.error(f"All PDF extractors failed: {e2}")
                return ""

    def _parse_listing_page(self, html: str) -> list:
        """Extract (title, pdf_url) from approved-laws blocks on the page."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        items = []
        seen_urls = set()

        # Target blocks: block_8 (constitution), block_3 (approved laws)
        for block_id in ["block_8", "block_3"]:
            block = soup.find("div", class_=f"view-display-id-{block_id}")
            if not block:
                continue
            for link in block.find_all("a", href=True):
                href = link["href"]
                if not href.lower().endswith(".pdf"):
                    continue
                text = link.get_text(strip=True)
                # Skip filename-only links (they duplicate the title links)
                if text.endswith(".pdf"):
                    continue

                href = _canonical_pdf_url(href)
                if href.startswith("/"):
                    pdf_url = BASE_URL + href
                elif href.startswith("http"):
                    pdf_url = href
                else:
                    pdf_url = BASE_URL + "/" + href

                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)
                items.append((text.strip(), pdf_url))

        return items

    def _issue_date(self, text: str) -> Optional[str]:
        """Publication date of the Gaceta issue the law was enacted in.

        The listing titles almost never name a date — only 3 of 37 do — so
        without this every record fell back to a null or a placeholder
        January 1st. The issue itself states its date three times: on the
        cover sheet, in the masthead, and in the footer of every page.
        """
        head = text[:3000]
        for pattern in (MASTHEAD_RE, COVER_DATE_RE):
            match = pattern.search(head)
            if match:
                day, month_name, year = match.groups()
                month = MONTHS.get(_normalize(month_name))
                if month:
                    return f"{year}-{month}-{day.zfill(2)}"

        footer = FOOTER_DATE_RE.search(text)
        if footer:
            day, month, year = footer.groups()
            return f"{year}-{month}-{day}"
        return None

    def _section_for_title(self, text: str, title: str) -> Optional[Tuple[str, str]]:
        """Slice the issue down to the one document the listing links to.

        A single issue PDF can carry several laws plus the decrees and
        resolutions implementing them, and the site links the same file once
        per law — the May 2026 issue is linked three times, for the Migration,
        Citizenship and Foreigners Acts. Storing the whole 573K-char issue
        under each of those titles both triples the text and makes the body
        disagree with the title, so resolve the title to its GOC code via the
        front-matter summary and cut from that code to the next one.
        """
        starts = list(SECTION_START_RE.finditer(text))
        if len(starts) < 2:
            return None

        front_matter = text[:starts[0].start()].replace("\n", " ")
        captions = []
        previous_end = 0
        for code in CODE_RE.finditer(front_matter):
            window = front_matter[max(previous_end, code.start() - CAPTION_WINDOW):code.start()]
            captions.append((code.group(0), _normalize(window)))
            previous_end = code.end()

        code = next(
            (code for key in _title_keys(title)
             for code, caption in captions if key in caption),
            None,
        )
        if not code:
            return None

        index = next((i for i, m in enumerate(starts) if m.group(1) == code), None)
        if index is None:
            return None

        end = starts[index + 1].start() if index + 1 < len(starts) else len(text)
        section = text[starts[index].end():end].strip()
        if len(section) < MIN_SECTION_CHARS:
            return None
        return code, section

    def _parse_law_metadata(self, title: str) -> Dict[str, Any]:
        meta = {"law_number": None, "law_type": None, "date": None}

        num_match = re.search(
            r'(?:Ley|Decreto[- ]?Ley|Decreto)\s*(?:No\.?\s*)?(\d+(?:/\d+)?)',
            title, re.IGNORECASE
        )
        if num_match:
            meta["law_number"] = num_match.group(1)

        if re.search(r'Decreto[- ]?Ley', title, re.IGNORECASE):
            meta["law_type"] = "decreto-ley"
        elif re.search(r'Decreto\b', title, re.IGNORECASE):
            meta["law_type"] = "decreto"
        elif re.search(r'Constituci[oó]n', title, re.IGNORECASE):
            meta["law_type"] = "constitucion"
        elif re.search(r'C[oó]digo', title, re.IGNORECASE):
            meta["law_type"] = "codigo"
        elif re.search(r'Ley\b', title, re.IGNORECASE):
            meta["law_type"] = "ley"

        date_match = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', title)
        if date_match:
            day, month_name, year = date_match.groups()
            month = MONTHS.get(_normalize(month_name), "01")
            meta["date"] = f"{year}-{month}-{day.zfill(2)}"
        else:
            year_match = re.search(r'/(\d{4})\b', title)
            if year_match:
                meta["date"] = f"{year_match.group(1)}-01-01"
            else:
                year_match2 = re.search(r'\b(19\d{2}|20\d{2})\b', title)
                if year_match2:
                    meta["date"] = f"{year_match2.group(1)}-01-01"

        return meta

    def _make_id(self, pdf_url: str) -> str:
        """Stable id from the PDF filename.

        Filenames carrying spaces, commas or accents arrive percent-encoded,
        so decode before slugifying; the plain ``goc-YYYY-oNN`` names that
        make up most of the corpus pass through unchanged.
        """
        filename = unquote(pdf_url.split("/")[-1])
        stem = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
        return re.sub(r'[^\w.\-]+', '_', stem).strip('_')

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        seen_urls = set()

        for page in range(10):  # max 10 pages, usually 6
            url = LISTING_URL if page == 0 else f"{LISTING_URL}?page={page}"
            logger.info(f"Fetching page {page}: {url}")
            html = self._fetch_html(url)
            if not html:
                break

            items = self._parse_listing_page(html)
            if not items:
                logger.info(f"No items on page {page}, stopping")
                break

            new_count = 0
            for title, pdf_url in items:
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)
                new_count += 1

                record = self._process_law(title, pdf_url)
                if record:
                    yield record

            logger.info(f"Page {page}: {new_count} new items")
            if new_count == 0:
                break

    def _process_law(self, title: str, pdf_url: str) -> Optional[Dict[str, Any]]:
        logger.info(f"Processing: {title[:80]}...")

        pdf_bytes = self._fetch_pdf_bytes(pdf_url)
        if not pdf_bytes:
            return None

        text = self._extract_pdf_text(pdf_bytes)
        if not text or len(text) < 100:
            logger.warning(f"No text from {pdf_url} ({len(text) if text else 0} chars)")
            return None

        meta = self._parse_law_metadata(title)
        doc_id = self._make_id(pdf_url)

        # The gazette issue date is a real, day-precision fact printed on the
        # document; a title like "Ley 161/2022" only gives the law's series
        # year, which is kept in law_number, and turned into a placeholder
        # January 1st here. Prefer the issue date whenever the PDF states one.
        # Read it before slicing — the masthead lives in the front matter.
        date = self._issue_date(text) or meta["date"]

        gazette_code = None
        section = self._section_for_title(text, title)
        if section:
            gazette_code, text = section
        elif len(SECTION_START_RE.findall(text)) > 1:
            logger.warning(f"Could not locate '{title[:60]}' in its issue, keeping the whole issue")

        return {
            "_id": doc_id,
            "_source": "CU/ParlamentoCubano",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text,
            "date": date,
            "url": pdf_url,
            "law_number": meta["law_number"],
            "law_type": meta["law_type"],
            "gazette_code": gazette_code,
            "language": "es",
            "jurisdiction": "CU",
        }

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CU/ParlamentoCubano scraper")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true",
                        help="Fetch all records")
    args = parser.parse_args()

    scraper = CubaParlamentoScraper()

    if args.command == "test":
        html = scraper._fetch_html(LISTING_URL)
        if html:
            items = scraper._parse_listing_page(html)
            print(f"OK: Found {len(items)} items on first page")
        else:
            print("FAIL: Could not fetch listing page")
            sys.exit(1)
        return

    # bootstrap-fast is the fleet's entry point and must run the FULL crawl;
    # only --sample caps the run (#1532).
    max_records = 15 if args.sample else 9999

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    jsonl_path = data_dir / "records.jsonl"

    count = 0
    with open(jsonl_path, "w", encoding="utf-8") as jsonl_f:
        for record in scraper.fetch_all():
            count += 1
            jsonl_f.write(json.dumps(record, ensure_ascii=False) + "\n")

            if count <= 15:
                safe_name = re.sub(r'[^\w\-.]', '_', record['_id'][:80])
                sample_path = sample_dir / f"{safe_name}.json"
                with open(sample_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(
                f"[{count}] {record['title'][:60]} — "
                f"{len(record.get('text', ''))} chars"
            )

            if count >= max_records:
                logger.info(f"Reached limit of {max_records} records")
                break

    print(f"\nDone: {count} records saved to {jsonl_path}")
    print(f"Samples: {min(count, 15)} files in {sample_dir}/")


if __name__ == "__main__":
    main()
