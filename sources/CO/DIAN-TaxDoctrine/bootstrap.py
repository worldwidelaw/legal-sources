#!/usr/bin/env python3
"""
CO/DIAN-TaxDoctrine -- Colombian Tax Authority Doctrine (Conceptos y Oficios)

Fetches tax doctrine documents from DIAN's Normograma legal compilation
(built by Avance Juridico for the Direccion de Impuestos y Aduanas Nacionales).

Discovery is INDEX-DRIVEN, not brute-force:
  The tree page t_2_doctrina_tributaria.html lazily loads its branches from
  sibling files t_2_doctrina_tributaria_parte_NN.html (NN = 01, 02, ... until
  404). Those parts hold an <a href="docs/....htm"> for every document in the
  compilation -- ~15,960 of them. Fetching 13 index pages replaces the old
  100,000-HEAD-request number sweep, and it also picks up documents the sweep
  could never reach: six-digit numbers (oficio_dian_915014_2022), letter
  suffixes (oficio_dian_8937a_2025), the concepto_tributario_dian_* family, and
  Consejo de Estado concepts (CE-SC-RAD2005-N1650).

Full text lives in the .panel-documento div of each document page and is
extracted by stripping tags. Pages are served as ISO-8859-1.

Usage:
  python bootstrap.py bootstrap             # Full corpus -> data/records.jsonl
  python bootstrap.py bootstrap-fast        # Alias used by the fleet wrapper
  python bootstrap.py bootstrap --sample    # 15 sample records -> sample/
  python bootstrap.py update                # Recent documents only
  python bootstrap.py test                  # Quick connectivity test
"""

import re
import sys
import json
import time
import html as htmlmod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.DIAN-TaxDoctrine")

COMPILATION_URL = "https://normograma.dian.gov.co/dian/compilacion/"
BASE_URL = COMPILATION_URL + "docs/"
INDEX_STEM = "t_2_doctrina_tributaria"
SOURCE_ID = "CO/DIAN-TaxDoctrine"

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
CHECKPOINT_FILE = DATA_DIR / "fetch_checkpoint.json"
INDEX_CACHE_FILE = DATA_DIR / "doc_index.json"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html",
}

# (connect, read) -- a bare int timeout still lets a slow-drip server hold the
# socket indefinitely between bytes, which is what wedged the 2026-08 run.
TIMEOUT = (10, 30)
DELAY = 0.6           # seconds between document requests
MAX_PARTS = 60        # safety ceiling on index-part probing
CHECKPOINT_EVERY = 100
INDEX_MAX_AGE_DAYS = 7

# Known valid documents for sample mode (verified to exist), spanning the
# oficio / concepto_tributario / six-digit / Consejo de Estado variants.
SAMPLE_DOCS = [
    "oficio_dian_0207_2025.htm",
    "oficio_dian_0991_2025.htm",
    "oficio_dian_3524_2025.htm",
    "oficio_dian_5035_2025.htm",
    "oficio_dian_11861_2025.htm",
    "oficio_dian_13272_2025.htm",
    "oficio_dian_18226_2025.htm",
    "oficio_dian_8937a_2025.htm",
    "oficio_dian_1513_2024.htm",
    "oficio_dian_3028_2024.htm",
    "oficio_dian_4772_2024.htm",
    "oficio_dian_9485_2024.htm",
    "oficio_dian_915014_2022.htm",
    "concepto_tributario_dian_0000001_2002.htm",
    "CE-SC-RAD2005-N1650.htm",
]

DOC_HREF_RE = re.compile(r'href="(docs/[^"]+\.htm)"', re.IGNORECASE)
LINK_RE = re.compile(
    r'<a\b[^>]*href="(docs/[^"]+\.htm)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
# e.g. oficio_dian_915014_2022 / concepto_tributario_dian_0000001_2002
NUM_YEAR_RE = re.compile(r'_0*(\d+[a-z]?)_(\d{4})$', re.IGNORECASE)

MONTHS_ES = {
    'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
    'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
    'septiembre': '09', 'setiembre': '09', 'octubre': '10',
    'noviembre': '11', 'diciembre': '12',
}


def slug_of(doc_path: str) -> str:
    """'docs/oficio_dian_207_2025.htm' -> 'oficio_dian_207_2025'."""
    return doc_path.rsplit("/", 1)[-1][: -len(".htm")]


def parse_number_year(slug: str) -> tuple:
    """Extract (concept_number, year) from a document slug, if present."""
    match = NUM_YEAR_RE.search(slug)
    if match:
        return match.group(1), int(match.group(2))
    year_match = re.search(r'(19|20)\d{2}', slug)
    return None, int(year_match.group(0)) if year_match else None


def extract_text(html_content: str) -> str:
    """Extract clean text from a DIAN normograma document page."""
    start = html_content.find('class="panel-documento"')
    if start < 0:
        return ""
    # Skip past the opening tag itself so its attributes don't leak into the text.
    tag_end = html_content.find('>', start)
    start = tag_end + 1 if tag_end > 0 else start + len('class="panel-documento"')

    end = len(html_content)
    for marker in ['class="ir-arriba"', 'class="contenedor-barra-creditos"',
                   'class="contenedor-footer"']:
        idx = html_content.find(marker, start)
        if 0 < idx < end:
            end = idx

    chunk = html_content[start:end]

    chunk = re.sub(r'<style[^>]*>.*?</style>', '', chunk, flags=re.DOTALL)
    chunk = re.sub(r'<script[^>]*>.*?</script>', '', chunk, flags=re.DOTALL)
    chunk = re.sub(r'<br\s*/?>', '\n', chunk, flags=re.IGNORECASE)
    chunk = re.sub(r'</(?:p|div|h[1-6]|li|tr|td)>', '\n', chunk, flags=re.IGNORECASE)
    chunk = re.sub(r'<[^>]+>', ' ', chunk)

    text = htmlmod.unescape(chunk)

    text = text.replace('\r\n', '\n').replace('\r', '\n')
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_title(html_content: str) -> str:
    """Extract document title from the HTML <title> tag."""
    match = re.search(r'<title[^>]*>(.*?)</title>', html_content, re.DOTALL | re.IGNORECASE)
    if match:
        title = htmlmod.unescape(match.group(1)).strip()
        title = re.sub(r'^Compilaci[oó]n Jur[ií]dica de la DIAN\s*-\s*', '', title)
        return re.sub(r'\s+', ' ', title)
    return ""


def extract_date(text: str) -> Optional[str]:
    """Extract the issue date from document text. Returns ISO format or None."""
    head = text[:600]

    # "OFICIO 915014 DE 2022 (octubre 14)"
    match = re.search(r'DE\s+((?:19|20)\d{2})\s*\n?\s*\(\s*(\w+)\s+(\d{1,2})\s*\)', head)
    if match:
        month = MONTHS_ES.get(match.group(2).lower())
        if month:
            return f"{match.group(1)}-{month}-{match.group(3).zfill(2)}"

    # "CONCEPTO TRIBUTARIO 1 DE 2002 (19 de Febrero)"
    match = re.search(
        r'DE\s+((?:19|20)\d{2})\s*\n?\s*\(\s*(\d{1,2})\s+de\s+(\w+)\s*\)', head,
        re.IGNORECASE,
    )
    if match:
        month = MONTHS_ES.get(match.group(3).lower())
        if month:
            return f"{match.group(1)}-{month}-{match.group(2).zfill(2)}"

    # "(DD de mes de YYYY)"
    match = re.search(r'\((\d{1,2})\s+de\s+(\w+)\s+de\s+((?:19|20)\d{2})\)', head, re.IGNORECASE)
    if match:
        month = MONTHS_ES.get(match.group(2).lower())
        if month:
            return f"{match.group(3)}-{month}-{match.group(1).zfill(2)}"

    return None


def extract_subject(text: str) -> str:
    """Extract subject/descriptors from the document text."""
    match = re.search(r'Descriptores?\s+(.*?)(?:\n|Fuentes)', text[:1500])
    if match:
        return re.sub(r'\s+', ' ', match.group(1)).strip()
    return ""


class DIANTaxDoctrineScraper(BaseScraper):
    """Scraper for CO/DIAN-TaxDoctrine -- Colombian tax doctrine documents."""

    def __init__(self):
        super().__init__(SOURCE_DIR)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._checkpoint = None
        # BaseScraper.bootstrap() calls fetch_all() with no arguments, so sample
        # mode has to be signalled out of band.
        self.sample_mode = False

    # ------------------------------------------------------------------ http

    def _get(self, url: str, attempts: int = 4) -> Optional[requests.Response]:
        """GET with a real (connect, read) timeout and bounded backoff."""
        for attempt in range(attempts):
            try:
                resp = self.session.get(url, timeout=TIMEOUT)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as exc:
                wait = min(30, 3 * (attempt + 1))
                logger.warning("GET %s failed (%s); retry %d/%d in %ds",
                               url, exc.__class__.__name__, attempt + 1, attempts, wait)
                time.sleep(wait)
                continue
            except Exception as exc:
                logger.error("GET %s failed: %s", url, exc)
                return None

            if resp.status_code == 404:
                return resp
            if resp.status_code >= 500 or resp.status_code == 429:
                wait = min(60, 5 * (attempt + 1))
                logger.warning("GET %s -> HTTP %d; retry %d/%d in %ds",
                               url, resp.status_code, attempt + 1, attempts, wait)
                time.sleep(wait)
                continue

            resp.encoding = resp.encoding or "ISO-8859-1"
            return resp

        return None

    # ----------------------------------------------------------------- index

    def _fetch_index(self, refresh: bool = False) -> list:
        """
        Return [{"path": "docs/x.htm", "title": "..."}] for the whole
        compilation, walking the lazily-loaded tree parts.
        """
        if not refresh and INDEX_CACHE_FILE.exists():
            try:
                cached = json.loads(INDEX_CACHE_FILE.read_text(encoding="utf-8"))
                built = datetime.fromisoformat(cached["built_at"])
                age = (datetime.now(timezone.utc) - built).days
                if age <= INDEX_MAX_AGE_DAYS and cached.get("documents"):
                    logger.info("Using cached index: %d documents (%d days old)",
                                len(cached["documents"]), age)
                    return cached["documents"]
            except Exception as exc:
                logger.warning("Ignoring unreadable index cache: %s", exc)

        documents = {}
        for part in range(1, MAX_PARTS + 1):
            url = f"{COMPILATION_URL}{INDEX_STEM}_parte_{part:02d}.html"
            resp = self._get(url)
            if resp is None:
                raise RuntimeError(f"Index part unreachable after retries: {url}")
            if resp.status_code == 404:
                logger.info("Index walk stopped at part %02d (404)", part)
                break
            resp.raise_for_status()

            found = 0
            for path, label in LINK_RE.findall(resp.text):
                title = re.sub(r'<[^>]+>', ' ', label)
                title = re.sub(r'\s+', ' ', htmlmod.unescape(title)).strip()
                if path not in documents or (title and not documents[path]):
                    documents[path] = title
                found += 1
            # Some entries are plain hrefs without an <a> body we can match.
            for path in DOC_HREF_RE.findall(resp.text):
                documents.setdefault(path, "")
            logger.info("Index part %02d: %d links (%d unique so far)",
                        part, found, len(documents))
            time.sleep(0.3)

        if not documents:
            raise RuntimeError(
                "DIAN index walk yielded 0 documents -- normograma.dian.gov.co is "
                "unreachable or the tree layout changed. Refusing to report success."
            )

        entries = [{"path": p, "title": t} for p, t in sorted(documents.items())]
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        INDEX_CACHE_FILE.write_text(
            json.dumps(
                {"built_at": datetime.now(timezone.utc).isoformat(), "documents": entries},
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        logger.info("Index built: %d unique documents", len(entries))
        return entries

    # ------------------------------------------------------------ checkpoint

    def _load_checkpoint(self) -> set:
        if self._checkpoint is None:
            done = set()
            if CHECKPOINT_FILE.exists():
                try:
                    done = set(json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))["done"])
                except Exception as exc:
                    logger.warning("Ignoring unreadable checkpoint: %s", exc)
            self._checkpoint = done
        return self._checkpoint

    def _save_checkpoint(self):
        if self._checkpoint is None:
            return
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = CHECKPOINT_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"done": sorted(self._checkpoint)}), encoding="utf-8")
        tmp.replace(CHECKPOINT_FILE)

    # ------------------------------------------------------------- documents

    def _fetch_document(self, path: str, index_title: str = "") -> Optional[dict]:
        url = COMPILATION_URL + path
        resp = self._get(url)
        if resp is None or resp.status_code == 404:
            return None
        if resp.status_code != 200:
            logger.warning("Unexpected HTTP %d for %s", resp.status_code, url)
            return None
        return {"html": resp.text, "path": path, "url": url, "index_title": index_title}

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a fetched document into the standard schema."""
        html_content = raw["html"]
        slug = slug_of(raw["path"])

        text = extract_text(html_content)
        if not text or len(text) < 100:
            return None

        title = extract_title(html_content) or raw.get("index_title") or slug
        number, year = parse_number_year(slug)
        date = extract_date(text)
        if not date and year:
            date = f"{year}-01-01"

        return {
            "_id": f"CO-DIAN-{slug}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw["url"],
            "language": "es",
            "concept_number": number,
            "year": year,
            "subject": extract_subject(text),
        }

    def fetch_all(self, sample: bool = None) -> Generator[dict, None, None]:
        """Yield RAW documents; BaseScraper.bootstrap() calls normalize()."""
        if self.sample_mode if sample is None else sample:
            for name in SAMPLE_DOCS:
                raw = self._fetch_document("docs/" + name)
                if raw:
                    yield raw
                time.sleep(DELAY)
            return

        entries = self._fetch_index()
        done = self._load_checkpoint()
        if done:
            logger.info("Checkpoint: %d documents already fetched, skipping", len(done))

        fetched = 0
        errors = 0
        since_flush = 0
        for i, entry in enumerate(entries, 1):
            path = entry["path"]
            if path in done:
                continue

            raw = self._fetch_document(path, entry.get("title", ""))
            time.sleep(DELAY)

            if raw is None:
                errors += 1
                # A dead link is permanent; mark it done so restarts don't re-probe.
                done.add(path)
            else:
                fetched += 1
                done.add(path)
                yield raw

            since_flush += 1
            if since_flush >= CHECKPOINT_EVERY:
                self._save_checkpoint()
                since_flush = 0
                logger.info("Progress: %d/%d indexed, %d fetched, %d errors",
                            i, len(entries), fetched, errors)

        self._save_checkpoint()
        logger.info("Fetch complete: %d documents, %d unreachable", fetched, errors)

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        """Yield RAW documents from the two most recent years in the index."""
        entries = self._fetch_index(refresh=True)
        current_year = datetime.now().year
        recent = []
        for entry in entries:
            _, year = parse_number_year(slug_of(entry["path"]))
            if year and year >= current_year - 1:
                recent.append(entry)

        logger.info("Update: %d documents from %d-%d", len(recent), current_year - 1, current_year)
        for entry in recent:
            raw = self._fetch_document(entry["path"], entry.get("title", ""))
            time.sleep(DELAY)
            if raw:
                yield raw

    def test(self) -> bool:
        """Quick connectivity test: index part 01 + one known document."""
        logger.info("Testing connectivity to DIAN Normograma...")
        try:
            resp = self._get(f"{COMPILATION_URL}{INDEX_STEM}_parte_01.html")
            if resp is None or resp.status_code != 200:
                logger.error("Test failed: index part 01 unreachable")
                return False
            links = set(DOC_HREF_RE.findall(resp.text))
            logger.info("Index part 01: %d document links", len(links))
            if not links:
                logger.error("Test failed: index part 01 has no document links")
                return False

            raw = self._fetch_document("docs/oficio_dian_0207_2025.htm")
            if not raw:
                logger.error("Test failed: could not fetch known document")
                return False
            record = self.normalize(raw)
            if not record:
                logger.error("Test failed: known document produced no text")
                return False
            logger.info("OK: '%s' (%d chars, date=%s)",
                        record["title"][:60], len(record["text"]), record["date"])
            logger.info("Test PASSED")
            return True
        except Exception as exc:
            logger.error("Test FAILED: %s", exc)
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description='CO/DIAN-TaxDoctrine fetcher')
    parser.add_argument('command', choices=['bootstrap', 'bootstrap-fast', 'update', 'test'])
    parser.add_argument('--sample', action='store_true', help='Fetch 15 sample records')
    parser.add_argument('--since', type=str, help='Date for update (YYYY-MM-DD)')
    parser.add_argument('--full', action='store_true', help='Fetch all records (default)')
    args = parser.parse_args()

    scraper = DIANTaxDoctrineScraper()

    if args.command == 'test':
        sys.exit(0 if scraper.test() else 1)

    if args.command in ('bootstrap', 'bootstrap-fast'):
        scraper.sample_mode = args.sample
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info("Bootstrap stats: %s", json.dumps(stats, indent=2, default=str))
        written = (stats.get("sample_records_saved")
                   if args.sample else stats.get("records_fetched", 0))
        sys.exit(0 if (written or 0) >= 10 else 1)

    if args.command == 'update':
        stats = scraper.update()
        logger.info("Update stats: %s", json.dumps(stats, indent=2, default=str))


if __name__ == '__main__':
    main()
