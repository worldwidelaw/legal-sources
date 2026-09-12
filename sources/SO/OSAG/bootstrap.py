#!/usr/bin/env python3
"""
SO/OSAG — Somalia Office of the State Attorney General — Official Gazette

Fetches legislation, cabinet decisions, resolutions, and agreements from
the OSAG Official Bulletin (Faafinta Rasmiga Ah). ~280 individual documents
published from 2014-present, each as a downloadable PDF.

Strategy:
  - Paginate bulletins-contents listing (10 per page, ~28 pages)
  - For each entry: reference number, title, issuing org, date, PDF URL
  - Download PDF and re-OCR the scanned body pages (see below)
  - Classify by URL path (legislation, cabinet-decisions, agreements, etc.)

Text extraction (issue #1411)
-----------------------------
Every OSAG bulletin is a *scan*: page 0 is a born-digital masthead, and every
body page is a single full-page bitmap (1700x2200 or 2550x3300). Two things
followed from that, and both corrupted the stored corpus:

  * Some PDFs ship an upstream text layer produced by the publisher's scanner
    running an English OCR model over Somali. Reading that layer verbatim is
    what produced `Qodcbbada` (Qodobbada), `dhucdhuclaya`, `Gudcliga`
    (Guddiga) and article numbers read as `zo'"` / `is'"` (20aad / 15aad).
  * The rest ship no text layer at all, so the old pdfplumber pass returned
    only the ~790-char masthead — the law itself was silently dropped while
    the record still looked like it had "full text".

So we rasterize each scanned page ourselves and run tesseract. Measured on
Sharciga Kaalma-Sharciyeedka Federaalka, our pass beats the upstream layer
outright: `Guddiga`/`Iskaashi`/`sharciyeedka` where the upstream layer had
`Gudcliga`/`lskaashi`/`shardyeedka`. Tesseract has no Somali model, but
Somali is plain ASCII Latin and `eng` at 300 DPI reads it cleanly; `swa`
(the nearest African Latin-script model) was markedly worse, substituting
z for x throughout. 300 DPI and the default page-segmentation mode won on
every page sampled — 400 DPI bought nothing and psm 6 was slightly worse.

Which `eng` build matters more than any of that tuning. Homebrew and apt
ship the small `tessdata` model, and on Somali it reads the extremely
common word `oo` as the digits `00` — 12 occurrences over three sampled
pages. The `tessdata_best` LSTM model gets all 12 right and every other
difference on those pages is also a repair, never a regression:
`Tyadoo`→`Iyadoo`, `etayada`→`erayada`, `goran`→`qoran`,
`Banaadit`→`Banaadir`, `shatci`→`sharci`. So we fetch that model once
(15 MB, checksum-pinned, cached under ~/.cache/legal-data-hunter) and
point tesseract at it; if the fetch fails we fall back to the system model,
which degrades accuracy but still reads the gazettes. Set
OSAG_TESSDATA_DIR to supply the model out-of-band instead.

Two tunings that sound promising do nothing and were dropped: disabling
the English dictionary (`load_system_dawg`/`load_freq_dawg`) is inert
because tesseract 5 defaults to the LSTM engine, which doesn't consult
those, and psm 4 traded one error class for another. The residual ceiling
is that tesseract has no Somali model at all — remaining misreads
(`Dastuut` for `Dastuur`, `shatciyeed` for `sharciyeed`) are r/t confusions
no English-model tuning can fix.

Born-digital pages keep their real text layer; only full-page bitmaps are
OCR'd, so the masthead is never degraded by a round trip through tesseract.

Source: https://osagsomalia.com/en/resources/bulletins-contents/
Rate limit: 1.5 req/sec

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py bootstrap-fast --full
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)

try:
    import fitz  # PyMuPDF
except ImportError:
    print("ERROR: PyMuPDF not installed. Run: pip3 install PyMuPDF")
    sys.exit(1)

try:
    import pytesseract
    from PIL import Image
except ImportError:
    print("ERROR: pytesseract/Pillow not installed. Run: pip3 install pytesseract Pillow")
    sys.exit(1)

SOURCE_ID = "SO/OSAG"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SO.OSAG")

BASE_URL = "https://osagsomalia.com"
LISTING_URL = f"{BASE_URL}/en/resources/bulletins-contents/"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,application/pdf",
    "Accept-Language": "en,so",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

RATE_LIMIT = 1.5
_last_request = 0.0

# OCR tuning — see the module docstring for how these were chosen.
OCR_LANG = os.environ.get("OSAG_OCR_LANG", "eng")
OCR_DPI = int(os.environ.get("OSAG_OCR_DPI", "300"))
OCR_MAX_PAGES = int(os.environ.get("OSAG_OCR_MAX_PAGES", "400"))
OCR_PAGE_TIMEOUT = int(os.environ.get("OSAG_OCR_PAGE_TIMEOUT", "120"))
OCR_DOC_TIMEOUT = int(os.environ.get("OSAG_OCR_DOC_TIMEOUT", "1800"))
# The high-accuracy LSTM model. Homebrew/apt ship the smaller `tessdata`
# build, which misreads Somali `oo` as `00`; see the docstring.
BEST_MODEL_URL = (
    "https://github.com/tesseract-ocr/tessdata_best/raw/main/eng.traineddata"
)
BEST_MODEL_SHA256 = (
    "8280aed0782fe27257a68ea10fe7ef324ca0f8d85bd2fd145d1c2b560bcb66ba"
)
# Where the downloaded model is cached. Point OSAG_TESSDATA_DIR at a
# directory holding eng.traineddata to supply the model out-of-band instead
# (e.g. baked into the fleet image) and skip the download entirely.
TESSDATA_DIR = os.environ.get("OSAG_TESSDATA_DIR")
# A page image at least this wide *and* tall is a full-page scan, not a logo.
SCAN_MIN_PX = 1000
# Below this many characters of scanned-page text the record is the gazette
# masthead and nothing else — the failure mode issue #1411 was filed for.
MIN_BODY_CHARS = 400

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def _throttle():
    global _last_request
    now = time.time()
    wait = RATE_LIMIT - (now - _last_request)
    if wait > 0:
        time.sleep(wait)
    _last_request = time.time()


def _get(url, **kwargs):
    _throttle()
    resp = SESSION.get(url, timeout=60, **kwargs)
    resp.raise_for_status()
    return resp


def parse_date(date_str):
    """Parse date like 'Apr 2026' or 'Dec 2023' to ISO format."""
    date_str = date_str.strip()
    match = re.match(r"(\w{3})\s+(\d{4})", date_str)
    if match:
        month_abbr = match.group(1).lower()
        year = match.group(2)
        month = MONTH_MAP.get(month_abbr)
        if month:
            return f"{year}-{month}-01"
    # Try full date patterns
    for fmt in ["%B %d, %Y", "%d %B %Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def classify_type(pdf_path):
    """Classify document type from PDF URL path."""
    path_lower = pdf_path.lower()
    if "/legislation/" in path_lower:
        return "legislation"
    elif "/cabinet-decisions/" in path_lower or "/xeer/" in path_lower:
        return "legislation"  # Cabinet decisions with legal force
    elif "/agreements/" in path_lower:
        return "legislation"
    elif "/resolutions/" in path_lower:
        return "legislation"
    elif "/budget/" in path_lower:
        return "legislation"
    return "legislation"


def slug_from_ref_and_title(ref, title):
    """Create a unique slug from reference number and title."""
    combined = f"{ref}_{title}" if ref else title
    slug = re.sub(r"[^\w\-]", "-", combined)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug[:120]


def _ocr_available():
    """True if tesseract is usable. OSAG is 100% scans, so this is required."""
    try:
        pytesseract.get_tesseract_version()
        return True
    except Exception as e:
        logger.error("tesseract is not usable: %s", e)
        return False


_tessdata_dir = None  # resolved once per process by _resolve_tessdata_dir()


def _resolve_tessdata_dir():
    """Return a tessdata dir holding the high-accuracy model, or None.

    None means "use whatever tesseract found on its own" — the standard
    model still reads the gazettes, just less accurately (see the docstring),
    so a failed download degrades quality rather than the run.
    """
    global _tessdata_dir
    if _tessdata_dir is not None:
        return _tessdata_dir or None

    if TESSDATA_DIR:
        if (Path(TESSDATA_DIR) / f"{OCR_LANG}.traineddata").exists():
            _tessdata_dir = TESSDATA_DIR
            return _tessdata_dir
        logger.warning(
            "OSAG_TESSDATA_DIR=%s has no %s.traineddata — ignoring",
            TESSDATA_DIR, OCR_LANG,
        )

    cache_dir = Path(
        os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")
    ) / "legal-data-hunter" / "tessdata_best"
    model = cache_dir / "eng.traineddata"

    if not model.exists():
        if OCR_LANG != "eng":
            _tessdata_dir = ""
            return None
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Downloading tessdata_best eng model (~15 MB, once)")
            resp = SESSION.get(BEST_MODEL_URL, timeout=300)
            resp.raise_for_status()
            digest = hashlib.sha256(resp.content).hexdigest()
            if digest != BEST_MODEL_SHA256:
                # Upstream re-published or the download was truncated: fall
                # back rather than OCR the whole corpus with an unknown model.
                logger.warning(
                    "tessdata_best checksum mismatch (%s) — using system model",
                    digest,
                )
                _tessdata_dir = ""
                return None
            tmp = model.with_suffix(".part")
            tmp.write_bytes(resp.content)
            tmp.replace(model)  # atomic: concurrent workers never read a partial file
        except Exception as e:
            logger.warning(
                "Could not fetch tessdata_best (%s) — using system model", e
            )
            _tessdata_dir = ""
            return None

    _tessdata_dir = str(cache_dir)
    return _tessdata_dir


def _page_is_scan(page):
    """True if the page is a full-page bitmap rather than born-digital text.

    Body pages carry one image at least SCAN_MIN_PX on a side (letter at
    200-300 DPI); the masthead's only image is a ~154x117 logo.
    """
    for img in page.get_images(full=True):
        if img[2] >= SCAN_MIN_PX and img[3] >= SCAN_MIN_PX:
            return True
    return False


def _ocr_page(page):
    """Rasterize one page and read it with tesseract.

    Renders and releases a single pixmap at a time: a whole-document list of
    RGB bitmaps is roughly 25 MB per letter page at 300 DPI, which is how the
    shared extractor once OOM-killed fleet workers (issue #1328).
    """
    pix = page.get_pixmap(dpi=OCR_DPI, colorspace=fitz.csRGB, alpha=False)
    try:
        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
    finally:
        del pix
    tessdata = _resolve_tessdata_dir()
    config = f'--tessdata-dir "{tessdata}"' if tessdata else ""
    try:
        return pytesseract.image_to_string(
            img, lang=OCR_LANG, config=config, timeout=OCR_PAGE_TIMEOUT
        ).strip()
    finally:
        img.close()


def _normalize_article_headers(text):
    """Repair `Qodobka <N>aad` headings mangled by the superscript "aad".

    The scans set the ordinal suffix as superscript, which tesseract reads as
    punctuation noise — `Qodobka 20aad` comes back as `Qodobka 20"` and
    `Qodobka 21**4`, or the suffix fuses into the digits (`Qodobka 474` for
    article 4). The digits themselves survive, which the upstream layer could
    not manage: there `20aad` and `15aad` read as `zo'"` and `is'"`.

    Only headings on a line of their own are touched, and only when the
    article number can actually be derived — from a punctuation-noise suffix,
    from a run that begins with the number we expected next, or from a run
    whose leading digits land just after the previous article. Anything else
    is left exactly as OCR'd rather than dressed up as a heading we invented;
    line-initial *citations* like `Qodobka 224` (to article 22 of the
    Constitution) are the case this protects.
    """
    # Headings are often broken across two lines ("Qodobka" / "15aad").
    text = re.sub(r"(?im)^([ \t]*Qodobka)[ \t]*\n[ \t]*(?=\d)", r"\1 ", text)

    # How far past the previous article a run's leading digits may land and
    # still be read as the next heading rather than a citation.
    lookahead = 3
    expected = [0]

    def repl(m):
        token = m.group(2)
        run = re.match(r"(\d{1,6})(.*)$", token)
        if not run:
            return m.group(0)
        digits, tail = run.group(1), run.group(2)
        nxt = expected[0] + 1

        if tail == "aad" or re.search(r"[^\w\s]", tail):
            # Suffix is either intact or unmistakable superscript noise.
            n = int(digits)
        elif digits.startswith(str(nxt)) and len(digits) > len(str(nxt)):
            # Suffix fused into the digits: "474" at article 4.
            n = nxt
        elif expected[0] < int(digits) <= expected[0] + lookahead:
            n = int(digits)
        elif len(digits) > 1 and expected[0] < int(digits[:-1]) <= expected[0] + lookahead:
            # Trailing digit is the tail of the suffix: "207" at article 19.
            n = int(digits[:-1])
        elif int(digits) < expected[0] and not tail:
            # A restart — the body repeating articles listed in the contents.
            n = int(digits)
        else:
            return m.group(0)

        expected[0] = n
        return f"{m.group(1)} {n}aad"

    return re.sub(
        r"(?im)^([ \t]*Qodobka)[ \t]+(\S{1,8})[ \t]*$",
        repl,
        text,
    )


def extract_pdf_text(pdf_bytes, max_pages=OCR_MAX_PAGES):
    """Return (full_text, body_chars) for one bulletin PDF.

    `body_chars` counts only the scanned pages — the law itself. It is what
    tells a real extraction apart from the masthead-only records the old
    pdfplumber pass produced for PDFs with no text layer.
    """
    parts = []
    body_chars = 0
    deadline = time.monotonic() + OCR_DOC_TIMEOUT if OCR_DOC_TIMEOUT > 0 else None

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        if doc.page_count > max_pages:
            logger.warning(
                "PDF has %d pages — capped at %d", doc.page_count, max_pages
            )
        for i in range(min(doc.page_count, max_pages)):
            page = doc.load_page(i)
            if not _page_is_scan(page):
                text = (page.get_text() or "").strip()
                if text:
                    parts.append(text)
                continue
            if deadline is not None and time.monotonic() >= deadline:
                logger.warning(
                    "OCR budget of %ds exhausted after %d page(s) — keeping partial text",
                    OCR_DOC_TIMEOUT, i,
                )
                break
            try:
                text = _ocr_page(page)
            except Exception as e:
                logger.warning("OCR failed on page %d: %s", i, e)
                continue
            if text:
                parts.append(text)
                body_chars += len(text)
    finally:
        doc.close()

    return _normalize_article_headers("\n\n".join(parts)), body_chars


def get_listing_page(page_num):
    """Fetch a listing page and extract document entries."""
    url = f"{LISTING_URL}?page={page_num}"
    logger.info(f"Fetching listing page {page_num}: {url}")
    resp = _get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    entries = []
    table = soup.find("table")
    if not table:
        return entries

    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        ref = cells[0].get_text(strip=True)
        title = cells[1].get_text(strip=True)
        issuing_org = cells[2].get_text(strip=True)
        date_str = cells[3].get_text(strip=True)

        # Find PDF link
        pdf_link = row.find("a", href=re.compile(r"\.pdf$", re.I))
        if not pdf_link:
            continue

        pdf_url = urljoin(BASE_URL, pdf_link["href"])

        entries.append({
            "ref": ref,
            "title": title,
            "issuing_org": issuing_org,
            "date_str": date_str,
            "pdf_url": pdf_url,
        })

    return entries


def fetch_all(sample=False):
    """Fetch all OSAG gazette documents."""
    if not _ocr_available():
        raise RuntimeError(
            "SO/OSAG bulletins are scanned images with no usable text layer; "
            "tesseract (pytesseract + the tesseract binary) is required. "
            "Refusing to fall back to the upstream OCR layer — that is the "
            "corruption issue #1411 was filed for."
        )

    all_entries = []
    max_pages = 30 if not sample else 3

    for page_num in range(1, max_pages + 1):
        entries = get_listing_page(page_num)
        if not entries:
            logger.info(f"No entries on page {page_num}, stopping pagination")
            break
        all_entries.extend(entries)
        logger.info(f"Page {page_num}: {len(entries)} entries (total: {len(all_entries)})")

    logger.info(f"Total entries found: {len(all_entries)}")

    seen_urls = set()
    skipped = []
    count = 0
    for entry in all_entries:
        pdf_url = entry["pdf_url"]
        if pdf_url in seen_urls:
            continue
        seen_urls.add(pdf_url)

        try:
            logger.info(f"Downloading: {entry['title'][:60]}")
            resp = _get(pdf_url)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {pdf_url}")
                continue

            text, body_chars = extract_pdf_text(resp.content)
            if body_chars < MIN_BODY_CHARS:
                # Masthead-only. Storing this is what made the corpus look
                # complete while the law itself was missing (issue #1411).
                skipped.append(pdf_url)
                logger.warning(
                    "Only %d chars of body text from %s — masthead only, skipping",
                    body_chars, pdf_url,
                )
                continue

            date = parse_date(entry["date_str"])
            doc_type = classify_type(pdf_url)
            doc_id = slug_from_ref_and_title(entry["ref"], entry["title"])

            doc = {
                "_id": doc_id,
                "_source": SOURCE_ID,
                "_type": doc_type,
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": entry["title"],
                "text": text,
                "date": date,
                "url": pdf_url,
                "reference_number": entry["ref"],
                "issuing_organization": entry["issuing_org"],
                "language": "so",
                "text_source": (
                    f"ocr:tesseract:{OCR_LANG}"
                    f"{'-best' if _resolve_tessdata_dir() else ''}@{OCR_DPI}dpi"
                ),
            }

            yield doc
            count += 1
            logger.info(f"[{count}] {entry['ref']} {entry['title'][:50]} — {len(text)} chars")

            if sample and count >= 15:
                logger.info("Sample limit reached")
                return

        except Exception as e:
            logger.error(f"Error fetching {pdf_url}: {e}")
            continue

    logger.info(f"Total documents fetched: {count} (skipped {len(skipped)} masthead-only)")


def save_sample(records, output_dir):
    """Save sample records as JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for rec in records:
        fname = re.sub(r"[^\w\-]", "_", rec["_id"])[:80] + ".json"
        path = output_dir / fname
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved: {path.name}")


def test_api():
    """Quick connectivity test."""
    print(f"Testing {LISTING_URL} ...")
    resp = _get(LISTING_URL)
    print(f"Status: {resp.status_code}")
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if table:
        rows = table.find_all("tr")
        print(f"Table rows: {len(rows)}")
        pdf_links = [a["href"] for a in table.find_all("a", href=re.compile(r"\.pdf$", re.I))]
        print(f"PDF links: {len(pdf_links)}")
        for link in pdf_links[:3]:
            print(f"  - {link.split('/')[-1]}")
    # Check pagination
    pagination = soup.find_all("a", href=re.compile(r"page="))
    if pagination:
        pages = set()
        for a in pagination:
            m = re.search(r"page=(\d+)", a["href"])
            if m:
                pages.add(int(m.group(1)))
        if pages:
            print(f"Pages: 1-{max(pages)}")
    print("API test passed." if table else "WARNING: No table found!")


def main():
    parser = argparse.ArgumentParser(description="SO/OSAG bootstrapper")
    # The fleet wrapper invokes `bootstrap-fast`; without it argparse exits 2
    # and the run falls back to re-ingesting sample/ (issues #1363, #1113).
    parser.add_argument("command", choices=["test-api", "bootstrap", "bootstrap-fast"])
    parser.add_argument("--sample", action="store_true", help="Save sample records only (15 docs)")
    parser.add_argument("--full", action="store_true", help="Run full bootstrap (all pages)")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
        return

    if args.sample:
        records = list(fetch_all(sample=True))
        if not records:
            print("ERROR: No records fetched!")
            sys.exit(1)
        save_sample(records, SAMPLE_DIR)
        print(f"\nBootstrap complete: {len(records)} records saved to {SAMPLE_DIR}")
        text_lens = [len(r.get("text", "")) for r in records]
        print(f"Text lengths: min={min(text_lens)}, max={max(text_lens)}, "
              f"avg={sum(text_lens)//len(text_lens)}")
        return

    # Full run: stream to data/records.jsonl, which is what the fleet ingests.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_file = DATA_DIR / "records.jsonl"
    count = 0
    with out_file.open("w", encoding="utf-8") as fh:
        for rec in fetch_all(sample=False):
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            count += 1
    print(f"\nBootstrap complete: {count} records written to {out_file}")
    if count == 0:
        print("ERROR: No records fetched!")
        sys.exit(1)


if __name__ == "__main__":
    main()
