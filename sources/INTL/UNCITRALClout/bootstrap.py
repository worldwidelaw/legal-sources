#!/usr/bin/env python3
"""
INTL/UNCITRALClout - UNCITRAL CLOUT Case Law Database Fetcher

Fetches international commercial law case abstracts from the UNCITRAL CLOUT database.
Covers CISG, Model Law on Arbitration, and other UNCITRAL texts.
~2,255 cases from courts worldwide.

Data source: https://www.uncitral.org/clout/search.jspx
Method: HTML scraping + PDF text extraction
License: UN public domain

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


BASE_URL = "https://www.uncitral.org"
SEARCH_URL = f"{BASE_URL}/clout/search.jspx"
SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"
INDEX_CACHE = DATA_DIR / "abstracts_index.json"
SOURCE_ID = "INTL/UNCITRALClout"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36 LegalDataHunter/1.0",
    "Accept": "text/html,application/xhtml+xml",
}

RATE_LIMIT_DELAY = 2.0
PAGE_SIZE = 10  # Fixed by the server

# CLOUT abstracts are published only as compilation documents in the UN document
# series A/CN.9/SER.C/ABSTRACTS/{N} (each covers ~9-12 consecutive cases). The old
# per-case links (daccess-ods.un.org ?JN=... / undocs.org) died when the UN document
# server migrated to documents.un.org; the modern PDF endpoint below is the stable
# way to retrieve the compilations. We fetch every compilation, split it into
# per-case abstracts, and index by CLOUT case number.
ABSTRACT_SYMBOL = "A/CN.9/SER.C/ABSTRACTS/{n}"
UN_DOC_PDF_API = "https://documents.un.org/api/symbol/access?s={sym}&l=en&t=pdf"
# Boilerplate that marks the compilation's Introduction section — a table-of-contents
# "Case N:" entry that runs into the Introduction must be discarded, not kept as text.
_ABSTRACT_BOILERPLATE = "This compilation of abstracts forms part of the system"

# Populated lazily by build_abstract_index(); maps CLOUT case number (str) -> abstract text.
_ABSTRACT_INDEX: Optional[dict] = None


class SimpleHTMLTextExtractor(HTMLParser):
    """Extract text content from HTML, stripping tags."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False
        if tag in ("br", "p", "div", "li"):
            self.text_parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self.text_parts.append(data)

    def get_text(self):
        return "".join(self.text_parts).strip()


def strip_html(html_str: str) -> str:
    """Remove HTML tags and return plain text."""
    extractor = SimpleHTMLTextExtractor()
    try:
        extractor.feed(html_str)
        return extractor.get_text()
    except Exception:
        return re.sub(r"<[^>]+>", " ", html_str).strip()


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="INTL/UNCITRALClout",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def fetch_abstract_pdf(n: int) -> Optional[bytes]:
    """Fetch compilation A/CN.9/SER.C/ABSTRACTS/{n} as PDF bytes (None if it doesn't exist)."""
    sym = ABSTRACT_SYMBOL.format(n=n)
    url = UN_DOC_PDF_API.format(sym=sym)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=120, allow_redirects=True)
    except requests.RequestException as e:
        print(f"    Error fetching abstract {sym}: {e}")
        return None
    # Missing documents return a small HTML "Document Viewer" error page, not a PDF.
    if resp.status_code != 200 or not resp.content[:4] == b"%PDF":
        return None
    return resp.content


def split_compilation_into_cases(text: str) -> dict:
    """Split a compilation's extracted text into {case_number: abstract_text}.

    Each case number appears at least twice (table of contents + body). The body
    segment is the real abstract; the TOC entry is short and the *last* TOC entry
    also swallows the Introduction boilerplate. We drop any segment containing the
    boilerplate and keep the longest remaining segment per case number.
    """
    out: dict = {}
    matches = list(re.finditer(r"\bCase\s+(\d{2,4})\s*:", text))
    for i, m in enumerate(matches):
        num = m.group(1)
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        seg = text[start:end].strip()
        # Trim trailing table-of-contents dot leaders ("... 11").
        seg = re.sub(r"\s*\.\s*(?:\.\s*){2,}\d+\s*$", "", seg).strip()
        if _ABSTRACT_BOILERPLATE in seg:
            continue
        if num not in out or len(seg) > len(out[num]):
            out[num] = seg
    return out


def discover_max_abstract(ceiling: int = 400) -> int:
    """Find the highest published A/CN.9/SER.C/ABSTRACTS/{n} via exponential + binary search."""
    lo, hi = 1, 1
    while hi <= ceiling and fetch_abstract_pdf(hi) is not None:
        lo = hi
        hi *= 2
    hi = min(hi, ceiling)
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if fetch_abstract_pdf(mid) is not None:
            lo = mid
        else:
            hi = mid
    return lo


def build_abstract_index(needed: Optional[set] = None, use_cache: bool = True) -> dict:
    """Build (or load) the case_number -> abstract_text index.

    If ``needed`` is given, scan compilations newest-first and stop early once every
    needed case number is covered (fast path for sample runs). Otherwise build the
    full index over all compilations and persist it to ``INDEX_CACHE``.
    """
    global _ABSTRACT_INDEX
    if _ABSTRACT_INDEX is not None and needed is None:
        return _ABSTRACT_INDEX

    if use_cache and needed is None and INDEX_CACHE.exists():
        try:
            _ABSTRACT_INDEX = json.loads(INDEX_CACHE.read_text(encoding="utf-8"))
            print(f"  Loaded abstract index from cache ({len(_ABSTRACT_INDEX)} cases)")
            return _ABSTRACT_INDEX
        except (ValueError, OSError):
            pass

    print("  Building abstract index from A/CN.9/SER.C/ABSTRACTS compilations...")
    max_n = discover_max_abstract()
    print(f"  Highest compilation: A/CN.9/SER.C/ABSTRACTS/{max_n}")
    index: dict = {}
    order = range(max_n, 0, -1) if needed else range(1, max_n + 1)
    for n in order:
        pdf_bytes = fetch_abstract_pdf(n)
        if not pdf_bytes:
            continue
        text = extract_text_from_pdf(pdf_bytes)
        if not text:
            continue
        cases = split_compilation_into_cases(text)
        for num, seg in cases.items():
            if num not in index or len(seg) > len(index[num]):
                index[num] = seg
        print(f"    ABSTRACTS/{n}: +{len(cases)} cases (index now {len(index)})")
        time.sleep(0.5)
        if needed and needed.issubset(index.keys()):
            print("    All needed cases covered — stopping early.")
            break

    if needed is None:
        _ABSTRACT_INDEX = index
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            INDEX_CACHE.write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")
        except OSError:
            pass
    return index


def fetch_html(url: str) -> Optional[str]:
    """Fetch an HTML page."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=120)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}")
        return None


def fetch_pdf(url: str) -> Optional[bytes]:
    """Fetch a PDF file as bytes."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=120, allow_redirects=True)
        resp.raise_for_status()
        if len(resp.content) > 50_000_000:  # Skip >50MB
            print(f"    PDF too large: {len(resp.content)} bytes")
            return None
        return resp.content
    except requests.RequestException as e:
        print(f"    Error fetching PDF {url}: {e}")
        return None


def parse_search_results(html: str) -> list[str]:
    """Extract case detail URLs from search results HTML."""
    # Pattern: onClick="document.location='...' or &apos;...&apos;"
    # HTML may use &apos; instead of actual quotes
    normalized = html.replace("&apos;", "'")
    pattern = r"document\.location='(/clout/clout/data/[^']+\.html)'"
    urls = re.findall(pattern, normalized)
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(f"{BASE_URL}{u}")
    return unique


def parse_case_detail(html: str) -> dict:
    """Parse a case detail page into structured data."""
    result = {}

    # Extract title: <h1>CLOUT case  {number}</h1>
    title_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
    if title_match:
        result["title"] = strip_html(title_match.group(1)).strip()

    # Extract fields from cloutDetailItem divs
    # Each item: <div class="cloutDetailItem"><span class="labelTitle">FIELD</span><span ...class="itemContent">VALUE</span></div>
    # Use a two-step approach: find each cloutDetailItem block, then extract label+content
    item_blocks = re.findall(
        r'<div\s+class="cloutDetailItem">(.*?)</div>(?=\s*<div)',
        html,
        re.DOTALL,
    )
    items = []
    for block in item_blocks:
        label_match = re.search(r'<span\s+class="labelTitle">(.*?)</span>', block, re.DOTALL)
        content_match = re.search(r'<span[^>]*class="itemContent">(.*?)$', block, re.DOTALL)
        if label_match and content_match:
            items.append((label_match.group(1), content_match.group(1)))

    for label_html, content_html in items:
        label = strip_html(label_html).strip().lower()
        content = content_html.strip()

        if "legislative text" in label:
            result["legislative_text"] = strip_html(content)
        elif "clout issue" in label:
            result["clout_issue"] = strip_html(content)
        elif "articles" in label:
            # Extract article numbers from links
            articles = re.findall(r">([^<]+)</a>", content)
            result["articles"] = [a.strip() for a in articles if a.strip()]
        elif "country" in label:
            # Extract country name (text after the flag image)
            country_text = strip_html(content)
            result["country"] = country_text.strip()
            # Extract ISO3 from flag image
            flag_match = re.search(r'/(\w+)\.png', content)
            if flag_match:
                result["country_code"] = flag_match.group(1)
        elif "court name" in label:
            result["court"] = strip_html(content)
        elif "court reference" in label:
            ref = strip_html(content)
            if ref and ref != "-":
                result["court_reference"] = ref
        elif "parties" in label:
            parties = strip_html(content)
            if parties and parties != "-":
                result["parties"] = parties
        elif "decision date" in label:
            # Format: DD/MM/YYYY
            date_text = strip_html(content)
            try:
                dt = datetime.strptime(date_text.strip(), "%d/%m/%Y")
                result["date"] = dt.strftime("%Y-%m-%d")
            except ValueError:
                result["date"] = date_text.strip()
        elif "comments" in label:
            comments = strip_html(content)
            if comments and comments != "-":
                result["comments"] = comments
        elif "abstracts published" in label:
            # Extract PDF URL from <a href="...">
            pdf_match = re.search(r'href="([^"]+)"', content)
            if pdf_match:
                result["abstract_pdf_url"] = pdf_match.group(1).replace("&amp;", "&")
        elif "keyword" in label:
            keywords = re.findall(r">([^<]+)</a>", content)
            result["keywords"] = [k.strip() for k in keywords if k.strip() and k.strip() != "-"]
        elif "original fulltext" in label:
            pdf_match = re.search(r'href="([^"#]+)', content)
            if pdf_match:
                url = pdf_match.group(1)
                if not url.startswith("http"):
                    url = urljoin(BASE_URL, url)
                result["fulltext_pdf_url"] = url

    return result


def get_case_text(case_data: dict, case_number: str, index: dict) -> str:
    """Get the CLOUT abstract text for a case.

    Priority 1: the per-case abstract split out of the A/CN.9/SER.C/ABSTRACTS
    compilation index (stable, covers ~all cases). Priority 2: an original-fulltext
    PDF hosted on uncitral.org, when the case links one.
    """
    if case_number and index.get(case_number):
        return index[case_number]

    # Fallback: original fulltext PDF still hosted on uncitral.org (a minority of cases).
    if case_data.get("fulltext_pdf_url"):
        print(f"    Index miss — fetching original fulltext PDF...")
        pdf_bytes = fetch_pdf(case_data["fulltext_pdf_url"])
        if pdf_bytes:
            text = extract_text_from_pdf(pdf_bytes)
            if text and len(text) > 50:
                return text
        time.sleep(1)

    return ""


def extract_case_number(title: str) -> str:
    """Extract the CLOUT case number from a title like 'CLOUT case 1935'."""
    m = re.search(r"case\s+(\d+)", title, re.IGNORECASE)
    return m.group(1) if m else ""


def normalize(case_data: dict, full_text: str, case_url: str) -> dict:
    """Normalize case data into standard schema."""
    title = case_data.get("title", "")
    case_number = extract_case_number(title)

    return {
        "_id": f"CLOUT-{case_number}" if case_number else f"CLOUT-{hash(case_url) % 10**8}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": case_data.get("date"),
        "url": case_url,
        "case_number": case_number,
        "country": case_data.get("country"),
        "country_code": case_data.get("country_code"),
        "court": case_data.get("court"),
        "court_reference": case_data.get("court_reference"),
        "parties": case_data.get("parties"),
        "legislative_text": case_data.get("legislative_text"),
        "clout_issue": case_data.get("clout_issue"),
        "articles": case_data.get("articles"),
        "keywords": case_data.get("keywords"),
        "comments": case_data.get("comments"),
        "document_type": "case_abstract",
    }


def fetch_all_case_urls(limit: int = 0) -> list[str]:
    """Fetch all case detail URLs from paginated search results."""
    all_urls = []
    offset = 0

    while True:
        print(f"  Fetching search page offset={offset}...")
        html = fetch_html(f"{SEARCH_URL}?inline=true&start={offset}")
        if not html:
            break

        urls = parse_search_results(html)
        if not urls:
            break

        all_urls.extend(urls)
        if limit > 0 and len(all_urls) >= limit:
            all_urls = all_urls[:limit]
            break

        # Check for "more results"
        if "moreResults" not in html:
            break

        offset += PAGE_SIZE
        time.sleep(RATE_LIMIT_DELAY)

    print(f"  Found {len(all_urls)} case URLs")
    return all_urls


def fetch_cases(limit: int = 0) -> Generator[dict, None, None]:
    """Fetch cases with full abstract text."""
    case_urls = fetch_all_case_urls(limit=limit)
    fetched = 0

    # Fetch all case metadata first so we know which case numbers to cover, then build
    # the abstract index (scoped/early-stop for small runs, full+cached for bootstrap).
    parsed = []
    for case_url in case_urls:
        print(f"  [{len(parsed)+1}/{len(case_urls)}] Fetching {case_url.split('/')[-1]}...")
        html = fetch_html(case_url)
        if not html:
            time.sleep(RATE_LIMIT_DELAY)
            continue
        case_data = parse_case_detail(html)
        parsed.append((case_url, case_data, extract_case_number(case_data.get("title", ""))))
        time.sleep(RATE_LIMIT_DELAY)

    needed = {num for (_, _, num) in parsed if num}
    index = build_abstract_index(needed=needed if limit else None)

    for case_url, case_data, case_number in parsed:
        full_text = get_case_text(case_data, case_number, index)
        if not full_text:
            print(f"    WARNING: No text extracted for {case_data.get('title', 'unknown')}")
        yield normalize(case_data, full_text, case_url)
        fetched += 1


def bootstrap_sample():
    """Fetch sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    total = 0

    for record in fetch_cases(limit=12):
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        total += 1
        text_len = len(record.get("text", ""))
        print(f"    Saved {fname.name} ({text_len} chars text)")

    print(f"\nSample complete: {total} records saved to {SAMPLE_DIR}")
    validate_sample()


def bootstrap_full():
    """Fetch all records, streaming to data/records.jsonl for pipeline ingest."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / "records.jsonl"
    total = 0
    with_text = 0

    with open(out_path, "w", encoding="utf-8") as out:
        for record in fetch_cases():
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            out.flush()
            total += 1
            if record.get("text"):
                with_text += 1

    print(f"\nFull bootstrap complete: {total} records ({with_text} with text) -> {out_path}")


def validate_sample():
    """Validate sample records."""
    files = list(SAMPLE_DIR.glob("*.json"))
    if not files:
        print("FAIL: No sample files found")
        return False

    print(f"\nValidating {len(files)} sample records...")
    issues = []
    text_present = 0
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            rec = json.load(fh)
        name = f.name
        if not rec.get("text"):
            issues.append(f"{name}: missing or empty 'text' field")
        elif len(rec["text"]) < 100:
            issues.append(f"{name}: text too short ({len(rec['text'])} chars)")
        else:
            text_present += 1
        if not rec.get("title"):
            issues.append(f"{name}: missing title")
        if not rec.get("_id"):
            issues.append(f"{name}: missing _id")

    if issues:
        print("ISSUES FOUND:")
        for i in issues:
            print(f"  - {i}")

    print(f"\n{text_present}/{len(files)} records have full text")
    if text_present >= len(files) * 0.7:
        print("VALIDATION PASSED (>=70% have text)")
        return True
    else:
        print("VALIDATION FAILED (<70% have text)")
        return False


def test_connectivity():
    """Test connectivity to CLOUT database."""
    print("Testing UNCITRAL CLOUT connectivity...\n")

    # Test search page
    html = fetch_html(f"{SEARCH_URL}?inline=true&start=0")
    if html:
        urls = parse_search_results(html)
        print(f"  Search page: OK ({len(urls)} results on first page)")
        # Check total from moreResults
        more_match = re.search(r"(\d+)\s+more\s+cases", html)
        if more_match:
            print(f"  Total cases: ~{int(more_match.group(1)) + len(urls)}")
    else:
        print("  Search page: FAILED")
        return False

    # Test case detail page
    if urls:
        html = fetch_html(urls[0])
        if html:
            case_data = parse_case_detail(html)
            print(f"  Case detail: OK")
            print(f"    Title: {case_data.get('title', 'N/A')}")
            print(f"    Country: {case_data.get('country', 'N/A')}")
            print(f"    Court: {case_data.get('court', 'N/A')}")
            print(f"    Date: {case_data.get('date', 'N/A')}")
            print(f"    Has abstract PDF: {'abstract_pdf_url' in case_data}")
            print(f"    Has fulltext PDF: {'fulltext_pdf_url' in case_data}")
        else:
            print("  Case detail: FAILED")
            return False

    # Test PDF extraction via common/pdf_extract
    print(f"\n  PDF extraction: via common/pdf_extract.extract_pdf_markdown")
    print("\nConnectivity test complete.")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="INTL/UNCITRALClout data fetcher")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test"], help="Command to run"
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
        # "bootstrap-fast" is the command the fleet runner invokes; treat it as a full run.
        if args.sample and args.command == "bootstrap":
            bootstrap_sample()
        else:
            bootstrap_full()
