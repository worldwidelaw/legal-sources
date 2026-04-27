#!/usr/bin/env python3
"""
GG/CICRA - Channel Islands Competition Authorities (GCRA + JCRA)

CICRA was disbanded in 2020. This fetcher combines its two successors:
- GCRA (Guernsey Competition and Regulatory Authority) at gcra.gg
- JCRA (Jersey Competition Regulatory Authority) at jcra.je

Fetches competition cases and decisions with full text from PDFs.

Data sources:
  GCRA: https://www.gcra.gg/cases (Drupal, ~320 cases)
  JCRA: https://www.jcra.je/cases-documents/cases/ (Umbraco, ~545 cases)
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GG.CICRA")

SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "GG/CICRA"

# Use a browser-like User-Agent to avoid being blocked on VPS/datacenter IPs
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds


def _get_with_retry(url: str, timeout: int = 30, **kwargs) -> requests.Response:
    """GET request with retry logic and exponential backoff."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                logger.warning(
                    f"  Request failed (attempt {attempt}/{MAX_RETRIES}): {e}. "
                    f"Retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                logger.error(f"  Request failed after {MAX_RETRIES} attempts: {e}")
    raise last_exc


# ─── GCRA (Guernsey) ────────────────────────────────────────────────

GCRA_BASE = "https://www.gcra.gg"


def gcra_fetch_case_list() -> list[dict]:
    """Fetch all GCRA cases via paginated Drupal views."""
    cases = []
    page = 0
    while True:
        url = f"{GCRA_BASE}/cases?page={page}&items_per_page=50"
        logger.info(f"GCRA: Fetching case list page {page}...")
        try:
            resp = _get_with_retry(url)
        except requests.RequestException as e:
            logger.error(f"GCRA: Failed to fetch page {page}: {e}")
            break
        soup = BeautifulSoup(resp.text, "html.parser")

        rows = soup.select("div.views-row")
        if not rows:
            # Fallback: try finding case links directly
            links = soup.select("h5 a[href^='/case/']")
            if not links:
                logger.warning(
                    f"GCRA: No div.views-row or h5 links on page {page}. "
                    f"Page may have changed structure or returned a block page."
                )
                break
            # Build cases from direct links
            for link in links:
                href = link.get("href", "")
                title = link.get_text(strip=True)
                cases.append({
                    "title": title,
                    "href": href,
                    "date": "",
                    "status": "",
                    "authority": "GCRA",
                })
        else:
            for row in rows:
                title_el = (
                    row.select_one("h5 a")
                    or row.select_one(".views-row-title a")
                    or row.select_one("a[href^='/case/']")
                )
                if not title_el:
                    continue
                href = title_el.get("href", "")
                title = title_el.get_text(strip=True)

                # Extract date from time element
                date_el = row.select_one("time.datetime")
                date_str = date_el.get("datetime", "")[:10] if date_el else ""

                # Extract status
                status_el = row.select_one(
                    ".views-field-field-case-status .field-content"
                )
                status = status_el.get_text(strip=True) if status_el else ""

                cases.append({
                    "title": title,
                    "href": href,
                    "date": date_str,
                    "status": status,
                    "authority": "GCRA",
                })

        # Check for next page
        next_link = soup.select_one("li.pager__item--next a")
        if not next_link:
            break
        page += 1
        time.sleep(2)

    logger.info(f"GCRA: Found {len(cases)} cases")
    return cases


def gcra_extract_page_text(soup: BeautifulSoup) -> str:
    """Extract structured text from a GCRA case detail page as fallback."""
    parts = []

    # Case metadata from .node__content
    meta_el = soup.select_one(".node__content")
    if meta_el:
        for field in meta_el.select("div.field"):
            label_el = field.select_one(".field__label")
            value_el = field.select_one(".field__item")
            if label_el and value_el:
                label = label_el.get_text(strip=True)
                value = value_el.get_text(strip=True)
                if label and value:
                    parts.append(f"**{label}**: {value}")

    # Case documents listing
    doc_section = soup.select_one("article") or soup.select_one(".case-documents")
    if doc_section:
        for item in doc_section.select("a"):
            text = item.get_text(strip=True)
            if text and text not in ("View", "Download") and len(text) > 3:
                parts.append(f"- {text}")

    # Main content text
    main = soup.select_one("main") or soup.select_one("#main-content")
    if main:
        main_text = main.get_text(separator="\n", strip=True)
        # Remove navigation/breadcrumb noise
        lines = [
            ln.strip()
            for ln in main_text.split("\n")
            if ln.strip()
            and ln.strip() not in ("Home", "Breadcrumb", "View", "Download")
            and not ln.strip().startswith("©")
        ]
        if lines:
            parts.append("\n".join(lines))

    return "\n\n".join(parts)


def gcra_fetch_case_detail(href: str) -> dict:
    """Fetch GCRA case detail page and extract PDF links + text."""
    url = urljoin(GCRA_BASE, href)
    try:
        resp = _get_with_retry(url)
    except requests.RequestException as e:
        logger.warning(f"GCRA: Failed to fetch case detail {href}: {e}")
        return {"case_number": "", "pdf_links": [], "description": "", "page_url": url}

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract case number
    case_number = ""
    num_el = soup.select_one("div.field--name-field-case-number .field__item")
    if num_el:
        case_number = num_el.get_text(strip=True)
    else:
        # Fallback: try the old pattern
        num_el = soup.select_one("div.field--name-field-case-number")
        if num_el:
            case_number = (
                num_el.get_text(strip=True)
                .replace("Case number", "")
                .replace("Case Number", "")
                .strip()
            )

    # Extract PDF document links (deduplicated by URL)
    seen_urls = set()
    pdf_links = []
    for link in soup.select("a[href$='.pdf']"):
        pdf_url = urljoin(GCRA_BASE, link.get("href", ""))
        if pdf_url in seen_urls:
            continue
        seen_urls.add(pdf_url)
        pdf_name = link.get_text(strip=True) or unquote(Path(pdf_url).stem)
        # Skip generic link text
        if pdf_name in ("View", "Download", "PDF"):
            pdf_name = unquote(Path(pdf_url).stem)
        pdf_links.append({"url": pdf_url, "name": pdf_name})

    # Extract description text from body field
    desc = ""
    desc_el = soup.select_one("div.field--name-body")
    if desc_el:
        desc = desc_el.get_text(separator="\n", strip=True)

    # Extract page text as additional fallback content
    page_text = gcra_extract_page_text(soup)

    return {
        "case_number": case_number,
        "pdf_links": pdf_links,
        "description": desc,
        "page_text": page_text,
        "page_url": url,
    }


# ─── JCRA (Jersey) ──────────────────────────────────────────────────

JCRA_BASE = "https://www.jcra.je"


def jcra_fetch_case_list() -> list[dict]:
    """Fetch all JCRA cases via Umbraco Surface API with HTML page fallback."""
    cases = _jcra_fetch_via_surface_api()
    if cases:
        return cases

    # Fallback: scrape the main cases HTML page
    logger.warning("JCRA: Surface API returned 0 cases, trying HTML fallback...")
    return _jcra_fetch_via_html()


def _jcra_fetch_via_surface_api() -> list[dict]:
    """Fetch JCRA cases via Umbraco Surface API."""
    url = f"{JCRA_BASE}/umbraco/surface/SearchSurface/CaseSearch?page=0&pageSize=1000"
    logger.info("JCRA: Fetching all cases via Surface API...")
    try:
        resp = _get_with_retry(url, timeout=60)
    except requests.RequestException as e:
        logger.error(f"JCRA: Surface API failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    cases = []

    for card in soup.select("a.result, a.search-card--component"):
        href = card.get("href", "")
        if not href:
            continue

        # Extract cells
        cells = card.select(".cell p, .cell .body-small")
        title = ""
        date_str = ""
        status = ""
        case_num = ""

        if len(cells) >= 4:
            case_num = cells[0].get_text(strip=True)
            date_str = cells[1].get_text(strip=True)
            status = cells[2].get_text(strip=True)
            title = cells[3].get_text(strip=True)
        elif len(cells) >= 1:
            title = cells[-1].get_text(strip=True)

        # Parse date
        date_iso = _parse_date(date_str)

        cases.append({
            "title": title or f"Case {case_num}",
            "href": href,
            "date": date_iso,
            "status": status,
            "case_number": case_num,
            "authority": "JCRA",
        })

    logger.info(f"JCRA: Found {len(cases)} cases via Surface API")
    return cases


def _jcra_fetch_via_html() -> list[dict]:
    """Fallback: scrape JCRA cases from the main HTML page."""
    url = f"{JCRA_BASE}/cases-documents/cases/"
    logger.info("JCRA: Fetching cases via HTML page fallback...")
    try:
        resp = _get_with_retry(url, timeout=30)
    except requests.RequestException as e:
        logger.error(f"JCRA: HTML page fallback failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    cases = []

    # Try finding case links with regex on raw HTML
    for match in re.finditer(
        r'href="(/cases-documents/cases/[^"]+)"[^>]*title="([^"]*)"',
        resp.text,
    ):
        href, title = match.group(1), match.group(2)
        if href and href not in [c["href"] for c in cases]:
            cases.append({
                "title": title.strip() or href.split("/")[-2],
                "href": href,
                "date": "",
                "status": "",
                "case_number": "",
                "authority": "JCRA",
            })

    # Also try BeautifulSoup selectors
    if not cases:
        for link in soup.select("a[href*='/cases-documents/cases/']"):
            href = link.get("href", "")
            if (
                href
                and "/cases/" in href
                and href != "/cases-documents/cases/"
                and href not in [c["href"] for c in cases]
            ):
                cases.append({
                    "title": link.get("title", "") or link.get_text(strip=True),
                    "href": href,
                    "date": "",
                    "status": "",
                    "case_number": "",
                    "authority": "JCRA",
                })

    logger.info(f"JCRA: Found {len(cases)} cases via HTML fallback")
    return cases


def _parse_date(date_str: str) -> str:
    """Parse various date formats to ISO 8601."""
    if not date_str:
        return ""
    for fmt in ["%d/%m/%Y", "%d %b %Y", "%d %B %Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def jcra_extract_page_text(soup: BeautifulSoup) -> str:
    """Extract structured text from a JCRA case detail page as fallback."""
    parts = []

    # Case description
    desc_el = soup.select_one("div.case-description")
    if desc_el:
        parts.append(desc_el.get_text(separator="\n", strip=True))

    # Case metadata from content-table divs
    for table_div in soup.select("div.content-table"):
        text = table_div.get_text(separator=": ", strip=True)
        if text and len(text) > 3:
            parts.append(text)

    # Publication/document descriptions
    for intro in soup.select("div.text-intro-content"):
        text = intro.get_text(separator="\n", strip=True)
        if text:
            parts.append(text)

    # Left-side content block (main case narrative)
    left = soup.select_one("div.case-left-side-content-block")
    if left:
        # Extract text excluding breadcrumb
        for el in left.select("nav, .breadcrumb"):
            el.decompose()
        text = left.get_text(separator="\n", strip=True)
        if text and len(text) > 20:
            parts.append(text)

    return "\n\n".join(parts)


def jcra_fetch_case_detail(href: str) -> dict:
    """Fetch JCRA case detail page and extract PDF links + text."""
    url = urljoin(JCRA_BASE, href)
    try:
        resp = _get_with_retry(url)
    except requests.RequestException as e:
        logger.warning(f"JCRA: Failed to fetch case detail {href}: {e}")
        return {"pdf_links": [], "description": "", "page_text": "", "page_url": url}

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract PDF links (deduplicated by URL)
    seen_urls = set()
    pdf_links = []
    for link in soup.select("a[href$='.pdf']"):
        pdf_url = urljoin(JCRA_BASE, link.get("href", ""))
        if pdf_url in seen_urls:
            continue
        seen_urls.add(pdf_url)
        pdf_name = link.get_text(strip=True) or unquote(Path(pdf_url).stem)
        # Skip generic link text
        if pdf_name in ("View PDF", "Download PDF", "View", "Download", "PDF"):
            pdf_name = unquote(Path(pdf_url).stem)
        # Clean download size suffix like "Download PDF (46kb)"
        pdf_name = re.sub(r"\s*\(\d+\s*[kKmM][bB]\)\s*$", "", pdf_name)
        if pdf_name in ("Download PDF", "View PDF"):
            pdf_name = unquote(Path(pdf_url).stem)
        pdf_links.append({"url": pdf_url, "name": pdf_name})

    # Extract description
    desc = ""
    desc_el = soup.select_one(".case-description") or soup.select_one(
        "div.field--name-body"
    )
    if desc_el:
        desc = desc_el.get_text(separator="\n", strip=True)

    # Extract page text as additional fallback content
    page_text = jcra_extract_page_text(soup)

    return {
        "pdf_links": pdf_links,
        "description": desc,
        "page_text": page_text,
        "page_url": url,
    }


# ─── Common ─────────────────────────────────────────────────────────


def extract_text_from_pdfs(
    pdf_links: list[dict], doc_id: str, max_pdfs: int = 3
) -> str:
    """Download and extract text from PDF links."""
    texts = []
    for i, pdf_info in enumerate(pdf_links[:max_pdfs]):
        pdf_url = pdf_info["url"]
        # Use a unique sub-id when extracting multiple PDFs for one case
        sub_id = f"{doc_id}__pdf{i}" if len(pdf_links) > 1 else doc_id
        try:
            text = extract_pdf_markdown(
                SOURCE_ID,
                sub_id,
                pdf_url=pdf_url,
                table="doctrine",
                force=True,
            )
            if text and len(text.strip()) > 50:
                header = f"## {pdf_info['name']}\n\n" if pdf_info.get("name") else ""
                texts.append(header + text)
        except Exception as e:
            logger.warning(f"  PDF extraction failed for {pdf_url}: {e}")
        time.sleep(1)

    return "\n\n---\n\n".join(texts)


def _make_doc_id(entry: dict) -> str:
    """Build a stable document ID from an entry."""
    authority = entry.get("authority", "GCRA")
    slug = re.sub(r"[^\w-]", "_", entry["href"].strip("/"))[:100]
    return f"GG_{authority}_{slug}"


def _build_text(detail: dict, pdf_text: str) -> str:
    """Assemble final text from PDF text, description, and page text fallback.

    Priority:
      1. PDF extracted text (best quality)
      2. Page description + page text (fallback when PDFs unavailable/fail)
    Always includes description as context even when PDF text is available.
    """
    parts = []

    # Always prepend the description if available
    desc = detail.get("description", "").strip()
    if desc and len(desc) > 20:
        parts.append(desc)

    if pdf_text and len(pdf_text.strip()) > 50:
        parts.append(pdf_text)
    else:
        # Fallback: use the page text extraction
        page_text = detail.get("page_text", "").strip()
        if page_text and len(page_text) > 20:
            parts.append(page_text)

    return "\n\n---\n\n".join(parts)


def normalize(entry: dict, text: str, detail: dict) -> dict:
    """Transform into standard schema."""
    authority = entry.get("authority", "GCRA")
    doc_id = _make_doc_id(entry)

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": entry["title"],
        "text": text,
        "date": entry.get("date") or None,
        "url": detail.get("page_url", ""),
        "authority": authority,
        "case_number": entry.get("case_number") or detail.get("case_number", ""),
        "status": entry.get("status", ""),
        "description": detail.get("description", ""),
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all records with full text from both authorities."""
    # GCRA
    gcra_cases = gcra_fetch_case_list()
    for entry in gcra_cases:
        doc_id = _make_doc_id(entry)
        try:
            detail = gcra_fetch_case_detail(entry["href"])
        except Exception as e:
            logger.warning(f"GCRA: Error fetching {entry['href']}: {e}")
            continue
        time.sleep(2)

        pdf_text = ""
        if detail["pdf_links"]:
            pdf_text = extract_text_from_pdfs(detail["pdf_links"], doc_id)

        text = _build_text(detail, pdf_text)
        if text and len(text) > 30:
            yield normalize(entry, text, detail)
        else:
            logger.info(f"  Skipped (no text): {entry['title'][:60]}")
        time.sleep(1)

    # JCRA
    jcra_cases = jcra_fetch_case_list()
    for entry in jcra_cases:
        doc_id = _make_doc_id(entry)
        try:
            detail = jcra_fetch_case_detail(entry["href"])
        except Exception as e:
            logger.warning(f"JCRA: Error fetching {entry['href']}: {e}")
            continue
        time.sleep(2)

        pdf_text = ""
        if detail["pdf_links"]:
            pdf_text = extract_text_from_pdfs(detail["pdf_links"], doc_id)

        text = _build_text(detail, pdf_text)
        if text and len(text) > 30:
            yield normalize(entry, text, detail)
        else:
            logger.info(f"  Skipped (no text): {entry['title'][:60]}")
        time.sleep(1)


def bootstrap_sample(count: int = 15):
    """Fetch sample records for testing."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    saved = 0

    # Get ~8 from GCRA, ~7 from JCRA
    gcra_target = count // 2 + 1
    jcra_target = count - gcra_target

    # GCRA samples
    logger.info("Fetching GCRA case list...")
    gcra_cases = gcra_fetch_case_list()
    for entry in gcra_cases[: gcra_target * 2]:  # try more in case some have no text
        if saved >= gcra_target:
            break
        doc_id = _make_doc_id(entry)
        logger.info(f"  [{saved + 1}/{count}] GCRA: {entry['title'][:60]}...")
        try:
            detail = gcra_fetch_case_detail(entry["href"])
        except Exception as e:
            logger.warning(f"    Error: {e}")
            continue
        time.sleep(2)

        pdf_text = ""
        if detail["pdf_links"]:
            pdf_text = extract_text_from_pdfs(detail["pdf_links"], doc_id, max_pdfs=1)

        text = _build_text(detail, pdf_text)

        if not text or len(text) < 30:
            logger.info("    Skipped (no text)")
            continue

        record = normalize(entry, text, detail)
        slug = re.sub(r"[^\w-]", "_", entry["href"].strip("/"))[:60]
        out_file = SAMPLE_DIR / f"GCRA_{slug}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        saved += 1
        logger.info(f"    Saved ({len(text)} chars)")
        time.sleep(2)

    # JCRA samples
    logger.info("Fetching JCRA case list...")
    jcra_cases = jcra_fetch_case_list()
    jcra_saved = 0
    for entry in jcra_cases[: jcra_target * 2]:
        if jcra_saved >= jcra_target:
            break
        doc_id = _make_doc_id(entry)
        logger.info(f"  [{saved + 1}/{count}] JCRA: {entry['title'][:60]}...")
        try:
            detail = jcra_fetch_case_detail(entry["href"])
        except Exception as e:
            logger.warning(f"    Error: {e}")
            continue
        time.sleep(2)

        pdf_text = ""
        if detail["pdf_links"]:
            pdf_text = extract_text_from_pdfs(detail["pdf_links"], doc_id, max_pdfs=1)

        text = _build_text(detail, pdf_text)

        if not text or len(text) < 30:
            logger.info("    Skipped (no text)")
            continue

        record = normalize(entry, text, detail)
        slug = re.sub(r"[^\w-]", "_", entry["href"].strip("/"))[:60]
        out_file = SAMPLE_DIR / f"JCRA_{slug}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        saved += 1
        jcra_saved += 1
        logger.info(f"    Saved ({len(text)} chars)")
        time.sleep(2)

    logger.info(f"\nSample complete: {saved} records saved to {SAMPLE_DIR}")
    return saved


def main():
    parser = argparse.ArgumentParser(
        description="GG/CICRA competition decisions fetcher"
    )
    parser.add_argument(
        "command",
        choices=["bootstrap", "updates", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample", action="store_true", help="Fetch sample records only"
    )
    parser.add_argument(
        "--count", type=int, default=15, help="Number of sample records"
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            gcra = _get_with_retry(f"{GCRA_BASE}/cases", timeout=15)
            logger.info(f"GCRA: {gcra.status_code} ({len(gcra.text)} bytes)")
        except Exception as e:
            logger.error(f"GCRA: FAILED - {e}")
        try:
            jcra = _get_with_retry(
                f"{JCRA_BASE}/cases-documents/cases/", timeout=15
            )
            logger.info(f"JCRA: {jcra.status_code} ({len(jcra.text)} bytes)")
        except Exception as e:
            logger.error(f"JCRA: FAILED - {e}")
        return

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample(args.count)
        else:
            count = 0
            for record in fetch_all():
                count += 1
                if count % 50 == 0:
                    logger.info(f"  Progress: {count} records...")
            logger.info(f"Full bootstrap complete: {count} records")


if __name__ == "__main__":
    main()
