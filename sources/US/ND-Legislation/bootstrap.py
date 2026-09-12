#!/usr/bin/env python3
"""
US/ND-Legislation -- North Dakota Century Code

Fetches ND statutes from ndlegis.gov chapter PDFs. No Cloudflare blocking.

Strategy:
  - Fetch title index to get all title page URLs
  - For each title page, find chapter PDF links (relative paths like t01c01.pdf)
  - Download each chapter PDF and extract text with pdfplumber
  - Parse sections using regex pattern "N-NN-NN. Title."

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample sections
  python bootstrap.py bootstrap             # Full extraction -> data/records.jsonl
  python bootstrap.py bootstrap-fast        # Alias the fleet wrapper calls
  python bootstrap.py test-api              # Test connectivity
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/ND-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"
CHECKPOINT_PATH = DATA_DIR / "chapter_checkpoint.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.ND-Legislation")

BASE_URL = "https://ndlegis.gov"
CENCODE_URL = f"{BASE_URL}/cencode"
INDEX_URL = f"{BASE_URL}/general-information/north-dakota-century-code/classic.html"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 1.5  # seconds between requests

# Section pattern: "1-01-01. Title text." or "1-01-01.1. Title text."
SECTION_RE = re.compile(
    r'^(\d+(?:\.\d+)?-\d+(?:\.\d+)?-\d+(?:\.\d+)?(?:\.\d+)?)\.\s+(.+?)$',
    re.MULTILINE
)


def fetch_title_links() -> list:
    """Get all title page links from the index."""
    try:
        resp = SESSION.get(INDEX_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch title index: {e}")
        return []

    links = re.findall(r'href="(/cencode/t[^"]+\.html)"', resp.text)
    links = list(dict.fromkeys(links))  # dedupe preserving order
    logger.info(f"Found {len(links)} title pages")
    return links


def fetch_chapter_pdfs(title_path: str) -> list:
    """Get chapter PDF links from a title page."""
    url = f"{BASE_URL}{title_path}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to fetch title page {title_path}: {e}")
        return []

    # Chapter PDFs are relative links like "t01c01.pdf"
    pdfs = re.findall(r'href="(t[^"]+\.pdf)"', resp.text)
    pdfs = list(dict.fromkeys(pdfs))
    return pdfs


def download_pdf(pdf_name: str) -> Optional[bytes]:
    """Download a chapter PDF."""
    url = f"{CENCODE_URL}/{pdf_name}"
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=60)
            resp.raise_for_status()
            if resp.headers.get("content-type", "").startswith("application/pdf"):
                return resp.content
            # Some might redirect to HTML
            if len(resp.content) < 500:
                logger.warning(f"PDF {pdf_name} too small ({len(resp.content)} bytes), skipping")
                return None
            return resp.content
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
            else:
                logger.warning(f"Failed to download {pdf_name}: {e}")
                return None
        except Exception as e:
            logger.warning(f"Download error for {pdf_name}: {e}")
            return None
    return None


def extract_sections_from_pdf(pdf_bytes: bytes, pdf_name: str) -> list:
    """Extract individual statute sections from a chapter PDF."""
    try:
        full_text = extract_pdf_markdown(
            SOURCE_ID, pdf_name, pdf_bytes=pdf_bytes,
            table="legislation", force=True,
        )
        if not full_text:
            full_text = ""
    except Exception as e:
        logger.warning(f"Failed to parse PDF {pdf_name}: {e}")
        return []

    if not full_text.strip():
        return []

    # Find all section starts
    matches = list(SECTION_RE.finditer(full_text))
    if not matches:
        return []

    sections = []
    for i, match in enumerate(matches):
        section_num = match.group(1)
        section_title = match.group(2).strip()

        # Extract text until the next section or end
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
        body = full_text[start:end].strip()

        # Clean up the body text
        # Remove page headers (title/chapter repeated)
        body = re.sub(r'\n(?:TITLE|CHAPTER)\s+\d+[A-Z\-.\s]*\n', '\n', body)
        body = re.sub(r'\nPage No\.\s*\d+\n?', '\n', body)
        body = re.sub(r'\n{3,}', '\n\n', body)
        body = body.strip()

        if len(body) < 10:
            continue

        # Parse title/chapter from section number
        parts = section_num.split("-")
        title_num = parts[0] if parts else ""
        chapter_num = f"{parts[0]}-{parts[1]}" if len(parts) >= 2 else ""

        sections.append({
            "section_number": section_num,
            "title": f"§ {section_num}. {section_title}",
            "text": body,
            "title_number": title_num,
            "chapter_number": chapter_num,
        })

    return sections


def normalize(section: dict) -> dict:
    """Normalize a parsed section into standard schema."""
    section_num = section["section_number"]
    parts = section_num.split("-")
    title_num = parts[0] if parts else ""

    return {
        "_id": f"US/ND-Legislation/{section_num}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": section["title"],
        "text": section["text"],
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "url": f"https://ndlegis.gov/cencode/t{title_num.zfill(2)}c{parts[1].zfill(2)}.pdf" if len(parts) >= 2 else f"https://ndlegis.gov/cencode/t{title_num.zfill(2)}.html",
        "section_number": section_num,
        "title_number": section.get("title_number", ""),
        "chapter_number": section.get("chapter_number", ""),
        "jurisdiction": "US-ND",
        "language": "en",
    }


def load_checkpoint() -> set:
    """Chapter PDFs already extracted by an earlier run."""
    if not CHECKPOINT_PATH.exists():
        return set()
    done = {ln.strip() for ln in CHECKPOINT_PATH.read_text().splitlines() if ln.strip()}
    logger.info(f"Checkpoint: {len(done)} chapters already done, skipping")
    return done


def mark_done(pdf_name: str) -> None:
    """Record a finished chapter so a restart does not re-download it."""
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_PATH, "a", encoding="utf-8") as f:
        f.write(f"{pdf_name}\n")


def fetch_all(resume: bool = True) -> Generator[dict, None, None]:
    """Yield all statute sections with full text."""
    title_links = fetch_title_links()
    if not title_links:
        raise RuntimeError(
            f"No title pages found at {INDEX_URL} — ndlegis.gov is unreachable or "
            "the index layout changed; refusing to report an empty corpus"
        )

    total = 0
    all_pdfs = []

    # Collect all chapter PDF names
    for title_path in title_links:
        pdfs = fetch_chapter_pdfs(title_path)
        all_pdfs.extend(pdfs)
        time.sleep(CRAWL_DELAY)

    all_pdfs = list(dict.fromkeys(all_pdfs))  # dedupe
    logger.info(f"Found {len(all_pdfs)} chapter PDFs across {len(title_links)} titles")
    if not all_pdfs:
        raise RuntimeError(
            "Title pages carried no chapter PDF links — ndlegis.gov changed its "
            "layout or is serving a stripped page to this vantage"
        )

    done = load_checkpoint() if resume else set()

    for i, pdf_name in enumerate(all_pdfs):
        if pdf_name in done:
            continue
        logger.info(f"Processing {i+1}/{len(all_pdfs)}: {pdf_name}")

        pdf_bytes = download_pdf(pdf_name)
        if not pdf_bytes:
            continue

        sections = extract_sections_from_pdf(pdf_bytes, pdf_name)
        for section in sections:
            record = normalize(section)
            if len(record["text"]) >= 20:
                total += 1
                yield record

        # Only after the chapter's sections are out the door, so a crash
        # re-fetches this chapter rather than losing it.
        mark_done(pdf_name)
        time.sleep(CRAWL_DELAY)

    logger.info(f"Total sections with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records spread across chapters from several titles.

    Title 1 chapter 1 alone yields 54 sections, so drawing the whole sample
    from it gave 15 one-line definitions that said nothing about how the
    substantive titles extract. Take a few from each chapter instead.
    """
    records = []

    sample_pdfs = [
        "t01c01.pdf",   # general provisions / definitions
        "t12c01.pdf",   # criminal code
        "t18c01.pdf",   # fire protection
        "t14c03.pdf",   # domestic relations
        "t47c01.pdf",   # property
    ]
    per_pdf = max(1, count // len(sample_pdfs) + 1)

    for pdf_name in sample_pdfs:
        if len(records) >= count:
            break

        logger.info(f"Downloading sample: {pdf_name}")
        pdf_bytes = download_pdf(pdf_name)
        if not pdf_bytes:
            continue

        taken = 0
        sections = extract_sections_from_pdf(pdf_bytes, pdf_name)
        # Longest first so the sample shows real statutory text, not the
        # "Repealed by omission from this code." stubs.
        sections.sort(key=lambda s: len(s["text"]), reverse=True)
        for section in sections:
            if taken >= per_pdf or len(records) >= count:
                break
            record = normalize(section)
            if len(record["text"]) >= 20:
                records.append(record)
                taken += 1

        time.sleep(CRAWL_DELAY)

    return records


def test_api():
    """Test connectivity to ndlegis.gov."""
    logger.info("Testing ndlegis.gov connectivity...")

    # Test title index
    title_links = fetch_title_links()
    if not title_links:
        logger.error("Title index fetch failed")
        return False
    logger.info(f"Title index OK - {len(title_links)} titles")

    time.sleep(CRAWL_DELAY)

    # Test chapter PDF download and parsing
    test_pdf = "t01c01.pdf"
    logger.info(f"Downloading test PDF: {test_pdf}")
    pdf_bytes = download_pdf(test_pdf)
    if not pdf_bytes:
        logger.error("Test PDF download failed")
        return False

    sections = extract_sections_from_pdf(pdf_bytes, test_pdf)
    logger.info(f"Parsed {len(sections)} sections from {test_pdf}")

    if sections:
        sample = normalize(sections[0])
        logger.info(f"Sample: {sample['title'][:80]}")
        logger.info(f"Text preview ({len(sample['text'])} chars): {sample['text'][:200]}...")
        return True

    logger.error("No sections parsed from test PDF")
    return False


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
        filename = f"sample_{i:02d}_{safe_id}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info("Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0}")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0}")

    return len(records) >= 10 and avg_text > 50


def bootstrap_full(resume: bool = True) -> bool:
    """Stream the whole Century Code to data/records.jsonl."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not resume:
        RECORDS_PATH.unlink(missing_ok=True)
        CHECKPOINT_PATH.unlink(missing_ok=True)

    count = 0
    # Append: on resume the checkpoint suppresses the chapters already written,
    # so truncating here would throw away every earlier run's records.
    with open(RECORDS_PATH, "a", encoding="utf-8") as f:
        for record in fetch_all(resume=resume):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
            if count % 500 == 0:
                f.flush()
                logger.info(f"  ... {count} sections written")

    logger.info(f"Wrote {count} sections to {RECORDS_PATH}")
    # A resumed run that finds every chapter already done writes nothing new but
    # is still a success — judge on the file, not on this run's delta.
    return RECORDS_PATH.stat().st_size > 0


def main():
    parser = argparse.ArgumentParser(description="US/ND-Legislation Data Fetcher")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"]
    )
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Ignore the chapter checkpoint and re-crawl from scratch",
    )

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    # `bootstrap-fast` is what the fleet wrapper invokes; without it argparse
    # exited 2 and the wrapper fell back to re-ingesting sample/ (#1459).
    if args.command in ("bootstrap", "bootstrap-fast"):
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        logger.info("Full bootstrap mode")
        success = bootstrap_full(resume=not args.restart)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
