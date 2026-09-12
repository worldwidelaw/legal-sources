#!/usr/bin/env python3
"""
Belize Financial Services Commission — Laws & Amendments

Fetches financial services legislation from belizefsc.org.bz using the
WordPress REST API v2. PDFs are downloaded and text extracted via pdfplumber.

WP categories used (legislation-related):
  62  Acts (laws-amendment)
  65  Accounting Records Act
  70  Money Laundering & Terrorism Prevention
  83  Other Legislation Act
  93  International Money Lending SI
  98  International Foundation SI
  99  International Limited Liability Companies SI
  100 Mutual Administrative Assistance SI
  142 Economic Substance
  147 High Seas Fishing
  153 Companies
  155 Business Names
  156 Companies (statutory instrument)
  158 Limited Liability Partnerships
  161 International Business Companies
  162 Intellectual Property Assets
  169 Financial Services Commission
  170 Securities Industry (laws-amendment)
  291 Movable Assets
  298 International Merchant Marine Registry
  305 Insolvency and Bankruptcy
"""

import html
import io
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SOURCE_ID = "BZ/FSC-Legislation"
BASE_URL = "https://www.belizefsc.org.bz"
API_URL = f"{BASE_URL}/wp-json/wp/v2"

# WP category IDs that contain legislation.
# Statutory-instrument categories (see SI_CATEGORIES) are listed here too; the
# subject categories below cover the Acts and their amendments.
LEGISLATION_CATEGORIES = [
    62, 63, 65, 68, 69, 70, 71, 83, 86, 142, 143, 147, 153, 154, 155,
    158, 161, 162, 168, 169, 170, 291, 298, 305,
    # statutory instruments
    90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 156, 157,
]

# Categories whose every post is a law or statutory instrument, so membership
# alone qualifies a post even when its title carries no law keyword.
SI_CATEGORIES = {90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 156, 157}
CORE_CATEGORIES = SI_CATEGORIES | {62, 170, 298, 305}

# Title keywords that indicate actual legislation (not press releases, forms, etc.)
LAW_KEYWORDS = re.compile(
    r'\b(act|regulation|rules|order|statutory instrument|'
    r'si no|code|amendment|consolidated)\b', re.IGNORECASE
)

# Titles that match LAW_KEYWORDS (they name the Act they explain or the office
# they fill) but are not themselves law.
NOT_LAW_KEYWORDS = re.compile(
    r'(guidance note|briefing note|flow ?chart|checklist|annual report|'
    r'application form|appointment of|membership of|press release)', re.IGNORECASE
)


def curl_get(url: str, max_attempts: int = 3, timeout: int = 30) -> Optional[str]:
    """GET via curl with retries."""
    for attempt in range(max_attempts):
        try:
            result = subprocess.run(
                ['curl', '-s', '-L', '--max-time', str(timeout),
                 '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
                 '-H', 'Accept: application/json, text/html',
                 url],
                capture_output=True, text=True, timeout=timeout + 10
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout
        except subprocess.TimeoutExpired:
            pass
        delay = min(5 * (2 ** attempt), 30)
        logger.warning(f"GET attempt {attempt + 1} failed for {url}, waiting {delay}s")
        time.sleep(delay)
    return None


def curl_download(url: str, dest: str, max_attempts: int = 3) -> bool:
    """Download a file via curl."""
    for attempt in range(max_attempts):
        try:
            result = subprocess.run(
                ['curl', '-s', '-L', '--max-time', '60',
                 '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
                 '-o', dest, url],
                capture_output=True, text=True, timeout=70
            )
            if result.returncode == 0 and os.path.getsize(dest) > 100:
                return True
        except (subprocess.TimeoutExpired, OSError):
            pass
        delay = min(5 * (2 ** attempt), 30)
        logger.warning(f"Download attempt {attempt + 1} failed for {url}")
        time.sleep(delay)
    return False


def extract_pdf_text(pdf_path: str, source_id: str = "") -> str:
    """Extract text from PDF using pdfplumber, falling back to OCR.

    A sixth of the corpus — including every Stamp Duties amendment and the 1998
    Money Laundering Regulations — is a scan with no text layer, so pdfplumber
    returns "" for it. Those go through the shared extractor, whose last resort
    is tesseract OCR (force=True: this scraper must emit its whole corpus on
    every run, so the skip-if-already-in-Neon guard must not fire — #1520).
    """
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            parts = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    parts.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            text = "\n\n".join(parts)
    except Exception as e:
        logger.warning(f"PDF extraction failed for {pdf_path}: {e}")
        text = ""

    if len(text.strip()) >= 50:
        return text

    try:
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        ocr = extract_pdf_markdown(
            SOURCE_ID, source_id, pdf_bytes=pdf_bytes,
            table="legislation", force=True,
        )
        if ocr and len(ocr.strip()) >= 50:
            logger.info(f"Recovered {len(ocr)} chars via the shared extractor/OCR")
            return ocr
    except Exception as e:
        logger.warning(f"OCR fallback failed: {e}")
    return text


def fetch_category_posts(category_ids: List[int], per_page: int = 100,
                         modified_after: Optional[str] = None) -> List[Dict]:
    """Fetch all posts from given WP categories, newest first."""
    all_posts = []
    seen_ids = set()
    cats_str = ",".join(str(c) for c in category_ids)
    extra = f"&modified_after={modified_after}" if modified_after else ""

    page = 1
    while True:
        url = (f"{API_URL}/posts?categories={cats_str}&per_page={per_page}"
               f"&page={page}&orderby=modified&order=desc{extra}")
        logger.info(f"Fetching page {page}: {url}")
        raw = curl_get(url)
        if not raw:
            break
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            break
        if not isinstance(data, list) or len(data) == 0:
            break
        for post in data:
            pid = post.get("id")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_posts.append(post)
        if len(data) < per_page:
            break
        page += 1
        time.sleep(1.5)

    return all_posts


def absolutize(url: str) -> str:
    """Resolve a PDF href against the site root.

    Posts link the same file three ways — absolute, root-relative
    (/wp-content/...) and path-only (2021/09/...) — and the last two 404 when
    handed straight to curl.
    """
    url = html.unescape(url.strip())
    if url.startswith(("http://", "https://")):
        return url
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return BASE_URL + url
    return f"{BASE_URL}/wp-content/uploads/{url.lstrip('./')}"


def extract_pdf_urls(content_html: str) -> List[str]:
    """Extract PDF URLs from WP post content HTML."""
    urls = re.findall(r'(?:href|data)="([^"]*\.pdf[^"]*)"', content_html)
    return list(dict.fromkeys(absolutize(u) for u in urls))  # dedup, keep order


def is_legislation(title: str, categories: List[int]) -> bool:
    """Check if a post is actual legislation vs press release/form/notice."""
    if NOT_LAW_KEYWORDS.search(title):
        return False
    if LAW_KEYWORDS.search(title):
        return True
    # Posts in the core "Acts" category (62) or a statutory-instrument category
    if set(categories) & CORE_CATEGORIES:
        return True
    return False


def normalize(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a raw post+PDF record into standard schema."""
    title = html.unescape(raw.get("title", ""))
    text = raw.get("text", "")
    if not text or len(text) < 50:
        return None

    date_str = raw.get("date", "")
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    return {
        "_id": f"bz-fsc-{raw['post_id']}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str or None,
        "url": raw.get("url", ""),
        "pdf_url": raw.get("pdf_url", ""),
        "category": raw.get("category", ""),
        "wp_post_id": raw.get("post_id"),
    }


def clean_title(rendered: str) -> str:
    """Strip the markup some titles carry (<strong>, <em>) and decode entities."""
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", rendered))).strip()


def fetch_all(sample: bool = False,
              modified_after: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    """Fetch all legislation documents with full text from PDFs."""
    # Also fetch category mapping for labels
    cat_map = {}
    for page in (1, 2):
        raw_cats = curl_get(f"{API_URL}/categories?per_page=100&page={page}")
        if not raw_cats:
            break
        try:
            batch = json.loads(raw_cats)
        except json.JSONDecodeError:
            break
        if not isinstance(batch, list) or not batch:
            break
        for c in batch:
            cat_map[c["id"]] = c["slug"]
        if len(batch) < 100:
            break

    posts = fetch_category_posts(LEGISLATION_CATEGORIES, modified_after=modified_after)
    logger.info(f"Fetched {len(posts)} posts from legislation categories")

    # Filter to actual legislation
    legislation_posts = []
    for p in posts:
        title = clean_title(p.get("title", {}).get("rendered", ""))
        cat_ids = p.get("categories", [])
        content = p.get("content", {}).get("rendered", "")
        pdf_urls = extract_pdf_urls(content)
        if not pdf_urls:
            continue
        if not is_legislation(title, cat_ids):
            logger.debug(f"Skipping non-legislation: {title}")
            continue
        legislation_posts.append((p, title, cat_ids, pdf_urls))

    # The FSC files one document under several subject categories, publishing a
    # separate post per category that links the very same PDF (the Securities
    # Industry Act, 2021 is posts 15034 and 16141). The second post re-uploads
    # the file rather than linking the first copy, so the URLs differ by upload
    # month and only the basename matches — every basename collision in the
    # corpus was verified byte-identical. Keep the lowest post id, the original
    # posting, so the surviving _id does not depend on API ordering.
    by_pdf: Dict[str, Any] = {}
    for entry in legislation_posts:
        key = entry[3][0].rsplit("/", 1)[-1].lower()
        if key not in by_pdf or entry[0]["id"] < by_pdf[key][0]["id"]:
            by_pdf[key] = entry
    if len(by_pdf) < len(legislation_posts):
        logger.info(f"Dropped {len(legislation_posts) - len(by_pdf)} posts re-linking "
                    f"a PDF already covered by an earlier post")
    legislation_posts = [e for e in legislation_posts
                         if by_pdf.get(e[3][0].rsplit("/", 1)[-1].lower()) is e]

    logger.info(f"Found {len(legislation_posts)} legislation posts with PDFs")

    if sample:
        legislation_posts = legislation_posts[:15]

    count = 0
    for p, title, cat_ids, pdf_urls in legislation_posts:
        post_id = p["id"]
        date = p.get("date", "")
        link = p.get("link", "")
        cat_slug = cat_map.get(cat_ids[0], "") if cat_ids else ""

        # Try each PDF URL until we get text
        text = ""
        used_pdf = ""
        for pdf_url in pdf_urls:
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp_path = tmp.name
            try:
                if curl_download(pdf_url, tmp_path):
                    text = extract_pdf_text(tmp_path, f"bz-fsc-{post_id}")
                    if text and len(text) >= 50:
                        used_pdf = pdf_url
                        break
            finally:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            time.sleep(1.0)

        if not text or len(text) < 50:
            logger.warning(f"No text extracted for post {post_id}: {title}")
            continue

        raw = {
            "post_id": post_id,
            "title": title,
            "date": date,
            "url": link,
            "pdf_url": used_pdf,
            "text": text,
            "category": cat_slug,
        }

        record = normalize(raw)
        if record:
            count += 1
            logger.info(f"[{count}] {title[:60]} ({len(text)} chars)")
            yield record
            time.sleep(1.5)

    logger.info(f"Total records: {count}")


def fetch_updates(since: Any) -> Iterator[Dict[str, Any]]:
    """Yield legislation whose WP post was published or edited after `since`.

    `modified_after` is the right comparator here: it tracks when the FSC put
    the document on the site, not the year the Act was passed (revised editions
    of 1990s Acts are posted today and would be missed by a title-year filter).
    """
    if isinstance(since, datetime):
        stamp = since.replace(tzinfo=None, microsecond=0).isoformat()
    else:
        stamp = str(since).strip().replace(" ", "T").replace("Z", "")
        if len(stamp) == 10:  # bare YYYY-MM-DD
            stamp += "T00:00:00"
    logger.info(f"Fetching posts modified after {stamp}")
    yield from fetch_all(modified_after=stamp)


def save_samples(records: List[Dict], sample_dir: Path):
    """Save sample records to JSON files."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    for r in records:
        fname = re.sub(r'[^\w\-]', '_', r["_id"])[:80] + ".json"
        path = sample_dir / fname
        with open(path, "w", encoding="utf-8") as f:
            json.dump(r, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Saved {len(records)} samples to {sample_dir}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="BZ/FSC-Legislation bootstrap")
    sub = parser.add_subparsers(dest="command")

    for name in ("bootstrap", "bootstrap-fast"):
        p = sub.add_parser(name, help="Fetch legislation")
        p.add_argument("--sample", action="store_true", help="Sample mode (15 docs)")
        p.add_argument("--full", action="store_true", help="Full fetch")

    upd = sub.add_parser("update", help="Fetch posts modified since a date")
    upd.add_argument("--since", required=True, help="ISO date, e.g. 2026-01-01")

    args = parser.parse_args()

    if args.command not in ("bootstrap", "bootstrap-fast", "update"):
        parser.print_help()
        return

    # bootstrap-fast is the fleet's entry point and must run the FULL crawl;
    # only --sample caps the run (#1532).
    sample_mode = getattr(args, "sample", False)
    source_dir = Path(__file__).parent
    sample_dir = source_dir / "sample"
    data_dir = source_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = data_dir / "records.jsonl"

    if args.command == "update":
        stream = fetch_updates(args.since)
    else:
        stream = fetch_all(sample=sample_mode)

    samples: List[Dict[str, Any]] = []
    count = 0
    with open(jsonl_path, "w", encoding="utf-8") as jsonl_f:
        for record in stream:
            count += 1
            jsonl_f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            if len(samples) < 15:
                samples.append(record)

    if not count:
        print("ERROR: No records fetched")
        sys.exit(1)

    if args.command != "update":
        save_samples(samples, sample_dir)
    print(f"SUCCESS: {count} records with full text -> {jsonl_path}")


if __name__ == "__main__":
    main()
