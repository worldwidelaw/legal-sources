#!/usr/bin/env python3
"""
MY/AGCLaws -- Malaysia Laws of Malaysia (Attorney General's Chambers)

Fetches legislation from the official AGC portal via DataTables JSON endpoints.
Full text is extracted from directly downloadable PDFs.

Strategy:
  - List acts via POST to json-updated-2024.php (consolidated acts, ~887 records)
  - List amendments via POST to json-amendment-2024.php (~406 records)
  - Download PDFs and extract text
  - No auth required, no anti-bot protection

API:
  - Base: https://lom.agc.gov.my
  - Updated acts: POST /json-updated-2024.php {draw, start, length, language}
  - Amendments: POST /json-amendment-2024.php {draw, start, length, language}
  - PDFs: GET /ilims/upload/portal/akta/...

Response encryption (issue #1465):
  Since 2026 the DataTables endpoints no longer return plain JSON. They return
  {"encrypted": true, "data": "<base64>"} where the payload is AES-256-GCM
  (12-byte IV || 16-byte tag || ciphertext). The key is a 64-hex-char constant
  published in the listing page as `SEARCH_RESPONSE_KEY` and used by the site's
  own js/responseCrypto.js, so decryption is exactly what any browser does.
  We read the key from the live page (so a rotation is picked up automatically)
  and fall back to the known constant.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap-fast     # Full pull (fleet entry point)
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import base64
import json
import logging
import re
import html as htmlmod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MY.AGCLaws")

BASE_URL = "https://lom.agc.gov.my"

# DataTables endpoints. `page` is the HTML listing that publishes SEARCH_RESPONSE_KEY.
ENDPOINTS = [
    {
        "path": "/json-updated-2024.php",
        "page": "/principal.php?type=updated",
        "label": "Updated Acts",
        "category": "act",
    },
    {
        "path": "/json-amendment-2024.php",
        "page": "/principal.php?type=amendment",
        "label": "Amendment Acts",
        "category": "amendment",
    },
]

# Rows per listing request. The endpoint accepts length=-1 ("all"), but a single
# ~900-row response is a multi-MB blob with no progress and no partial recovery,
# so we page. Also bounds the damage if the site ever grows.
PAGE_SIZE = 100

# Hard ceiling on listing pages per endpoint — a server that ignores `start`
# would otherwise loop forever re-serving page 0.
MAX_PAGES = 200

# Fallback for the AES-256-GCM response key when the listing page cannot be
# read. Published verbatim in the page source as SEARCH_RESPONSE_KEY.
FALLBACK_RESPONSE_KEY = "ecdf7a016e103d01314ce0e3be4ac00bd6b1b931a276cb1a000abdad1b89bfff"

RESPONSE_KEY_RE = re.compile(r"SEARCH_RESPONSE_KEY\s*=\s*['\"]([0-9a-fA-F]{64})['\"]")


class AGCResponseError(RuntimeError):
    """Raised when a listing response cannot be decrypted or is malformed."""


def aes_gcm_decrypt(payload_b64: str, key_hex: str) -> bytes:
    """Decrypt a PHP openssl AES-256-GCM payload: IV(12) || tag(16) || ciphertext."""
    raw = base64.b64decode(payload_b64)
    if len(raw) < 29:
        raise AGCResponseError(f"encrypted payload too short ({len(raw)} bytes)")
    iv, tag, ciphertext = raw[:12], raw[12:28], raw[28:]
    key = bytes.fromhex(key_hex)

    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError:
        pass
    else:
        return AESGCM(key).decrypt(iv, ciphertext + tag, None)

    try:
        from Crypto.Cipher import AES
    except ImportError as exc:
        raise AGCResponseError(
            "lom.agc.gov.my now returns AES-256-GCM encrypted responses; "
            "install `cryptography` (or `pycryptodome`) to read them"
        ) from exc
    return AES.new(key, AES.MODE_GCM, nonce=iv).decrypt_and_verify(ciphertext, tag)


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="MY/AGCLaws",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="legislation",
    ) or ""

def clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = htmlmod.unescape(text)
    return text.strip()


def extract_pdf_url_from_html(html_str: str) -> Optional[str]:
    """Extract PDF URL from an HTML link string."""
    if not html_str:
        return None
    match = re.search(r'href=["\']([^"\']+\.pdf)["\']', html_str, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r'((?:/ilims|/upload)[^\s"\'<>]+\.pdf)', html_str, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _pdf_url_from_entry(entry: Dict[str, Any]) -> Optional[str]:
    """Build a PDF URL from one {"path": ..., "docName": ...} descriptor."""
    path = entry.get("path") or entry.get("url") or entry.get("pdfPath") or entry.get("pdfUrl")
    if not isinstance(path, str) or not path:
        return None
    if path.lower().endswith(".pdf"):
        return path
    # `path` is a directory; the filename lives in a sibling field.
    doc_name = entry.get("docName") or entry.get("docname") or entry.get("fileName")
    if isinstance(doc_name, str) and doc_name:
        return path.rstrip("/") + "/" + doc_name
    return None


def extract_pdf_url_from_json_field(json_str: str) -> Optional[str]:
    """Extract a PDF path from a JSON-encoded field.

    The field holds a *list* of per-language descriptors, e.g.
      [{"path": "/upload/.../3563475_BI/", "docName": "Act 884 - ....pdf"}, ...]
    Earlier code returned `path` alone, which is a directory and 404s, and only
    handled the dict form. Prefer the `_BI` (English) variant.
    """
    if not json_str:
        return None
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return None

    entries = data if isinstance(data, list) else [data]
    urls = [
        u for u in (
            _pdf_url_from_entry(e) for e in entries if isinstance(e, dict)
        ) if u
    ]
    if not urls:
        return None
    for url in urls:
        if "_BI/" in url or "_bi/" in url:
            return url
    return urls[0]


class AGCLawsScraper(BaseScraper):
    """Scraper for MY/AGCLaws -- Malaysia Attorney General's Chambers legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "en-MY,en;q=0.9",
            },
            timeout=90,
        )
        self._response_key: Optional[str] = None

    # ── Response decryption ───────────────────────────────────────────

    def _get_response_key(self, page_path: Optional[str]) -> str:
        """Read SEARCH_RESPONSE_KEY from the listing page, cached for the run."""
        if self._response_key:
            return self._response_key

        if page_path:
            try:
                self.rate_limiter.wait()
                resp = self.client.get(page_path, headers={"Accept": "text/html"})
                if resp is not None and resp.status_code == 200:
                    match = RESPONSE_KEY_RE.search(resp.text)
                    if match:
                        self._response_key = match.group(1).lower()
                        logger.info(f"Read response key from {page_path}")
                        return self._response_key
                logger.warning(
                    f"No SEARCH_RESPONSE_KEY in {page_path} "
                    f"(HTTP {getattr(resp, 'status_code', 'N/A')}); using fallback key"
                )
            except Exception as e:
                logger.warning(f"Could not read response key from {page_path}: {e}")

        self._response_key = FALLBACK_RESPONSE_KEY
        return self._response_key

    def _decode_response(self, body: Any, key_hex: str) -> Dict[str, Any]:
        """Turn a raw endpoint body into the decrypted DataTables dict."""
        payload = None
        if isinstance(body, dict) and body.get("encrypted") is True:
            payload = body.get("data")
            if not isinstance(payload, str) or not payload:
                raise AGCResponseError("encrypted response carries no data")
        elif isinstance(body, str):
            payload = body
        elif isinstance(body, dict):
            # Legacy plaintext DataTables response.
            return body
        else:
            raise AGCResponseError(f"unexpected response type {type(body).__name__}")

        decrypted = json.loads(aes_gcm_decrypt(payload, key_hex))
        if not isinstance(decrypted, dict):
            raise AGCResponseError(
                f"decrypted payload is {type(decrypted).__name__}, expected object"
            )
        return decrypted

    # ── Listing ───────────────────────────────────────────────────────

    def _fetch_listing(self, endpoint: str, language: str = "BI",
                       start: int = 0, length: int = PAGE_SIZE,
                       page_path: Optional[str] = None) -> Dict[str, Any]:
        """Fetch one page of a DataTables listing.

        Returns a dict with validated ``records`` (list) and ``recordsTotal``
        (int). Raises AGCResponseError rather than returning a malformed shape:
        the previous version fell back to ``len(records)`` on a *string* body,
        which reported "3551448 records returned" (the payload's character
        count) and then iterated the string one character at a time — the hang
        in issue #1465.
        """
        self.rate_limiter.wait()
        form_data = {
            "draw": "1",
            "start": str(start),
            "length": str(length),
            "search[value]": "",
            "language": language,
        }
        resp = self.client.post(
            endpoint,
            data=form_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if resp is None or resp.status_code != 200:
            raise AGCResponseError(
                f"{endpoint} start={start}: HTTP {getattr(resp, 'status_code', 'N/A')}"
            )

        try:
            body = resp.json()
        except ValueError as exc:
            raise AGCResponseError(f"{endpoint} start={start}: response is not JSON") from exc

        data = self._decode_response(body, self._get_response_key(page_path))

        records = data.get("records")
        if records is None:
            records = data.get("data")
        if not isinstance(records, list):
            raise AGCResponseError(
                f"{endpoint} start={start}: 'records' is "
                f"{type(records).__name__}, expected list"
            )

        try:
            total = int(data.get("recordsTotal", len(records)))
        except (TypeError, ValueError):
            total = len(records)

        return {"records": records, "recordsTotal": total}

    def _download_pdf(self, pdf_url: str) -> Optional[bytes]:
        """Download a PDF file."""
        self.rate_limiter.wait()
        try:
            # Normalize relative paths like ../../../ilims/...
            if pdf_url.startswith("../") or pdf_url.startswith("./"):
                # Strip all leading ../
                cleaned = re.sub(r'^(?:\.\./)+', '', pdf_url)
                full_url = "/" + cleaned
            elif pdf_url.startswith("/"):
                full_url = pdf_url
                # JSON field returns /upload/... but correct path is /ilims/upload/...
                if full_url.startswith("/upload/"):
                    full_url = "/ilims" + full_url
            elif pdf_url.startswith("http"):
                full_url = pdf_url.replace(BASE_URL, "")
            else:
                full_url = "/" + pdf_url

            # Act filenames contain spaces and commas ("Act 884 - JOHOR ....pdf").
            full_url = quote(full_url, safe="/%?&=")

            resp = self.client.get(full_url, headers={"Accept": "application/pdf"})
            if not resp or resp.status_code != 200:
                return None
            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type and len(resp.content) < 1000:
                return None
            return resp.content
        except Exception as e:
            logger.debug(f"PDF download error {pdf_url}: {e}")
            return None

    def _get_pdf_url_for_record(self, record: Dict, category: str) -> Optional[str]:
        """Extract the best PDF URL from a listing record."""
        # Try JSON-encoded PDF path fields first, English (BI) before Malay (BM).
        # The two endpoints use different field names: the updated-acts listing
        # has one combined `doc2downloadgeneratepdf` list, the amendment listing
        # has per-language `DOC2DOWNLOAD{BI,BM}generatepdf` objects.
        for field in ("DOC2DOWNLOADBIgeneratepdf", "doc2downloadgeneratepdf",
                       "DOC2DOWNLOADBMgeneratepdf"):
            val = record.get(field)
            if val:
                url = extract_pdf_url_from_json_field(val)
                if url:
                    return url

        # Try direct URL fields
        for field in ("URLDOCBI", "URLDOCBM", "DOC2DOWNLOADBI", "DOC2DOWNLOADBM",
                       "doc2download", "DOC2DOWNLOAD"):
            val = record.get(field)
            if val:
                url = extract_pdf_url_from_html(val)
                if url:
                    return url
                if isinstance(val, str) and val.strip().endswith(".pdf"):
                    return val.strip()

        return None

    def _extract_title(self, record: Dict) -> str:
        """Extract clean title from record."""
        # LEGISLATIONTITLEBI carries the real English title on the amendment
        # listing, where TajukBI is a copy of the Malay one.
        for field in ("LEGISLATIONTITLEBI", "title", "TajukBI", "titleBI",
                       "TAJUK_BI", "tajukBI"):
            val = record.get(field)
            if val:
                # For updated acts, extract just the first link text (English title)
                first_link = re.search(r'<a[^>]*>([^<]+)</a>', val)
                if first_link:
                    title = first_link.group(1).strip()
                    # Clean up newlines within title
                    title = re.sub(r'\s+', ' ', title)
                    return title
                return clean_html(val).strip()
        for field in ("TajukBM", "titleBM"):
            val = record.get(field)
            if val:
                return clean_html(val).strip()
        return "Unknown"

    def _extract_act_number(self, record: Dict) -> str:
        """Extract act number from record."""
        for field in ("lgt_act_no", "ACTNO_LEGISLATION", "nombor", "ACT_NO", "noPU"):
            val = record.get(field)
            if val:
                return clean_html(str(val)).strip()
        return ""

    def _extract_date(self, record: Dict) -> Optional[str]:
        """Extract and format date from record."""
        # Try explicit date fields first
        for field in ("PUBLICATIONDATE", "publicationDate", "ROYALASSENTDATE",
                       "dateCreated", "lgt_update_date"):
            val = record.get(field)
            if not val:
                continue
            val = clean_html(str(val)).strip()
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d %b %Y", "%Y-%m-%dT%H:%M:%S"):
                try:
                    dt = datetime.strptime(val[:19], fmt)
                    return dt.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    continue

        # Try extracting date from title HTML (e.g. <i>25-03-2026</i>)
        title_html = record.get("title", "")
        if title_html:
            date_match = re.search(r'(\d{2}-\d{2}-\d{4})', title_html)
            if date_match:
                try:
                    dt = datetime.strptime(date_match.group(1), "%d-%m-%Y")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all legislation records from AGC DataTables endpoints."""
        yielded_any = False
        failures: List[str] = []

        for ep in ENDPOINTS:
            path = ep["path"]
            label = ep["label"]
            category = ep["category"]

            logger.info(f"Fetching {label} from {path}...")
            start = 0
            total = None
            seen = 0

            for page in range(MAX_PAGES):
                try:
                    data = self._fetch_listing(
                        path, start=start, length=PAGE_SIZE, page_path=ep.get("page")
                    )
                except AGCResponseError as e:
                    failures.append(f"{label}: {e}")
                    logger.warning(f"{label}: {e}")
                    break

                records = data["records"]
                if total is None:
                    total = data["recordsTotal"]
                    logger.info(f"{label}: {total} records available")

                if not records:
                    break

                for record in records:
                    if not isinstance(record, dict):
                        continue
                    yielded_any = True
                    yield {
                        "_category": category,
                        "_endpoint": path,
                        "_record": record,
                    }

                seen += len(records)
                start += len(records)
                logger.info(f"{label}: {seen}/{total} listed")

                if seen >= total or len(records) < PAGE_SIZE:
                    break
            else:
                logger.warning(f"{label}: hit MAX_PAGES={MAX_PAGES}, stopping")

        # Fail loud rather than exiting 0 with an empty corpus — a silent 0 here
        # is what let the endpoint change go unnoticed (issue #1465).
        if not yielded_any:
            raise AGCResponseError(
                "No records listed from any lom.agc.gov.my endpoint: "
                + ("; ".join(failures) if failures else "all endpoints returned 0 rows")
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch all records (no date filtering available in API)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema, downloading and extracting PDF text."""
        record = raw.get("_record", {})
        category = raw.get("_category", "act")

        title = self._extract_title(record)
        act_no = self._extract_act_number(record)
        date_str = self._extract_date(record)

        if not title or title == "Unknown":
            return None

        pdf_url = self._get_pdf_url_for_record(record, category)
        if not pdf_url:
            logger.debug(f"No PDF URL for: {title}")
            return None

        pdf_bytes = self._download_pdf(pdf_url)
        if not pdf_bytes:
            logger.debug(f"Failed to download PDF for: {title}")
            return None

        text = extract_pdf_text(pdf_bytes)
        if not text or len(text) < 50:
            logger.debug(f"No text extracted from PDF for: {title}")
            return None

        doc_id = act_no or title[:50]
        doc_id = re.sub(r'[^a-zA-Z0-9._-]', '_', doc_id)

        web_url = f"{BASE_URL}/act-detail.php?language=BI&act={act_no}" if act_no else BASE_URL

        return {
            "_id": f"MY-AGC-{category}-{doc_id}",
            "_source": "MY/AGCLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{act_no} - {title}" if act_no else title,
            "text": text,
            "date": date_str,
            "url": web_url,
            "act_number": act_no,
            "category": category,
            "language": "en",
            "jurisdiction": "MY",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Malaysia AGC Laws API...")

        for ep in ENDPOINTS:
            path = ep["path"]
            label = ep["label"]
            category = ep["category"]

            print(f"\n--- {label} ({path}) ---")
            try:
                data = self._fetch_listing(path, length=3, page_path=ep.get("page"))
            except AGCResponseError as e:
                print(f"  FAILED: {e}")
                continue

            records = data["records"]
            total = data["recordsTotal"]
            print(f"  Total: {total:,} records")

            if records:
                record = records[0]
                title = self._extract_title(record)
                act_no = self._extract_act_number(record)
                print(f"  First: {act_no} - {title[:80]}")

                pdf_url = self._get_pdf_url_for_record(record, category)
                if pdf_url:
                    print(f"  PDF URL: {pdf_url[:100]}")
                    pdf_bytes = self._download_pdf(pdf_url)
                    if pdf_bytes:
                        print(f"  PDF size: {len(pdf_bytes):,} bytes")
                        text = extract_pdf_text(pdf_bytes)
                        print(f"  Extracted text: {len(text)} chars")
                        if text:
                            print(f"  Sample: {text[:200]}...")
                    else:
                        print("  FAILED: Could not download PDF")
                else:
                    print("  FAILED: No PDF URL found")

        print("\nTest complete!")


def main():
    scraper = AGCLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
              "[--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast", "bootstrap_fast"):
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
