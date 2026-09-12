#!/usr/bin/env python3
"""
IL/HasadnaKnesset -- Israeli legislation from Hasadna Open Knesset data pipeline

Fetches Israeli laws from the Hasadna Open Knesset CSV data pipeline at
production.oknesset.org. Joins kns_law (legislation records) with
kns_document_law (PDF document links on fs.knesset.gov.il).
Text extracted from PDFs via pdfplumber.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import io
import csv
import json
import logging
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IL.HasadnaKnesset")

BASE_CSV_URL = "https://production.oknesset.org/pipelines/data/laws"
KNS_LAW_CSV = f"{BASE_CSV_URL}/kns_law/kns_law.csv"
KNS_DOC_CSV = f"{BASE_CSV_URL}/kns_document_law/kns_document_law.csv"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/)",
    "Accept": "text/csv, */*",
}

# --- document_type (issue #1533, item 4) --------------------------------
#
# The customer could not tell a proposal from binding law: drafts
# (הצעת תקנות / טיוטת תקנות) were served next to enacted law with
# document_type null. The Knesset's own kns_law taxonomy carries most of the
# distinction, so it is read from the data rather than guessed:
#
#   TypeDesc    חוק בן חיצוני      1,048  primary legislation (statutes)
#               פעולה על פי חוק   59,603  an action taken under a law
#   SubTypeDesc חקיקת משנה        56,986  secondary legislation (regulations)
#               דיווח על פי חוק    1,510  a report filed under a law — not law
#               פעולה אחרת על פי חוק 1,107 other statutory action
#               נוסח משולב / נוסח חדש / השלטון הבריטי / העותומני /
#               מועצת המדינה הזמנית      consolidated or historic statutes
#
# What the taxonomy does *not* encode is draft status: all 1,116 drafts are
# filed under חקיקת משנה like enacted regulations. The Knesset does mark it in
# its own title (הצעת = proposed, טיוטת = draft), so that is read from the
# title — the publisher's wording, not an inference of ours.
_PRIMARY_SUBTYPES = {
    "נוסח משולב",           # consolidated version
    "נוסח חדש",             # new version
    "השלטון הבריטי",         # British Mandate legislation
    "השלטון העותומני",       # Ottoman legislation
    "מועצת המדינה הזמנית",   # Provisional State Council
}
_DRAFT_MARKERS = ("הצעת", "טיוטת", "הצעה ל", "טיוטה")

# --- PDF selection (issue #1559) ----------------------------------------
#
# kns_document_law holds every document filed against a LawID, of which only
# some are the law. The old rule — "newest gazette publication, else newest of
# anything" — put an errata notice under the title of the consolidated statute
# it corrected (9 laws, all high-traffic: חוק הביטוח הלאומי, חוק בתי המשפט,
# חוק סדר הדין הפלילי …), because for those laws the errata are the *only*
# PDFs on file, so the gazette filter matched nothing and the fallback took
# them. The same fallback handed back committee background material as the law
# text for ~107 more (verified: 7K–12K chars of legal-advice memos to a
# committee, filed under a צו's title).
#
# GroupTypeDesc already names what each document is, so selection reads it
# instead of guessing from recency. Ordered most authoritative first:
_TEXT_GROUP_PRIORITY = (
    "חוק - נוסח חדש",                   # consolidated new version — is the statute
    "חוק - פרסום ברשומות",              # published in Reshumot (official gazette)
    "פרסום ברשומות",
    "נוסח מהגורם המוסמך",               # signed text from the authorising body
    "הנוסח שאושר על-ידי הוועדה",         # text as approved by the Knesset committee
    "חקיקת משנה - פניית הגורם המומסך",   # secondary legislation as submitted
    "דיווח על-פי חוק",                  # statutory report — the substance for report entries
)

# Documents that are never the operative text, whatever their date. Choosing
# any of these means storing something the title does not describe, which is
# worse than storing nothing: a law with no usable PDF is simply skipped.
_NEVER_THE_LAW = frozenset({
    "חוק - תיקון טעות",          # errata ("correction of errors") — a typo notice
    "הצעת נוסח חדש",             # *draft* of a consolidated version
    "חומר רקע",                  # background material prepared for a committee
    "אסמכתא לקיום הליך מקדים",   # evidence a preliminary step was taken
    "הודעה לעיתונות",            # press release
    "מכתב אישור לגורם המוסמך",   # approval letter
    "מסמך של מרכז המחקר והמידע", # Knesset Research Centre paper
    "מסמכים לא משויכים",         # unassigned documents
})


def _classify_document(type_desc: str, sub_type_desc: str, title: str):
    """
    Return ``(document_type, is_draft)`` for a kns_law record.

    ``document_type`` is None when nothing in the source fixes it. Per issue
    #1533 requirement 5 an unknown stays an explicit unknown — a confident
    false label is worse than a null for a consumer citing this to a court.
    """
    type_desc = (type_desc or "").strip()
    sub_type_desc = (sub_type_desc or "").strip()
    is_draft = any(m in (title or "") for m in _DRAFT_MARKERS)

    if sub_type_desc == "דיווח על פי חוק":
        # A report filed with a Knesset committee. Never binding law, and a
        # draft marker in its title would describe what it reports *on*.
        return "report", False
    if type_desc == "חוק בן חיצוני" or sub_type_desc in _PRIMARY_SUBTYPES:
        return ("draft_primary_legislation" if is_draft
                else "primary_legislation"), is_draft
    if sub_type_desc == "חקיקת משנה":
        return ("draft_secondary_legislation" if is_draft
                else "secondary_legislation"), is_draft
    if sub_type_desc == "פעולה אחרת על פי חוק":
        return ("draft_statutory_action" if is_draft
                else "statutory_action"), is_draft
    return None, is_draft


class ILHasadnaKnessetScraper(BaseScraper):
    """Scraper for IL/HasadnaKnesset - Israeli legislation via Hasadna pipeline."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None
        self._law_map = None
        self._doc_map = None

    def _get_session(self):
        if self.session is None:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            self.session = requests.Session()
            self.session.headers.update(_HEADERS)

            retry_strategy = Retry(
                total=3,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        return self.session

    def _download_csv(self, url: str) -> list:
        """Download and parse a CSV file. Returns list of dicts."""
        sess = self._get_session()
        logger.info(f"Downloading CSV: {url}")
        resp = sess.get(url, timeout=120)
        resp.raise_for_status()
        text = resp.content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        logger.info(f"  → {len(rows)} rows")
        return rows

    def _load_data(self):
        """Download and index both CSV tables."""
        if self._law_map is not None:
            return

        laws = self._download_csv(KNS_LAW_CSV)
        docs = self._download_csv(KNS_DOC_CSV)

        # Index laws by LawID
        self._law_map = {}
        for law in laws:
            lid = law.get("LawID", "")
            if lid:
                self._law_map[lid] = law

        # Index documents by LawID (only PDF-type documents)
        self._doc_map = defaultdict(list)
        for doc in docs:
            lid = doc.get("LawID", "")
            app = doc.get("ApplicationDesc", "")
            path = doc.get("FilePath", "")
            if lid and path and app == "PDF":
                self._doc_map[lid].append(doc)

        logger.info(f"Loaded {len(self._law_map)} laws, "
                     f"{sum(len(v) for v in self._doc_map.values())} PDF docs "
                     f"for {len(self._doc_map)} laws")

    def _pick_best_pdf(self, docs: list) -> Optional[str]:
        """Pick the PDF that actually carries the law's text, or None.

        Ranks candidates by what the Knesset says each document *is*
        (``GroupTypeDesc``, see ``_TEXT_GROUP_PRIORITY``) and only then by
        recency, so a later errata or amendment can no longer outrank the
        consolidated text it amends. Documents in ``_NEVER_THE_LAW`` are
        dropped outright; a law left with no candidate returns None and is
        skipped rather than stored under text that is not it (issue #1559).
        """
        candidates = [d for d in docs
                      if d.get("GroupTypeDesc", "") not in _NEVER_THE_LAW]
        if not candidates:
            return None

        def rank(d):
            group = d.get("GroupTypeDesc", "")
            try:
                tier = _TEXT_GROUP_PRIORITY.index(group)
            except ValueError:
                # Unrecognised group: usable, but only after every known one,
                # so a new Knesset document type degrades to last-resort
                # instead of silently taking over selection.
                tier = len(_TEXT_GROUP_PRIORITY)
            return tier

        # Newest first within a tier: stable sort, recency applied underneath.
        candidates = sorted(candidates,
                            key=lambda d: d.get("LastUpdatedDate", ""),
                            reverse=True)
        candidates.sort(key=rank)
        return candidates[0].get("FilePath")

    def _fetch_pdf_bytes(self, url: str) -> Optional[bytes]:
        """Download a PDF. Returns bytes or None on error."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) < 200:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF {url}: {e}")
            return None

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """
        Extract text from PDF bytes in *logical* (readable) Hebrew order.

        pdfplumber emits glyphs in the order the content stream lists them,
        which for these Knesset PDFs is visual order — so every Hebrew line
        came out character-reversed (``חוק`` stored as ``קוח``). Nothing
        downstream could match that: keyword search over the corpus was
        scoring against gibberish, which is what surfaced unrelated patent and
        customs documents for a small-claims query (issue #1533, item 3).

        common.arabic_pdf reorders glyph clusters by x-geometry rather than
        trusting the emitted sequence. That is script-neutral, so it repairs
        Hebrew as well as Arabic. pdfplumber stays as the fallback for PDFs
        whose text layer PyMuPDF cannot read, or if PyMuPDF is unavailable.
        """
        from common.arabic_pdf import extract_rtl_pdf_text

        try:
            rtl_text = extract_rtl_pdf_text(pdf_bytes)
        except Exception as e:
            logger.warning(f"RTL extraction failed, falling back to pdfplumber: {e}")
            rtl_text = None
        if rtl_text:
            return rtl_text

        return self._extract_text_with_pdfplumber(pdf_bytes)

    def _extract_text_with_pdfplumber(self, pdf_bytes: bytes) -> str:
        """Fallback extractor. Emits visual order for RTL — see the caller."""
        import pdfplumber

        text_parts = []
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"PDF extraction error: {e}")
            return ""
        return "\n\n".join(text_parts)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all law records that have PDF documents."""
        self._load_data()

        yielded = 0
        for law_id, doc_list in self._doc_map.items():
            law = self._law_map.get(law_id)
            if not law:
                continue

            pdf_url = self._pick_best_pdf(doc_list)
            if not pdf_url:
                continue

            pdf_bytes = self._fetch_pdf_bytes(pdf_url)
            if not pdf_bytes:
                continue

            yielded += 1
            if yielded % 50 == 0:
                logger.info(f"Progress: {yielded} records yielded")

            yield {
                "law_id": law_id,
                "name": law.get("Name", ""),
                "type_desc": law.get("TypeDesc", ""),
                "sub_type_desc": law.get("SubTypeDesc", ""),
                "knesset_num": law.get("KnessetNum", ""),
                "publication_date": law.get("PublicationDate", ""),
                "publication_series": law.get("PublicationSeriesDesc", ""),
                "magazine_number": law.get("MagazineNumber", ""),
                "page_number": law.get("PageNumber", ""),
                "pdf_url": pdf_url,
                "pdf_bytes": pdf_bytes,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental updates — re-fetch laws updated after the given date."""
        self._load_data()

        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        yielded = 0

        for law_id, doc_list in self._doc_map.items():
            law = self._law_map.get(law_id)
            if not law:
                continue

            last_updated = law.get("LastUpdatedDate", "")
            if last_updated <= since_str:
                continue

            pdf_url = self._pick_best_pdf(doc_list)
            if not pdf_url:
                continue

            pdf_bytes = self._fetch_pdf_bytes(pdf_url)
            if not pdf_bytes:
                continue

            yielded += 1
            yield {
                "law_id": law_id,
                "name": law.get("Name", ""),
                "type_desc": law.get("TypeDesc", ""),
                "sub_type_desc": law.get("SubTypeDesc", ""),
                "knesset_num": law.get("KnessetNum", ""),
                "publication_date": law.get("PublicationDate", ""),
                "publication_series": law.get("PublicationSeriesDesc", ""),
                "magazine_number": law.get("MagazineNumber", ""),
                "page_number": law.get("PageNumber", ""),
                "pdf_url": pdf_url,
                "pdf_bytes": pdf_bytes,
            }

        logger.info(f"Updates: {yielded} records since {since_str}")

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw data into standardized record."""
        pdf_bytes = raw.get("pdf_bytes")
        if not pdf_bytes:
            return None

        text = self._extract_text_from_pdf(pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text ({len(text)} chars) from {raw.get('pdf_url', '?')}")
            return None

        law_id = raw.get("law_id", "")
        name = raw.get("name", "")
        pdf_url = raw.get("pdf_url", "")

        # Parse publication date
        pub_date = raw.get("publication_date", "")
        date_str = None
        if pub_date and "T" in pub_date:
            date_str = pub_date.split("T")[0]

        # Stable ID from law_id
        doc_id = f"IL-KNS-{law_id}"

        type_desc = raw.get("type_desc", "")
        sub_type_desc = raw.get("sub_type_desc", "")
        document_type, is_draft = _classify_document(type_desc, sub_type_desc, name)

        return {
            "_id": doc_id,
            "_source": "IL/HasadnaKnesset",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": name,
            "text": text,
            "date": date_str,
            "url": pdf_url,
            "law_id": law_id,
            # Enacted law vs proposal, so consumers can tell binding law from a
            # draft (#1533 item 4). None where the source does not fix it.
            "document_type": document_type,
            "is_draft": is_draft,
            "type_desc": type_desc,
            "sub_type_desc": sub_type_desc,
            "knesset_num": raw.get("knesset_num", ""),
            "publication_series": raw.get("publication_series", ""),
            "magazine_number": raw.get("magazine_number", ""),
            "page_number": raw.get("page_number", ""),
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ILHasadnaKnessetScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        import requests
        try:
            resp = requests.get(KNS_LAW_CSV, headers=_HEADERS, timeout=30, stream=True)
            print(f"kns_law CSV: HTTP {resp.status_code}")
            resp.close()

            resp = requests.get(KNS_DOC_CSV, headers=_HEADERS, timeout=30, stream=True)
            print(f"kns_document_law CSV: HTTP {resp.status_code}")
            resp.close()

            # Test a PDF download
            test_pdf = "https://fs.knesset.gov.il//2/law/2_lsr_311000.PDF"
            resp = requests.head(test_pdf, timeout=15)
            print(f"PDF endpoint: HTTP {resp.status_code}")
            print("Connection OK")
        except Exception as e:
            print(f"Connection FAILED: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete:")
        print(f"  Records fetched: {stats['records_fetched']}")
        if sample_mode:
            print(f"  Sample records saved: {stats.get('sample_records_saved', 0)}")
        else:
            print(f"  New: {stats['records_new']}")
            print(f"  Updated: {stats['records_updated']}")
            print(f"  Skipped: {stats['records_skipped']}")
        print(f"  Errors: {stats['errors']}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
