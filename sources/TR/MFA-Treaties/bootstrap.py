#!/usr/bin/env python3
"""
TR/MFA-Treaties -- Turkish International Agreements (MFA Treaties Database)

Fetches international agreements (Uluslararası Antlaşmalar) to which the Republic
of Türkiye is a party, from the Ministry of Foreign Affairs treaty database at
https://ua.mfa.gov.tr.

Strategy:
  - Each treaty has a detail page at /detay.aspx?{id} (sequential numeric ids).
  - The detail page carries structured metadata as <b>Label: </b>Value<br/> rows
    (type, kind, subjects, state parties, signature place/date, Resmî Gazete ref)
    and a list of associated PDF files linked as files.ashx?{fileId}.
  - The full text of the ratification law and the treaty itself lives in those
    PDFs (as published in the Resmî Gazete). We download each PDF and extract
    text. Modern treaties are text PDFs; older treaties are scanned images with
    no text layer -- those yield no text and are skipped.

URL patterns:
  - Detail page:  https://ua.mfa.gov.tr/detay.aspx?{id}
  - PDF download: https://ua.mfa.gov.tr/files.ashx?{fileId}
  - Search (POST, ASP.NET WebForms): https://ua.mfa.gov.tr/  (not needed; we
    enumerate detail ids directly which is more complete than paged search)
  - No auth required.

Enumeration:
  Treaty ids run from 1 up to roughly ~8400 (with metadata-only stubs a little
  beyond). We iterate descending from START_ID so the newest, machine-readable
  treaties come first -- this makes --sample reliably full-text and keeps the
  full crawl complete.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import unicodedata
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from bs4 import BeautifulSoup

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import _extract as extract_pdf_text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TR.MFA-Treaties")

BASE_URL = "https://ua.mfa.gov.tr"

# The site rejects non-browser User-Agents with HTTP 403.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,tr;q=0.8",
}

# Where the bootstrap crawl begins. Treaty ids below ~8200 carry the full text
# of the agreement (ratification law + the treaty annex published in the Resmî
# Gazete); the ~220 newest ids above this are mostly short amendment/protocol
# ratification stubs whose treaty annex has not been uploaded. We start the bulk
# crawl here (descending to id 1) for rich full-text coverage; fetch_updates
# probes the higher ids to catch newly-published treaties.
START_ID = 8200
# Ceiling probed by fetch_updates to discover newly-added (highest-id) treaties.
UPDATE_CEILING = 9000
# Below this many extracted characters we treat the treaty as scanned / empty /
# stub (no usable full text) and skip it.
MIN_TEXT_CHARS = 1000
# Cap on PDFs downloaded per treaty (law + decree + annexes).
MAX_PDFS_PER_TREATY = 8

# Map common Turkish metadata labels to stable English keys.
META_LABEL_MAP = {
    "Türü": "treaty_class",            # Çok Taraflı / İki Taraflı (multi/bilateral)
    "Tipi": "treaty_kind",             # Anlaşma / Mukavelename / Sözleşme ...
    "Konuları": "subjects",
    "Konusu": "subjects",
    "Taraf Devletler (Ülke veya Uluslararası Kuruluşlar)": "parties",
    "Dili": "languages",
    "Uyuşmazlık Halinde Geçerli Dil": "authentic_language",
    "İmza Yerleri ve Tarihleri": "signature_place_date",
    "Yürürlülüğe Giriş Tarihi": "entry_into_force",
    "Yürürlülük Süresi": "duration",
}


def _slugify(text: str, maxlen: int = 80) -> str:
    """ASCII slug for use in identifiers."""
    # Turkish-specific transliteration before NFKD.
    trans = str.maketrans("çğıİöşüÇĞÖŞÜ", "cgiIosuCGOSU")
    text = text.translate(trans)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text[:maxlen].strip("-")


def _parse_tr_date(s: str) -> Optional[str]:
    """Parse a DD.MM.YYYY date out of a string -> ISO YYYY-MM-DD."""
    if not s:
        return None
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", s)
    if not m:
        return None
    d, mo, y = m.groups()
    try:
        return datetime(int(y), int(mo), int(d)).date().isoformat()
    except ValueError:
        return None


class MFATreatiesScraper(BaseScraper):
    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers=BROWSER_HEADERS,
            timeout=60,
            respect_robots=False,
        )

    # ── Detail page parsing ────────────────────────────────────────────

    def _fetch_detail(self, tid: int) -> Optional[Dict[str, Any]]:
        """
        Fetch and parse a treaty detail page.

        Returns a dict with title, metadata, and file ids, or None if the id
        does not correspond to a treaty.
        """
        try:
            self.rate_limiter.wait()
            resp = self.http.get(f"/detay.aspx?{tid}", rate_limiter=self.rate_limiter)
        except Exception as e:
            logger.debug(f"detail {tid}: request failed: {e}")
            return None
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        h3 = soup.find("h3")
        title = h3.get_text(" ", strip=True) if h3 else ""
        body_text = soup.get_text(" ", strip=True)

        # A real treaty page has the "Türü:" metadata label and a title.
        if not title or "Türü:" not in body_text:
            return None

        container = h3.find_parent() if h3 else soup
        meta: Dict[str, str] = {}
        for b in container.find_all("b"):
            label = b.get_text(" ", strip=True).rstrip(":").strip()
            if not label:
                continue
            nxt = b.next_sibling
            val = nxt.strip() if isinstance(nxt, str) else ""
            if not val:
                continue
            key = META_LABEL_MAP.get(label)
            if key and key not in meta:
                meta[key] = val

        # Resmî Gazete (Official Gazette) publication date — first occurrence.
        rg_date = ""
        rg_m = re.search(r"Resmi Gazete Tarihi:\s*</b>\s*([0-9.]+)", resp.text)
        if rg_m:
            rg_date = rg_m.group(1)

        # The "İlişkili Dosyalar" (associated files) block is a separate div,
        # outside the metadata container — search the whole page.
        files: List[Dict[str, str]] = []
        seen_fids = set()
        for a in soup.find_all("a", href=re.compile(r"files\.ashx", re.I)):
            href = a["href"]
            fid = re.search(r"files\.ashx\?(\d+)", href)
            if fid and fid.group(1) not in seen_fids:
                seen_fids.add(fid.group(1))
                files.append({"file_id": fid.group(1), "name": a.get_text(strip=True)})

        return {
            "id": tid,
            "title": title,
            "metadata": meta,
            "rg_date": rg_date,
            "files": files,
            "url": f"{BASE_URL}/detay.aspx?{tid}",
        }

    def _extract_files(self, detail: Dict[str, Any]) -> str:
        """Download associated PDFs and concatenate extracted text."""
        parts: List[str] = []
        for f in detail["files"][:MAX_PDFS_PER_TREATY]:
            fid = f["file_id"]
            try:
                self.rate_limiter.wait()
                resp = self.http.get(f"/files.ashx?{fid}", rate_limiter=self.rate_limiter)
            except Exception as e:
                logger.debug(f"file {fid}: download failed: {e}")
                continue
            content = resp.content
            if not content[:4] == b"%PDF":
                continue
            try:
                txt = extract_pdf_text(content)
            except Exception as e:
                logger.debug(f"file {fid}: extraction failed: {e}")
                txt = None
            if txt and txt.strip():
                parts.append(txt.strip())
        return "\n\n".join(parts).strip()

    # ── BaseScraper interface ──────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Iterate treaty detail ids descending from START_ID to 1, yielding raw
        records that have a usable full-text PDF.
        """
        for tid in range(START_ID, 0, -1):
            detail = self._fetch_detail(tid)
            if not detail or not detail["files"]:
                continue
            text = self._extract_files(detail)
            if len(text) < MIN_TEXT_CHARS:
                logger.debug(f"treaty {tid}: only {len(text)} chars (scanned/empty), skip")
                continue
            detail["text"] = text
            yield detail

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Treaties are append-mostly and rarely revised. Re-scan the newest block
        of ids (newest treaties get the highest ids) from UPDATE_CEILING down;
        upsert handles dedup.
        """
        floor = max(1, START_ID - 1000)
        for tid in range(UPDATE_CEILING, floor, -1):
            detail = self._fetch_detail(tid)
            if not detail or not detail["files"]:
                continue
            text = self._extract_files(detail)
            if len(text) < MIN_TEXT_CHARS:
                continue
            detail["text"] = text
            yield detail

    def normalize(self, raw: dict) -> Optional[dict]:
        text = raw.get("text", "").strip()
        if len(text) < MIN_TEXT_CHARS:
            return None

        meta = raw.get("metadata", {})
        title = raw["title"]

        # Date: prefer signature date, else Resmî Gazete date.
        date = _parse_tr_date(meta.get("signature_place_date", "")) or _parse_tr_date(
            raw.get("rg_date", "")
        ) or _parse_tr_date(meta.get("entry_into_force", ""))

        tid = raw["id"]
        slug = _slugify(title)
        _id = f"TR-treaty-{tid}" + (f"-{slug}" if slug else "")

        record = {
            "_id": _id,
            "_source": "TR/MFA-Treaties",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw["url"],
            "treaty_class": meta.get("treaty_class"),
            "treaty_kind": meta.get("treaty_kind"),
            "subjects": meta.get("subjects"),
            "parties": meta.get("parties"),
            "languages": meta.get("languages"),
            "signature_place_date": meta.get("signature_place_date"),
            "entry_into_force": meta.get("entry_into_force"),
            "official_gazette_date": _parse_tr_date(raw.get("rg_date", "")),
        }
        return record

    # ── Connectivity test ──────────────────────────────────────────────

    def test_connection(self):
        print(f"Testing {BASE_URL} ...")
        resp = self.http.get("/", rate_limiter=self.rate_limiter)
        print(f"  Home: HTTP {resp.status_code}, {len(resp.text)} bytes")
        for tid in (8200, 8000, 7000):
            d = self._fetch_detail(tid)
            if d:
                print(f"  detay {tid}: '{d['title'][:60]}' files={len(d['files'])}")
                if d["files"]:
                    txt = self._extract_files(d)
                    print(f"    extracted {len(txt)} chars; sample: {txt[:120]!r}")
        print("Test complete!")


def main():
    scraper = MFATreatiesScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2, default=str))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
