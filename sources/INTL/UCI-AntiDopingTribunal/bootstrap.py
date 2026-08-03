#!/usr/bin/env python3
"""
INTL/UCI-AntiDopingTribunal -- UCI Anti-Doping Tribunal Decisions & CAS Appeal Awards

Fetches the published decisions of the UCI Anti-Doping Tribunal (Tribunal
Antidopage de l'UCI), the independent first-instance body that hears anti-doping
rule violation cases in international cycling under the UCI Anti-Doping Rules and
the World Anti-Doping Code, together with the Court of Arbitration for Sport (CAS)
awards rendered on appeal against those decisions. Both the tribunal judgments and
the CAS appeal awards are published in full by the UCI.

Strategy:
  - The "UCI Anti-Doping Tribunal" page on uci.org is a Contentful-backed page
    whose rich-text body (including every decision link) is server-embedded in the
    HTML as a unicode-escaped JSON island. We unescape it and parse every
    `<a href="//assets.ctfassets.net/.../<file>.pdf">Title</a>` anchor.
  - Each decision is a full reasoned PDF judgment hosted openly on Contentful's
    asset CDN (assets.ctfassets.net) — no login, no WAF, reachable from any IP.
  - We download each PDF and extract its full text via the shared OOM-hardened
    PDF extractor. A short metadata header (case reference, type) is prepended.

The procedural-rules document on the page is a regulation, not a case, and is
skipped so the source stays purely case_law.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Print parsed listing entries
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.UCI-AntiDopingTribunal")

SOURCE_ID = "INTL/UCI-AntiDopingTribunal"
LISTING_URL = "https://www.uci.org/uci-anti-doping-tribunal/5JsEGc56ZHWXlcVkHh66d9"

# Contentful asset anchor inside the unescaped rich-text island.
_ANCHOR_RE = re.compile(
    r'<a[^>]+href="(//assets\.ctfassets\.net/[^"]+\.pdf)"[^>]*>(.*?)</a>',
    re.I | re.S,
)
# Asset id = the path segment immediately after the Contentful space id.
_ASSET_ID_RE = re.compile(r"assets\.ctfassets\.net/[^/]+/([^/]+)/")

# Month names -> number, English + French (decisions are in EN or FR).
_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9, "octobre": 10,
    "novembre": 11, "décembre": 12, "decembre": 12,
}
_DATE_RE = re.compile(r"(\d{1,2})(?:er)?\s+([A-Za-zéûôà]+)\s+(\d{4})")


class UCIAntiDopingTribunalScraper(BaseScraper):
    """Scraper for UCI Anti-Doping Tribunal decisions and CAS appeal awards."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "LegalDataHunter/1.0 (legal research; "
                "+https://github.com/worldwidelaw/legal-sources)"
            ),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en",
        })

    # ── parsing helpers ──────────────────────────────────────────────

    def _asset_id(self, url: str) -> str:
        m = _ASSET_ID_RE.search(url)
        return m.group(1) if m else re.sub(r"[^A-Za-z0-9]+", "", url)[-22:]

    def _case_ref(self, title: str) -> Optional[str]:
        """Extract a normalized case reference from the anchor title."""
        t = title.strip()
        m = re.match(r"UCI\s+ADT\s+(.+?)\s+UCI\b", t, re.I)
        if m:
            ref = m.group(1).strip()
            ref = re.sub(r"\s*&\s*", " & ", ref)
            return f"ADT {ref}"
        m = re.match(r"(CAS\s+\d{4}\s*/\s*A\s*/\s*\d+)", t, re.I)
        if m:
            return re.sub(r"\s*/\s*", "/", m.group(1)).replace(" ", " ").strip()
        m = re.match(r"(CAS\s+\d{4})\s*/\s*A", t, re.I)
        if m:
            return f"{m.group(1)}/A"
        return None

    def _doc_type(self, title: str) -> str:
        return "cas_appeal" if title.upper().lstrip().startswith("CAS") else "tribunal"

    def _parse_date(self, text: str) -> Optional[str]:
        """Parse the decision date from the PDF body (EN or FR 'DD Month YYYY')."""
        if not text:
            return None
        # Prefer a date that follows a place name like "Aigle, " or "Lausanne, ".
        head = text[:6000]
        for m in _DATE_RE.finditer(head):
            mon = m.group(2).lower()
            month = _MONTHS.get(mon)
            if not month:
                continue
            try:
                return datetime(int(m.group(3)), month, int(m.group(1))).date().isoformat()
            except ValueError:
                continue
        return None

    def _get_entries(self) -> list[dict]:
        """Parse the page's embedded rich-text into decision entries."""
        resp = self.session.get(LISTING_URL, timeout=60)
        resp.raise_for_status()
        # The decision links live in a unicode-escaped JSON island; unescape it.
        try:
            text = resp.content.decode("utf-8", errors="replace")
            unescaped = text.encode("utf-8").decode("unicode_escape", errors="replace")
        except Exception:
            unescaped = resp.text

        entries = []
        seen = set()
        for url, raw_title in _ANCHOR_RE.findall(unescaped):
            title = re.sub(r"<[^>]+>", "", raw_title)
            title = re.sub(r"\s+", " ", title).strip()
            if not title:
                continue
            # Skip non-case documents (procedural rules, regulations, forms).
            if not re.match(r"(UCI\s+ADT|CAS)\b", title, re.I):
                continue
            full_url = ("https:" + url) if url.startswith("//") else url
            asset_id = self._asset_id(full_url)
            if asset_id in seen:
                continue
            seen.add(asset_id)
            entries.append({
                "asset_id": asset_id,
                "title": title,
                "case_ref": self._case_ref(title),
                "doc_type": self._doc_type(title),
                "pdf_url": full_url,
            })

        logger.info(f"Parsed {len(entries)} decision entries from listing page")
        return entries

    # ── fetch ────────────────────────────────────────────────────────

    def _fetch_pdf_text(self, entry: dict) -> Optional[str]:
        try:
            time.sleep(1.0)
            resp = self.session.get(entry["pdf_url"], timeout=120)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"  PDF fetch failed for {entry['asset_id']}: {e}")
            return None
        ctype = resp.headers.get("Content-Type", "")
        if "pdf" not in ctype.lower() and not resp.content[:4] == b"%PDF":
            logger.warning(f"  Not a PDF ({ctype}) for {entry['asset_id']}")
            return None
        return extract_pdf_markdown(
            SOURCE_ID,
            entry["asset_id"],
            pdf_bytes=resp.content,
            table="case_law",
            force=True,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._get_entries()
        logger.info(f"Total entries to process: {len(entries)}")
        for i, entry in enumerate(entries):
            try:
                logger.info(
                    f"[{i+1}/{len(entries)}] {entry['title'][:60]}"
                )
                text = self._fetch_pdf_text(entry)
                if not text or len(text) < 300:
                    logger.warning(
                        f"  Insufficient text for {entry['asset_id']}, skipping"
                    )
                    continue
                entry["_text"] = text
                entry["date"] = self._parse_date(text)
                yield entry
            except Exception as e:
                logger.error(f"  Error processing {entry['asset_id']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_iso = since.date().isoformat()
        for entry in self.fetch_all():
            if not entry.get("date") or entry["date"] >= since_iso:
                yield entry

    # ── normalize ────────────────────────────────────────────────────

    def normalize(self, raw: dict) -> dict:
        case_ref = raw.get("case_ref")
        doc_type = raw.get("doc_type", "tribunal")
        header_lines = []
        if case_ref:
            header_lines.append(f"Case reference: {case_ref}")
        header_lines.append(
            "Body: UCI Anti-Doping Tribunal"
            if doc_type == "tribunal"
            else "Body: Court of Arbitration for Sport (CAS) — appeal against UCI ADT"
        )
        header = "\n".join(header_lines)
        body = raw.get("_text", "")
        text = f"{header}\n\n{body}" if body else body

        return {
            "_id": f"uci-adt-{raw['asset_id']}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", "UCI Anti-Doping Tribunal decision"),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("pdf_url", LISTING_URL),
            "case_reference": case_ref or "",
            "document_type": doc_type,
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = UCIAntiDopingTribunalScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_entries()
        for e in entries[:60]:
            print(f"  {e['doc_type']:9}  {str(e['case_ref'])[:22]:22}  {e['title'][:50]}")
        print(f"\nTotal: {len(entries)} entries")
        sys.exit(0)

    if command in ("bootstrap", "bootstrap-fast"):
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
