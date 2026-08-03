#!/usr/bin/env python3
"""
INTL/EnergyCharterTreaty -- Energy Charter Treaty investment-arbitration documents

Fetches the full-text legal documents published by the Energy Charter Secretariat
for investment-arbitration cases brought under Article 26 of the Energy Charter
Treaty (ECT). For a curated set of landmark ECT disputes the Secretariat hosts, in
full, the arbitral awards, decisions on jurisdiction, provisional-measures orders
and the national-court judgments rendered in the subsequent set-aside / enforcement
proceedings (Svea Court of Appeal, Hague District Court and Court of Appeal, etc.).

Strategy:
  - The public "List of cases" page (energychartertreaty.org/cases/list-of-cases/)
    embeds, per case folder, anchors to every document PDF hosted under
    /fileadmin/DocumentsMedia/Cases/<folder>/<file>.pdf. The page is plain server
    HTML — no login, no WAF, reachable from any IP.
  - We parse every such anchor, drop the per-case flowchart/summary "Chart" PDFs,
    derive a clean case name from the folder + filename, and download each document.
  - Each PDF is a full reasoned award/decision/judgment; we extract its full text
    via the shared OOM-hardened PDF extractor and prepend a short metadata header.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Print parsed document entries
"""

import sys
import json
import html
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.EnergyCharterTreaty")

SOURCE_ID = "INTL/EnergyCharterTreaty"
BASE_URL = "https://www.energychartertreaty.org"
LISTING_URL = "https://www.energychartertreaty.org/cases/list-of-cases/"

# Anchor capturing a case-document PDF under the Cases folder, plus its link text.
_ANCHOR_RE = re.compile(
    r'<a[^>]+href="([^"]*?/fileadmin/DocumentsMedia/Cases/[^"]+?\.pdf)"[^>]*>(.*?)</a>',
    re.I | re.S,
)
_FOLDER_RE = re.compile(r"/Cases/([^/]+)/", re.I)

# Curated full case names for the landmark ECT disputes the Secretariat publishes.
# Keyed by a lowercase token found in the case folder or the document filename.
_CASE_NAMES = {
    "nykomb": "Nykomb Synergetics Technology Holding AB v. Republic of Latvia "
              "(SCC Case No. 118/2001)",
    "plama": "Plama Consortium Limited v. Republic of Bulgaria "
             "(ICSID Case No. ARB/03/24)",
    "petrobart": "Petrobart Limited v. Kyrgyz Republic (SCC Case No. 126/2003)",
    "yukos": "Yukos Universal Limited v. Russian Federation "
             "(PCA Case No. AA 227)",
    "hulley": "Hulley Enterprises Limited v. Russian Federation "
              "(PCA Case No. AA 226)",
    "veteran": "Veteran Petroleum Limited v. Russian Federation "
               "(PCA Case No. AA 228)",
    "kardassopoulos": "Ioannis Kardassopoulos v. Republic of Georgia "
                      "(ICSID Case No. ARB/05/18)",
    "amto": "Limited Liability Company Amto v. Ukraine (SCC Case No. 080/2005)",
}

_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}
_DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})")


class EnergyCharterTreatyScraper(BaseScraper):
    """Scraper for Energy Charter Treaty investment-arbitration documents."""

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

    def _case_name(self, subpath: str, folder: str) -> str:
        """Resolve a full case name from the case folder and the sub-path.

        When the folder unambiguously names a single claimant we trust it (its
        award files are often code-named, e.g. ISDSC-002a.pdf, with no claimant
        token). The shared "6_8_Yukos_Hulley_Veteran" folder names all three
        claimants, so there we disambiguate on the sub-path (subfolder/filename).
        """
        tokens = ("hulley", "veteran", "yukos", "kardassopoulos", "nykomb",
                  "plama", "petrobart", "amto")
        in_folder = [t for t in tokens if t in folder.lower()]
        if len(in_folder) == 1:
            return _CASE_NAMES[in_folder[0]]
        # Multi-claimant (or unmarked) folder: disambiguate on the sub-path.
        for token in tokens:
            if token in subpath.lower():
                return _CASE_NAMES[token]
        if len(in_folder) > 1:
            # Shared Yukos-group enforcement judgment — no single claimant.
            return ("Yukos Universal Limited, Hulley Enterprises Limited and "
                    "Veteran Petroleum Limited v. Russian Federation "
                    "(PCA Cases No. AA 226/227/228)")
        # Fallback: humanize the folder name (drop leading index, underscores).
        name = re.sub(r"^\d+(_\d+)*_", "", folder).replace("_", " ").strip()
        return name or folder

    def _doc_label(self, link_text: str, filename: str) -> str:
        """Human label for the document type (award/decision/judgment)."""
        t = re.sub(r"<[^>]+>", "", link_text)
        t = html.unescape(t).replace("\xa0", " ")
        t = re.sub(r"\s+", " ", t).strip(" -–—\t")
        if t and t.lower() not in ("flowchart", "dutch", "english",
                                   "english (unofficial translation)"):
            return t
        # Decode the Secretariat's ISDSC document-code suffix, if present.
        m = re.match(r"ISDSC-\d+([a-z]+)", Path(filename).stem, re.I)
        if m:
            suffix = m.group(1).lower()
            return {
                "a": "Arbitral Award",
                "dj": "Decision on Jurisdiction",
                "ija": "Interim Award on Jurisdiction and Admissibility",
            }.get(suffix, "Arbitral Award")
        # Derive from the filename.
        stem = unquote(Path(filename).stem)
        stem = re.sub(r"^(UPD_|\d+\.)", "", stem)
        stem = stem.replace("_", " ").strip()
        return stem or "Document"

    def _parse_date(self, text: str) -> Optional[str]:
        if not text:
            return None
        for m in _DATE_RE.finditer(text[:8000]):
            month = _MONTHS.get(m.group(2).lower())
            if not month:
                continue
            try:
                return datetime(int(m.group(3)), month,
                                int(m.group(1))).date().isoformat()
            except ValueError:
                continue
        return None

    def _get_entries(self) -> list[dict]:
        resp = self.session.get(LISTING_URL, timeout=60)
        resp.raise_for_status()
        html = resp.content.decode("utf-8", errors="replace")

        # Collect every anchor, then pick the best (most descriptive) label per
        # unique PDF — the page links each document twice, once with a blank/icon
        # anchor and once with a descriptive one.
        best_label: dict[str, str] = {}
        order: list[str] = []
        for href, link_text in _ANCHOR_RE.findall(html):
            filename = href.split("/")[-1]
            # Skip per-case flowchart/summary PDFs (named "Chart" or numbered "N._").
            if "chart" in href.lower() or re.match(r"^\d+\.", filename):
                continue
            label = self._doc_label(link_text, filename)
            if href not in best_label:
                order.append(href)
                best_label[href] = label
            elif len(label) > len(best_label[href]):
                best_label[href] = label

        entries = []
        for href in order:
            if best_label[href].strip().lower() == "flowchart":
                continue
            full_url = urljoin(BASE_URL, href)
            m = _FOLDER_RE.search(href)
            folder = m.group(1) if m else "Case"
            filename = href.split("/")[-1]
            subpath = href.split(f"/{folder}/", 1)[-1] if folder != "Case" else filename
            doc_id = re.sub(r"[^A-Za-z0-9]+", "-",
                            f"{folder}-{Path(unquote(filename)).stem}").strip("-").lower()
            case_name = self._case_name(subpath, folder)
            label = best_label[href]
            entries.append({
                "doc_id": doc_id,
                "case_name": case_name,
                "doc_label": label,
                "title": f"{case_name} — {label}",
                "pdf_url": full_url,
            })

        logger.info(f"Parsed {len(entries)} document entries from listing page")
        return entries

    # ── fetch ────────────────────────────────────────────────────────

    def _fetch_pdf_text(self, entry: dict) -> Optional[str]:
        try:
            time.sleep(1.0)
            resp = self.session.get(entry["pdf_url"], timeout=120)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"  PDF fetch failed for {entry['doc_id']}: {e}")
            return None
        ctype = resp.headers.get("Content-Type", "")
        if "pdf" not in ctype.lower() and resp.content[:4] != b"%PDF":
            logger.warning(f"  Not a PDF ({ctype}) for {entry['doc_id']}")
            return None
        return extract_pdf_markdown(
            SOURCE_ID,
            entry["doc_id"],
            pdf_bytes=resp.content,
            table="case_law",
            force=True,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._get_entries()
        logger.info(f"Total entries to process: {len(entries)}")
        for i, entry in enumerate(entries):
            try:
                logger.info(f"[{i+1}/{len(entries)}] {entry['title'][:70]}")
                text = self._fetch_pdf_text(entry)
                if not text or len(text) < 300:
                    logger.warning(
                        f"  Insufficient text for {entry['doc_id']}, skipping"
                    )
                    continue
                entry["_text"] = text
                entry["date"] = self._parse_date(text)
                yield entry
            except Exception as e:
                logger.error(f"  Error processing {entry['doc_id']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_iso = since.date().isoformat()
        for entry in self.fetch_all():
            if not entry.get("date") or entry["date"] >= since_iso:
                yield entry

    # ── normalize ────────────────────────────────────────────────────

    def normalize(self, raw: dict) -> dict:
        header = (
            f"Case: {raw['case_name']}\n"
            f"Document: {raw['doc_label']}\n"
            f"Forum: Investment arbitration under Article 26 of the Energy "
            f"Charter Treaty"
        )
        body = raw.get("_text", "")
        text = f"{header}\n\n{body}" if body else body
        return {
            "_id": f"ect-{raw['doc_id']}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": text,
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "case_name": raw["case_name"],
            "document_type": raw["doc_label"],
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = EnergyCharterTreatyScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_entries()
        for e in entries:
            print(f"  {e['doc_label'][:35]:35}  {e['case_name'][:45]}")
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
