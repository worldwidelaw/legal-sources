#!/usr/bin/env python3
"""
FJ/Laws -- Laws of Fiji (laws.gov.fj)

Official consolidated legislation of Fiji, published by the Office of the
Attorney-General.

Rebuilt 2026-08-03 for issue #1355. laws.gov.fj is now an Angular SPA: the old
server-rendered scheme (/acts/actlist/{A-Z}, /Acts/DisplayAct/{id},
/Acts/ViewSection/{id}) no longer exists, so the previous scraper enumerated
nothing and every run fell back to the committed samples.

The SPA is backed by a plain unauthenticated JSON API at /api. Endpoints used
(all discovered from the app bundle's ApiService):

  /api/get_all_acts                  index of consolidated principal acts
  /api/get_act_by_id/{ActId}         nested section tree (LegalId per node)
  /api/retrieve_html/{LegalId}       the node's own HTML body
  /api/toc_lawsaspublished           index of "Laws as Published" PDFs
                                     (Acts + Legal Notices, i.e. subsidiary
                                     legislation), keyed by year
  /api/show_pdf_lawsaspublished/{Id} that PDF, base64 in JSON
  /api/toc_omittedrepealed           index of omitted/repealed law volumes
  /api/show_pdf_omittedrepealed/{Id} that volume, base64 in JSON
  /api/retrieve_constitution         the 2013 Constitution, base64 in JSON

A consolidated act's text is assembled by walking its section tree in
pre-order and concatenating each node's section_html: parent nodes carry the
part/chapter heading, leaves carry the body.

Usage:
  python bootstrap.py bootstrap            # sample pull into sample/
  python bootstrap.py bootstrap --sample   # same, explicit
  python bootstrap.py bootstrap --full     # full pull to data/records.jsonl
  python bootstrap.py bootstrap-fast       # full pull, concurrent
  python bootstrap.py test                 # connectivity / inventory test
"""

import sys
import json
import base64
import logging
import re
import html as html_module
import threading
from itertools import zip_longest
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    from common.pdf_extract import _extract as _pdf_extract
except Exception:  # pragma: no cover - fall back to bare PyMuPDF
    _pdf_extract = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FJ.Laws")

BASE_URL = "https://www.laws.gov.fj"
API = "/api"

MIN_TEXT_CHARS = 200


def clean_html(text: str) -> str:
    """Strip HTML tags and decode entities, keeping block-level line breaks."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.I)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.I)
    # The section HTML hides cross-reference anchors in zero-content spans.
    text = re.sub(r'<span class="hidden"[^>]*>.*?</span>', "", text, flags=re.DOTALL | re.I)
    text = re.sub(r"<br\s*/?\s*>", "\n", text, flags=re.I)
    # Every text run sits in its own <span>; without a separator the runs weld
    # together ("accidentmeans an accident...", "(a)in the case of...").
    text = re.sub(r"</span\s*>", " ", text, flags=re.I)
    text = re.sub(r"</dt\s*>", " ", text, flags=re.I)
    for tag in ("p", "div", "li", "tr", "dd", "dl", "h1", "h2", "h3", "h4", "section"):
        text = re.sub(rf"</{tag}\s*>", "\n", text, flags=re.I)
    text = re.sub(r"</t[dh]\s*>", "\t", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_module.unescape(text)
    text = re.sub("[\\xa0\\u2000-\\u200a\\u202f\\u205f\\u3000]", " ", text)
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.split("\n")]
    out = "\n".join(ln for ln in lines if ln).strip()
    # Repair the separator we just inserted in front of punctuation.
    out = re.sub(r" +([,.;:)\]])", r"\1", out)
    out = re.sub(r"([(\[]) +", r"\1", out)
    return out


def pdf_to_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes via the shared extractor, else PyMuPDF."""
    if _pdf_extract is not None:
        try:
            text = _pdf_extract(pdf_bytes)
            if text and text.strip():
                return text.strip()
        except Exception as e:
            logger.debug(f"Shared PDF extractor failed: {e}")
    try:
        import fitz

        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            return "\n".join(page.get_text() for page in doc).strip()
    except Exception as e:
        logger.debug(f"PyMuPDF failed: {e}")
        return ""


def walk_sections(nodes: List[Dict[str, Any]]) -> Generator[Dict[str, Any], None, None]:
    """Yield every node of an act's section tree in pre-order."""
    for node in nodes or []:
        yield node
        for child in walk_sections(node.get("SectionData") or []):
            yield child


def iso_date(value: Optional[str]) -> Optional[str]:
    """Normalize the API's assorted date spellings to ISO 8601 (date only)."""
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", value)
    if m:
        return m.group(0)
    if re.match(r"\d{1,2}\s+\w+\s+\d{4}$", value):
        try:
            return datetime.strptime(value, "%d %B %Y").date().isoformat()
        except ValueError:
            pass
    if re.match(r"^\d{4}$", value):
        return f"{value}-01-01"
    return None


class FijiLawsScraper(BaseScraper):
    """
    Scraper for FJ/Laws -- Laws of Fiji.
    Country: FJ
    URL: https://www.laws.gov.fj

    Data types: legislation
    Auth: none (free public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json, text/plain, */*",
                "Referer": f"{BASE_URL}/",
            },
            timeout=120,
        )
        self._ckpt_path = source_dir / "data" / "checkpoint.json"
        self._ckpt_lock = threading.Lock()
        self._done = self._load_checkpoint()
        self._pending = 0
        # Sample runs must not poison the checkpoint for the full run.
        self._checkpoint_enabled = True

    # ── checkpoint ───────────────────────────────────────────────────

    def _load_checkpoint(self) -> set:
        try:
            data = json.loads(self._ckpt_path.read_text())
            return set(data.get("done") or [])
        except Exception:
            return set()

    def _flush_checkpoint(self) -> None:
        if not self._checkpoint_enabled:
            return
        try:
            self._ckpt_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._ckpt_path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"done": sorted(self._done)}))
            tmp.replace(self._ckpt_path)
        except Exception as e:
            logger.debug(f"Checkpoint write failed: {e}")

    def _mark_done(self, key: str) -> None:
        with self._ckpt_lock:
            self._done.add(key)
            self._pending += 1
            if self._pending >= 25:
                self._pending = 0
                self._flush_checkpoint()

    # ── API helpers ──────────────────────────────────────────────────

    def _api_json(self, path: str) -> Any:
        self.rate_limiter.wait()
        resp = self.client.get(f"{API}{path}")
        resp.raise_for_status()
        return resp.json()

    def _section_html(self, legal_id: str) -> Dict[str, Any]:
        from urllib.parse import quote

        return self._api_json(f"/retrieve_html/{quote(legal_id, safe='')}")

    @staticmethod
    def _flatten_toc(toc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """toc_* payloads are {group: [{year_or_key: [items]}, ...]} or
        {group: [items]}. Flatten to a plain item list."""
        items: List[Dict[str, Any]] = []
        for group, groups in (toc or {}).items():
            for entry in groups or []:
                if not isinstance(entry, dict):
                    continue
                if "Id" in entry:  # already an item
                    entry.setdefault("Type", group)
                    items.append(entry)
                    continue
                for _key, bucket in entry.items():
                    for item in bucket or []:
                        item.setdefault("Type", group)
                        items.append(item)
        return items

    # ── inventory ────────────────────────────────────────────────────

    def _inventory(self) -> List[Dict[str, Any]]:
        """Build the full work list of documents to fetch.

        The three families are round-robined so that a --sample run covers
        consolidated acts, as-published PDFs and repealed volumes alike.
        """
        act_entries: List[Dict[str, Any]] = []
        pub_entries: List[Dict[str, Any]] = []
        om_entries: List[Dict[str, Any]] = []

        acts = self._api_json("/get_all_acts")
        for act in acts:
            act_entries.append({
                "kind": "act",
                "id": str(act["ActId"]),
                "title": (act.get("ActName") or "").strip(),
                "last_updated": act.get("LastUpdated"),
                "status": act.get("Status"),
            })
        logger.info(f"Consolidated acts: {len(acts)}")

        published = self._flatten_toc(self._api_json("/toc_lawsaspublished"))
        for item in published:
            pub_entries.append({
                "kind": "published",
                "id": str(item["Id"]),
                "title": (item.get("Title") or item.get("PDFFile_Name") or "").strip(),
                "year": item.get("Year"),
                "doc_type": item.get("Type"),
                "status": item.get("Status"),
            })
        logger.info(f"Laws as published (PDF): {len(published)}")

        omitted = self._flatten_toc(self._api_json("/toc_omittedrepealed"))
        for item in omitted:
            om_entries.append({
                "kind": "omittedrepealed",
                "id": str(item["Id"]),
                "title": (item.get("Title") or "").strip(),
                "doc_type": item.get("Type"),
                "status": item.get("Status"),
            })
        logger.info(f"Omitted/repealed volumes (PDF): {len(omitted)}")

        inv: List[Dict[str, Any]] = [{
            "kind": "constitution",
            "id": "constitution",
            "title": "Constitution of the Republic of Fiji",
        }]
        for group in zip_longest(act_entries, pub_entries, om_entries):
            inv.extend(e for e in group if e is not None)

        logger.info(f"Total inventory: {len(inv)} documents")
        return inv

    # ── per-document full text ───────────────────────────────────────

    def _act_text(self, act_id: str) -> Dict[str, Any]:
        """Assemble a consolidated act's full text from its section tree."""
        tree = self._api_json(f"/get_act_by_id/{act_id}")
        if not tree:
            return {}
        act = tree[0]
        nodes = list(walk_sections(act.get("Sections") or []))

        parts: List[str] = []
        description = ""
        last_updated = None
        fetched = 0
        for node in nodes:
            legal_id = node.get("LegalId")
            if not legal_id:
                continue
            try:
                payload = self._section_html(legal_id)
            except Exception as e:
                logger.debug(f"section {legal_id} failed: {e}")
                continue
            fetched += 1
            if not description:
                description = clean_html(payload.get("act_description") or "")
            if not last_updated:
                last_updated = payload.get("act_last_updated")
            body = clean_html(payload.get("section_html") or "")
            if body:
                parts.append(body)

        return {
            "text": "\n\n".join(parts),
            "description": description,
            "last_updated": last_updated,
            "section_count": len(nodes),
            "sections_fetched": fetched,
            "act_name": (act.get("ActName") or "").strip(),
        }

    def _pdf_text(self, endpoint: str, doc_id: str) -> Dict[str, Any]:
        path = f"/{endpoint}/{doc_id}" if doc_id else f"/{endpoint}"
        payload = self._api_json(path)
        if isinstance(payload, list):
            payload = payload[0] if payload else {}
        b64 = payload.get("PDFFile") or payload.get("English_PDFFile") or ""
        if not b64:
            return {}
        try:
            pdf_bytes = base64.b64decode(b64)
        except Exception as e:
            logger.debug(f"base64 decode failed for {endpoint}/{doc_id}: {e}")
            return {}
        return {
            "text": pdf_to_text(pdf_bytes),
            "title": (payload.get("Title") or payload.get("PDFFile_Name") or "").strip(),
            "year": payload.get("Year"),
            "doc_type": payload.get("Type"),
        }

    # ── BaseScraper interface ────────────────────────────────────────

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        kind = raw["kind"]
        doc_id = raw["id"]
        key = f"{kind}:{doc_id}"
        now = datetime.now(timezone.utc).isoformat()

        title = raw.get("title") or ""
        extra: Dict[str, Any] = {}

        try:
            if kind == "act":
                info = self._act_text(doc_id)
                text = info.get("text", "")
                title = title or info.get("act_name") or ""
                date = iso_date(raw.get("last_updated")) or iso_date(info.get("last_updated"))
                url = f"{BASE_URL}/acts/displayact/{doc_id}"
                extra = {
                    "description": info.get("description") or None,
                    "section_count": info.get("section_count", 0),
                    "sections_fetched": info.get("sections_fetched", 0),
                    "document_class": "consolidated_act",
                    "status": raw.get("status"),
                }
            elif kind == "published":
                info = self._pdf_text("show_pdf_lawsaspublished", doc_id)
                text = info.get("text", "")
                title = title or info.get("title") or ""
                date = iso_date(raw.get("year") or info.get("year"))
                url = f"{BASE_URL}{API}/show_pdf_lawsaspublished/{doc_id}"
                extra = {
                    "document_class": "law_as_published",
                    "category": raw.get("doc_type") or info.get("doc_type"),
                    "year": raw.get("year") or info.get("year"),
                    "status": raw.get("status"),
                }
            elif kind == "omittedrepealed":
                info = self._pdf_text("show_pdf_omittedrepealed", doc_id)
                text = info.get("text", "")
                title = title or info.get("title") or ""
                date = iso_date(info.get("year"))
                url = f"{BASE_URL}{API}/show_pdf_omittedrepealed/{doc_id}"
                extra = {
                    "document_class": "omitted_or_repealed",
                    "category": raw.get("doc_type"),
                    "status": raw.get("status"),
                }
            elif kind == "constitution":
                info = self._pdf_text("retrieve_constitution", "")
                text = info.get("text", "")
                title = "Constitution of the Republic of Fiji (2013)"
                date = "2013-09-07"
                url = f"{BASE_URL}{API}/retrieve_constitution"
                extra = {"document_class": "constitution"}
            else:
                return None
        except Exception as e:
            logger.warning(f"Failed {key}: {e}")
            return None

        if not text or len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text for {key} ({len(text)} chars): {title[:60]}")
            return None

        record = {
            "_id": "FJ/Laws/constitution" if kind == "constitution" else f"FJ/Laws/{kind}/{doc_id}",
            "_source": "FJ/Laws",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title or f"Laws of Fiji {kind} {doc_id}",
            "text": text,
            "date": date,
            "url": url,
            "jurisdiction": "FJ",
            "language": "en",
            "publisher": "Office of the Attorney-General of Fiji",
        }
        record.update({k: v for k, v in extra.items() if v is not None})

        self._mark_done(key)
        return record

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        inventory = self._inventory()
        if not inventory:
            raise RuntimeError("laws.gov.fj API returned an empty inventory")

        skipped = 0
        for entry in inventory:
            key = f"{entry['kind']}:{entry['id']}"
            if key in self._done:
                skipped += 1
                continue
            yield entry

        if skipped:
            logger.info(f"Skipped {skipped} documents already in checkpoint")
        self._flush_checkpoint()

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch acts consolidated on/after `since`. PDFs are immutable once
        published, so only the years from `since` onwards are re-pulled."""
        cutoff = since.date().isoformat()
        for entry in self._inventory():
            if entry["kind"] == "act":
                updated = iso_date(entry.get("last_updated"))
                if updated is None or updated >= cutoff:
                    yield entry
            elif entry["kind"] == "published":
                year = iso_date(entry.get("year"))
                if year is None or year >= f"{cutoff[:4]}-01-01":
                    yield entry


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
              "[--sample|--full]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv or (
        command == "bootstrap" and "--full" not in sys.argv
    )

    scraper = FijiLawsScraper()
    if sample_mode or command == "test":
        scraper._checkpoint_enabled = False

    if command == "test":
        inv = scraper._inventory()
        if not inv:
            logger.error("FAILED — empty inventory")
            sys.exit(1)
        probe = scraper.normalize(next(e for e in inv if e["kind"] == "act"))
        if not probe:
            logger.error("FAILED — could not assemble a sample act")
            sys.exit(1)
        print(f"Inventory OK: {len(inv)} documents")
        print(f"Sample act: {probe['title']} — {len(probe['text'])} chars")
        sys.exit(0)

    if command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command in ("bootstrap-fast", "bootstrap_fast"):
        scraper.bootstrap_fast()
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
