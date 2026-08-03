#!/usr/bin/env python3
"""
IE/CoimisiunNaMean -- Coimisiún na Meán (Ireland's media regulator) — Broadcasting
& Video-on-Demand Complaint Decisions.

Coimisiún na Meán is the statutory media regulator of Ireland (established
15 March 2023 under the Online Safety and Media Regulation Act 2022, succeeding
the Broadcasting Authority of Ireland). Among its statutory functions it
adjudicates complaints made by the public about content on broadcasters and
audiovisual on-demand media services under the Broadcasting Act 2009 (as amended)
and the associated media service codes/rules. The Commission is required to
publish notice of each complaint decision. These decisions — the Commission's
determination (and, where a complaint is referred, the Authorised Person's
investigation decision) — are quasi-judicial adjudications = case_law.

Each published decision records: the case reference (CAS-NNNNN), the programme,
the broadcast/VOD service, the broadcast/access date, the statute and/or
regulatory code invoked, a short description of the complaint, the outcome, the
Commission's decision (and date), and any Authorised Person's decision (and date).

Strategy:
  - The decisions live in a server-rendered, paginated list ("decision-box"
    cards) at the complaint-decisions page, /page/N/ (100 per page).
  - Each card is an <h3>CAS-NNNNN</h3> header followed by a <dl class="row"> of
    <dt> label / <dd> value pairs. We enumerate every page until a page has no
    cards, parse the fields per card, and assemble the full published decision
    text from those fields (born-digital HTML — no OCR/PDF needed).

Data:
  - ~205 complaint decisions (2023-present + transitional pre-2023 complaints
    carried over from the Broadcasting Authority of Ireland).
  - Language: English
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (re-scan, loader dedups)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.CoimisiunNaMean")

BASE_URL = "https://www.cnam.ie"
LIST_PATH = ("/general-public/report-complain/"
             "something-i-saw-on-tv-on-demand-or-heard-on-radio/complaint-decisions")
MAX_PAGES = 50  # safety ceiling; real corpus is ~3 pages

# One decision "card": <h3> CAS-NNNNN </h3> ... <dl class="row"> ... </dl>
CARD_RE = re.compile(
    r"<h3>\s*(CAS-\d+)\s*</h3>.*?<dl class=\"row\">(.*?)</dl>",
    re.IGNORECASE | re.DOTALL,
)
# Ordered (label, value) pairs inside a <dl>.
PAIR_RE = re.compile(
    r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>",
    re.IGNORECASE | re.DOTALL,
)
DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")


def _strip(fragment: str) -> str:
    """Strip tags and decode entities from an HTML fragment -> clean text."""
    text = re.sub(r"<[^>]+>", " ", fragment)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    return text.strip()


def _iso_date(value: str) -> Optional[str]:
    """Convert a 'dd.mm.yyyy' value to ISO 'yyyy-mm-dd'."""
    m = DATE_RE.search(value or "")
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    try:
        datetime(int(yyyy), int(mm), int(dd))
    except ValueError:
        return None
    return f"{yyyy}-{mm}-{dd}"


class CoimisiunNaMeanScraper(BaseScraper):
    """Scraper for Coimisiún na Meán complaint decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-IE,en;q=0.9",
            },
            timeout=90,
        )

    def _list_page(self, page: int) -> Optional[str]:
        path = LIST_PATH + "/" if page == 1 else f"{LIST_PATH}/page/{page}/"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            if resp.status_code != 200:
                logger.warning(f"list page {page}: HTTP {resp.status_code}")
                return None
            return resp.content.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"Error listing page {page}: {e}")
            return None

    @staticmethod
    def _parse_cards(page_html: str) -> List[Tuple[str, List[Tuple[str, str]]]]:
        """Return [(case_ref, [(label, value), ...]), ...] for a listing page."""
        cards = []
        for ref, dl in CARD_RE.findall(page_html):
            pairs = []
            for label_frag, value_frag in PAIR_RE.findall(dl):
                label = _strip(label_frag).rstrip(":").strip()
                value = _strip(value_frag)
                if label:
                    pairs.append((label, value))
            cards.append((ref.strip(), pairs))
        return cards

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        seen = set()
        any_card = False
        for page in range(1, MAX_PAGES + 1):
            html_doc = self._list_page(page)
            if not html_doc:
                break
            cards = self._parse_cards(html_doc)
            if not cards:
                # No decision cards on this page -> reached the end.
                break
            new_on_page = 0
            for ref, pairs in cards:
                if ref in seen:
                    continue
                seen.add(ref)
                any_card = True
                new_on_page += 1
                yield {"ref": ref, "pairs": pairs}
            logger.info(f"page {page}: {len(cards)} cards ({new_on_page} new)")
            if new_on_page == 0:
                break
        if not any_card:
            raise RuntimeError(
                "Coimisiún na Meán listing returned 0 decision cards — "
                "site blocked or markup changed"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        # The listing is small and unpartitioned by date; re-scan fully and let
        # the loader dedup on _id. `since` filtering is applied on decision date.
        for raw in self.fetch_all():
            fields = {label: value for label, value in raw["pairs"]}
            iso = (_iso_date(fields.get("Authorised Person's Decision Date", ""))
                   or _iso_date(fields.get("Commission Decision Date", "")))
            if iso and iso >= since.strftime("%Y-%m-%d"):
                yield raw
            elif not iso:
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ref = raw.get("ref", "")
        pairs: List[Tuple[str, str]] = raw.get("pairs", [])
        if not ref or not pairs:
            return None

        fields = {label: value for label, value in pairs}

        programme = fields.get("Programme", "")
        service = fields.get("Broadcast / VOD service", "")
        broadcast_date = fields.get("Date broadcast or accessed", "")
        code = fields.get("Statute and/or regulatory code", "")
        description = fields.get("Short description", "")
        outcome = fields.get("Outcome", "")
        commission_decision = fields.get("Commission Decision", "")
        commission_date = fields.get("Commission Decision Date", "")
        ap_decision = fields.get("Authorised Person's Decision", "")
        ap_date = fields.get("Authorised Person's Decision Date", "")

        # Assemble the full published decision text from all rendered fields,
        # preserving the ordered label/value structure.
        text_lines = []
        for label, value in pairs:
            if value:
                text_lines.append(f"{label}: {value}")
        text = "\n".join(text_lines).strip()
        if len(text) < 80:
            return None

        # Decision date: prefer the Authorised Person's (later) decision date,
        # else the Commission's decision date.
        iso_date = _iso_date(ap_date) or _iso_date(commission_date)

        title_bits = [ref]
        if programme:
            title_bits.append(programme)
        if service:
            title_bits.append(service)
        title = " — ".join(title_bits)

        url = f"{BASE_URL}/decision/{ref.lower()}/"

        record = {
            "_id": f"CnaM-{ref}",
            "_source": "IE/CoimisiunNaMean",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date,
            "url": url,
            "case_reference": ref,
            "programme": programme,
            "service": service,
            "broadcast_date": broadcast_date,
            "statute_code": code,
            "short_description": description,
            "outcome": outcome,
            "commission_decision": commission_decision,
            "commission_decision_date": _iso_date(commission_date),
            "authorised_person_decision": ap_decision,
            "authorised_person_decision_date": _iso_date(ap_date),
            "court": "Coimisiún na Meán",
            "jurisdiction": "IE",
            "language": "en",
        }
        return record

    def test_connection(self):
        print("Testing Coimisiún na Meán decisions listing...")
        html_doc = self._list_page(1)
        if not html_doc:
            print("  page 1 FETCH FAILED")
            return
        cards = self._parse_cards(html_doc)
        print(f"  page 1: {len(cards)} decision cards")
        if cards:
            ref, pairs = cards[0]
            print(f"  first: {ref} ({len(pairs)} fields)")
            rec = self.normalize({"ref": ref, "pairs": pairs})
            if rec:
                print(f"    title: {rec['title']}")
                print(f"    date:  {rec['date']}")
                print(f"    text:  {len(rec['text'])} chars")


def main():
    scraper = CoimisiunNaMeanScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
