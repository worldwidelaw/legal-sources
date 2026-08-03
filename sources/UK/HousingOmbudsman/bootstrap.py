#!/usr/bin/env python3
"""
Legal Data Hunter - UK Housing Ombudsman Service (England) Decisions Scraper

Fetches the published determinations of the Housing Ombudsman Service — the
statutory dispute-resolution body (established under the Housing Act 1996,
operating the Housing Ombudsman Scheme approved by the Secretary of State) that
investigates complaints by residents against member landlords (social landlords,
local authorities and voluntary members). Its determinations decide whether the
landlord's handling amounted to maladministration / service failure and impose
orders and recommendations; they are binding on scheme members = case_law.

Source: https://www.housing-ombudsman.org.uk/decisions/
  - The decisions listing is a paginated WordPress archive
    (/decisions/page/{n}/) linking to each individual determination page
    /decisions/{landlord-slug}-{caseref}/.
  - Each determination page embeds the FULL TEXT in HTML: a metadata table
    (Case ID, Decision type, Jurisdiction, Landlord, Landlord type, Occupancy,
    Date) followed by the Background / What the complaint is about / Our
    decision (determination) / Reasons / Orders narrative.
  - No public WP REST API for the `decision` post type (404); we scrape the
    server-rendered pages directly.

Coverage: ~17,000+ determinations (2020-present; the archive currently spans
~1,674 listing pages). Born-digital HTML, no OCR needed.

License: no explicit Open Government Licence statement; the site asserts
"© Housing Ombudsman Service". Treated as custom terms, commercial use flagged.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/HousingOmbudsman")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

MIN_TEXT_CHARS = 200
# Skip generic listing/navigation noise lines in the decision body.
NOISE_LINES = {"back to top", "decision", ""}
# Individual decision permalink: /decisions/{slug}/ (not page/, feed/, or bare)
DECISION_RE = re.compile(
    r"^https://www\.housing-ombudsman\.org\.uk/decisions/(?!page/|feed/)[^/]+/$"
)


class UKHousingOmbudsmanScraper(BaseScraper):
    """Scraper for Housing Ombudsman Service (England) determinations."""

    BASE_URL = "https://www.housing-ombudsman.org.uk"
    LISTING_URL = BASE_URL + "/decisions/page/{page}/"
    LISTING_FIRST = BASE_URL + "/decisions/"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; legal research)",
            "Accept": "text/html,application/xhtml+xml",
        })

    # ------------------------------------------------------------------- fetch
    def _get(self, url: str) -> Optional[str]:
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=45)
            if resp.status_code == 200:
                return resp.text
            logger.warning(f"{resp.status_code} for {url}")
            return None
        except Exception as e:
            logger.warning(f"Request failed for {url}: {e}")
            return None

    # --------------------------------------------------------------- discovery
    def _last_page(self, html: str) -> int:
        pages = [int(n) for n in re.findall(r"/decisions/page/(\d+)/", html)]
        return max(pages) if pages else 1

    def _listing_links(self, html: str) -> list:
        soup = BeautifulSoup(html, "html.parser")
        links = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].split("?")[0].split("#")[0]
            if DECISION_RE.match(href) and href not in seen:
                seen.add(href)
                links.append(href)
        return links

    # --------------------------------------------------------------- extraction
    @staticmethod
    def _parse_meta_and_text(html: str) -> Optional[dict]:
        soup = BeautifulSoup(html, "html.parser")
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""

        main = soup.find("main") or soup
        for tag in main(["script", "style", "nav", "header", "footer", "form"]):
            tag.decompose()

        # Metadata table (label / value pairs).
        meta = {}
        table = main.find("table")
        if table:
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    label = cells[0].get_text(" ", strip=True).rstrip(":").strip()
                    value = cells[1].get_text(" ", strip=True)
                    value = re.sub(r"\s+", " ", value).strip()
                    if label:
                        meta[label] = value

        # Full text: the whole article content (metadata table + narrative),
        # cleaned of navigation noise.
        raw = main.get_text("\n", strip=True)
        lines = []
        for ln in raw.splitlines():
            ln = ln.strip()
            if ln.lower() in NOISE_LINES:
                continue
            lines.append(ln)
        text = "\n".join(lines).strip()

        return {"title": title, "meta": meta, "text": text}

    def _build_raw(self, url: str, parsed: dict) -> dict:
        meta = parsed["meta"]
        return {
            "url": url,
            "title": parsed["title"],
            "text": parsed["text"],
            "case_id": meta.get("Case ID", "").strip(),
            "decision_type": meta.get("Decision type", "").strip(),
            "jurisdiction_field": meta.get("Jurisdiction", "").strip(),
            "landlord": meta.get("Landlord", "").strip(),
            "landlord_type": meta.get("Landlord type", "").strip(),
            "occupancy": meta.get("Occupancy", "").strip(),
            "date_text": meta.get("Date", "").strip(),
        }

    # ---------------------------------------------------------------- iteration
    def fetch_all(self) -> Generator[dict, None, None]:
        first = self._get(self.LISTING_FIRST)
        if not first:
            raise RuntimeError(
                "Housing Ombudsman listing unreachable — possible IP block "
                "(fail loud rather than emit an empty corpus)"
            )
        last = self._last_page(first)
        logger.info(f"Decisions archive: {last} listing pages")

        count = 0
        skipped = 0
        for page in range(1, last + 1):
            html = first if page == 1 else self._get(self.LISTING_URL.format(page=page))
            if not html:
                continue
            for url in self._listing_links(html):
                dhtml = self._get(url)
                if not dhtml:
                    skipped += 1
                    continue
                parsed = self._parse_meta_and_text(dhtml)
                if not parsed or len(parsed["text"]) < MIN_TEXT_CHARS:
                    skipped += 1
                    continue
                count += 1
                yield self._build_raw(url, parsed)
            if page % 25 == 0:
                logger.info(f"  page {page}/{last} — {count} decisions ({skipped} skipped)")
        logger.info(f"Total: {count} determinations ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Newest decisions are listed first; walk pages until we pass `since`.

        The listing has no per-item date, so we fetch each decision page and
        stop once a full page yields only decisions older than `since`.
        """
        first = self._get(self.LISTING_FIRST)
        if not first:
            return
        last = self._last_page(first)
        for page in range(1, last + 1):
            html = first if page == 1 else self._get(self.LISTING_URL.format(page=page))
            if not html:
                continue
            page_new = 0
            for url in self._listing_links(html):
                dhtml = self._get(url)
                if not dhtml:
                    continue
                parsed = self._parse_meta_and_text(dhtml)
                if not parsed or len(parsed["text"]) < MIN_TEXT_CHARS:
                    continue
                raw = self._build_raw(url, parsed)
                d = self._parse_date(raw.get("date_text", ""))
                if d and d >= since.strftime("%Y-%m-%d"):
                    page_new += 1
                    yield raw
            if page > 1 and page_new == 0:
                break

    # ---------------------------------------------------------------- normalize
    @staticmethod
    def _parse_date(text: str) -> Optional[str]:
        if not text:
            return None
        # Cells sometimes render "18May 2026" (day digits in separate spans).
        m = re.search(r"(\d{1,2})\s*([A-Za-z]+)\s*(\d{4})", text)
        if not m:
            return None
        day, mon, year = m.group(1), m.group(2).lower(), m.group(3)
        month = MONTHS.get(mon)
        if not month:
            return None
        try:
            return f"{int(year):04d}-{month:02d}-{int(day):02d}"
        except ValueError:
            return None

    def normalize(self, raw: dict) -> dict:
        text = (raw.get("text", "") or "").strip()
        if not text:
            return None

        case_id = raw.get("case_id", "").strip()
        if not case_id:
            # Fall back to the case reference embedded in the URL/title.
            m = re.search(r"(\d{6,})", raw.get("url", "") + " " + raw.get("title", ""))
            case_id = m.group(1) if m else raw.get("url", "").rstrip("/").split("/")[-1]

        return {
            "_id": f"UK/HousingOmbudsman/{case_id}",
            "_source": "UK/HousingOmbudsman",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_id": case_id,
            "title": raw.get("title", ""),
            "text": text,
            "date": self._parse_date(raw.get("date_text", "")),
            "decision_type": raw.get("decision_type", "") or None,
            "jurisdiction_outcome": raw.get("jurisdiction_field", "") or None,
            "landlord": raw.get("landlord", "") or None,
            "landlord_type": raw.get("landlord_type", "") or None,
            "occupancy": raw.get("occupancy", "") or None,
            "url": raw.get("url", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKHousingOmbudsmanScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd in ("bootstrap", "bootstrap-fast"):
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
