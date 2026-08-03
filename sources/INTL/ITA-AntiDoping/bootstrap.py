#!/usr/bin/env python3
"""
INTL/ITA-AntiDoping -- International Testing Agency (ITA) Anti-Doping Rule Violations

Fetches the published anti-doping rule violation (ADRV) cases managed by the
International Testing Agency (ITA), the independent body that delivers anti-doping
programmes on behalf of the IOC and ~70 international federations and major-event
organisers. The ITA Legal Affairs Department conducts results management of ADRVs
from initial review to first-instance hearing panels (incl. the CAS Anti-Doping
Division) and CAS appeals, in line with the World Anti-Doping Code and the ITA's
Public Disclosure Policy.

Strategy:
  - The "Anti-Doping Rule Violations" page is a single server-rendered HTML table.
    Each case is a `<tr class="accordion sanction">` row (Athlete, Nationality,
    Sport, Sanction, Status) followed by a `<tr class="collapse">` detail panel
    carrying structured fields (Individual Type, ADRV article, Violation Date,
    Ineligibility, Results Management Authority, Disqualification, Means of
    Resolution) and a "READ MORE" link to the case's full reasoned news article
    at ita.sport/news/{slug}/.
  - We parse every case row + its detail panel, fetch the linked article, and
    extract the clean article body (substance, regulatory framework, ADRV finding,
    sanction and consequences). A structured metadata header is prepended so the
    full text record captures both the reasoning and the case particulars.

The cases are openly published (no login, no WAF) and reachable from any IP.

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

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ITA-AntiDoping")

LISTING_URL = "https://ita.sport/anti-doping-rule-violations/"

# Months for "DD Month YYYY" -> ISO.
_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
_DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})")


class ITAAntiDopingScraper(BaseScraper):
    """Scraper for ITA Anti-Doping Rule Violations."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en",
        })

    def _parse_date(self, text: str) -> Optional[str]:
        """Parse the first 'DD Month YYYY' found in text to ISO 'YYYY-MM-DD'."""
        if not text:
            return None
        m = _DATE_RE.search(text)
        if not m:
            return None
        day, mon, year = m.group(1), m.group(2).lower(), m.group(3)
        month = _MONTHS.get(mon)
        if not month:
            return None
        try:
            return datetime(int(year), month, int(day)).date().isoformat()
        except ValueError:
            return None

    def _slug_from_url(self, url: str) -> str:
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        slug = re.sub(r"[^a-z0-9]+", "-", slug.lower()).strip("-")
        return slug or "case"

    def _get_entries(self) -> list[dict]:
        """Parse the ADRV table into structured case entries."""
        resp = self.session.get(LISTING_URL, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        entries = []
        seen = set()
        for row in soup.select("tr.accordion.sanction"):
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td", recursive=False)]
            if not cells:
                continue
            name = re.sub(r"\s*,\s*", ", ", cells[0]).strip() if cells else ""
            nationality = cells[1].strip() if len(cells) > 1 else ""
            sport = cells[2].strip() if len(cells) > 2 else ""
            sanction = cells[3].strip() if len(cells) > 3 else ""
            status = cells[4].strip() if len(cells) > 4 else ""

            detail = row.find_next_sibling("tr")
            fields = {}
            article_url = None
            if detail is not None:
                for lbl in detail.select("div.field__label"):
                    val = lbl.find_next_sibling("div", class_="field__item")
                    key = lbl.get_text(" ", strip=True).rstrip(":").strip()
                    if val is not None and key:
                        fields[key] = val.get_text(" ", strip=True)
                for a in detail.find_all("a", href=True):
                    href = a["href"].strip()
                    if "/news/" in href:
                        article_url = href.replace("http://", "https://")
                        break

            if not article_url:
                # No reasoned article -> cannot guarantee full text; skip.
                continue
            if article_url in seen:
                continue
            seen.add(article_url)

            date_iso = (
                self._parse_date(fields.get("Violation Date", ""))
                or self._parse_date(fields.get("Ineligibility", ""))
            )

            entries.append({
                "id_slug": self._slug_from_url(article_url),
                "name": name,
                "nationality": nationality,
                "sport": sport,
                "sanction": sanction,
                "status": status,
                "fields": fields,
                "article_url": article_url,
                "date": date_iso,
            })

        logger.info(f"Parsed {len(entries)} case entries from listing page")
        return entries

    def _fetch_article_body(self, url: str) -> Optional[str]:
        """Fetch a case article and return the clean body text."""
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"  Article fetch failed: {e}")
            return None

        soup = BeautifulSoup(resp.content, "html.parser")
        # The article body is the main content column.
        body = None
        for sel in ("div.col-10.col-lg-9.py-5", "div.col-lg-9", "article", "main"):
            els = soup.select(sel)
            if els:
                body = max(els, key=lambda e: len(e.get_text(" ", strip=True)))
                break
        if body is None:
            return None

        # Drop scripts/styles/nav noise inside the container.
        for tag in body.find_all(["script", "style", "nav", "form"]):
            tag.decompose()
        text = body.get_text("\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip() or None

    def _build_text(self, entry: dict, body: str) -> str:
        """Prepend a structured metadata header to the reasoned article body."""
        f = entry["fields"]
        header_lines = []
        if entry["name"]:
            header_lines.append(f"Athlete/Person: {entry['name']}")
        if entry["nationality"]:
            header_lines.append(f"Nationality: {entry['nationality']}")
        if entry["sport"]:
            header_lines.append(f"Sport: {entry['sport']}")
        for k in ("Individual Type", "ADRV", "Violation Date", "Ineligibility",
                  "Results Management Authority", "Disqualification",
                  "Means Of Resolution"):
            if f.get(k) and f[k].strip().upper() != "N/A":
                header_lines.append(f"{k}: {f[k]}")
        if entry["sanction"]:
            header_lines.append(f"Sanction: {entry['sanction']}")
        if entry["status"]:
            header_lines.append(f"Status: {entry['status']}")
        header = "\n".join(header_lines)
        return f"{header}\n\n{body}" if header else body

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._get_entries()
        logger.info(f"Total entries to process: {len(entries)}")
        for i, entry in enumerate(entries):
            try:
                logger.info(
                    f"[{i+1}/{len(entries)}] {entry['id_slug'][:50]} - {entry['name'][:40]}"
                )
                body = self._fetch_article_body(entry["article_url"])
                if not body or len(body) < 150:
                    logger.warning(f"  Insufficient body text for {entry['id_slug']}, skipping")
                    continue
                entry["_text"] = self._build_text(entry, body)
                # Prefer the article's own published date if present in the body.
                yield entry
            except Exception as e:
                logger.error(f"  Error processing {entry['id_slug']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_iso = since.date().isoformat()
        for entry in self.fetch_all():
            if not entry.get("date") or entry["date"] >= since_iso:
                yield entry

    def normalize(self, raw: dict) -> dict:
        name = raw.get("name", "").strip()
        sport = raw.get("sport", "").strip()
        title = f"ITA Anti-Doping Case — {name}" if name else "ITA Anti-Doping Case"
        if sport:
            title += f" ({sport})"
        return {
            "_id": f"ita-{raw.get('id_slug', 'case')}",
            "_source": "INTL/ITA-AntiDoping",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": raw.get("date"),
            "url": raw.get("article_url", LISTING_URL),
            "athlete": name,
            "nationality": raw.get("nationality", ""),
            "sport": sport,
            "sanction": raw.get("sanction", ""),
            "status": raw.get("status", ""),
            "adrv": raw.get("fields", {}).get("ADRV", ""),
            "results_management_authority": raw.get("fields", {}).get(
                "Results Management Authority", ""),
            "means_of_resolution": raw.get("fields", {}).get("Means Of Resolution", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = ITAAntiDopingScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_entries()
        for e in entries[:40]:
            print(f"  {e['date']}  {e['id_slug'][:45]:45}  {e['name'][:28]:28}  {e['sport'][:14]}")
        print(f"\nTotal: {len(entries)} entries")
        sys.exit(0)

    if command in ("bootstrap", "bootstrap-fast"):
        result = scraper.bootstrap(sample_mode=sample, sample_size=10)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
