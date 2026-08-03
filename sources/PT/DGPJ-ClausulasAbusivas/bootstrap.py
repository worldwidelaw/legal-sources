#!/usr/bin/env python3
"""
PT/DGPJ-ClausulasAbusivas -- Portuguese Unfair Contract Terms case law (DGPJ)

The "Cláusulas Abusivas" database of the Direção-Geral da Política de Justiça
(DGPJ, Portuguese Ministry of Justice), published on dgsi.pt as the Lotus Domino
database `jdgpj.nsf`. It collects court decisions (mostly first-instance and
appeal) that declare specific standard/general contract terms abusive and null
under the Portuguese unfair-terms regime (DL 446/85 — Cláusulas Contratuais
Gerais) and consumer protection law. Each record contains the operative decision
text (which clauses were struck down and why), party names, contract type, court,
descriptors and decision date.

Strategy:
  - JSON enumeration via the Lotus Domino ReadViewEntries endpoint
    (`/jdgpj.nsf/Por+Ano?ReadViewEntries&...&OutputFormat=JSON`)
  - Full text via OpenDocument with ExpandSection=1
  - ISO-8859-1 encoded HTML pages; values live in the `bgcolor="#E0F1FF"` cells

Data:
  - ~392 decisions (single database)
  - Language: Portuguese
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PT.DGPJ-ClausulasAbusivas")

BASE_URL = "http://www.dgsi.pt"
DB = "jdgpj"
PAGE_SIZE = 200


def clean_html(html_text: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(p|div|tr|li)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_mod.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    lines = [line.strip() for line in text.split('\n')]
    return '\n'.join(lines).strip()


class DGPJClausulasScraper(BaseScraper):
    """Scraper for the DGPJ Cláusulas Abusivas (unfair contract terms) database."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
            },
            timeout=60,
        )

    def _fetch_json_page(self, start: int = 1, count: int = PAGE_SIZE) -> Optional[dict]:
        url = f"/{DB}.nsf/Por+Ano?ReadViewEntries&Start={start}&Count={count}&OutputFormat=JSON"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"JSON page start={start}: HTTP {resp.status_code}")
                return None
            text = resp.content.decode("utf-8", errors="replace").lstrip("﻿")
            return json.loads(text)
        except Exception as e:
            logger.warning(f"Error fetching JSON start={start}: {e}")
            return None

    @staticmethod
    def _parse_entries(data: dict) -> List[Dict[str, Any]]:
        entries = []
        for ve in data.get("viewentry", []):
            unid = ve.get("@unid", "")
            if not unid:
                continue
            entry = {"unid": unid}
            for ed in ve.get("entrydata", []):
                name = ed.get("@name", "")
                if name == "DATADEC":
                    dt = ed.get("datetime", {})
                    entry["date_raw"] = dt.get("0", "") if isinstance(dt, dict) else ""
                elif name == "PROCESSO":
                    t = ed.get("text", {})
                    entry["case_number"] = t.get("0", "") if isinstance(t, dict) else ""
                elif name == "REU":
                    t = ed.get("text", {})
                    entry["defendant"] = t.get("0", "") if isinstance(t, dict) else ""
                elif name == "TipoContrato":
                    t = ed.get("text", {})
                    entry["contract_type"] = t.get("0", "") if isinstance(t, dict) else ""
            entries.append(entry)
        return entries

    def _fetch_document(self, unid: str) -> Optional[str]:
        url = f"/{DB}.nsf/0/{unid}?OpenDocument&ExpandSection=1"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                return None
            try:
                return resp.content.decode("iso-8859-1")
            except Exception:
                return resp.content.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"Error fetching doc {unid}: {e}")
            return None

    @staticmethod
    def _field(raw_html: str, label: str) -> Optional[str]:
        """Extract the value cell (bgcolor #E0F1FF) that follows a labelled header cell.

        `label` is treated as a regex fragment (accented chars are passed as `.`).
        """
        m = re.search(
            label + r':\s*</font>.*?bgcolor="#E0F1FF"[^>]*>(.*?)</td>',
            raw_html, re.DOTALL | re.IGNORECASE,
        )
        if not m:
            return None
        val = clean_html(m.group(1))
        return val or None

    def _parse_document(self, html_content: str) -> Dict[str, Any]:
        f = lambda lbl: self._field(html_content, lbl)
        result = {
            "case_number": f("Processo"),
            "court_first_instance": f(r"Tribunal 1.{1,3} inst.ncia"),
            "section": f(r"Ju.zo ou Sec.{1,3}o"),
            "action_type": f(r"Tipo de A.{1,3}o"),
            "contract_type": f("Tipo de Contrato"),
            "plaintiff": f("Autor"),
            "defendant": f(r"R.u"),
            "date_doc": f(r"Data da Decis.o"),
        }
        descriptors_raw = f("Descritores")
        result["descriptors"] = (
            [d.strip() for d in descriptors_raw.split("\n") if d.strip()]
            if descriptors_raw else []
        )
        clausulas = self._field(html_content, r"Texto das Cl.usulas Abusivas") or ""
        integral = self._field(html_content, "Texto Integral") or ""
        # Prefer the full ruling text when present; otherwise the operative
        # unfair-clause decision text (always populated).
        if len(integral) > 200:
            body = integral
            if clausulas and clausulas not in integral:
                body = clausulas + "\n\n" + integral
        else:
            body = clausulas
        result["full_text"] = body
        return result

    @staticmethod
    def _parse_date(date_str: Optional[str]) -> Optional[str]:
        if not date_str:
            return None
        s = date_str.strip()
        # YYYYMMDD (from JSON DATADEC)
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        # MM/DD/YYYY (US-format string in the HTML page)
        m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
        if m:
            mm, dd, yyyy = m.groups()
            return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"
        return None

    def _iter_raw(self) -> Generator[Dict[str, Any], None, None]:
        first = self._fetch_json_page(start=1, count=1)
        if not first:
            raise RuntimeError("Cannot access DGPJ jdgpj database (JSON view failed)")
        total = int(first.get("@toplevelentries", "0"))
        logger.info(f"jdgpj: {total} total entries")
        start, fetched = 1, 0
        while start <= total:
            data = self._fetch_json_page(start=start, count=PAGE_SIZE)
            if not data:
                break
            entries = self._parse_entries(data)
            if not entries:
                break
            for entry in entries:
                html_content = self._fetch_document(entry["unid"])
                if not html_content:
                    continue
                doc = self._parse_document(html_content)
                doc["unid"] = entry["unid"]
                doc["listing"] = entry
                yield doc
                fetched += 1
            start += PAGE_SIZE
            logger.info(f"jdgpj: fetched {fetched} documents so far")

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_raw()

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_iso = since.strftime("%Y-%m-%d")
        data = self._fetch_json_page(start=1, count=PAGE_SIZE)
        if not data:
            return
        for entry in self._parse_entries(data):
            d = self._parse_date(entry.get("date_raw", ""))
            if d and d < since_iso:
                break
            html_content = self._fetch_document(entry["unid"])
            if not html_content:
                continue
            doc = self._parse_document(html_content)
            doc["unid"] = entry["unid"]
            doc["listing"] = entry
            yield doc

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        full_text = (raw.get("full_text") or "").strip()
        if len(full_text) < 100:
            return None

        listing = raw.get("listing", {})
        unid = raw.get("unid", "")
        case_number = raw.get("case_number") or listing.get("case_number", "")

        if case_number:
            id_str = f"DGPJ-CA-{case_number.replace('/', '-').replace(' ', '')}"
        else:
            id_str = f"DGPJ-CA-{unid[:16]}"

        iso_date = self._parse_date(raw.get("date_doc")) or self._parse_date(listing.get("date_raw"))

        defendant = raw.get("defendant") or listing.get("defendant", "")
        contract_type = raw.get("contract_type") or listing.get("contract_type", "")
        title_bits = ["Cláusulas Abusivas"]
        if case_number:
            title_bits.append(case_number)
        if defendant:
            title_bits.append(f"c/ {defendant}")
        title = " — ".join([title_bits[0], " ".join(title_bits[1:])]) if len(title_bits) > 1 else title_bits[0]

        url = f"{BASE_URL}/{DB}.nsf/0/{unid}?OpenDocument"

        return {
            "_id": id_str,
            "_source": "PT/DGPJ-ClausulasAbusivas",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": iso_date,
            "url": url,
            "case_number": case_number,
            "court": raw.get("court_first_instance", ""),
            "section": raw.get("section", ""),
            "action_type": raw.get("action_type", ""),
            "contract_type": contract_type,
            "plaintiff": raw.get("plaintiff", ""),
            "defendant": defendant,
            "descriptors": raw.get("descriptors", []),
            "jurisdiction": "PT",
            "language": "pt",
            "doc_id": unid,
        }

    def test_connection(self):
        print("Testing DGPJ jdgpj (Cláusulas Abusivas)...")
        data = self._fetch_json_page(start=1, count=1)
        if not data:
            print("  FAILED: JSON view unreachable")
            return
        total = data.get("@toplevelentries", "?")
        entries = self._parse_entries(data)
        print(f"  total entries: {total}")
        if entries:
            html_content = self._fetch_document(entries[0]["unid"])
            if html_content:
                doc = self._parse_document(html_content)
                print(f"  sample text len: {len(doc.get('full_text',''))} chars - OK")
            else:
                print("  doc fetch FAILED")


def main():
    scraper = DGPJClausulasScraper()
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
