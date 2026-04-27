#!/usr/bin/env python3
"""
BR/RFB-SolucoesConsulta -- Brazilian Federal Revenue Tax Rulings Fetcher

Fetches Soluções de Consulta (tax rulings) from Brazil's Federal Revenue
Service (Receita Federal do Brasil) via the SIJUT2 public consultation system.

Source: http://normas.receita.fazenda.gov.br/sijut2consulta/
Volume: 15,000+ Soluções de Consulta covering IRPJ, IRPF, PIS/COFINS, CSLL,
        customs classification, and more.

Listing: /consulta.action?tiposAtosSelecionados=72&p={page} (10 per page)
Detail:  /link.action?antigo=1&idAto={id} (full text HTML)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
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
logger = logging.getLogger("legal-data-hunter.BR.RFB-SolucoesConsulta")

BASE_URL = "http://normas.receita.fazenda.gov.br/sijut2consulta"
LISTING_URL = f"{BASE_URL}/consulta.action"
DETAIL_URL = f"{BASE_URL}/link.action"
DELAY = 1.5  # seconds between requests

# Document type codes for tax doctrine
DOC_TYPE_SC = "72"  # Solução de Consulta


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class RFBSolucoesConsulta(BaseScraper):
    SOURCE_ID = "BR/RFB-SolucoesConsulta"

    def __init__(self):
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.5",
                "User-Agent": "LegalDataHunter/1.0 (academic research; open legal data)",
            },
            timeout=30,
        )

    def fetch_listing_page(self, page: int, doc_type: str = DOC_TYPE_SC) -> List[int]:
        """Fetch a listing page and extract document IDs (only vigente/in-force rulings)."""
        resp = self.http.get(
            f"{LISTING_URL}?tiposAtosSelecionados={doc_type}&somente_atos_vigentes=on&p={page}"
        )
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            logger.warning("Failed to fetch listing page %d (status=%s)",
                           page, resp.status_code if resp else "None")
            return []

        html = resp.text
        ids = re.findall(r'idAto=(\d+)', html)
        seen = set()
        unique_ids = []
        for id_str in ids:
            if id_str not in seen:
                seen.add(id_str)
                unique_ids.append(int(id_str))
        return unique_ids

    def fetch_document(self, id_ato: int) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single document detail page."""
        resp = self.http.get(f"{DETAIL_URL}?antigo=1&idAto={id_ato}")
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            return None

        html = resp.text

        # Check for "Ato não encontrado"
        if "Ato não encontrado" in html or "Ato nao encontrado" in html:
            return None

        # Extract og:title
        title = ""
        title_match = re.search(r'<meta\s+property="og:title"\s+content="([^"]*)"', html)
        if title_match:
            title = html_module.unescape(title_match.group(1)).strip()

        # Extract content from conteudoAto div
        content_match = re.search(r'<div class="conteudoAto">(.*)', html, re.DOTALL)
        if not content_match:
            logger.warning("Could not find conteudoAto for idAto=%d", id_ato)
            return None

        content_html = content_match.group(1)
        # Cut at the closing structure (row div or footer)
        content_html = re.split(r'<div class="row">', content_html)[0]
        # Also cut at rodape or footer divs
        content_html = re.split(r'<div[^>]*class="[^"]*rodape[^"]*"', content_html)[0]

        # Remove scripts and styles
        content_html = re.sub(r'<script.*?</script>', '', content_html, flags=re.DOTALL)
        content_html = re.sub(r'<style.*?</style>', '', content_html, flags=re.DOTALL)

        text = strip_html(content_html)

        # Remove the "Multivigente Vigente Original Relacional" navigation text
        text = re.sub(r'Multivigente\s+Vigente\s+Original\s+Relacional', '', text)
        # Remove the --> comment artifact
        text = re.sub(r'\s*-->\s*', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        # Extract metadata from the text header
        # Publication info: (Publicado(a) no DOU de DD/MM/YYYY, seção N, página N)
        pub_match = re.search(
            r'Publicado\(a\) no DOU de (\d{2}/\d{2}/\d{4}),\s*seção\s*(\d+),\s*página\s*(\d+)',
            text
        )
        dou_date = ""
        dou_section = ""
        dou_page = ""
        if pub_match:
            dou_date = pub_match.group(1)
            dou_section = pub_match.group(2)
            dou_page = pub_match.group(3)

        # Extract document date from the header line
        # Pattern: "Solução de Consulta ... nº XXXXX, de DD de MMMM de YYYY"
        date_match = re.search(
            r'de\s+(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})',
            text[:500]
        )
        iso_date = None
        if date_match:
            day = date_match.group(1)
            month_name = date_match.group(2).lower()
            year = date_match.group(3)
            months_pt = {
                'janeiro': '01', 'fevereiro': '02', 'março': '03', 'marco': '03',
                'abril': '04', 'maio': '05', 'junho': '06', 'julho': '07',
                'agosto': '08', 'setembro': '09', 'outubro': '10',
                'novembro': '11', 'dezembro': '12',
            }
            month_num = months_pt.get(month_name, '01')
            iso_date = f"{year}-{month_num}-{day.zfill(2)}"

        # Extract subject (Assunto:)
        subject_match = re.search(r'Assunto:\s*(.+?)(?:\n|Dispositivos|SOLUÇÃO|Ementa)', text)
        subject = subject_match.group(1).strip() if subject_match else ""

        return {
            "id_ato": id_ato,
            "title": title,
            "text": text,
            "date": iso_date,
            "dou_date": dou_date,
            "dou_section": dou_section,
            "dou_page": dou_page,
            "subject": subject,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document into the standard schema."""
        return {
            "_id": f"BR-RFB-SC-{raw['id_ato']}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw["date"],
            "url": f"{DETAIL_URL}?antigo=1&idAto={raw['id_ato']}",
            "language": "pt",
            "dou_date": raw["dou_date"],
            "dou_section": raw["dou_section"],
            "dou_page": raw["dou_page"],
            "subject": raw["subject"],
            "id_ato": raw["id_ato"],
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Soluções de Consulta by paginating through listings."""
        total_yielded = 0
        sample_limit = 15 if sample else None
        page = 1
        max_pages = 5 if sample else 2000  # ~1,525 pages for 15,249 docs
        consecutive_empty = 0

        while page <= max_pages:
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("Fetching listing page %d...", page)
            ids = self.fetch_listing_page(page)

            if not ids:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("3 consecutive empty pages, stopping.")
                    break
                page += 1
                continue
            consecutive_empty = 0

            for id_ato in ids:
                if sample_limit and total_yielded >= sample_limit:
                    break

                raw = self.fetch_document(id_ato)
                if not raw:
                    logger.warning("Failed to fetch document idAto=%d", id_ato)
                    continue

                record = self.normalize(raw)
                if not record["text"]:
                    logger.warning("Empty text for idAto=%d: %s", id_ato, record["title"][:60])
                    continue

                yield record
                total_yielded += 1

                if total_yielded % 50 == 0:
                    logger.info("  Progress: %d documents fetched", total_yielded)

            page += 1

        logger.info("Fetch complete. Total documents: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents published since a given date (YYYY-MM-DD)."""
        page = 1
        found_older = False

        while not found_older:
            logger.info("Checking page %d for updates since %s...", page, since)
            ids = self.fetch_listing_page(page)
            if not ids:
                break

            for id_ato in ids:
                raw = self.fetch_document(id_ato)
                if not raw:
                    continue

                if raw["date"] and raw["date"] < since:
                    found_older = True
                    break

                record = self.normalize(raw)
                if record["text"]:
                    yield record

            page += 1
            if page > 2000:
                break

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            ids = self.fetch_listing_page(1)
            logger.info("Test: found %d IDs on listing page 1", len(ids))
            if not ids:
                return False
            raw = self.fetch_document(ids[0])
            if raw and raw["text"]:
                logger.info("Test passed: idAto=%d has %d chars of text",
                            ids[0], len(raw["text"]))
                return True
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="BR/RFB-SolucoesConsulta bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = RFBSolucoesConsulta()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                "  [%d] idAto=%d | %s | %s | text=%d chars",
                count, record["id_ato"], record["date"], record["title"][:50], text_len
            )

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["date"], record["title"][:60])
        logger.info("Update complete: %d new records since %s", count, since)


if __name__ == "__main__":
    main()
