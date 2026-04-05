#!/usr/bin/env python3
"""
AR/SCBA -- Suprema Corte de Buenos Aires - JUBA Data Fetcher

Fetches Argentine Buenos Aires Province case law from the JUBA database.

Data source: https://juba.scba.gov.ar
License: Open public access (government data)

Strategy:
  - ASP.NET PostBack search with ViewState/EventValidation tokens
  - Search by materia (legal area) to enumerate decisions
  - Full text retrieved via download link returning HTML-as-.doc
  - Pagination via UpdatePanel AJAX requests

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import html
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "AR/SCBA"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.SCBA")

BASE_URL = "https://juba.scba.gov.ar"
SEARCH_URL = f"{BASE_URL}/Buscar.aspx"
FULLTEXT_URL = f"{BASE_URL}/VerTextoCompleto.aspx"

MATERIAS = [
    "Civil y Comercial",
    "Laboral",
    "Penal",
    "Contencioso administrativa",
    "Inconstitucionalidad",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
}

# Legal terms to enumerate decisions (JUBA requires non-empty search terms)
SEARCH_TERMS = [
    "derecho", "propiedad", "contrato", "daños", "responsabilidad",
    "indemnización", "despido", "trabajo", "alimentos", "divorcio",
    "homicidio", "robo", "recurso", "apelación", "nulidad",
    "prescripción", "embargo", "medida cautelar", "inconstitucionalidad",
    "amparo", "habeas corpus", "cobro", "escrituración", "locación",
    "sucesión", "sociedad", "concurso", "quiebra", "accidente",
]


def clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_aspnet_fields(page_html: str) -> dict:
    """Extract ASP.NET hidden form fields from HTML."""
    fields = {}
    for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION", "__PREVIOUSPAGE"]:
        m = re.search(f'name="{name}"[^>]*value="([^"]*)"', page_html)
        if m:
            fields[name] = m.group(1)
    return fields


def parse_date(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY date to ISO 8601."""
    if not date_str:
        return None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str.strip())
    if m:
        day, month, year = m.groups()
        try:
            return f"{year}-{int(month):02d}-{int(day):02d}"
        except ValueError:
            return None
    return None


class JUBAFetcher:
    """Fetcher for JUBA (Jurisprudencia Buenos Aires) database."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.request_count = 0

    def _rate_limit(self):
        """Respect rate limits."""
        self.request_count += 1
        if self.request_count % 5 == 0:
            time.sleep(2.0)
        else:
            time.sleep(1.0)

    def search(self, term: str = "", materia: str = "Todos", max_results: int = 50) -> list:
        """
        Search JUBA and return a list of fallo IDs with metadata.
        """
        logger.info(f"Searching JUBA: term='{term}', materia='{materia}'")

        # Get initial search page
        r = self.session.get(SEARCH_URL, timeout=30)
        r.raise_for_status()
        fields = extract_aspnet_fields(r.text)

        if not fields.get("__VIEWSTATE"):
            logger.error("Failed to extract ViewState from search page")
            return []

        # Submit search
        data = {
            **fields,
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "ctl00$cphMainContent$txtExpresionBusquedaRapida": term,
            "ctl00$cphMainContent$ddlMateria": materia,
            "ctl00$cphMainContent$btnUnicaBusqueda": "Buscar",
            "ctl00$cphMainContent$Anclar": "",
            "ctl00$cphMainContent$txtPalabrasResaltar": "",
            "ctl00$cphMainContent$txtDivImprimir": "",
            "ctl00$cphMainContent$txtPrimeraCarga": "S",
            "ctl00$cphMainContent$solapaMostrada": "",
        }

        self._rate_limit()
        r2 = self.session.post(SEARCH_URL, data=data, timeout=60)
        r2.raise_for_status()

        results = self._parse_search_results(r2.text)
        logger.info(f"Found {len(results)} results for materia='{materia}'")

        if len(results) >= max_results:
            return results[:max_results]
        return results

    def _parse_search_results(self, page_html: str) -> list:
        """Extract fallo IDs and metadata from search results page."""
        results = []
        seen_ids = set()

        # Extract all fallo IDs
        fallo_ids = re.findall(r"idFallo=(\d+)", page_html)

        for fid in fallo_ids:
            if fid in seen_ids:
                continue
            seen_ids.add(fid)
            results.append({"id": fid})

        return results

    def fetch_decision(self, fallo_id: str) -> Optional[dict]:
        """
        Fetch a single decision by fallo ID.
        Returns metadata + full text.
        """
        logger.debug(f"Fetching decision {fallo_id}")

        # Get the decision page
        url = f"{FULLTEXT_URL}?idFallo={fallo_id}"
        self._rate_limit()
        r = self.session.get(url, timeout=30)
        r.raise_for_status()

        page_html = r.text
        metadata = self._extract_metadata(page_html, fallo_id)

        # Try to load metadata via AJAX PostBack
        fields = extract_aspnet_fields(page_html)
        if fields.get("__VIEWSTATE"):
            ajax_data = {
                "ctl00$cphMainContent$ScriptManager1": (
                    "ctl00$cphMainContent$UpdatePanelRepeaterGeneral|"
                    "ctl00$cphMainContent$lnkDatosFallo"
                ),
                **fields,
                "__EVENTTARGET": "ctl00$cphMainContent$lnkDatosFallo",
                "__EVENTARGUMENT": "",
                "__ASYNCPOST": "true",
            }
            ajax_headers = {
                "X-Requested-With": "XMLHttpRequest",
                "X-MicrosoftAjax": "Delta=true",
            }
            self._rate_limit()
            r_meta = self.session.post(
                f"{FULLTEXT_URL}?idFallo={fallo_id}",
                data=ajax_data,
                headers=ajax_headers,
                timeout=30,
            )
            if r_meta.status_code == 200:
                metadata.update(self._extract_metadata_from_ajax(r_meta.text, fallo_id))

            # Now download full text
            # Re-extract fields from the AJAX response for the download PostBack
            # We need to re-get the page to get fresh ViewState for download
            self._rate_limit()
            r_page = self.session.get(url, timeout=30)
            r_page.raise_for_status()
            dl_fields = extract_aspnet_fields(r_page.text)

            if dl_fields.get("__VIEWSTATE"):
                dl_data = {
                    **dl_fields,
                    "__EVENTTARGET": "ctl00$cphMainContent$lnkDescargar",
                    "__EVENTARGUMENT": "",
                }
                self._rate_limit()
                r_dl = self.session.post(
                    f"{FULLTEXT_URL}?idFallo={fallo_id}",
                    data=dl_data,
                    timeout=30,
                )
                if r_dl.status_code == 200:
                    ct = r_dl.headers.get("Content-Type", "")
                    if "octet-stream" in ct or "msword" in ct:
                        full_text = clean_html(r_dl.text)
                        if full_text:
                            metadata["text"] = full_text

        if not metadata.get("text"):
            logger.warning(f"No full text for fallo {fallo_id}")
            return None

        return metadata

    def _extract_metadata(self, page_html: str, fallo_id: str) -> dict:
        """Extract metadata from the decision page HTML."""
        meta = {
            "_id": f"SCBA-{fallo_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "url": f"{FULLTEXT_URL}?idFallo={fallo_id}",
        }

        # Try to extract from visible HTML
        text = re.sub(r"<[^>]+>", "\n", page_html)
        text = html.unescape(text)

        # Carátula (case title)
        m = re.search(r"Car[áa]tula\s*P[úu]blica:\s*\n*\s*(.+?)(?:\n|Magistrados)", text, re.I)
        if m:
            meta["title"] = m.group(1).strip()

        # Tribunal
        m = re.search(r"Tribunal\s*Emisor:\s*\n*\s*(.+?)(?:\n|Causa)", text, re.I)
        if m:
            meta["tribunal"] = m.group(1).strip()

        # Date
        m = re.search(r"Fecha:\s*\n*\s*(\d{1,2}/\d{1,2}/\d{4})", text, re.I)
        if m:
            meta["date"] = parse_date(m.group(1))

        # Materia
        m = re.search(r"Materia:\s*\n*\s*(.+?)(?:\n|Tipo)", text, re.I)
        if m:
            meta["materia"] = m.group(1).strip()

        return meta

    def _extract_metadata_from_ajax(self, ajax_text: str, fallo_id: str) -> dict:
        """Extract metadata from AJAX response."""
        meta = {}
        text = clean_html(ajax_text)

        m = re.search(r"Car[áa]tula\s*P[úu]blica:\s*(.+?)(?:Magistrados|Tribunal)", text, re.I)
        if m:
            meta["title"] = m.group(1).strip()

        m = re.search(r"Tribunal\s*Emisor:\s*(.+?)(?:Causa|Fecha)", text, re.I)
        if m:
            meta["tribunal"] = m.group(1).strip()

        m = re.search(r"Fecha:\s*(\d{1,2}/\d{1,2}/\d{4})", text, re.I)
        if m:
            meta["date"] = parse_date(m.group(1))

        m = re.search(r"Materia:\s*(.+?)(?:Tipo|Car[áa]tula)", text, re.I)
        if m:
            meta["materia"] = m.group(1).strip()

        m = re.search(r"Causa:\s*(\S+)", text, re.I)
        if m:
            meta["causa"] = m.group(1).strip()

        m = re.search(r"Magistrados\s*Votantes:\s*(.+?)(?:Tribunal|$)", text, re.I)
        if m:
            meta["magistrados"] = m.group(1).strip()

        return meta

    def fetch_all(self, max_per_materia: int = 0) -> Generator[dict, None, None]:
        """Yield all decisions across all materias using multiple search terms."""
        seen_ids = set()
        for materia in MATERIAS:
            for term in SEARCH_TERMS:
                logger.info(f"Processing materia={materia}, term='{term}'")
                try:
                    results = self.search(
                        term=term, materia=materia,
                        max_results=max_per_materia or 10000,
                    )
                except Exception as e:
                    logger.error(f"Search failed for {materia}/{term}: {e}")
                    continue

                for result in results:
                    if result["id"] in seen_ids:
                        continue
                    seen_ids.add(result["id"])
                    try:
                        decision = self.fetch_decision(result["id"])
                        if decision:
                            yield decision
                    except Exception as e:
                        logger.error(f"Failed to fetch decision {result['id']}: {e}")
                        continue

    def fetch_sample(self, count: int = 15) -> list:
        """Fetch a sample of decisions for validation."""
        decisions = []
        seen_ids = set()

        # Use a mix of materias and search terms
        search_combos = [
            ("derecho", "Civil y Comercial"),
            ("despido", "Laboral"),
            ("homicidio", "Penal"),
            ("recurso", "Contencioso administrativa"),
            ("amparo", "Inconstitucionalidad"),
            ("contrato", "Civil y Comercial"),
            ("indemnización", "Laboral"),
            ("robo", "Penal"),
        ]

        for term, materia in search_combos:
            if len(decisions) >= count:
                break

            logger.info(f"Sampling: term='{term}', materia='{materia}'")
            try:
                results = self.search(term=term, materia=materia, max_results=5)
            except Exception as e:
                logger.error(f"Search failed: {e}")
                continue

            for result in results:
                if len(decisions) >= count:
                    break
                if result["id"] in seen_ids:
                    continue
                seen_ids.add(result["id"])
                try:
                    decision = self.fetch_decision(result["id"])
                    if decision and decision.get("text"):
                        decisions.append(decision)
                        logger.info(
                            f"  [{len(decisions)}/{count}] {decision.get('title', 'N/A')[:60]}"
                        )
                except Exception as e:
                    logger.error(f"Failed to fetch {result['id']}: {e}")
                    continue

        return decisions

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw decision to standard schema."""
        return {
            "_id": raw.get("_id", ""),
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": raw.get("_fetched_at", datetime.now(timezone.utc).isoformat()),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "tribunal": raw.get("tribunal", ""),
            "materia": raw.get("materia", ""),
            "causa": raw.get("causa", ""),
            "magistrados": raw.get("magistrados", ""),
        }


def test_api():
    """Quick API connectivity test."""
    fetcher = JUBAFetcher()

    print("Testing JUBA connectivity...")
    try:
        r = fetcher.session.get(SEARCH_URL, timeout=15)
        print(f"  Search page: {r.status_code} ({len(r.text)} bytes)")
        fields = extract_aspnet_fields(r.text)
        print(f"  ViewState: {'OK' if fields.get('__VIEWSTATE') else 'MISSING'}")
        print(f"  EventValidation: {'OK' if fields.get('__EVENTVALIDATION') else 'MISSING'}")
    except Exception as e:
        print(f"  FAILED: {e}")
        return False

    print("\nTesting search...")
    try:
        results = fetcher.search(term="derecho", materia="Civil y Comercial", max_results=5)
        print(f"  Found {len(results)} results")
        if results:
            print(f"  First ID: {results[0]['id']}")
    except Exception as e:
        print(f"  FAILED: {e}")
        return False

    if results:
        print("\nTesting full text download...")
        try:
            decision = fetcher.fetch_decision(results[0]["id"])
            if decision:
                text = decision.get("text", "")
                print(f"  Title: {decision.get('title', 'N/A')[:80]}")
                print(f"  Date: {decision.get('date', 'N/A')}")
                print(f"  Tribunal: {decision.get('tribunal', 'N/A')[:80]}")
                print(f"  Text length: {len(text)} chars")
                print(f"  Text preview: {text[:150]}...")
            else:
                print("  No text retrieved")
                return False
        except Exception as e:
            print(f"  FAILED: {e}")
            return False

    print("\nAll tests passed!")
    return True


def bootstrap(sample: bool = False, full: bool = False):
    """Run the bootstrap process."""
    fetcher = JUBAFetcher()

    if sample:
        logger.info("Running sample bootstrap (15 records)...")
        decisions = fetcher.fetch_sample(count=15)
    else:
        logger.info("Running full bootstrap...")
        decisions = list(fetcher.fetch_all())

    if not decisions:
        logger.error("No decisions fetched!")
        return

    # Save to sample directory
    SAMPLE_DIR.mkdir(exist_ok=True)

    for i, raw_decision in enumerate(decisions):
        normalized = fetcher.normalize(raw_decision)
        filename = SAMPLE_DIR / f"{normalized['_id']}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(decisions)} records to {SAMPLE_DIR}")

    # Validation summary
    has_text = sum(1 for d in decisions if d.get("text"))
    has_title = sum(1 for d in decisions if d.get("title"))
    has_date = sum(1 for d in decisions if d.get("date"))
    avg_text_len = (
        sum(len(d.get("text", "")) for d in decisions) / len(decisions) if decisions else 0
    )

    print(f"\n{'='*60}")
    print(f"AR/SCBA Bootstrap Summary")
    print(f"{'='*60}")
    print(f"Total records: {len(decisions)}")
    print(f"With full text: {has_text}/{len(decisions)}")
    print(f"With title: {has_title}/{len(decisions)}")
    print(f"With date: {has_date}/{len(decisions)}")
    print(f"Avg text length: {avg_text_len:.0f} chars")
    print(f"Saved to: {SAMPLE_DIR}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="AR/SCBA JUBA Data Fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    boot = subparsers.add_parser("bootstrap", help="Run bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    boot.add_argument("--full", action="store_true", help="Full bootstrap")

    subparsers.add_parser("test-api", help="Test API connectivity")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample, full=args.full)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
