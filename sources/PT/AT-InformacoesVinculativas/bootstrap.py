#!/usr/bin/env python3
"""
PT/AT-InformacoesVinculativas -- Portuguese Tax Authority Binding Information

Fetches tax doctrine (Informações Vinculativas / Fichas Doutrinárias) from the
Portuguese Tax Authority (Autoridade Tributária e Aduaneira). ~5,637 PDF documents
covering IRS, IRC, IVA, IMI, IMT, Selo, EBF, RITI, CIUC, LGT, CESE, CSB.

Data access:
  - WCF service at /_vti_bin/portalat/docs.svc/listdocs returns full catalog as JSON
  - Each document is a PDF with structured "Ficha Doutrinária" content
  - Text extracted via pdfplumber

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
import io
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote

import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PT.AT-InformacoesVinculativas")

BASE_URL = "https://info.portaldasfinancas.gov.pt"
DELAY = 1.0

# Subsites with their configuration
SUBSITES = [
    {
        "code": "CIRS",
        "name": "IRS (Income Tax)",
        "path": "/pt/informacao_fiscal/informacoes_vinculativas/rendimento/cirs",
        "fields": "DocIcon,Disponibilizada_x0020_em,NumeroVinculativa,Diploma,Artigo,Assunto",
        "sort": "Disponibilizada_x0020_em:DESC,Diploma:ASC,Artigo:ASC",
        "filter": '<IsNotNull><FieldRef Name="ID"></FieldRef></IsNotNull>',
        "id": 35,
    },
    {
        "code": "CIVA",
        "name": "IVA (VAT)",
        "path": "/pt/informacao_fiscal/informacoes_vinculativas/despesa/civa",
        "fields": "DocIcon,Data,Vinc_x002e__x0020_n_x002e__x00ba_,Diploma,Anterior_x0020_Artigo,Assunto",
        "sort": "Data:DESC,Vinc_x002e__x0020_n_x002e__x00ba_:DESC",
        "filter": '<IsNotNull><FieldRef Name="ID"></FieldRef></IsNotNull>',
        "id": 53,
    },
    {
        "code": "CIRC",
        "name": "IRC (Corporate Tax)",
        "path": "/pt/informacao_fiscal/informacoes_vinculativas/rendimento/circ",
        "fields": "DocIcon,Disponibilizada_x0020_em,NumeroVinculativa,Diploma,Artigo,Assunto",
        "sort": "Disponibilizada_x0020_em:DESC,Diploma:ASC,Artigo:ASC",
        "filter": '<IsNotNull><FieldRef Name="ID"></FieldRef></IsNotNull>',
        "id": 34,
    },
    {
        "code": "EBF",
        "name": "EBF (Tax Benefits)",
        "path": "/pt/informacao_fiscal/informacoes_vinculativas/beneficios_fiscais",
        "fields": "DocIcon,Disponibilizada_x0020_em,NumeroVinculativa,Artigo0,Assunto",
        "sort": "Created_x0020_Date:DESC,Artigo0:ASC,Assunto_Resumo:ASC",
        "filter": '<IsNotNull><FieldRef Name="ID"></FieldRef></IsNotNull>',
        "id": 34,
    },
    {
        "code": "CIMT",
        "name": "IMT (Property Transfer Tax)",
        "path": "/pt/informacao_fiscal/informacoes_vinculativas/patrimonio/cimt",
        "fields": "DocIcon,Disponibilizada_x0020_em,NumeroVinculativa,Diploma,Artigo,Assunto",
        "sort": "Created_x0020_Date:DESC,Diploma:ASC,Artigo:ASC",
        "filter": '<IsNotNull><FieldRef Name="ID"></FieldRef></IsNotNull>',
        "id": 31,
    },
    {
        "code": "IS",
        "name": "Selo (Stamp Tax)",
        "path": "/pt/informacao_fiscal/informacoes_vinculativas/patrimonio/selo",
        "fields": "DocIcon,Disponibilizada_x0020_em,NumeroVinculativa,Artigo,Assunto",
        "sort": "Disponibilizada_x0020_em:DESC,Artigo:ASC",
        "filter": '<IsNotNull><FieldRef Name="ID"></FieldRef></IsNotNull>',
        "id": 30,
    },
    {
        "code": "DSRI",
        "name": "DSRI (International Tax Relations)",
        "path": "/pt/informacao_fiscal/informacoes_vinculativas/rendimento/DSRI",
        "fields": "DocIcon,Disponibilizada_x0020_em,NumeroVinculativa,Diploma,Artigo,Assunto",
        "sort": "Disponibilizada_x0020_em:DESC,Diploma:ASC,Artigo:ASC",
        "filter": '<IsNotNull><FieldRef Name="ID"></FieldRef></IsNotNull>',
        "id": 22,
    },
    {
        "code": "CIMI",
        "name": "IMI (Property Tax)",
        "path": "/pt/informacao_fiscal/informacoes_vinculativas/patrimonio/cimi",
        "fields": "DocIcon,Disponibilizada_x0020_em,NumeroVinculativa,Diploma,Artigo,Assunto",
        "sort": "Disponibilizada_x0020_em:DESC,Diploma:ASC,Artigo:ASC",
        "filter": '<IsNotNull><FieldRef Name="ID"></FieldRef></IsNotNull>',
        "id": 35,
    },
    {
        "code": "RITI",
        "name": "RITI (Intra-Community VAT)",
        "path": "/pt/informacao_fiscal/informacoes_vinculativas/despesa/riti",
        "fields": "DocIcon,Disponibilizada_x0020_em,Vinc_x002e__x0020_n_x002e__x00ba_,Artigo,Assunto",
        "sort": "Disponibilizada_x0020_em:DESC,Artigo:ASC",
        "filter": '<IsNotNull><FieldRef Name="ID"></FieldRef></IsNotNull>',
        "id": 32,
    },
    {
        "code": "CIUC",
        "name": "IUC (Vehicle Tax)",
        "path": "/pt/informacao_fiscal/informacoes_vinculativas/patrimonio/ciuc",
        "fields": "DocIcon,Disponibilizada_x0020_em,NumeroVinculativa,Diploma,Artigo,Assunto",
        "sort": "Disponibilizada_x0020_em:DESC,Diploma:ASC,Artigo:ASC",
        "filter": '<Eq><FieldRef Name="Diploma" /><Value Type="Text">CIUC</Value></Eq>',
        "id": 28,
    },
]

def parse_date(date_str: str) -> Optional[str]:
    """Parse various date formats to ISO 8601."""
    if not date_str:
        return None
    # Strip HTML tags (dates come wrapped in <span> tags)
    date_str = re.sub(r'<[^>]+>', '', date_str).strip()
    for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"]:
        try:
            return datetime.strptime(date_str[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class ATInformacoesVinculativas(BaseScraper):
    SOURCE_ID = "PT/AT-InformacoesVinculativas"

    def __init__(self):
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/json",
            },
        )

    def get_catalog(self, subsite: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch the document catalog for a subsite via WCF service."""
        path = subsite["path"]
        fields = subsite["fields"]
        sort = subsite["sort"]
        filt = subsite["filter"]
        page_id = subsite["id"]

        url = (
            f"{BASE_URL}{path}/_vti_bin/portalat/docs.svc/listdocs"
            f"?fields={quote(fields, safe='_,')}"
            f"&sort={quote(sort, safe='_,:')}"
            f"&filter={quote(filt)}"
            f"&id={page_id}"
        )

        resp = self.http.get(url)
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            logger.warning("Failed to fetch catalog for %s: status=%s",
                           subsite["code"], resp.status_code if resp else "None")
            return []

        try:
            data = resp.json()
            rows = data.get("data", [])
        except Exception as e:
            logger.warning("Failed to parse catalog for %s: %s", subsite["code"], e)
            return []

        documents = []
        for row in rows:
            if not row or len(row) < 2:
                continue

            # First column contains HTML with PDF link
            doc_html = str(row[0])
            href_match = re.search(r"href=['\"]([^'\"]+)['\"]", doc_html)
            if not href_match:
                continue

            pdf_path = href_match.group(1)
            if not pdf_path.lower().endswith(".pdf"):
                continue

            # Parse remaining columns based on field order
            field_names = fields.split(",")
            doc_meta = {"pdf_path": pdf_path, "tax_code": subsite["code"]}

            for i, field in enumerate(field_names):
                if i < len(row):
                    val = str(row[i]).strip() if row[i] else ""
                    # Map fields to standard names
                    if "Disponibilizada" in field or field == "Data":
                        doc_meta["date"] = val
                    elif "Vinculativa" in field or "NumeroVinculativa" in field:
                        doc_meta["vinculativa_number"] = val
                    elif field == "Diploma":
                        doc_meta["diploma"] = val
                    elif "Artigo" in field:
                        doc_meta["article"] = val
                    elif field == "Assunto":
                        doc_meta["subject"] = val

            documents.append(doc_meta)

        return documents

    def fetch_pdf_text(self, pdf_path: str) -> str:
        """Download a PDF and extract its text."""
        url = f"{BASE_URL}{pdf_path}" if pdf_path.startswith("/") else pdf_path
        resp = self.http.get(url)
        time.sleep(DELAY)
        if resp is None or resp.status_code != 200:
            return ""

        try:
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                return "\n\n".join(pages)
        except Exception as e:
            logger.warning("Failed to extract PDF text from %s: %s", pdf_path, e)
            return ""

    def normalize(self, doc_meta: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Normalize a document into the standard schema."""
        pdf_path = doc_meta.get("pdf_path", "")
        subject = doc_meta.get("subject", "")
        vinc_num = doc_meta.get("vinculativa_number", "")
        tax_code = doc_meta.get("tax_code", "")
        diploma = doc_meta.get("diploma", "")

        title = subject or f"{tax_code} {vinc_num}".strip()
        if diploma and diploma != tax_code:
            title = f"[{diploma}] {title}"

        # Generate unique ID from PDF path
        doc_id = re.sub(r'[^\w]', '_', pdf_path.split("/")[-1].replace(".pdf", ""))

        return {
            "_id": doc_id,
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": parse_date(doc_meta.get("date", "")),
            "url": f"{BASE_URL}{pdf_path}",
            "language": "pt",
            "tax_code": tax_code,
            "vinculativa_number": vinc_num,
            "diploma": diploma,
            "article": doc_meta.get("article", ""),
            "subject": subject,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all binding information documents."""
        total_yielded = 0
        sample_limit = 15 if sample else None

        for subsite in SUBSITES:
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("Fetching catalog: %s (%s)...", subsite["code"], subsite["name"])
            catalog = self.get_catalog(subsite)
            logger.info("  Found %d documents", len(catalog))

            for doc_meta in catalog:
                if sample_limit and total_yielded >= sample_limit:
                    break

                pdf_path = doc_meta.get("pdf_path", "")
                text = self.fetch_pdf_text(pdf_path)
                if not text:
                    logger.warning("Empty text for %s", pdf_path)
                    continue

                record = self.normalize(doc_meta, text)
                yield record
                total_yielded += 1

                if total_yielded % 50 == 0:
                    logger.info("  Progress: %d documents fetched", total_yielded)

            logger.info("  Done with %s. Total so far: %d", subsite["code"], total_yielded)

        logger.info("Fetch complete. Total documents: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents published since a given date."""
        for subsite in SUBSITES:
            logger.info("Checking updates for %s since %s...", subsite["code"], since)
            catalog = self.get_catalog(subsite)

            for doc_meta in catalog:
                doc_date = parse_date(doc_meta.get("date", ""))
                if doc_date and doc_date >= since:
                    text = self.fetch_pdf_text(doc_meta.get("pdf_path", ""))
                    if text:
                        yield self.normalize(doc_meta, text)
                elif doc_date and doc_date < since:
                    break  # Catalog is sorted by date DESC

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            catalog = self.get_catalog(SUBSITES[0])  # CIRS
            logger.info("Test passed: %d CIRS documents in catalog", len(catalog))
            return len(catalog) > 0
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="PT/AT-InformacoesVinculativas bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    args = parser.parse_args()

    scraper = ATInformacoesVinculativas()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', str(record['_id']))
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                "  [%d] %s | %s | text=%d chars",
                count, record["date"], record["title"][:60], text_len
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
