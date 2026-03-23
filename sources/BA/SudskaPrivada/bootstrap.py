"""
World Wide Law - Bosnia & Herzegovina Judicial Practice Portal Scraper

Fetches case law from the BiH Judicial Practice Portal (Sudska praksa).
Data source: https://sudskapraksa.pravosudje.ba
Method: REST API for listings + PDF download for full text
Coverage: ~15,000 decisions from 4 highest courts (Court of BiH, Supreme Court RS,
         Supreme Court FBiH, Appeals Court Brčko)
Language: Bosnian/Croatian/Serbian

The API provides:
- GET /api/case-law-documents - all case law documents (summaries + court case refs)
- GET /api/case-law-documents/{id} - detail with attachment IDs
- GET /api/case-decisions - all decisions with attachment IDs
- GET /api/case-law-documents/attachments/{id}/download - PDF full text
- GET /api/case-decisions/attachments/{id}/download - PDF full text
"""

import io
import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import pdfplumber

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("BA/SudskaPrivada")

# Organisation ID to court name mapping
COURT_NAMES = {
    1: "Vrhovni sud Republike Srpske",
    2: "Vrhovni sud Federacije BiH",
    3: "Sud Bosne i Hercegovine",
    4: "Apelacioni sud Brčko distrikta",
}


class BiHSudskaPrivadaScraper(BaseScraper):
    """
    Scraper for: BiH Judicial Practice Portal
    Country: BA
    URL: https://sudskapraksa.pravosudje.ba

    Data types: case_law
    Auth: none

    The portal aggregates decisions from 4 highest courts in BiH.
    Full text is available as PDF downloads via attachment endpoints.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.config.get("api", {}).get(
                "base_url", "https://sudskapraksa.pravosudje.ba"
            ),
            headers={
                **self._auth_headers,
                "Accept": "application/json",
            },
            verify=True,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decisions by fetching case-law-documents with their details
        and extracting full text from PDFs.
        """
        # First, fetch all case-law documents (returns all ~7000 in one call)
        logger.info("Fetching case-law-documents listing...")
        self.rate_limiter.wait()
        resp = self.client.get("/api/case-law-documents")
        all_docs = resp.json()

        published = [d for d in all_docs if d.get("status") == "PUBLISHED"]
        logger.info(f"Found {len(published)} published case-law documents")

        for i, doc in enumerate(published):
            doc_id = doc.get("id")
            if not doc_id:
                continue

            logger.info(
                f"Processing case-law document {i+1}/{len(published)}: id={doc_id}"
            )

            try:
                detail = self._fetch_document_detail(doc_id)
                if not detail:
                    continue

                # Each document may have multiple court cases with decisions
                court_cases = detail.get("courtCaseList", [])
                for court_case in court_cases:
                    decisions = court_case.get("decisions", [])
                    for decision in decisions:
                        if decision.get("status") != "PUBLISHED":
                            continue

                        attachment_id = decision.get("attachmentId")
                        if not attachment_id:
                            continue

                        raw = self._build_raw_record(doc, detail, court_case, decision)
                        if raw and raw.get("full_text"):
                            yield raw

            except Exception as e:
                logger.warning(f"Failed to process document {doc_id}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given datetime."""
        since_ms = int(since.timestamp() * 1000)

        self.rate_limiter.wait()
        resp = self.client.get("/api/case-law-documents")
        all_docs = resp.json()

        recent = [
            d
            for d in all_docs
            if d.get("status") == "PUBLISHED"
            and (d.get("caseLawDocDate") or 0) >= since_ms
        ]
        logger.info(f"Found {len(recent)} documents since {since.isoformat()}")

        for doc in recent:
            doc_id = doc.get("id")
            if not doc_id:
                continue

            try:
                detail = self._fetch_document_detail(doc_id)
                if not detail:
                    continue

                for court_case in detail.get("courtCaseList", []):
                    for decision in court_case.get("decisions", []):
                        if decision.get("status") != "PUBLISHED":
                            continue
                        attachment_id = decision.get("attachmentId")
                        if not attachment_id:
                            continue

                        raw = self._build_raw_record(doc, detail, court_case, decision)
                        if raw and raw.get("full_text"):
                            yield raw

            except Exception as e:
                logger.warning(f"Failed to process document {doc_id}: {e}")
                continue

    def _fetch_document_detail(self, doc_id: int) -> dict:
        """Fetch detailed info for a case-law document."""
        self.rate_limiter.wait()
        resp = self.client.get(f"/api/case-law-documents/{doc_id}")
        return resp.json()

    def _build_raw_record(
        self, doc: dict, detail: dict, court_case: dict, decision: dict
    ) -> dict:
        """Build a raw record from document, court case, and decision data."""
        attachment_id = decision.get("attachmentId")
        if not attachment_id:
            return None

        # Extract full text from PDF
        full_text = self._extract_pdf_text(attachment_id)
        if not full_text or len(full_text) < 100:
            logger.warning(
                f"Insufficient text for attachment {attachment_id}: "
                f"{len(full_text) if full_text else 0} chars"
            )
            return None

        # Parse decision date (milliseconds timestamp)
        decision_date = decision.get("decisionDate")
        date_iso = None
        if decision_date:
            try:
                dt = datetime.fromtimestamp(decision_date / 1000, tz=timezone.utc)
                date_iso = dt.strftime("%Y-%m-%d")
            except (ValueError, OSError):
                pass

        # Get case number
        case_number = court_case.get("courtCaseNumber", "")

        # Get court name
        org_id = doc.get("organisationId") or decision.get("organisationId")
        court_name = COURT_NAMES.get(org_id, f"Court {org_id}")

        # Get judge
        judge = court_case.get("judge1", "") or court_case.get("closedByJudge", "")

        # Build title from decision name or case number
        title = decision.get("name", "") or f"Decision {case_number}"

        # Get summary from the parent document
        summary = doc.get("summary", "")

        # Decision ext ID for unique identification
        ext_id = decision.get("extId", "") or decision.get("id", "")

        return {
            "ext_id": str(ext_id),
            "doc_id": doc.get("id"),
            "case_number": case_number,
            "title": title,
            "summary": summary,
            "full_text": full_text,
            "date": date_iso,
            "court_name": court_name,
            "organisation_id": org_id,
            "judge": judge,
            "case_type": court_case.get("caseType", ""),
            "case_phase": court_case.get("casePhaseType", ""),
            "case_closing_way": court_case.get("caseClosingWay", ""),
            "attachment_id": attachment_id,
            "url": f"https://sudskapraksa.pravosudje.ba/api/case-law-documents/attachments/{attachment_id}/download",
        }

    def _extract_pdf_text(self, attachment_id: int) -> str:
        """Download PDF attachment and extract text."""
        self.rate_limiter.wait()

        try:
            resp = self.client.get(
                f"/api/case-law-documents/attachments/{attachment_id}/download",
                headers={"Accept": "application/pdf"},
            )

            if resp.status_code != 200:
                logger.warning(
                    f"PDF download failed for attachment {attachment_id}: "
                    f"HTTP {resp.status_code}"
                )
                return ""

            pdf_bytes = resp.content
            if not pdf_bytes or len(pdf_bytes) < 100:
                return ""

            text_parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)

            text = "\n\n".join(text_parts)

            # Clean up whitespace
            text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
            text = re.sub(r" +", " ", text)
            text = text.replace("\xa0", " ")

            return text.strip()

        except Exception as e:
            logger.warning(f"PDF extraction failed for attachment {attachment_id}: {e}")
            return ""

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into the standard schema.

        CRITICAL: Includes FULL TEXT from PDF content.
        """
        ext_id = raw.get("ext_id", "")
        case_number = raw.get("case_number", "")

        title = raw.get("title", "")
        if not title:
            title = f"Decision {case_number}"

        full_text = raw.get("full_text", "")

        safe_id = re.sub(r"[^\w\-]", "_", ext_id or case_number)

        return {
            "_id": f"BA/SudskaPrivada/{safe_id}",
            "_source": "BA/SudskaPrivada",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": raw.get("date"),
            "url": raw.get("url"),
            # Source-specific fields
            "ext_id": ext_id,
            "case_number": case_number,
            "summary": raw.get("summary"),
            "court_name": raw.get("court_name"),
            "judge": raw.get("judge"),
            "case_type": raw.get("case_type"),
            "case_phase": raw.get("case_phase"),
            "case_closing_way": raw.get("case_closing_way"),
            "attachment_id": raw.get("attachment_id"),
            # Keep raw data for debugging
            "_raw": raw,
        }


# -- CLI Entry Point ---


def main():
    scraper = BiHSudskaPrivadaScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, {stats['records_skipped']} skipped"
            )
    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
