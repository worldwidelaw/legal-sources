#!/usr/bin/env python3
"""
INTL/ECCB -- Eastern Caribbean Central Bank Regulations & Standards

Downloads prudential standards, banking regulations, AML legislation, and
guidance notes from the ECCB CDN. Full text extracted from PDFs.

Strategy:
  - The ECCB website (eccb-centralbank.org) returns 403 from non-browser IPs,
    but their CDN (cdn.eccb-centralbank.org) serves individual document PDFs.
  - We maintain a curated catalog of known regulatory documents discovered
    from the Wayback Machine CDX index plus the site structure.
  - Each PDF is downloaded from the CDN and text is extracted via pdfplumber.

Coverage:
  - Banking Act 2015, ECAMC Act, ECPCGC Act, Payment System Services Act
  - Prudential Standards (credit risk, outsourcing, permissible activities,
    internal audit, valuation, liquidity, governance, market risk, etc.)
  - Banking regulations (capital adequacy, disclosures, abandoned property)
  - AML/CFT legislation for ECCU member states (AG, DM, GD, SVG)
  - ECCB Agreement, guidelines, clearing house rules

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ECCB")

CDN_BASE = "https://cdn.eccb-centralbank.org/documents/"
SITE_BASE = "https://www.eccb-centralbank.org"

# Curated catalog of ECCB regulatory/legal documents.
# Discovered via Wayback Machine CDX index of eccb-centralbank.org/viewPDF/documents/*.
# Format: (filename, title, category)
DOCUMENT_CATALOG = [
    # === Acts ===
    ("2022-03-17-02-37-19-BankingBill2015.pdf",
     "Banking Act 2015", "Acts"),
    ("2022-03-24-04-17-37-ECAMC-Act.pdf",
     "Eastern Caribbean Asset Management Corporation (ECAMC) Act", "Acts"),
    ("2022-03-24-05-02-10-ECPCGC-Act.pdf",
     "Eastern Caribbean Partial Credit Guarantee Corporation (ECPCGC) Act", "Acts"),
    ("2022-03-24-07-23-41-PaymentSystemServices-Act-2007.pdf",
     "Payment System Services Act 2007", "Acts"),
    ("2022-05-13-07-32-13-ECCB-Agreement---31-Jan-2016.pdf",
     "ECCB Agreement (31 January 2016)", "Acts"),

    # === Prudential Standards ===
    ("2022-03-03-04-45-48-ValuationStandardsforInstitutionsLicensedUnderthe-Banking-Act.pdf",
     "Valuation Standards for Institutions Licensed Under the Banking Act", "Standards"),
    ("2022-03-03-04-57-31-PrudentialStandardonPermissibleActivitiesUndertheBankingAct20152.pdf",
     "Prudential Standard on Permissible Activities Under the Banking Act 2015", "Standards"),
    ("2022-03-03-05-00-34-Revised-Standard-On-Credit-Risk-Management-and-Credit-Underwriting.pdf",
     "Revised Standard on Credit Risk Management and Credit Underwriting", "Standards"),
    ("2022-03-03-05-32-45-PrudentialStandardsfortheOutsourcingofServicesforInstitutionsLicensedtoConductBunisnessUnderthe.pdf",
     "Prudential Standards for the Outsourcing of Services for Institutions Licensed Under the Banking Act", "Standards"),
    ("2022-05-05-02-33-14-PrudentialStandardforInternalAuditingofInstitutionsLicensedUndertheBankingAct1.pdf",
     "Prudential Standard for Internal Auditing of Institutions Licensed Under the Banking Act", "Standards"),
    ("2022-05-05-02-42-33-prudliquidity.pdf",
     "Prudential Standard on Liquidity Risk Management", "Standards"),
    ("2022-03-03-05-21-54-prudgovernance.pdf",
     "Prudential Standard on Corporate Governance", "Standards"),
    ("2022-03-03-05-19-19-prudrelated.pdf",
     "Prudential Standard on Related Party Transactions", "Standards"),
    ("2022-03-03-05-12-17-prud-controlrisks.pdf",
     "Prudential Standard on Control of Risks", "Standards"),

    # === Regulations ===
    ("2023-03-21-17-30-41-Abandoned-Properties-Regulations.pdf",
     "Abandoned Properties Regulations", "Regulations"),
    ("2022-05-12-05-27-37-BankingCapitalAdequacyandCapitalRatiosRegulations.pdf",
     "Banking Capital Adequacy and Capital Ratios Regulations", "Regulations"),
    ("2022-05-12-05-46-19-BankingDisclosuresinStatementofAccountsRegulations.pdf",
     "Banking Disclosures in Statement of Accounts Regulations", "Regulations"),
    ("2022-05-12-05-34-26-Abandoned-Properties-Regulations-Draft.pdf",
     "Abandoned Properties Regulations (Draft)", "Regulations"),

    # === AML/CFT ===
    ("2022-03-03-05-05-05-Anti-Money.pdf",
     "Anti-Money Laundering Guidance Notes", "AML/CFT"),
    ("2022-04-08-06-52-02-ECCBAMLCFTCPFRiskBasedSupervisoryFramework2022.pdf",
     "ECCB AML/CFT/CPF Risk-Based Supervisory Framework 2022", "AML/CFT"),

    # AML — Antigua and Barbuda
    ("2022-05-13-07-22-56-AntiguaandBarbudaMoneyLaunderingandPreventionActAmemdment2020MLPA.pdf",
     "Antigua and Barbuda - Money Laundering Prevention Act Amendment 2020", "AML/CFT"),
    ("2022-05-13-07-23-23-AntiguaandBarbudaMoneyLaunderingGuidelines2017MLFTG.pdf",
     "Antigua and Barbuda - Money Laundering Guidelines 2017", "AML/CFT"),
    ("2022-05-13-07-23-46-AntiguaandBarbudaMoneyLaunderingPreventionActAmendmentMLPANo8of2018.pdf",
     "Antigua and Barbuda - Money Laundering Prevention Act Amendment No. 8 of 2018", "AML/CFT"),
    ("2022-05-13-07-24-09-AntiguaandBarbudaMoneyLaunderingRegulationsConsolidation20092017.pdf",
     "Antigua and Barbuda - Money Laundering Regulations Consolidation 2009-2017", "AML/CFT"),
    ("2022-05-13-07-24-33-AntiguaandBarbudaPreventionofTerrorismAct2005.pdf",
     "Antigua and Barbuda - Prevention of Terrorism Act 2005", "AML/CFT"),
    ("2022-05-13-07-24-56-AntiguaandBarbudaPreventionofTerrorismAct2008.pdf",
     "Antigua and Barbuda - Prevention of Terrorism Act 2008", "AML/CFT"),
    ("2022-05-13-07-25-21-AntiguaandBarbudaPreventionofTerrorismAct2010.pdf",
     "Antigua and Barbuda - Prevention of Terrorism Act 2010", "AML/CFT"),

    # AML — Dominica
    ("2022-05-13-07-16-34-Dominica-AML-and-Suppression-of-Terrorist-Financing-2014--Code-of-Practice.PDF",
     "Dominica - AML and Suppression of Terrorist Financing 2014 Code of Practice", "AML/CFT"),
    ("2022-05-13-07-16-57-Dominica-AML-GUIDANCE-NOTES-Revised-2013-Final.pdf",
     "Dominica - AML Guidance Notes (Revised 2013)", "AML/CFT"),
    ("2022-05-13-07-17-22-Dominica-Money-Laundering-Prevention-Amendment-Act-2-of-2016.PDF",
     "Dominica - Money Laundering Prevention Amendment Act 2 of 2016", "AML/CFT"),
    ("2022-05-13-07-17-53-Dominica-Money-Laundering-Prevention-Amendment-Act-4-of-2004.PDF",
     "Dominica - Money Laundering Prevention Amendment Act 4 of 2004", "AML/CFT"),
    ("2022-05-13-07-18-16-Dominica-Money-Laundering-Prevention-Amendment-Act-5-of-2013.PDF",
     "Dominica - Money Laundering Prevention Amendment Act 5 of 2013", "AML/CFT"),
    ("2022-05-13-07-18-39-Dominica-Money-Laundering-Prevention-Amendment-Act-6-of-2020.PDF",
     "Dominica - Money Laundering Prevention Amendment Act 6 of 2020", "AML/CFT"),
    ("2022-05-13-07-19-02-Dominica-Money-Laundering-Prevention-Amendment-Act-8-of-2013.PDF",
     "Dominica - Money Laundering Prevention Amendment Act 8 of 2013", "AML/CFT"),
    ("2022-05-13-07-19-25-Dominica-Money-Laundering-Prevention-Amendment-Act-13-of-2001.PDF",
     "Dominica - Money Laundering Prevention Amendment Act 13 of 2001", "AML/CFT"),
    ("2022-05-13-07-19-49-Dominica-Money-Laundering-Prevention-Amendment-Regulations-2013.PDF",
     "Dominica - Money Laundering Prevention Amendment Regulations 2013", "AML/CFT"),
    ("2022-05-13-07-20-13-Dominica-Money-Laundering-Prevention-Amendment-Regulations-2014.PDF",
     "Dominica - Money Laundering Prevention Amendment Regulations 2014", "AML/CFT"),
    ("2022-05-13-07-20-51-Dominica-Money-Laundering-Prevention-Act-8-of-2011.PDF",
     "Dominica - Money Laundering Prevention Act 8 of 2011", "AML/CFT"),
    ("2022-05-13-07-21-14-Dominica-Money-Laundering-Prevention-Regulation-SRO-4-of-2013.PDF",
     "Dominica - Money Laundering Prevention Regulation SRO 4 of 2013", "AML/CFT"),
    ("2022-05-13-07-21-37-DominicaCodeofPratice2014.PDF",
     "Dominica - Code of Practice 2014", "AML/CFT"),

    # AML — Grenada
    ("2022-05-12-10-07-21-Grenada-ProceedsofCrimeAMLAmendmentRegulations2018.pdf",
     "Grenada - Proceeds of Crime AML Amendment Regulations 2018", "AML/CFT"),
    ("2022-05-12-10-07-55-Grenada-ProceedsofCrimeAMLGuidelinesSRONo6of2012.pdf",
     "Grenada - Proceeds of Crime AML Guidelines SRO No. 6 of 2012", "AML/CFT"),
    ("2022-05-12-10-08-23-Grenada-ProceedsofCrimeActNo6of2012.pdf",
     "Grenada - Proceeds of Crime Act No. 6 of 2012", "AML/CFT"),
    ("2022-05-12-10-08-50-Grenada-SRO-of2015ProceedofCrime-No2Act-Notice2015.pdf",
     "Grenada - SRO 2015 Proceeds of Crime No. 2 Act Notice 2015", "AML/CFT"),
    ("2022-05-12-10-09-19-GrenadaProceedsofCrimeAmendmentActNo10of2013.pdf",
     "Grenada - Proceeds of Crime Amendment Act No. 10 of 2013", "AML/CFT"),
    ("2022-05-12-10-09-47-GrenadaProceedsofCrimeAmendmentActNo11of2014.pdf",
     "Grenada - Proceeds of Crime Amendment Act No. 11 of 2014", "AML/CFT"),
    ("2022-05-12-10-10-15-GrenadaProceedsofCrimeAmendmentActNo33of2013.pdf",
     "Grenada - Proceeds of Crime Amendment Act No. 33 of 2013", "AML/CFT"),
    ("2022-05-12-10-10-41-GrenadaProceedsofCrimeAmendmentActNo35of2014.pdf",
     "Grenada - Proceeds of Crime Amendment Act No. 35 of 2014", "AML/CFT"),
    ("2022-05-12-10-11-08-GrenadaProceedsofCrimeAMLAmendmentGuidelines2018.pdf",
     "Grenada - Proceeds of Crime AML Amendment Guidelines 2018", "AML/CFT"),
    ("2022-05-12-10-35-39-GrenadaProceedsofCrimeAMLAmendmentRegulations-2013.pdf",
     "Grenada - Proceeds of Crime AML Amendment Regulations 2013", "AML/CFT"),
    ("2022-05-12-10-36-05-GrenadaProceedsofCrimeAMLAmendmentRegulations-2014.pdf",
     "Grenada - Proceeds of Crime AML Amendment Regulations 2014", "AML/CFT"),
    ("2022-05-12-10-36-30-GrenadaProceedsofCrimeAMLGuidelinesSRONo24of2013.pdf",
     "Grenada - Proceeds of Crime AML Guidelines SRO No. 24 of 2013", "AML/CFT"),
    ("2022-05-12-10-36-54-GrenadaProceedsofCrimeAMLGuidelinesSRONo58of2014.pdf",
     "Grenada - Proceeds of Crime AML Guidelines SRO No. 58 of 2014", "AML/CFT"),
    ("2022-05-12-10-37-18-GrenadaProceedsofCrimeAMLRegulationsSRONo5of2012.pdf",
     "Grenada - Proceeds of Crime AML Regulations SRO No. 5 of 2012", "AML/CFT"),
    ("2022-05-12-10-37-47-GrenadaProceedsofCrimeAmendmentAct19of2017.pdf",
     "Grenada - Proceeds of Crime Amendment Act 19 of 2017", "AML/CFT"),
    ("2022-05-12-10-38-11-GrenadaProceedsofCrimeAmendmentActNo4of2015.pdf",
     "Grenada - Proceeds of Crime Amendment Act No. 4 of 2015", "AML/CFT"),
    ("2022-05-12-10-38-37-GrenadaSRO26of2018ProceedsofCrime-Act-Commencementorder2018.pdf",
     "Grenada - SRO 26 of 2018 Proceeds of Crime Act Commencement Order 2018", "AML/CFT"),

    # AML — St Vincent and the Grenadines
    ("2022-05-12-09-45-02-SVG-AMLCFT-Legislation-Regulations-2014.PDF",
     "SVG - AML/CFT Legislation Regulations 2014", "AML/CFT"),
    ("2022-05-12-09-47-28-SVGAnti-TerroristFinancingandProliferationAct2015.PDF",
     "SVG - Anti-Terrorist Financing and Proliferation Act 2015", "AML/CFT"),
    ("2022-05-12-09-47-57-SVGFinancial-IntelligenceUnitActCap174.PDF",
     "SVG - Financial Intelligence Unit Act Cap. 174", "AML/CFT"),
    ("2022-05-12-09-48-23-SVGProcceds-of-Crime-Act2013.PDF",
     "SVG - Proceeds of Crime Act 2013", "AML/CFT"),
    ("2022-05-12-09-48-47-SVGProceeds-of-Crime-Amendment-Act-2017.PDF",
     "SVG - Proceeds of Crime Amendment Act 2017", "AML/CFT"),
    ("2022-05-12-10-06-10-AMLCFTSectionsofSVG-LegislationDesignating.pdf",
     "SVG - AML/CFT Sections of Legislation (Designating)", "AML/CFT"),
    ("2022-05-12-10-06-13-SVGAMLTFAmendmentRegulations2017.PDF",
     "SVG - AML/TF Amendment Regulations 2017", "AML/CFT"),
    ("2022-05-12-10-06-17-SVGAnti-Terrorist-Financing-and-Proliferation-Amendment-Act-2017.PDF",
     "SVG - Anti-Terrorist Financing and Proliferation Amendment Act 2017", "AML/CFT"),

    # === Guidelines ===
    ("2022-03-03-05-06-48-Admin-Guidelines-1of2002.pdf",
     "Administrative Guidelines 1 of 2002", "Guidelines"),
    ("2022-03-03-04-51-42-Guidelines-for-External-Auditing.pdf",
     "Guidelines for External Auditing", "Guidelines"),
    ("2022-03-03-08-03-57-BaselIIIIIImplementationRoadMap1.pdf",
     "Basel II/III Implementation Roadmap", "Guidelines"),

    # === Payment System Rules ===
    ("2022-03-24-03-09-07-Eastern-Caribbean-Automated-Clearing-House-Rules-Amendments-2015.pdf",
     "Eastern Caribbean Automated Clearing House Rules - Amendments 2015", "Payment System"),
    ("2022-03-24-03-09-07-Eastern-Caribbean-Automated-Clearing-House-Rules-Amendments-2019.pdf",
     "Eastern Caribbean Automated Clearing House Rules - Amendments 2019", "Payment System"),
    ("2022-03-24-03-09-12-Eastern-Caribbean-Automated-Clearing-House-Rules-SKN-2013.pdf",
     "Eastern Caribbean Automated Clearing House Rules SKN 2013", "Payment System"),
    ("2022-03-24-03-09-12-Eastern-Caribbean-Automated-Clearing-House-Rules-SKN-Amendments-2021.pdf",
     "Eastern Caribbean Automated Clearing House Rules SKN - Amendments 2021", "Payment System"),
    ("2024-04-23-10-59-50-Payment-System-Oversight-Policy-Framework.pdf",
     "Payment System Oversight Policy Framework", "Payment System"),
    ("2023-09-25-04-19-22-The-Payment-System-Strategy-Report.pdf",
     "The Payment System Strategy Report", "Payment System"),

    # === Compliance ===
    ("2022-03-03-07-15-52-LettertoAntiguaLFIsDirectiveonCompliancewithUSSanctions.pdf",
     "Directive on Compliance with US Sanctions (Antigua LFIs)", "Compliance"),
    ("2022-04-08-06-59-01-PresentationontheECCUCreditReportingLegislativeFramework-Read-Only-Compatibility-Mode.pdf",
     "ECCU Credit Reporting Legislative Framework", "Compliance"),

    # === Governance ===
    ("2022-03-02-07-39-26-governancecharter2011.pdf",
     "Governance Charter 2011", "Governance"),
    ("2022-03-02-09-40-20-Conflict-of-Interest-Policy-11082017.pdf",
     "Conflict of Interest Policy (11 August 2017)", "Governance"),
]


def _extract_date_from_filename(filename: str) -> str:
    """Extract upload date from ECCB CDN filename pattern like 2024-07-30-11-52-38-Name.pdf."""
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})-\d{2}-\d{2}-\d{2}-', filename)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""


def _make_doc_id(filename: str) -> str:
    """Create a stable, short document ID from filename."""
    return hashlib.md5(filename.encode()).hexdigest()[:12]


class ECCBScraper(BaseScraper):
    """Scraper for INTL/ECCB — ECCB regulatory documents via CDN."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "application/pdf,*/*",
            })
        return self.session

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"INTL/ECCB/{raw['doc_id']}",
            "_source": "INTL/ECCB",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date", ""),
            "url": raw["url"],
            "category": raw.get("category", ""),
        }

    def _download_and_extract(self, filename: str, doc_id: str) -> Optional[str]:
        """Download PDF from CDN and extract text."""
        url = CDN_BASE + filename
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to download %s: %s", filename, e)
            return None

        if len(resp.content) < 200:
            logger.warning("Skipping %s - too small (%d bytes)", filename, len(resp.content))
            return None

        text = extract_pdf_markdown(
            source="INTL/ECCB",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="legislation",
        ) or ""

        return text if len(text) >= 50 else None

    def fetch_all(self, sample=False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        for filename, title, category in DOCUMENT_CATALOG:
            if limit and count >= limit:
                break

            doc_id = _make_doc_id(filename)
            logger.info("[%d/%d] %s", count + 1, len(DOCUMENT_CATALOG), title[:60])

            text = self._download_and_extract(filename, doc_id)
            if not text:
                continue

            date = _extract_date_from_filename(filename)
            url = f"{SITE_BASE}/viewPDF/documents/{filename}"

            yield {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": url,
                "category": category,
            }
            count += 1
            logger.info("  OK: %s (%d chars)", title[:40], len(text))

        logger.info("Total records yielded: %d / %d catalog entries", count, len(DOCUMENT_CATALOG))

    def fetch_updates(self, since=None):
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick connectivity test — download one small PDF from CDN."""
        try:
            sess = self._get_session()
            test_file = DOCUMENT_CATALOG[0][0]
            url = CDN_BASE + test_file
            resp = sess.get(url, timeout=30)
            resp.raise_for_status()
            ok = len(resp.content) > 200
            logger.info("CDN test: %s (%d bytes) — %s",
                        test_file[:40], len(resp.content),
                        "OK" if ok else "FAIL")
            return ok
        except Exception as e:
            logger.error("CDN test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ECCBScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
