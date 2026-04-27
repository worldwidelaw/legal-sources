#!/usr/bin/env python3
"""
US/EPA -- Environmental Protection Agency Enforcement Actions

Fetches EPA enforcement cases from the ECHO (Enforcement and Compliance History
Online) REST API. ~307K+ enforcement cases with case summaries, penalties,
violations, milestones, and regulatory citations.

Data access:
  - ECHO Case REST Services (no auth required)
  - get_cases: search/filter → returns QID
  - get_qid: paginate results using QID
  - get_case_report: detailed per-case report with CaseSummary text

Usage:
  python bootstrap.py bootstrap          # Full initial pull (all states)
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (recent cases)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.EPA")

API_BASE = "https://echodata.epa.gov/echo"
DELAY = 1.5
PAGE_SIZE = 100

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
]


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal research; https://github.com/ZachLaik/LegalDataHunter)",
        "Accept": "application/json",
    })
    return session


def _retry_get(session: requests.Session, url: str, params: dict,
               timeout: int = 60, max_retries: int = 3) -> requests.Response:
    """GET with exponential backoff retry on 5xx / connection errors."""
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code >= 500 and attempt < max_retries - 1:
                wait = DELAY * (2 ** attempt)
                logger.warning(f"  {resp.status_code} on attempt {attempt+1}, retrying in {wait:.0f}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            if attempt < max_retries - 1:
                wait = DELAY * (2 ** attempt)
                logger.warning(f"  Connection error on attempt {attempt+1}: {e}, retrying in {wait:.0f}s")
                time.sleep(wait)
            else:
                raise
    raise requests.exceptions.RetryError(f"Failed after {max_retries} attempts")


def search_cases(session: requests.Session, state: str) -> Optional[str]:
    """Search for enforcement cases in a state, return QID."""
    url = f"{API_BASE}/case_rest_services.get_cases"
    params = {
        "output": "JSON",
        "p_state": state,
        "p_page": 1,
        "p_per_page": 1,
    }
    try:
        resp = _retry_get(session, url, params)
        data = resp.json()
        results = data.get("Results", {})
        if results.get("Message") == "Success":
            qid = results.get("QueryID")
            total = results.get("QueryRows", "0")
            logger.info(f"  State {state}: {total} cases, QID={qid}")
            return qid
        else:
            err = results.get("Error", {}).get("ErrorMessage", "Unknown error")
            logger.warning(f"  State {state} search failed: {err}")
            return None
    except Exception as e:
        logger.warning(f"  State {state} search error: {e}")
        return None


def get_cases_page(session: requests.Session, qid: str, page: int) -> List[Dict]:
    """Get a page of case results from a QID."""
    url = f"{API_BASE}/case_rest_services.get_qid"
    params = {
        "output": "JSON",
        "qid": qid,
        "pageno": page,
        "pagesize": PAGE_SIZE,
    }
    try:
        resp = _retry_get(session, url, params)
        data = resp.json()
        cases = data.get("Results", {}).get("Cases", [])
        return cases if cases else []
    except Exception as e:
        logger.warning(f"  QID {qid} page {page} error: {e}")
        return []


def get_case_report(session: requests.Session, case_number: str) -> Optional[Dict]:
    """Fetch detailed case report with CaseSummary text."""
    url = f"{API_BASE}/case_rest_services.get_case_report"
    params = {"output": "JSON", "p_id": case_number}
    try:
        resp = _retry_get(session, url, params)
        data = resp.json()
        results = data.get("Results", {})
        if results.get("Message") == "Success":
            return results
        return None
    except Exception as e:
        logger.warning(f"  Case report {case_number} error: {e}")
        return None


def build_text(report: Dict) -> str:
    """Build comprehensive text from case report fields."""
    parts = []

    info = report.get("CaseInformation", {})
    case_name = info.get("CaseName", "")
    case_number = info.get("CaseNumber", "")
    case_type = info.get("CaseType", "")
    case_status = info.get("CaseStatus", "")
    enf_type = info.get("EnforcementType", "")
    enf_outcome = info.get("EnforcementOutcome", "")
    relief = info.get("ReliefSought", "")
    lead = info.get("Lead", "")

    parts.append(f"EPA Enforcement Case {case_number}: {case_name}")
    parts.append(f"Type: {enf_type}")
    parts.append(f"Status: {case_status}")
    parts.append(f"Outcome: {enf_outcome}")
    if relief:
        parts.append(f"Relief Sought: {relief}")
    if lead:
        parts.append(f"Lead Agency: {lead}")

    # Case summary (the main narrative text)
    summary = info.get("CaseSummary")
    if summary and summary.strip():
        parts.append(f"\nCase Summary:\n{summary.strip()}")

    # Penalties
    penalties = []
    fed_pen = info.get("TotalFederalPenalty")
    state_pen = info.get("TotalStatePenalty")
    sep_cost = info.get("TotalSEPCost")
    comp_cost = info.get("TotalComplianceActionCost")
    cost_rec = info.get("TotalCostRecovery")
    if fed_pen and fed_pen != "$0":
        penalties.append(f"Federal Penalty: {fed_pen}")
    if state_pen and state_pen != "$0":
        penalties.append(f"State Penalty: {state_pen}")
    if sep_cost and sep_cost != "$0":
        penalties.append(f"SEP Cost: {sep_cost}")
    if comp_cost and comp_cost != "$0":
        penalties.append(f"Compliance Action Cost: {comp_cost}")
    if cost_rec and cost_rec != "$0":
        penalties.append(f"Cost Recovery: {cost_rec}")
    if penalties:
        parts.append("\nPenalties and Costs:\n" + "\n".join(penalties))

    # Laws and sections
    laws = report.get("LawsAndSections", [])
    if laws:
        law_lines = []
        for law in laws:
            law_name = law.get("Law", "")
            sections = law.get("Sections", "")
            programs = law.get("Programs", "")
            line = f"  {law_name}: {sections}"
            if programs:
                line += f" ({programs})"
            law_lines.append(line)
        parts.append("\nLaws and Sections Violated:\n" + "\n".join(law_lines))

    # Citations (CFR references)
    citations = report.get("Citations", [])
    if citations:
        cite_lines = []
        for c in citations:
            title = c.get("Title", "")
            part = c.get("Part", "")
            section = c.get("Section", "")
            cite_lines.append(f"  {title} Part {part} Section {section}")
        parts.append("\nRegulatory Citations:\n" + "\n".join(cite_lines))

    # Defendants
    defendants = report.get("Defendants", [])
    if defendants:
        def_lines = [f"  {d.get('DefendantName', '')}" for d in defendants]
        parts.append("\nDefendants:\n" + "\n".join(def_lines))

    # Facilities
    facilities = report.get("Facilities", [])
    if facilities:
        fac_lines = []
        for f in facilities:
            name = f.get("FacilityName", "")
            addr = f.get("FacilityAddress", "")
            city = f.get("FacilityCity", "")
            state = f.get("FacilityState", "")
            fac_lines.append(f"  {name}, {addr}, {city}, {state}")
        parts.append("\nFacilities:\n" + "\n".join(fac_lines))

    # Milestones
    milestones = report.get("CaseMilestones", [])
    if milestones:
        ms_lines = []
        for m in milestones:
            event = m.get("Event", "")
            date = m.get("ActualDate", "")
            ms_lines.append(f"  {date}: {event}")
        parts.append("\nCase Milestones:\n" + "\n".join(ms_lines))

    # Pollutants
    pollutants = report.get("Pollutants", [])
    if pollutants:
        poll_lines = [f"  {p.get('PollutantName', '')}" for p in pollutants if p.get("PollutantName")]
        if poll_lines:
            parts.append("\nPollutants:\n" + "\n".join(poll_lines))

    # Enforcement conclusions
    conclusions = report.get("EnforcementConclusions", [])
    if conclusions:
        for i, ec in enumerate(conclusions, 1):
            ec_type = ec.get("EnforcementConclusionType", "")
            ec_name = ec.get("EnforcementConclusionName", "")
            entered = ec.get("SettlementEnteredDate", "")
            fed_pen = ec.get("FederalPenalty", "")
            parts.append(f"\nEnforcement Conclusion #{i}: {ec_type}")
            if ec_name:
                parts.append(f"  Name: {ec_name}")
            if entered:
                parts.append(f"  Settlement Entered: {entered}")
            if fed_pen and fed_pen != "$0":
                parts.append(f"  Federal Penalty: {fed_pen}")

    # Violations
    violations = info.get("Violations")
    if violations and violations.strip():
        parts.append(f"\nViolations:\n{violations.strip()}")

    return "\n".join(parts)


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Convert MM/DD/YYYY to ISO 8601."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str


def normalize(report: Dict, case_basic: Dict = None) -> Dict[str, Any]:
    """Normalize a case report into standard schema."""
    info = report.get("CaseInformation", {})
    case_number = info.get("CaseNumber", "")
    case_name = info.get("CaseName", "")

    # Use settlement date or status date
    date_str = (info.get("CaseStatusDate") or
                (case_basic.get("SettlementDate") if case_basic else None) or
                (case_basic.get("DateFiled") if case_basic else None))

    text = build_text(report)

    # Determine type: case_law for judicial/enforcement, doctrine for guidance
    case_type = info.get("CaseType", "")
    _type = "case_law"  # enforcement actions are case_law

    return {
        "_id": f"US-EPA-{case_number}",
        "_source": "US/EPA",
        "_type": _type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"EPA v. {case_name}" if case_name else f"EPA Case {case_number}",
        "text": text,
        "date": parse_date(date_str),
        "url": f"https://echo.epa.gov/enforcement-case-report?id={case_number}",
        "case_number": case_number,
        "case_name": case_name,
        "case_type": info.get("CaseType"),
        "case_status": info.get("CaseStatus"),
        "enforcement_type": info.get("EnforcementType"),
        "enforcement_outcome": info.get("EnforcementOutcome"),
        "primary_law": (report.get("LawsAndSections", [{}])[0].get("Law")
                        if report.get("LawsAndSections") else None),
        "federal_penalty": info.get("TotalFederalPenalty"),
        "state_penalty": info.get("TotalStatePenalty"),
        "lead_agency": info.get("Lead"),
        "court": "US Environmental Protection Agency",
    }


def fetch_all(sample: bool = False) -> Generator[Dict[str, Any], None, None]:
    """Fetch EPA enforcement cases via ECHO API."""
    session = get_session()
    total_yielded = 0
    sample_limit = 15

    states_to_search = ["NY", "CA", "TX"] if sample else STATES

    for state in states_to_search:
        logger.info(f"Searching state: {state}")
        time.sleep(DELAY)

        qid = search_cases(session, state)
        if not qid:
            continue

        # Paginate through cases
        page = 1
        consecutive_empty = 0
        while True:
            time.sleep(DELAY)
            cases = get_cases_page(session, qid, page)

            if not cases:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    break
                page += 1
                continue
            consecutive_empty = 0

            logger.info(f"  Page {page}: {len(cases)} cases")

            for case_basic in cases:
                case_number = case_basic.get("CaseNumber")
                if not case_number:
                    continue

                time.sleep(DELAY)
                report = get_case_report(session, case_number)
                if not report:
                    continue

                record = normalize(report, case_basic)

                # Skip if text is too short (no meaningful content)
                if len(record.get("text", "")) < 100:
                    logger.warning(f"  Skipping {case_number}: text too short")
                    continue

                yield record
                total_yielded += 1
                logger.info(f"  Record {total_yielded}: {case_number} ({len(record['text'])} chars)")

                if sample and total_yielded >= sample_limit:
                    logger.info(f"Sample complete: {total_yielded} records")
                    return

            page += 1

            # Safety: don't paginate forever
            if page > 5000:
                break

        if sample and total_yielded >= sample_limit:
            return

    logger.info(f"Total records: {total_yielded}")


def fetch_updates(since: str) -> Generator[Dict[str, Any], None, None]:
    """Fetch recently updated cases."""
    # ECHO doesn't have a direct date filter on case updates,
    # so we search recent cases by filing year
    since_year = datetime.fromisoformat(since).year
    current_year = datetime.now().year

    session = get_session()
    total_yielded = 0

    for year in range(current_year, since_year - 1, -1):
        for state in STATES:
            time.sleep(DELAY)
            url = f"{API_BASE}/case_rest_services.get_cases"
            params = {
                "output": "JSON",
                "p_state": state,
                "p_cas_yr": str(year),
            }
            try:
                resp = session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                results = data.get("Results", {})
                qid = results.get("QueryID")
                if not qid:
                    continue
            except Exception:
                continue

            page = 1
            while True:
                time.sleep(DELAY)
                cases = get_cases_page(session, qid, page)
                if not cases:
                    break

                for case_basic in cases:
                    case_number = case_basic.get("CaseNumber")
                    if not case_number:
                        continue

                    time.sleep(DELAY)
                    report = get_case_report(session, case_number)
                    if not report:
                        continue

                    record = normalize(report, case_basic)
                    if len(record.get("text", "")) < 100:
                        continue
                    yield record
                    total_yielded += 1

                page += 1


def test_connectivity() -> bool:
    """Quick connectivity test."""
    session = get_session()
    try:
        # Test case search
        resp = _retry_get(session,
            f"{API_BASE}/case_rest_services.get_cases",
            params={"output": "JSON", "p_state": "NY", "p_per_page": 1},
            timeout=15,
        )
        data = resp.json()
        qid = data.get("Results", {}).get("QueryID")
        total = data.get("Results", {}).get("QueryRows", "0")
        logger.info(f"Search OK: {total} cases in NY, QID={qid}")

        # Test case report
        time.sleep(1)
        resp2 = _retry_get(session,
            f"{API_BASE}/case_rest_services.get_case_report",
            params={"output": "JSON", "p_id": "01-2003-0107"},
            timeout=15,
        )
        data2 = resp2.json()
        summary = data2.get("Results", {}).get("CaseInformation", {}).get("CaseSummary", "")
        logger.info(f"Report OK: CaseSummary length={len(summary)}")

        return True
    except Exception as e:
        logger.error(f"Connectivity test failed: {e}")
        return False


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "test"
    sample = "--sample" in sys.argv

    if cmd == "test":
        ok = test_connectivity()
        sys.exit(0 if ok else 1)

    elif cmd == "bootstrap":
        out_dir = Path(__file__).parent / "sample"
        out_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_all(sample=sample):
            fname = out_dir / f"{record['_id']}.json"
            fname.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
            logger.info(f"Saved {fname.name}")
        logger.info(f"Bootstrap complete: {count} records in {out_dir}")

    elif cmd == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else "2024-01-01"
        out_dir = Path(__file__).parent / "sample"
        out_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_updates(since):
            fname = out_dir / f"{record['_id']}.json"
            fname.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
        logger.info(f"Update complete: {count} records since {since}")

    else:
        print(f"Usage: {sys.argv[0]} [test|bootstrap|update] [--sample]")
        sys.exit(1)
