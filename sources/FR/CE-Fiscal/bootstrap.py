#!/usr/bin/env python3
"""
FR/CE-Fiscal -- French Council of State Tax Chamber Decisions

Fetches fiscal/tax case law from the Conseil d'État open data archives.
Filters decisions by fiscal chamber assignment and fiscal keyword content.

The same archive data as FR/CouncilState but filtered to tax matters only.

Fiscal chambers at the Conseil d'État:
- 8ème chambre (jugeant seule)
- 3ème et 8ème / 8ème et 3ème chambres réunies
- 9ème chambre (jugeant seule)
- 9ème et 10ème / 10ème et 9ème chambres réunies

Usage:
    python bootstrap.py bootstrap --sample
    python bootstrap.py bootstrap --full
    python bootstrap.py updates --since YYYY-MM-DD
"""

import argparse
import io
import json
import os
import re
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import requests
import yaml

SOURCE_ID = "FR/CE-Fiscal"
BASE_URL = "https://opendata.justice-administrative.fr"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"
REQUEST_DELAY = 1.0

# Chambers that primarily handle fiscal/tax matters
FISCAL_CHAMBER_PATTERNS = [
    r"8[èe]me\s+chambre",
    r"3[èe]me\s+et\s+8[èe]me\s+chambres",
    r"8[èe]me\s+et\s+3[èe]me\s+chambres",
    r"9[èe]me\s+chambre",
    r"9[èe]me\s+et\s+10[èe]me\s+chambres",
    r"10[èe]me\s+et\s+9[èe]me\s+chambres",
]

# Keywords indicating fiscal/tax content
FISCAL_KEYWORDS = [
    "code général des impôts",
    "livre des procédures fiscales",
    "impôt sur le revenu",
    "impôt sur les sociétés",
    "taxe sur la valeur ajoutée",
    "taxe foncière",
    "taxe d'habitation",
    "cotisation foncière",
    "cotisation sur la valeur ajoutée",
    "contribution économique territoriale",
    "droits d'enregistrement",
    "droits de succession",
    "droits de mutation",
    "plus-values",
    "redressement fiscal",
    "vérification de comptabilité",
    "avis de mise en recouvrement",
    "administration fiscale",
    "direction générale des finances publiques",
]

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
STATUS_FILE = SCRIPT_DIR / "status.yaml"


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/xml,application/json,*/*",
        "Accept-Language": "fr,en;q=0.5",
    })
    return session


def clean_html_text(text: str) -> str:
    if not text:
        return ""
    import html as html_mod
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?p[^>]*>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_mod.unescape(text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n ', '\n', text)
    return text.strip()


def is_fiscal_chamber(formation: str) -> bool:
    if not formation:
        return False
    for pattern in FISCAL_CHAMBER_PATTERNS:
        if re.search(pattern, formation, re.IGNORECASE):
            return True
    return False


def has_fiscal_content(text: str) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    matches = sum(1 for kw in FISCAL_KEYWORDS if kw in text_lower)
    return matches >= 2


def is_fiscal_decision(formation: str, text: str) -> bool:
    return is_fiscal_chamber(formation) or has_fiscal_content(text)


def parse_xml_decision(xml_content: str) -> Optional[dict]:
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"  XML parse error: {e}", file=sys.stderr)
        return None

    def get_text(parent, path):
        elem = parent.find(path)
        return elem.text.strip() if elem is not None and elem.text else None

    donnees = root.find('Donnees_Techniques')
    identification = get_text(donnees, 'Identification') if donnees is not None else None
    date_maj = get_text(donnees, 'Date_Mise_Jour') if donnees is not None else None

    dossier = root.find('Dossier')
    if dossier is None:
        return None

    code_juridiction = get_text(dossier, 'Code_Juridiction')
    nom_juridiction = get_text(dossier, 'Nom_Juridiction')
    numero_dossier = get_text(dossier, 'Numero_Dossier')
    date_lecture = get_text(dossier, 'Date_Lecture')
    numero_ecli = get_text(dossier, 'Numero_ECLI')
    avocat_requerant = get_text(dossier, 'Avocat_Requerant')
    type_decision = get_text(dossier, 'Type_Decision')
    type_recours = get_text(dossier, 'Type_Recours')
    code_publication = get_text(dossier, 'Code_Publication')
    solution = get_text(dossier, 'Solution')

    audience = root.find('Audience')
    date_audience = get_text(audience, 'Date_Audience') if audience is not None else None
    numero_role = get_text(audience, 'Numero_Role') if audience is not None else None
    formation_jugement = get_text(audience, 'Formation_Jugement') if audience is not None else None

    decision = root.find('Decision')
    texte_elem = decision.find('Texte_Integral') if decision is not None else None

    if texte_elem is not None:
        raw_text = ET.tostring(texte_elem, encoding='unicode', method='html')
        raw_text = re.sub(r'^<Texte_Integral[^>]*>', '', raw_text)
        raw_text = re.sub(r'</Texte_Integral>$', '', raw_text)
        full_text = clean_html_text(raw_text)
    else:
        full_text = ""

    return {
        'identification': identification,
        'date_mise_jour': date_maj,
        'code_juridiction': code_juridiction,
        'nom_juridiction': nom_juridiction,
        'numero_dossier': numero_dossier,
        'date_lecture': date_lecture,
        'numero_ecli': numero_ecli,
        'avocat_requerant': avocat_requerant,
        'type_decision': type_decision,
        'type_recours': type_recours,
        'code_publication': code_publication,
        'solution': solution,
        'date_audience': date_audience,
        'numero_role': numero_role,
        'formation_jugement': formation_jugement,
        'full_text': full_text,
    }


def normalize(raw: dict) -> dict:
    ecli = raw.get('numero_ecli', '')
    dossier = raw.get('numero_dossier', '')

    doc_id = ecli if ecli else f"CE_FISCAL_{dossier}_{raw.get('date_lecture', '')}"

    title_parts = []
    if raw.get('nom_juridiction'):
        title_parts.append(raw['nom_juridiction'])
    else:
        title_parts.append("Conseil d'État")
    title_parts.append("(Fiscal)")
    if raw.get('type_decision'):
        title_parts.append(raw['type_decision'])
    if raw.get('formation_jugement'):
        title_parts.append(raw['formation_jugement'])
    if dossier:
        title_parts.append(f"n° {dossier}")
    if raw.get('date_lecture'):
        title_parts.append(f"du {raw['date_lecture']}")

    title = " - ".join(title_parts) if title_parts else f"Décision fiscale {dossier}"

    if ecli:
        url = f"https://www.conseil-etat.fr/arianeweb/CE/decision/{raw.get('date_lecture', '').replace('-', '')}/{dossier}"
    else:
        url = "https://opendata.justice-administrative.fr/recherche/"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get('full_text', ''),
        "date": raw.get('date_lecture'),
        "url": url,
        "ecli": ecli,
        "case_number": dossier,
        "court": raw.get('nom_juridiction'),
        "court_code": raw.get('code_juridiction'),
        "court_type": "CE",
        "court_tier": 1,
        "decision_type": raw.get('type_decision'),
        "appeal_type": raw.get('type_recours'),
        "publication_code": raw.get('code_publication'),
        "formation": raw.get('formation_jugement'),
        "solution": raw.get('solution'),
        "hearing_date": raw.get('date_audience'),
        "lawyer": raw.get('avocat_requerant'),
        "fiscal_chamber": is_fiscal_chamber(raw.get('formation_jugement', '')),
        "fiscal_keywords_detected": has_fiscal_content(raw.get('full_text', '')),
    }


def get_available_archives(session: requests.Session) -> List[Tuple[int, int]]:
    archives = []
    try:
        response = session.get(f"{BASE_URL}/DCE/", timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching archive list: {e}", file=sys.stderr)
        return []

    pattern = r'/DCE/(\d{4})/(\d{2})/CE_\d+\.zip'
    for match in re.finditer(pattern, response.text):
        year = int(match.group(1))
        month = int(match.group(2))
        archives.append((year, month))

    return sorted(archives, reverse=True)


def fetch_archive(session: requests.Session, year: int, month: int) -> Generator[dict, None, None]:
    url = f"{BASE_URL}/DCE/{year}/{month:02d}/CE_{year}{month:02d}.zip"
    print(f"  Downloading {url}...")

    try:
        response = session.get(url, timeout=120)
        if response.status_code == 404:
            print(f"    Archive not found: {url}")
            return
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"    Error downloading archive: {e}", file=sys.stderr)
        return

    try:
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            total = 0
            fiscal = 0
            for name in zf.namelist():
                if not name.endswith('.xml'):
                    continue
                total += 1

                try:
                    xml_content = zf.read(name).decode('utf-8')
                    raw = parse_xml_decision(xml_content)
                    if not raw or not raw.get('full_text'):
                        continue

                    formation = raw.get('formation_jugement', '')
                    text = raw.get('full_text', '')

                    if is_fiscal_decision(formation, text):
                        fiscal += 1
                        yield normalize(raw)

                except Exception as e:
                    print(f"    Error parsing {name}: {e}", file=sys.stderr)
                    continue

            print(f"    {fiscal}/{total} decisions identified as fiscal")

    except zipfile.BadZipFile as e:
        print(f"    Invalid ZIP file: {e}", file=sys.stderr)
        return


def fetch_sample(session: requests.Session, count: int = 15) -> List[dict]:
    records = []
    archives = get_available_archives(session)

    if not archives:
        print("No archives found!", file=sys.stderr)
        return []

    print(f"Found {len(archives)} CE archives. Filtering for fiscal decisions...")

    for year, month in archives[:6]:
        if len(records) >= count:
            break

        print(f"\nProcessing CE {year}-{month:02d}...")

        for record in fetch_archive(session, year, month):
            if len(records) >= count:
                break
            if len(record.get('text', '')) > 500:
                records.append(record)
                print(f"  [{len(records)}/{count}] {record['_id']}: {len(record['text'])} chars "
                      f"({record.get('formation', 'unknown')})")

        time.sleep(REQUEST_DELAY)

    return records


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    archives = get_available_archives(session)
    for year, month in archives:
        print(f"Processing CE {year}-{month:02d}...")
        for record in fetch_archive(session, year, month):
            if record.get('text'):
                yield record
        time.sleep(REQUEST_DELAY)


def fetch_updates(session: requests.Session, since: datetime) -> Generator[dict, None, None]:
    archives = get_available_archives(session)
    since_year = since.year
    since_month = since.month

    for year, month in archives:
        if (year, month) < (since_year, since_month):
            continue
        print(f"Processing CE {year}-{month:02d}...")
        for record in fetch_archive(session, year, month):
            record_date = record.get('date')
            if record_date and record_date >= since.strftime("%Y-%m-%d"):
                if record.get('text'):
                    yield record
        time.sleep(REQUEST_DELAY)


def save_samples(records: List[dict]) -> None:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def update_status(records_fetched: int, errors: int, sample_count: int = 0) -> None:
    now = datetime.now(timezone.utc).isoformat()
    status = {
        "last_run": now,
        "last_bootstrap": now if sample_count > 0 else None,
        "last_error": None,
        "total_records": 0,
        "run_history": [{
            "started_at": now,
            "finished_at": now,
            "records_fetched": records_fetched,
            "sample_records_saved": sample_count,
            "errors": errors,
        }]
    }
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                existing = yaml.safe_load(f) or {}
            if "run_history" in existing:
                status["run_history"] = existing["run_history"][-9:] + status["run_history"]
        except Exception:
            pass

    with open(STATUS_FILE, 'w') as f:
        yaml.dump(status, f, default_flow_style=False)


def main():
    parser = argparse.ArgumentParser(description="FR/CE-Fiscal tax chamber decisions fetcher")
    subparsers = parser.add_subparsers(dest="command")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full fetch")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")

    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    session = get_session()

    if args.command == "bootstrap":
        if args.sample:
            print(f"Fetching {args.count} fiscal sample records from Conseil d'État...")
            records = fetch_sample(session, args.count)
            if records:
                save_samples(records)
                update_status(len(records), 0, len(records))

                text_lengths = [len(r.get('text', '')) for r in records]
                avg_len = sum(text_lengths) / len(text_lengths)
                chamber_count = sum(1 for r in records if r.get('fiscal_chamber'))
                keyword_count = sum(1 for r in records if r.get('fiscal_keywords_detected'))

                print(f"\n=== SUMMARY ===")
                print(f"  Records: {len(records)}")
                print(f"  Avg text length: {avg_len:.0f} chars")
                print(f"  Min text length: {min(text_lengths)} chars")
                print(f"  Max text length: {max(text_lengths)} chars")
                print(f"  From fiscal chamber: {chamber_count}")
                print(f"  Fiscal keywords detected: {keyword_count}")
            else:
                print("No records fetched!", file=sys.stderr)
                update_status(0, 1)
                sys.exit(1)

        elif args.full:
            print("Starting full fiscal fetch...")
            count = 0
            for record in fetch_all(session):
                count += 1
                if count % 100 == 0:
                    print(f"  {count} fiscal records...")
            print(f"Fetched {count} fiscal records")
            update_status(count, 0)

    elif args.command == "updates":
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        print(f"Fetching fiscal updates since {since.date()}...")
        count = 0
        for record in fetch_updates(session, since):
            count += 1
        print(f"Fetched {count} updated fiscal records")
        update_status(count, 0)


if __name__ == "__main__":
    main()
