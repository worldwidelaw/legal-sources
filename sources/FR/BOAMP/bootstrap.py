#!/usr/bin/env python3
"""
BOAMP (Bulletin officiel des annonces des marchés publics) Data Fetcher

Fetches French public procurement notices via the OpenDataSoft API.
Contains full text of procurement announcements including contract details,
lot descriptions, award information, and procedural requirements.

Data source: https://boamp-datadila.opendatasoft.com/api/v2/
Total records: ~1.6M notices
License: Licence Ouverte (Open Licence 2.0)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests

# Constants
API_BASE = "https://boamp-datadila.opendatasoft.com/api/v2/catalog/datasets/boamp/records"
RATE_LIMIT_DELAY = 0.5
PAGE_SIZE = 20

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LegalDataHunter/1.0 (academic research; legal data collection)",
    "Accept": "application/json",
})


def extract_text_from_donnees(donnees_str: str) -> str:
    """Extract full text content from the donnees JSON field."""
    if not donnees_str:
        return ""

    try:
        donnees = json.loads(donnees_str) if isinstance(donnees_str, str) else donnees_str
    except (json.JSONDecodeError, TypeError):
        return str(donnees_str)[:5000]

    if not isinstance(donnees, dict):
        return str(donnees)[:5000]

    text_parts = []

    # Standard BOAMP format (pre-2024)
    # IDENTITE section
    identite = donnees.get('IDENTITE', {})
    if isinstance(identite, dict):
        nom = identite.get('DENOMINATION', '') or identite.get('NOM_ORGANISME', '')
        if nom:
            text_parts.append(f"Acheteur: {nom}")

    # OBJET section - main content
    objet = donnees.get('OBJET', {})
    if isinstance(objet, dict):
        titre = objet.get('TITRE_MARCHE', '')
        if titre:
            text_parts.append(f"Objet du marché: {titre}")
        objet_complet = objet.get('OBJET_COMPLET', '')
        if objet_complet:
            text_parts.append(objet_complet)
        # Lots
        lots = objet.get('DIV_EN_LOTS', {})
        if isinstance(lots, dict):
            lot_list = lots.get('LOT', [])
            if isinstance(lot_list, list):
                for lot in lot_list:
                    if isinstance(lot, dict):
                        lot_text = lot.get('INTITULE', '') or lot.get('DESCRIPTION', '')
                        if lot_text:
                            num = lot.get('NUM', '')
                            text_parts.append(f"Lot {num}: {lot_text}")

    # PROCEDURE section
    proc = donnees.get('PROCEDURE', {})
    if isinstance(proc, dict):
        proc_type = proc.get('TYPE_PROCEDURE', '')
        if proc_type:
            text_parts.append(f"Procédure: {proc_type}")

    # CONDITIONS section
    for key in ['CONDITION_PARTICIPATION', 'CONDITION_RELATIVE_MARCHE',
                'CONDITION_ADMINISTRATIVE', 'CONDITION_DELAI']:
        cond = donnees.get(key, {})
        if isinstance(cond, dict):
            for sub_key, sub_val in cond.items():
                if isinstance(sub_val, str) and len(sub_val) > 20:
                    text_parts.append(sub_val)

    # RENSEIGNEMENTS_COMPLEMENTAIRES
    rens = donnees.get('RENSEIGNEMENTS_COMPLEMENTAIRES', {})
    if isinstance(rens, dict):
        for sub_key, sub_val in rens.items():
            if isinstance(sub_val, str) and len(sub_val) > 20:
                text_parts.append(sub_val)

    # eForms format (2024+) - FNSimple structure
    fn = donnees.get('FNSimple', {})
    if isinstance(fn, dict):
        org = fn.get('organisme', {})
        if isinstance(org, dict):
            nom = org.get('nom', '')
            if nom:
                text_parts.append(f"Acheteur: {nom}")

        attribution = fn.get('attribution', {})
        if isinstance(attribution, dict):
            for k, v in attribution.items():
                if isinstance(v, str) and len(v) > 20:
                    text_parts.append(v)
                elif isinstance(v, dict):
                    for sub_v in v.values():
                        if isinstance(sub_v, str) and len(sub_v) > 20:
                            text_parts.append(sub_v)

    # Fallback: if no structured text found, dump all string values
    if not text_parts:
        def extract_strings(obj, depth=0):
            if depth > 5:
                return
            if isinstance(obj, str) and len(obj) > 30:
                text_parts.append(obj)
            elif isinstance(obj, dict):
                for v in obj.values():
                    extract_strings(v, depth + 1)
            elif isinstance(obj, list):
                for item in obj:
                    extract_strings(item, depth + 1)
        extract_strings(donnees)

    # Filter out UUID-only lines and clean up
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    text_parts = [p for p in text_parts if not uuid_pattern.match(p.strip())]

    return '\n\n'.join(text_parts)


def normalize(raw: dict) -> dict:
    """Transform raw BOAMP record into normalized schema."""
    fields = raw.get('fields', raw)

    idweb = fields.get('idweb', '')
    text = extract_text_from_donnees(fields.get('donnees', ''))

    # Build title from objet field + nature
    title = fields.get('objet', '')
    nature = fields.get('nature_libelle', '')
    if nature and title:
        title = f"{nature} - {title}"
    elif nature:
        title = nature

    url = fields.get('url_avis', '') or f"https://www.boamp.fr/avis/detail/{idweb}"

    return {
        '_id': f"boamp-{idweb}",
        '_source': 'FR/BOAMP',
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': title,
        'text': text,
        'date': fields.get('dateparution', None),
        'url': url,
        'buyer_name': fields.get('nomacheteur', ''),
        'contract_type': fields.get('type_marche', ''),
        'nature': fields.get('nature_libelle', ''),
        'procedure': fields.get('procedure_libelle', ''),
        'department': fields.get('code_departement_prestation', ''),
        'family': fields.get('famille_libelle', ''),
    }


def fetch_all(max_items: int = None) -> Generator[dict, None, None]:
    """Fetch all BOAMP records via API."""
    offset = 0
    count = 0

    while True:
        if max_items and count >= max_items:
            return

        try:
            resp = SESSION.get(API_BASE, params={
                'limit': PAGE_SIZE,
                'offset': offset,
                'order_by': 'dateparution DESC',
            }, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"  [WARN] API request failed at offset {offset}: {e}", file=sys.stderr)
            break

        records = data.get('records', [])
        if not records:
            break

        for rec in records:
            if max_items and count >= max_items:
                return
            fields = rec.get('record', {}).get('fields', {})
            doc = normalize({'fields': fields})
            if doc['text']:
                yield doc
                count += 1
                if count % 10 == 0:
                    print(f"  Fetched {count} records...", file=sys.stderr)

        offset += PAGE_SIZE
        if offset >= data.get('total_count', 0) or offset >= 10000:
            break
        time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch records published since a given date."""
    offset = 0
    count = 0

    while True:
        try:
            resp = SESSION.get(API_BASE, params={
                'limit': PAGE_SIZE,
                'offset': offset,
                'where': f"dateparution>='{since}'",
                'order_by': 'dateparution DESC',
            }, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"  [WARN] API request failed: {e}", file=sys.stderr)
            break

        records = data.get('records', [])
        if not records:
            break

        for rec in records:
            fields = rec.get('record', {}).get('fields', {})
            doc = normalize({'fields': fields})
            if doc['text']:
                yield doc
                count += 1

        offset += PAGE_SIZE
        if offset >= data.get('total_count', 0) or offset >= 10000:
            break
        time.sleep(RATE_LIMIT_DELAY)


def bootstrap(sample: bool = False):
    """Bootstrap the data source with sample or full data."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    max_items = 15 if sample else None
    count = 0

    for doc in fetch_all(max_items=max_items):
        count += 1
        filename = re.sub(r'[^\w\-]', '_', doc['_id'])[:100] + '.json'
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        print(f"  [{count}] {doc['title'][:80]}", file=sys.stderr)

    print(f"\nTotal: {count} documents saved to {sample_dir}", file=sys.stderr)
    if count == 0:
        print("[ERROR] No records written!", file=sys.stderr)
        sys.exit(1)

    # Validate
    has_text = 0
    for f in sample_dir.glob('*.json'):
        with open(f) as fh:
            rec = json.load(fh)
            if rec.get('text') and len(rec['text']) > 50:
                has_text += 1
    print(f"Records with full text: {has_text}/{count}", file=sys.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='BOAMP Data Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch only sample data (15 records)')
    parser.add_argument('--since', type=str, default=None,
                        help='Fetch updates since date (ISO format)')
    args = parser.parse_args()

    if args.command == 'bootstrap':
        bootstrap(sample=args.sample)
    elif args.command == 'updates':
        if not args.since:
            print("Error: --since required for updates", file=sys.stderr)
            sys.exit(1)
        for doc in fetch_updates(args.since):
            print(json.dumps(doc, ensure_ascii=False))
