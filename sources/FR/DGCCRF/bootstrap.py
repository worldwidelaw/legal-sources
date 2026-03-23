#!/usr/bin/env python3
"""
DGCCRF RappelConso Data Fetcher

Fetches French product recall notices from the RappelConso platform via the
data.economie.gouv.fr OpenDataSoft API.

Data source: https://data.economie.gouv.fr/explore/dataset/rappelconso-v2-gtin-trie/
Total records: ~16,800 product recalls
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
from typing import Generator

import requests

# Constants
API_BASE = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/rappelconso-v2-gtin-trie/records"
RATE_LIMIT_DELAY = 0.5
PAGE_SIZE = 20

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "WorldWideLaw/1.0 (academic research; legal data collection)",
    "Accept": "application/json",
})


def build_text(rec: dict) -> str:
    """Build full text content from recall record fields."""
    parts = []

    if rec.get("libelle"):
        parts.append(f"Produit: {rec['libelle']}")

    if rec.get("marque_produit"):
        parts.append(f"Marque: {rec['marque_produit']}")

    if rec.get("modeles_ou_references"):
        parts.append(f"Modèles/Références: {rec['modeles_ou_references']}")

    if rec.get("nature_juridique_rappel"):
        parts.append(f"Nature juridique du rappel: {rec['nature_juridique_rappel']}")

    if rec.get("motif_rappel"):
        parts.append(f"Motif du rappel: {rec['motif_rappel']}")

    if rec.get("risques_encourus"):
        parts.append(f"Risques encourus: {rec['risques_encourus']}")

    if rec.get("description_complementaire_risque"):
        parts.append(f"Description complémentaire du risque: {rec['description_complementaire_risque']}")

    if rec.get("preconisations_sanitaires"):
        parts.append(f"Préconisations sanitaires: {rec['preconisations_sanitaires']}")

    if rec.get("conduites_a_tenir_par_le_consommateur"):
        conduites = rec["conduites_a_tenir_par_le_consommateur"].replace("|", ", ")
        parts.append(f"Conduites à tenir: {conduites}")

    if rec.get("modalites_de_compensation"):
        parts.append(f"Modalités de compensation: {rec['modalites_de_compensation']}")

    if rec.get("zone_geographique_de_vente"):
        parts.append(f"Zone géographique: {rec['zone_geographique_de_vente']}")

    if rec.get("distributeurs"):
        parts.append(f"Distributeurs: {rec['distributeurs']}")

    if rec.get("informations_complementaires"):
        parts.append(f"Informations complémentaires: {rec['informations_complementaires']}")

    if rec.get("informations_complementaires_publiques"):
        parts.append(f"Informations publiques: {rec['informations_complementaires_publiques']}")

    return "\n\n".join(parts)


def normalize(rec: dict) -> dict:
    """Transform raw RappelConso record into normalized schema."""
    text = build_text(rec)
    fiche = rec.get("numero_fiche", "")
    doc_id = rec.get("id", fiche)

    title_parts = []
    if rec.get("nature_juridique_rappel"):
        title_parts.append(f"Rappel {rec['nature_juridique_rappel']}")
    if rec.get("libelle"):
        title_parts.append(rec["libelle"])
    if rec.get("marque_produit"):
        title_parts.append(f"({rec['marque_produit']})")
    title = " - ".join(title_parts) if title_parts else f"Rappel {fiche}"

    date_val = rec.get("date_publication", "")
    if date_val:
        date_val = date_val[:10]

    url = rec.get("lien_vers_la_fiche_rappel", "")
    if not url:
        url = f"https://rappel.conso.gouv.fr/fiche-rappel/{doc_id}/interne"

    return {
        "_id": f"rappelconso-{doc_id}",
        "_source": "FR/DGCCRF",
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_val,
        "url": url,
        "category": rec.get("categorie_produit", ""),
        "subcategory": rec.get("sous_categorie_produit", ""),
        "nature_juridique": rec.get("nature_juridique_rappel", ""),
    }


def fetch_all(max_items: int = None) -> Generator[dict, None, None]:
    """Fetch all RappelConso records via API."""
    offset = 0
    count = 0

    while True:
        if max_items and count >= max_items:
            return

        try:
            resp = SESSION.get(API_BASE, params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "order_by": "date_publication DESC",
            }, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"  [WARN] API request failed at offset {offset}: {e}", file=sys.stderr)
            break

        records = data.get("results", [])
        if not records:
            break

        for rec in records:
            if max_items and count >= max_items:
                return
            doc = normalize(rec)
            if doc["text"] and len(doc["text"]) > 50:
                yield doc
                count += 1
                if count % 10 == 0:
                    print(f"  Fetched {count} records...", file=sys.stderr)

        offset += PAGE_SIZE
        total = data.get("total_count", 0)
        if offset >= total or offset >= 10000:
            break
        time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch records published since a given date."""
    offset = 0
    count = 0

    while True:
        try:
            resp = SESSION.get(API_BASE, params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "where": f"date_publication>='{since}'",
                "order_by": "date_publication DESC",
            }, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"  [WARN] API request failed: {e}", file=sys.stderr)
            break

        records = data.get("results", [])
        if not records:
            break

        for rec in records:
            doc = normalize(rec)
            if doc["text"] and len(doc["text"]) > 50:
                yield doc
                count += 1

        offset += PAGE_SIZE
        total = data.get("total_count", 0)
        if offset >= total or offset >= 10000:
            break
        time.sleep(RATE_LIMIT_DELAY)


def bootstrap(sample: bool = False):
    """Bootstrap the data source with sample or full data."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    max_items = 25 if sample else None
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
    parser = argparse.ArgumentParser(description='DGCCRF RappelConso Data Fetcher')
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
