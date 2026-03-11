# FR/DefenseurDesDroits - French Ombudsman Decisions

## Source Information

- **Name:** Défenseur des Droits (French Ombudsman)
- **URL:** https://juridique.defenseurdesdroits.fr
- **Country:** France
- **Data Types:** case_law, doctrine
- **License:** Licence Ouverte 2.0

## Description

The Défenseur des Droits (Defender of Rights) is the French Ombudsman, an independent
constitutional authority created in 2011 by merging:
- Le Médiateur de la République (national ombudsman)
- La CNDS (police ethics commission)
- Le Défenseur des enfants (children's rights defender)
- La HALDE (anti-discrimination authority)

This source contains the full archive of decisions, recommendations, and other
interventions published by the Défenseur des Droits.

## Document Types

- **Recommandations:** Recommendations to public services for individual cases or
  systemic issues
- **Rappels à la loi:** Formal notices reminding entities of their legal obligations
- **Règlements amiables:** Amicable settlements between parties
- **Observations devant les juridictions:** Written observations submitted to courts
  in litigation

## Technical Details

The documentation portal runs on PMB (PhpMyBibli), an open-source library management
system. Documents are organized in "shelves" (étagères) with decisions in shelf ID 33.

Each document page contains:
- Z3988 (COinS) metadata span with title, date, and decision number
- PDF attachment via `doc_num.php?explnum_id=` endpoint

## API Access

No REST API. Data is collected via HTML scraping of the PMB library interface:

1. Paginate through shelf listing: `index.php?lvl=etagere_see&id=33&page=N`
2. Extract notice IDs and explnum_ids from listing HTML
3. Fetch document detail pages for Z3988 metadata
4. Download PDFs and extract text via pdfplumber

## Usage

```bash
# Generate sample data (12 records)
python3 bootstrap.py bootstrap --sample

# Fetch all decisions
python3 bootstrap.py bootstrap

# Fetch updates since a date
python3 bootstrap.py updates --since 2026-01-01
```

## Statistics

- **Total documents:** ~2,928
- **Coverage:** 2011-present
- **Update frequency:** New decisions added regularly
