# Luxembourg — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Luxembourg is a **civil law** country (Napoleonic tradition) with a **unitary** structure. Grand Duchy with a unicameral parliament (Chambre des Deputes). Luxembourg chairs the EU ELI Task Force, resulting in excellent data quality and structured linked data.

Official languages: Luxembourgish, French, German (legislation primarily in French).

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| LegalDatabase | legislation | Yes | OK | 12 | 12 | **Working** |
| Parliament | parliamentary_proceedings | Yes | OK | 12 | 0 | Runs OK, no samples |
| SupremeCourt | case_law | Yes | Never run | - | 0 | Untested |

**3 sources total:** 1 working, 1 runs OK (no samples), 1 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Legilux | Consolidated + gazette | SPARQL endpoint, full ELI, CC BY 4.0 | Yes | Excellent — best-in-class ELI |
| Parliament | Parliamentary | Written Q&A via data.public.lu | Yes | CC0 | Good |
| Cour de Cassation | Supreme court | Via data.public.lu | Yes | CC BY-ND | 2,346 decisions |
| **Conseil d'Etat** | Admin court / advisory | Administrative supreme court | **No** | justice.public.lu | Not in structured format |
| **Cour Constitutionnelle** | Constitutional court | Constitutional rulings | **No** | justice.public.lu | Limited number |
| **CSSF** | Financial regulator | Financial sector regulator | **No** | cssf.lu | Circulars published |
| **Conseil de la concurrence** | Competition | Competition decisions | **No** | Web | Very limited |

## Consolidated Legislation vs. Official Journal

Both via Legilux: consolidated texts, Memorial A (laws), Memorial B (regulations). Single authoritative source with SPARQL and ELI.

## Sub-jurisdictions

**Unitary state** with no federated sub-jurisdictions. 100 communes have local regulations not in Legilux.

## Access Notes

- **License:** CC BY 4.0 for Legilux. CC0 for parliamentary data. CC BY-ND for Cour de Cassation.
- **SPARQL:** Legilux offers a SPARQL endpoint — powerful structured queries.
- **ELI:** Luxembourg is the EU ELI Task Force chair. Reference model implementation.

## How to Contribute

Priority: Conseil d'Etat, Cour Constitutionnelle, CSSF. Create directories under `sources/LU/[SourceName]/`.
