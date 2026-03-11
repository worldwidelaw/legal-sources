# Finland — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Finland is a **civil law** country in the **Nordic tradition**, **unitary** with one autonomous region — the **Aland Islands**. Officially **bilingual** (Finnish/Swedish). Finland provides **consolidated legislation** through **Finlex** in Akoma Ntoso XML (CC BY 4.0). Court decisions via **LawSampo** SPARQL.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| Eduskunta | unknown | Yes | OK | 12 | 12 | **Working** |
| Finlex | legislation | Yes | OK | 12 | 12 | **Working** |
| SupremeAdministrativeCourt | case_law | Yes | Never run | - | 0 | Untested |
| SupremeCourt | case_law | Yes | Never run | - | 0 | Untested |

**4 sources total:** 2 working, 2 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Finlex | Consolidated legislation | Akoma Ntoso XML, CC BY 4.0 | Yes | Excellent |
| Eduskunta | Parliamentary | Parliament open data | Yes | Good |
| KKO (via LawSampo) | Supreme Court | SPARQL endpoint, CC BY 4.0 | Yes | Good |
| KHO (via LawSampo) | Supreme Admin Court | SPARQL endpoint, CC BY 4.0 | Yes | Good |
| **Lower courts** | Case law | District/appellate courts | **No** | Not systematically published |
| **Aland Islands** | Autonomous legislation | Aland laws | **No** | regeringen.ax | Available |
| **Fin-FSA** | Financial regulator | Financial Supervisory Authority | **No** | finanssivalvonta.fi | Published |
| **FCCA** | Competition | Finnish Competition and Consumer Authority | **No** | kkv.fi | Published |
| **Data Protection Ombudsman** | Data protection | Finnish DPA | **No** | tietosuoja.fi | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via Finlex (Akoma Ntoso XML). Official gazette: Suomen Saadoskokoelma. Both available through Finlex.

## Sub-jurisdictions

**Aland Islands**: autonomous, Swedish-speaking region with own parliament (Lagting). Not currently indexed.

## How to Contribute

Priority: lower courts, Aland legislation, regulatory decisions. Create directories under `sources/FI/[SourceName]/`.
