# Spain — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Spain is a **civil law** country organized as a **quasi-federal state** with **17 Comunidades Autonomas** (Autonomous Communities) and 2 Autonomous Cities (Ceuta, Melilla). Each community has its own parliament, executive, and statutory law with significant legislative competence.

**Court hierarchy:** Juzgados -> Audiencias Provinciales -> Tribunales Superiores de Justicia -> **Tribunal Supremo**; **Tribunal Constitucional**; **Audiencia Nacional**.

Spain publishes consolidated legislation through the **BOE** (Boletin Oficial del Estado) with ELI support.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| Andalusia | legislation | Yes | OK | 12 | 12 | **Working** |
| BOE | legislation | Yes | OK | 12 | 12 | **Working** |
| BasqueCountry | legislation | Yes | Never run | - | 0 | Untested |
| Catalonia | legislation | Yes | OK | 12 | 12 | **Working** |
| ConstitutionalCourt | case_law | Yes | OK | 12 | 12 | **Working** |

**5 sources total:** 4 working, 1 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|------------------------|
| BOE | Consolidated + gazette | Official gazette with consolidated texts, ELI | Yes | Web, ELI, open data | Excellent |
| Tribunal Constitucional | Constitutional court | Decisions and orders | Yes | Web scraping | Good |
| CENDOJ | Case law (all courts) | Centro de Documentacion Judicial | Yes | cendoj.ramajudicial.es | Good — largest case law DB |
| **17 Community gazettes** | Regional legislation | Each community has its own gazette | **No** | Various regional portals | Variable |
| **CNMV** | Financial regulator | Securities market commission | **No** | cnmv.es | Published |
| **CNMC** | Competition authority | Markets and competition commission | **No** | cnmc.es | Searchable |
| **AEPD** | Data protection | Spanish DPA | **No** | aepd.es | Published |
| **Banco de Espana** | Central bank | Banking supervision | **No** | bde.es | Available |

## Consolidated Legislation vs. Official Journal

Spain has **both** via the BOE: consolidated texts ("textos consolidados") with ELI URIs and the daily official gazette. We index the BOE as a unified source.

## Sub-jurisdictions

Spain's **17 Autonomous Communities** have significant legislative autonomy. Each has its own official gazette (BOJA, BOA, DOGC, etc.). Basque Country and Navarra have fiscal autonomy. Catalonia, Galicia, Basque Country, and Valencia have co-official languages. **No autonomous community legislation is currently indexed** — a significant gap.

## Regulatory & Administrative Authorities

| Authority | Domain | Indexed? |
|-----------|--------|----------|
| CNMV | Securities, financial markets | No |
| CNMC | Competition, telecom, energy, transport | No |
| AEPD | Data protection (GDPR) | No |
| Banco de Espana | Banking supervision | No |

## Access Notes

- **BOE:** Excellent digital infrastructure with ELI support. One of Europe's best gazette platforms.
- **CENDOJ:** Free web access. Covers all court levels.
- **Language:** Spanish (Castellano). Catalan, Basque, Galician, Valencian are co-official in their communities.

## How to Contribute

Priority: Autonomous Community gazettes (Cataluna, Pais Vasco, Andalucia, Madrid), CNMC, CNMV, AEPD. Create directories under `sources/ES/[SourceName]/`.
