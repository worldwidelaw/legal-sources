# Belgium — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Belgium is a **federal state** with a **civil law** system and a complex institutional structure: **3 regions** (Flanders, Wallonia, Brussels-Capital), **3 communities** (French, Flemish, German-speaking), and the federal level. Belgium is officially **trilingual** (French, Dutch, German). Each entity has its own parliament and legislative competence.

**Court hierarchy:** Tribunaux de premiere instance -> Cours d'appel -> **Cour de cassation**; Conseil d'Etat (administrative); **Cour constitutionnelle**.

Belgium publishes legislation through the **Moniteur belge/Belgisch Staatsblad** with ELI support and SPARQL access. Belgium is an ELI pioneer.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| ConseilEtat | case_law | Yes | Never run | - | 0 | Untested |
| CourConstitutionnelle | case_law | Yes | OK (0 records) | 0 | 0 | Runs OK, no samples |
| cass | case_law | Yes | OK | 12 | 12 | **Working** |
| moniteurbelge | legislation | Yes | OK | 12 | 12 | **Working** |

**4 sources total:** 2 working, 1 runs OK (no samples), 1 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|------------------------|
| Moniteur belge | Legislation | Official gazette, ELI, SPARQL endpoint | Yes | CC0, SPARQL | Excellent — ELI pioneer |
| Cour constitutionnelle | Constitutional court | Constitutional review decisions | Yes | Web | Good |
| Cour de cassation | Supreme court | Civil and criminal cassation | Yes | Web | Good |
| Conseil d'Etat | Admin supreme court | Administrative decisions | Yes | Web | Good |
| JUPORTAL | Multiple courts | Judicial portal aggregating decisions | Yes | Web | Good |
| **Vlaamse Codex** | Flemish legislation | Consolidated Flemish regional law | **No** | codex.vlaanderen.be | Good — has own API |
| **Lower courts** | Case law | Tribunaux/rechtbanken decisions | **No** | Varies | Limited availability |
| **FSMA** | Financial regulator | Financial Services and Markets Authority | **No** | fsma.be | Published |
| **Autorite belge de la concurrence** | Competition | Competition decisions | **No** | Web | Published |
| **APD/GBA** | Data protection | Data Protection Authority | **No** | Web | Published |

## Consolidated Legislation vs. Official Journal

Belgium publishes through the **Moniteur belge** (official gazette). Consolidated texts are available via the federal legislative database with ELI URIs. Belgium chairs EU ELI initiatives.

## Sub-jurisdictions

| Entity | Legislature | Notes |
|--------|------------|-------|
| Flemish Region/Community | Vlaams Parlement | Merged region + community parliament |
| Walloon Region | Parlement wallon | Walloon decrees |
| Brussels-Capital Region | Brussels Parliament | Ordinances |
| French Community | Parlement de la Communaute francaise | Community decrees |
| German-speaking Community | Parlament der Deutschsprachigen Gemeinschaft | In German |

**Gap**: Vlaamse Codex (Flemish consolidated legislation) has a separate API not yet indexed.

## Regulatory & Administrative Authorities

| Authority | Domain | Indexed? |
|-----------|--------|----------|
| FSMA | Financial markets | No |
| Autorite belge de la concurrence | Competition | No |
| APD/GBA | Data protection | No |
| IBPT/BIPT | Telecommunications | No |
| CREG | Energy regulation | No |

## Access Notes

- **License:** CC0 for most official sources.
- **Languages:** All federal legislation in French, Dutch, and German.
- **ELI:** Belgium is an ELI pioneer with stable, well-structured URIs.

## How to Contribute

Priority: Vlaamse Codex, lower court decisions, FSMA, competition authority. Create directories under `sources/BE/[SourceName]/`.
