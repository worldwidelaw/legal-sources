# Portugal — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Portugal is a **civil law** country with a **unitary** structure. It has autonomous regions (Azores, Madeira) with their own regional legislatures.

**Court hierarchy:** Tribunais de primeira instancia -> Tribunais da Relacao -> **Supremo Tribunal de Justica**; Tribunais administrativos -> **Supremo Tribunal Administrativo**; **Tribunal Constitucional**.

Portugal publishes through the **DRE** (Diario da Republica Eletronico) with ELI support.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| ConstitutionalCourt | case_law | Yes | OK | 12 | 12 | **Working** |
| DiarioRepublica | legislation | Yes | OK | 12 | 12 | **Working** |
| SupremeCourt | case_law | Yes | OK | 12 | 12 | **Working** |

**3 sources total:** 3 working.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| DRE | Legislation + gazette | Official gazette with ELI support | Yes | dre.pt | Good |
| **DGSI** | Case law | Databases of superior court decisions | **No** | dgsi.pt | Published |
| **Tribunal Constitucional** | Constitutional court | Constitutional decisions | **No** | tribunalconstitucional.pt | Published |
| **Banco de Portugal** | Financial regulator | Central bank | **No** | bportugal.pt | Published |
| **AdC** | Competition authority | Autoridade da Concorrencia | **No** | concorrencia.pt | Published |
| **CNPD** | Data protection | Comissao Nacional de Proteccao de Dados | **No** | cnpd.pt | Published |

## Consolidated Legislation vs. Official Journal

Portugal has the DRE providing both the official gazette (Diario da Republica) and consolidated legislation with ELI support.

## Sub-jurisdictions

| Region | Legislature | Coverage |
|--------|------------|---------|
| Azores | Assembleia Legislativa Regional | Not indexed |
| Madeira | Assembleia Legislativa Regional | Not indexed |

## Access Notes

- **DRE:** Official gazette with ELI support, good digital infrastructure.
- **Language:** Portuguese.

## How to Contribute

Priority: DGSI (court decisions), Tribunal Constitucional, Banco de Portugal, AdC. Create directories under `sources/PT/[SourceName]/`.
