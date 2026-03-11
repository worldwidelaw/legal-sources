# Switzerland — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Switzerland is a **federal state** with a **civil law** system and **26 cantons**, each with its own constitution, parliament, and courts. Switzerland is **quadrilingual** (German, French, Italian, Romansh). Federal legislation is published through **Fedlex** (the Federal Chancellery's platform) with SPARQL access under CC0.

**Court hierarchy:** Cantonal courts -> **Bundesgericht/Tribunal federal (BGer/TF)** (Federal Supreme Court). Separate Federal Criminal Court (BStGer), Federal Administrative Court (BVGer), and Federal Patent Court.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| Entscheidsuche | case_law | Yes | Never run | - | 0 | Untested |
| Fedlex | unknown | Yes | Never run | - | 0 | Untested |
| Kantone | unknown | Yes | Never run | - | 0 | Untested |

**3 sources total:** 3 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Fedlex | Consolidated federal law | SPARQL endpoint, CC0 | Yes | Excellent |
| BGE/ATF (Leitentscheide) | Leading supreme court decisions | SPARQL, CC0 | Yes | Excellent |
| BGer API | All supreme court decisions | REST API | Yes | Good |
| **26 cantonal legislations** | Cantonal law | Each canton has its own legal database | **No** | Various portals | Variable by canton |
| **Cantonal courts** | Case law | Cantonal court decisions | **No** | Various | Very variable |
| **BStGer** | Federal Criminal Court | Bundesstrafgericht | **No** | bstger.ch | Published |
| **BVGer** | Federal Admin Court | Bundesverwaltungsgericht | **No** | bvger.ch | Published |
| **FINMA** | Financial regulator | Financial Market Supervisory Authority | **No** | finma.ch | Published |
| **WEKO/COMCO** | Competition authority | Competition Commission | **No** | weko.admin.ch | Published |
| **EDOB/PFPDT** | Data protection | Federal Data Protection Commissioner | **No** | edoeb.admin.ch | Published |

## Consolidated Legislation vs. Official Journal

**Both**: Fedlex provides consolidated federal law (Systematische Sammlung/Recueil systematique) and the official compilation (Amtliche Sammlung/Recueil officiel). Both accessible via SPARQL under CC0.

## Sub-jurisdictions

Switzerland's **26 cantons** each have full legislative competence. **No cantonal legislation is currently indexed** — a significant gap. Major cantons: Zurich, Bern, Geneva, Vaud, Basel-Stadt, etc.

## Access Notes

- **License:** CC0 for Fedlex and BGE/ATF.
- **SPARQL:** Fedlex and BGE/ATF both offer SPARQL endpoints.
- **Languages:** German, French, Italian, Romansh. Legislation published in DE/FR/IT.

## How to Contribute

Priority: cantonal legislation (start with ZH, BE, GE, VD), BStGer, BVGer, FINMA, WEKO. Create directories under `sources/CH/[SourceName]/`.
