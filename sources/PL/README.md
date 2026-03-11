# Poland — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Poland is a **civil law** country, **unitary** (16 voivodeships without legislative power). Excellent digital infrastructure: **Sejm ELI API** (96K+ acts since 1918) and **SAOS API** (CC-BY) for court decisions.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| ConstitutionalCourt | case_law | Yes | OK | 12 | 12 | **Working** |
| DziennikUrzedowy | legislation | Yes | OK | 12 | 12 | **Working** |
| Sejm | parliamentary_proceedings | Yes | Never run | - | 0 | Untested |
| SupremeCourt | case_law | Yes | OK | 12 | 12 | **Working** |

**4 sources total:** 3 working, 1 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Sejm ELI API | Consolidated legislation | 96K+ acts, ELI-compliant | Yes | Excellent |
| SAOS (Constitutional Tribunal) | Case law | CC-BY, REST API | Yes | Excellent |
| SAOS (Supreme Court) | Case law | 38K+ judgments, CC-BY | Yes | Excellent |
| Dziennik Urzedowy | Official journals | | Yes | Good |
| **NSA** | Supreme Admin Court | Potentially via SAOS | **No** | Available |
| **Lower courts** | Case law | Potentially via SAOS | **No** | Available |
| **KNF** | Financial regulator | | **No** | knf.gov.pl | Published |
| **UOKiK** | Competition | | **No** | uokik.gov.pl | Published |
| **UODO** | Data protection | | **No** | uodo.gov.pl | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via Sejm ELI API (Dziennik Ustaw). Monitor Polski is the secondary gazette (not indexed).

## Sub-jurisdictions

**Unitary state** — 16 voivodeships have no legislative power.

## How to Contribute

Priority: NSA via SAOS, lower courts, KNF, UOKiK, UODO. Create directories under `sources/PL/[SourceName]/`.
