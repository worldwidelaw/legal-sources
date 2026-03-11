# Slovenia — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Slovenia is a **civil law** country, **unitary**. Provides **consolidated legislation** through **PIS RS** (Pravno-informacijski sistem) with ELI. Constitutional Court decisions via SPARQL. Supreme Court decisions also indexed.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| LegislativeDatabase | unknown | Yes | Never run | - | 0 | Untested |
| SupremeCourt | unknown | Yes | Never run | - | 0 | Untested |

**2 sources total:** 2 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| PIS RS | Consolidated legislation | ELI | Yes | Good |
| Constitutional Court | Case law | SPARQL | Yes | Excellent |
| Supreme Court | Case law | | Yes | Good |
| **Lower courts** | Case law | | **No** | Limited |
| **Banka Slovenije** | Central bank | | **No** | bsi.si | Published |
| **AVK** | Competition | Agencija za varstvo konkurence | **No** | varfruhkonkurence.si | Published |
| **IP RS** | Data protection | Informacijski pooblascenec | **No** | ip-rs.si | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via PIS RS (ELI). Uradni list is the official gazette.

## Sub-jurisdictions

**Unitary state** — no autonomous sub-jurisdictions.

## How to Contribute

Priority: lower courts, Banka Slovenije, competition, data protection. Create directories under `sources/SI/[SourceName]/`.
