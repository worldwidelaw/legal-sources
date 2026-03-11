# Lithuania — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Lithuania is a **civil law** country, **unitary**. Provides **consolidated legislation** through **TAIS** (Teises Aktu Informacine Sistema). Court decisions from Supreme Court, Constitutional Court, and Parliament (Seimas) proceedings are indexed.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| ConstitutionalCourt | case_law | Yes | OK | 12 | 12 | **Working** |
| LegalBase | legislation | Yes | OK | 12 | 12 | **Working** |
| Parliament | legislation | Yes | OK | 12 | 12 | **Working** |
| SupremeCourt | case_law | Yes | OK | 12 | 12 | **Working** |

**4 sources total:** 4 working.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| TAIS | Consolidated legislation | | Yes | Good |
| Supreme Court | Supreme court | | Yes | Good |
| Constitutional Court | Constitutional court | | Yes | Good |
| Seimas | Parliamentary | | Yes | Good |
| **Lower courts** | Case law | | **No** | Limited |
| **Lietuvos bankas** | Financial regulator | Central bank | **No** | lb.lt | Published |
| **Konkurencijos taryba** | Competition | Competition Council | **No** | kt.gov.lt | Published |
| **VDAI** | Data protection | | **No** | ada.lt | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via TAIS. Valstybes zinios is the official gazette.

## Sub-jurisdictions

**Unitary state** — no autonomous sub-jurisdictions.

## How to Contribute

Priority: lower courts, regulatory decisions. Create directories under `sources/LT/[SourceName]/`.
