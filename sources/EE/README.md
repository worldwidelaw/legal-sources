# Estonia — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Estonia is a **civil law** country, **unitary**. Provides **consolidated legislation** through **Riigi Teataja** (SPARQL, CC0) — one of the most advanced e-government legal databases in Europe. Court decisions available via kohtulahendid.ee (CC0).

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| RiigiTeatajaLoomal | legislation | Yes | Never run | - | 0 | Untested |
| Riigikogu | legislation | Yes | Never run | - | 0 | Untested |
| SupremeCourt | case_law | Yes | Never run | - | 0 | Untested |

**3 sources total:** 3 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Riigi Teataja | Consolidated legislation | SPARQL, CC0 | Yes | Excellent |
| kohtulahendid.ee | Court decisions | All courts, CC0 | Yes | Excellent |
| **Riigikogu** | Parliamentary | Parliament proceedings | **No** | riigikogu.ee | Available |
| **Finantsinspektsioon** | Financial regulator | | **No** | fi.ee | Published |
| **Konkurentsiamet** | Competition | | **No** | konkurentsiamet.ee | Published |
| **AKI** | Data protection | | **No** | aki.ee | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via Riigi Teataja (CC0, SPARQL). Leading digital legal infrastructure.

## Sub-jurisdictions

**Unitary state** — no autonomous sub-jurisdictions.

## How to Contribute

Priority: Riigikogu, regulatory decisions. Create directories under `sources/EE/[SourceName]/`.
