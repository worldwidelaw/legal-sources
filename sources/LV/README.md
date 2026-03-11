# Latvia — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Latvia is a **civil law** country, **unitary**. Provides **consolidated legislation** through **Likumi.lv** (CC BY). Court decisions from Supreme Court, Constitutional Court (Satversmes tiesa), and Parliament (Saeima) proceedings are indexed.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| LegislativeDatabase | legislation | Yes | OK | 12 | 12 | **Working** |
| Parliament | parliamentary_proceedings | Yes | OK | 12 | 12 | **Working** |
| SupremeCourt | case_law | Yes | OK | 12 | 12 | **Working** |

**3 sources total:** 3 working.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Likumi.lv | Consolidated legislation | CC BY | Yes | Good |
| Supreme Court | Supreme court | | Yes | Good |
| Satversmes tiesa | Constitutional court | | Yes | Good |
| Saeima | Parliamentary | | Yes | Good |
| **Lower courts** | Case law | | **No** | Limited |
| **FKTK** | Financial regulator | Financial and Capital Market Commission | **No** | fktk.lv | Published |
| **Konkurences padome** | Competition | Competition Council | **No** | kp.gov.lv | Published |
| **DVI** | Data protection | | **No** | dvi.gov.lv | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via Likumi.lv (CC BY). Latvijas Vestnesis is the official gazette.

## Sub-jurisdictions

**Unitary state** — no autonomous sub-jurisdictions.

## How to Contribute

Priority: lower courts, regulatory decisions. Create directories under `sources/LV/[SourceName]/`.
