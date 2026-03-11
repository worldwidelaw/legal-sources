# Norway — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Norway is a **civil law** country in the **Nordic tradition**, **unitary**. Provides **consolidated legislation** through **Lovdata** (bulk tar.bz2 downloads, NLOD 2.0). Two official written forms (Bokmal, Nynorsk).

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| Høyesterett | unknown | Yes | Never run | - | 12 | Untested (has samples) |
| Lovdata | unknown | Yes | Never run | - | 12 | Untested (has samples) |

**2 sources total:** 2 untested (has samples).

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Lovdata | Consolidated legislation | Bulk tar.bz2, NLOD 2.0 | Yes | Good |
| Hoyesterett | Supreme Court | Free from 2008 | Yes | Good |
| **Lagmannsretter** | Courts of Appeal | Appellate decisions | **No** | Lovdata subscription | Partially available |
| **Tingretter** | District courts | First-instance | **No** | Not systematically published |
| **Stortinget** | Parliamentary | Parliament proceedings | **No** | Open Data API available |
| **Finanstilsynet** | Financial regulator | | **No** | finanstilsynet.no | Published |
| **Konkurransetilsynet** | Competition | | **No** | konkurransetilsynet.no | Published |
| **Datatilsynet** | Data protection | | **No** | datatilsynet.no | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via Lovdata (NLOD 2.0). Norsk Lovtidend is the official gazette.

## Sub-jurisdictions

**Unitary state**. Svalbard has special regulations.

## How to Contribute

Priority: lower courts, Stortinget, regulatory decisions. Create directories under `sources/NO/[SourceName]/`.
