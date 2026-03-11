# Iceland — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Iceland is a **civil law** country in the **Nordic tradition**, **unitary**. Provides **consolidated legislation** through **Lagasafn** (ZIP archive from Althingi, public domain). Court system reformed in 2018 with new Court of Appeal (Landsrettur).

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| Lagasafn | unknown | Yes | Never run | - | 15 | Untested (has samples) |
| SupremeCourt | case_law | Yes | Never run | - | 12 | Untested (has samples) |

**2 sources total:** 2 untested (has samples).

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Lagasafn | Consolidated legislation | ZIP from Althingi, public domain | Yes | Good |
| Haestiriettur | Supreme Court | Full text, HTML scraping | Yes | Good |
| **Landsrettur** | Court of Appeal | Established 2018 | **No** | landsrettur.is | Available |
| **Heradsdomar** | District courts | | **No** | Not systematically published |
| **Althingi** | Parliamentary | Debates, bills | **No** | althingi.is | Available |
| **FME** | Financial regulator | | **No** | fme.is | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via Lagasafn (public domain). Stjornartidindi is the official gazette.

## Sub-jurisdictions

**Unitary state** — no autonomous sub-jurisdictions.

## How to Contribute

Priority: Landsrettur, Althingi, lower courts. Create directories under `sources/IS/[SourceName]/`.
