# Hungary — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Hungary is a **civil law** country, **unitary**. Provides **consolidated legislation** via **NJT** (Nemzeti Jogszabalytar) with ELI support since 2023. Court decisions from Constitutional Court and Kuria (Supreme Court).

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| Constitutional | unknown | Yes | Never run | - | 12 | Untested (has samples) |
| FelsoBirosag | unknown | Yes | Never run | - | 0 | Untested |
| NJT | unknown | Yes | OK | 12 | 12 | **Working** |
| Parlament | legislation, parliamentary_documents | Yes | OK | 12 | 12 | **Working** |

**4 sources total:** 2 working, 1 untested (has samples), 1 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| NJT | Consolidated legislation | ELI since 2023 | Yes | Good |
| Constitutional Court | Case law | Web scraping | Yes | Good |
| Kuria | Supreme Court | Sitemap | Yes | Good |
| Parlament | Parliamentary | PDF extraction | Yes | Moderate |
| **Lower courts** | Case law | | **No** | Limited |
| **MNB** | Central bank | Magyar Nemzeti Bank | **No** | mnb.hu | Published |
| **GVH** | Competition | | **No** | gvh.hu | Published |
| **NAIH** | Data protection | | **No** | naih.hu | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via NJT (njt.hu) with ELI since 2023. Magyar Kozlony is the official gazette.

## Sub-jurisdictions

**Unitary state** — no sub-national legislative bodies.

## How to Contribute

Priority: lower courts, MNB, GVH, NAIH. Create directories under `sources/HU/[SourceName]/`.
