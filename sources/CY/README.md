# Cyprus — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Cyprus has a **mixed legal system** (common law heritage from UK + civil law elements), EU member. **Consolidated legislation** via CyLaw.org (XML export). Supreme Court decisions (35,485+ from 1961+) also indexed.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| CYLAW | legislation | Yes | OK | 12 | 12 | **Working** |
| SupremeCourt | case_law | Yes | OK | 5 | 12 | **Working** |

**2 sources total:** 2 working.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| CyLaw.org | Consolidated legislation | XML export | Yes | Good |
| Supreme Court | Case law | 35,485+ decisions since 1961 | Yes | Good |
| **Administrative Court** | Case law | Established 2015 | **No** | No known public database |
| **Lower courts** | Case law | | **No** | Limited |
| **CBC** | Central bank | Central Bank of Cyprus | **No** | centralbank.cy | Published |
| **Commission for Protection of Competition** | Competition | | **No** | competition.gov.cy | Published |
| **Commissioner for Data Protection** | Data protection | | **No** | dataprotection.gov.cy | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via CyLaw.org (XML). Official Gazette of the Republic is the official journal.

## Sub-jurisdictions

**None** — unitary state. Northern Cyprus (TRNC) operates a separate de facto legal system (not indexed).

## How to Contribute

Priority: Administrative Court, lower courts, CBC, competition. Create directories under `sources/CY/[SourceName]/`.
