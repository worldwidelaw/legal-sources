# Bulgaria — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Bulgaria is a **civil law** country, **unitary**. Publishes through the **Darzhaven vestnik** (State Gazette). Court decisions from Supreme Court (VKS), Supreme Administrative Court (VAS), and Constitutional Court are indexed.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| ConstitutionalCourt | case_law | Yes | OK | 12 | 12 | **Working** |
| StateGazette | legislation | Yes | OK | 15 | 15 | **Working** |
| SupremeCourt | case_law | Yes | OK | 12 | 12 | **Working** |

**3 sources total:** 3 working.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Darzhaven vestnik | Official gazette | | Yes | Good |
| VKS | Supreme Court | | Yes | Good |
| VAS | Supreme Admin Court | | Yes | Good |
| Constitutional Court | | | Yes | Good |
| **Consolidated legislation** | Legislation | | **No** | lex.bg (private) | Limited — no official consolidated database |
| **Lower courts** | Case law | | **No** | Limited |
| **BNB** | Central bank | Bulgarian National Bank | **No** | bnb.bg | Published |
| **CPC** | Competition | Commission on Protection of Competition | **No** | cpc.bg | Published |
| **KZLD** | Data protection | | **No** | cpdp.bg | Published |

## Consolidated Legislation vs. Official Journal

Bulgaria does **not** have an official consolidated legislation database. The **Darzhaven vestnik** (State Gazette) publishes laws as enacted. Private services (lex.bg) offer consolidated versions.

## Sub-jurisdictions

**Unitary state** — 28 provinces with no legislative autonomy.

## How to Contribute

Priority: consolidated legislation source, lower courts, BNB, CPC. Create directories under `sources/BG/[SourceName]/`.
