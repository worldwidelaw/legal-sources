# Croatia — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Croatia is a **civil law** country, **unitary**, EU member since 2013. Publishes through **Narodne novine** (official gazette, with consolidated versions available). Court decisions from Constitutional Court and Supreme Court indexed.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| OfficialGazette | legislation | Yes | OK | 12 | 12 | **Working** |
| SupremeCourt | unknown | Yes | OK | 12 | 12 | **Working** |

**2 sources total:** 2 working.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Narodne novine | Legislation + gazette | Consolidated available | Yes | Good |
| Constitutional Court | Case law | | Yes | Good |
| Supreme Court | Case law | | Yes | Good |
| **Lower courts** | Case law | | **No** | Limited |
| **HNB** | Central bank | Hrvatska narodna banka | **No** | hnb.hr | Published |
| **AZTN** | Competition | Agencija za zastitu trzisnog natjecanja | **No** | aztn.hr | Published |
| **AZOP** | Data protection | | **No** | azop.hr | Published |

## Consolidated Legislation vs. Official Journal

Narodne novine provides both official gazette and consolidated texts.

## Sub-jurisdictions

**Unitary state** — 20 counties with no legislative autonomy.

## How to Contribute

Priority: lower courts, HNB, AZTN, AZOP. Create directories under `sources/HR/[SourceName]/`.
