# Ireland — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Ireland has a **common law** legal system. It is a **unitary state** with no sub-jurisdictions. The Constitution (Bunreacht na hEireann) is the supreme law. Courts: District Court -> Circuit Court -> High Court -> Court of Appeal -> **Supreme Court**.

Ireland publishes legislation through the **eISB** (electronic Irish Statute Book) maintained by the Office of the Attorney General.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| Acts | legislation | Yes | OK | 12 | 12 | **Working** |
| BAILII | case_law | No | Never run | - | 0 | No script |
| Oireachtas | parliamentary_proceedings | Yes | OK | 12 | 12 | **Working** |
| SupremeCourt | case_law | Yes | OK | 12 | 12 | **Working** |

**4 sources total:** 3 working, 1 no script.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| eISB | Legislation | Irish Statute Book — Acts and SIs | Yes | irishstatutebook.ie | Good |
| **Courts.ie** | Case law | Irish court judgments | **No** | courts.ie | Published |
| **BAILII (Ireland section)** | Case law | Irish court decisions via BAILII | **No** | bailii.org | Free access |
| **Oireachtas** | Parliamentary | Houses of the Oireachtas proceedings | **No** | oireachtas.ie | Open data |
| **CCPC** | Competition/consumer | Competition and Consumer Protection Commission | **No** | ccpc.ie | Published |
| **Central Bank of Ireland** | Financial regulator | CBI decisions and enforcement | **No** | centralbank.ie | Published |
| **DPC** | Data protection | Data Protection Commission | **No** | dataprotection.ie | Published — major GDPR enforcer |

## Consolidated Legislation vs. Official Journal

Ireland publishes legislation through the eISB with consolidated "revised acts" available. The Iris Oifigiuil is the official gazette.

## Sub-jurisdictions

Ireland is a **unitary state** with no sub-jurisdictions.

## Access Notes

- **eISB:** Free access to all Irish legislation.
- **Language:** English and Irish (Gaeilge). Acts published bilingually.
- **DPC:** Ireland's Data Protection Commission is a significant EU GDPR enforcer (many tech companies HQ'd in Ireland).

## How to Contribute

Priority: court decisions (courts.ie), DPC decisions (important for GDPR), CCPC, CBI. Create directories under `sources/IE/[SourceName]/`.
