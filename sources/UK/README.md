# United Kingdom — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

The United Kingdom has a **common law** system (except Scotland, which has a mixed system). It is a **unitary state** with **devolved nations**: Scotland, Wales, and Northern Ireland each have their own parliament/assembly with legislative competence.

legislation.gov.uk provides comprehensive access to UK legislation including primary (Acts) and secondary (Statutory Instruments) legislation, maintained by The National Archives.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| CaseLaw | case_law | Yes | Never run | - | 12 | Untested (has samples) |
| Legislation | legislation | Yes | Never run | - | 0 | Untested |
| LegislationGovUK | legislation | Yes | Never run | - | 0 | Untested |

**3 sources total:** 1 untested (has samples), 2 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| legislation.gov.uk | Legislation | UK Acts, SIs, SSIs, Welsh SIs, NI legislation | Yes | Open, API | Excellent |
| **BAILII** | Case law | British and Irish Legal Information Institute | **No** | bailii.org | Free access, no bulk API |
| **National Archives (Gazette)** | Official gazette | The London/Edinburgh/Belfast Gazettes | **No** | thegazette.co.uk | Good API |
| **Scottish legislation** | Devolved legislation | Acts of Scottish Parliament, SSIs | Partially (via legislation.gov.uk) | legislation.gov.uk | Good |
| **Welsh legislation** | Devolved legislation | Acts/Measures of Senedd, Welsh SIs | Partially (via legislation.gov.uk) | legislation.gov.uk | Good |
| **NI legislation** | Devolved legislation | NI Acts, NI SRs | Partially (via legislation.gov.uk) | legislation.gov.uk | Good |
| **FCA** | Financial regulator | Financial Conduct Authority | **No** | fca.org.uk | Published |
| **CMA** | Competition authority | Competition and Markets Authority | **No** | gov.uk/cma | Published |
| **ICO** | Data protection | Information Commissioner's Office | **No** | ico.org.uk | Published |
| **Ofcom** | Telecom regulator | Communications regulator | **No** | ofcom.org.uk | Published |

## Consolidated Legislation vs. Official Journal

The UK publishes legislation on legislation.gov.uk with "as enacted" and "revised" (consolidated) versions. The revised versions incorporate amendments. The Gazettes serve as official journals.

## Sub-jurisdictions

| Nation | Legislature | Coverage |
|--------|------------|---------|
| England | UK Parliament | Via legislation.gov.uk |
| Scotland | Scottish Parliament | Via legislation.gov.uk (Acts, SSIs) |
| Wales | Senedd Cymru | Via legislation.gov.uk (Acts, WSIs) |
| Northern Ireland | NI Assembly | Via legislation.gov.uk (NI Acts, SRs) |

## Access Notes

- **legislation.gov.uk:** Comprehensive, well-structured. API available.
- **BAILII:** Primary free case law source. No bulk download API.
- **Language:** English. Welsh legislation bilingual (English/Welsh).

## How to Contribute

Priority: case law (BAILII or other sources), FCA, CMA, ICO decisions. Create directories under `sources/UK/[SourceName]/`.
