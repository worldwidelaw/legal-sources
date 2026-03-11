# Denmark — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Denmark is a **civil law** country in the **Nordic tradition** with a **unitary** structure. The **Faroe Islands** and **Greenland** are autonomous territories with their own legislatures. Denmark provides **consolidated legislation** through **Retsinformation** (retsinformation.dk) with SRU API access under CC0.

**Court hierarchy:** Byretter (district) -> Landsretter (high courts) -> **Hojesteret** (Supreme Court).

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| CourtOfAppeal | unknown | Yes | Never run | - | 0 | Untested |
| Lovdata | legislation | Yes | OK | 15 | 15 | **Working** |

**2 sources total:** 1 working, 1 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Retsinformation | Consolidated legislation | SRU API, CC0 | Yes | Excellent |
| Hojesteret | Supreme Court | Supreme court decisions | Yes | Good |
| Folketing | Parliamentary | Parliamentary data | Yes | Good |
| **Landsretter** | High courts | Appellate court decisions | **No** | Limited |
| **Byretter** | District courts | First-instance decisions | **No** | Not systematically published |
| **Faroe Islands** | Autonomous legislation | Faroese law | **No** | logir.fo | Available |
| **Greenland** | Autonomous legislation | Greenlandic law | **No** | lovgivning.gl | Available |
| **Finanstilsynet** | Financial regulator | Financial Supervisory Authority | **No** | finanstilsynet.dk | Published |
| **Konkurrence- og Forbrugerstyrelsen** | Competition | Competition and Consumer Authority | **No** | kfst.dk | Published |
| **Datatilsynet** | Data protection | Danish DPA | **No** | datatilsynet.dk | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via Retsinformation (CC0, SRU API). Lovtidende is the official gazette.

## Sub-jurisdictions

| Territory | Legislature | Coverage |
|-----------|------------|---------|
| Faroe Islands | Logting | Not indexed (logir.fo) |
| Greenland | Inatsisartut | Not indexed (lovgivning.gl) |

## How to Contribute

Priority: lower courts, Faroe Islands, Greenland, financial regulator. Create directories under `sources/DK/[SourceName]/`.
