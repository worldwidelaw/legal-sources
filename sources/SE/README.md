# Sweden — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Sweden is a **civil law** country of the **Scandinavian variant**, **unitary** with no autonomous sub-jurisdictions. Provides **consolidated legislation** through the Riksdag open data portal (11,000+ documents, public domain). Excellent court data coverage via Domstolsverket REST API.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| Domstolverket | unknown | Yes | Never run | - | 12 | Untested (has samples) |
| RiksdagenDB | legislation | Yes | Never run | - | 0 | Untested |
| SupremeAdministrativeCourt | unknown | Yes | Never run | - | 0 | Untested |
| SupremeCourt | case_law | Yes | Never run | - | 15 | Untested (has samples) |
| SvenskaForfattningssamlingen | legislation | Yes | OK (0 records) | 0 | 0 | Runs OK, no samples |

**5 sources total:** 1 runs OK (no samples), 2 untested (has samples), 2 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| Riksdag SFS | Consolidated legislation | Public domain, 11K+ docs | Yes | Excellent |
| Domstolsverket | All courts | REST API | Yes | Excellent |
| HDO (Supreme Court) | Supreme Court | Via Domstolsverket | Yes | 5,500+ decisions |
| HFD | Supreme Admin Court | rattspraxis API | Yes | Good |
| RiksdagenDB | Parliamentary | 500K+ documents, public domain | Yes | Excellent |
| **Lower courts** | District courts | Not systematically published | **No** | Limited |
| **Finansinspektionen** | Financial regulator | | **No** | fi.se | Published |
| **Konkurrensverket** | Competition | | **No** | konkurrensverket.se | Published |
| **IMY** | Data protection | Swedish DPA | **No** | imy.se | Published |

## Consolidated Legislation vs. Official Journal

**Consolidated** via Riksdag open data portal (SFS, public domain). Both as-enacted and consolidated versions available.

## Sub-jurisdictions

**Unitary state** — no autonomous sub-jurisdictions.

## How to Contribute

Priority: lower courts, regulatory decisions. Create directories under `sources/SE/[SourceName]/`.
