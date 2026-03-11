# Austria — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Austria is a **federal state** with a **civil law** system and **9 Bundeslander**. The **RIS (Rechtsinformationssystem des Bundes)** is one of Europe's most comprehensive legal information systems, covering federal law, all 9 state laws, and court decisions via OGD API v2.6 (CC BY 4.0).

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| Bundesgesetzblatt | legislation | Yes | OK | 12 | 12 | **Working** |
| Landesrecht | legislation | Yes | OK | 12 | 12 | **Working** |
| OGH | case_law | Yes | OK | 12 | 12 | **Working** |
| VfGH | case_law | Yes | OK | 12 | 12 | **Working** |
| ris | legislation, case_law | Yes | OK | 15 | 15 | **Working** |

**5 sources total:** 5 working.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| RIS (Bundesrecht + Landesrecht + Judikatur) | All | Central legal system, OGD API v2.6 | Yes | CC BY 4.0 | Excellent |
| BGBl | Official gazette | Federal Law Gazette (I, II, III) | Yes | Open | Good |
| All 9 Bundeslander | State legislation | Via RIS OGD API | Yes | CC BY 4.0 | Excellent — 275K+ entries |
| OGH | Supreme court | 131K+ decisions | Yes | CC BY 4.0 | Excellent |
| VfGH | Constitutional court | 24K+ decisions | Yes | CC BY 4.0 | Good |
| **VwGH** | Supreme admin court | Verwaltungsgerichtshof | **No** (in RIS) | Via RIS | Available but not separately indexed |
| **BVwG** | Federal admin court | Bundesverwaltungsgericht | **No** (in RIS) | Via RIS | Available |
| **FMA** | Financial regulator | Financial Market Authority | **No** | fma.gv.at | Published |
| **BWB** | Competition authority | Federal Competition Authority | **No** | Web | Published |
| **DSB** | Data protection | Datenschutzbehorde | **No** | dsb.gv.at | Published |

## Consolidated Legislation vs. Official Journal

**Both**: RIS (consolidated federal + state law) and BGBl (official gazette). RIS is the primary reference.

## Sub-jurisdictions

All **9 Bundeslander** are covered via RIS OGD API — one of the most complete sub-jurisdictional datasets available.

| Bundesland | Legislature | RIS Coverage |
|-----------|------------|-------------|
| Burgenland | Landtag | Yes |
| Karnten | Landtag | Yes |
| Niederosterreich | Landtag | Yes |
| Oberosterreich | Landtag | Yes |
| Salzburg | Landtag | Yes |
| Steiermark | Landtag | Yes |
| Tirol | Landtag | Yes |
| Vorarlberg | Landtag | Yes |
| Wien | Landtag | Yes |

## Access Notes

- **License:** CC BY 4.0 for all RIS OGD data.
- **API:** RIS OGD API v2.6, well-documented, JSON responses.
- **Language:** German.

## How to Contribute

Priority: separate VwGH/BVwG indexing, FMA, BWB. Create directories under `sources/AT/[SourceName]/`.
