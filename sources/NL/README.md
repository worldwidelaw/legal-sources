# Netherlands — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

The Netherlands is a **civil law** country with a **unitary** government. The Kingdom includes Caribbean constituent countries (Aruba, Curacao, Sint Maarten) and special municipalities (Bonaire, Sint Eustatius, Saba).

The Netherlands has **consolidated legislation** via wetten.overheid.nl (BWB identifiers). Rechtspraak.nl is one of Europe's most comprehensive open court databases (CC0).

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| BAILII | case_law | Yes | Never run | - | 12 | Untested (has samples) |
| Staatsblad | legislation | Yes | OK | 12 | 12 | **Working** |
| SupremeCourt | unknown | Yes | Never run | - | 0 | Untested |
| TweedeKamer | parliamentary_proceedings | Yes | Never run | - | 12 | Untested (has samples) |
| wetten_overheid_nl | legislation | Yes | Never run | - | 12 | Untested (has samples) |

**5 sources total:** 1 working, 3 untested (has samples), 1 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| wetten.overheid.nl | Consolidated legislation | All acts, decrees, regulations in force (BWB) | Yes | CC0, API | Excellent — 45K+ regulations |
| Staatsblad | Official gazette | Original text of acts and royal decrees | Yes | Open | Good |
| Rechtspraak.nl | Case Law | All Dutch court decisions | Yes | CC0, 10 req/s | Excellent — best open court data in Europe |
| Hoge Raad | Supreme Court | Supreme court decisions | Yes | Open | Good |
| Tweede Kamer | Parliamentary | Proceedings, motions, questions via OData v4 | Yes | Open, OData v4 | Good |
| **Eerste Kamer** | Parliamentary | Senate proceedings | **No** | Public website | Available, less structured |
| **Raad van State** | Advisory opinions | Council of State advisory opinions | **No** | raadvanstate.nl | No bulk API |
| **Caribbean legislation** | Legislation | Laws of Aruba, Curacao, Sint Maarten | **No** | Various gazettes | Fragmented |
| **AFM** | Financial regulator | Authority for Financial Markets | **No** | afm.nl | Published |
| **ACM** | Competition/consumer | Authority for Consumers & Markets | **No** | acm.nl | Published |
| **Autoriteit Persoonsgegevens** | Data protection | DPA decisions | **No** | autoriteitpersoonsgegevens.nl | Published |

## Consolidated Legislation vs. Official Journal

**Both**: wetten.overheid.nl (consolidated BWB database) and Staatsblad/Staatscourant (official gazettes). Both freely accessible.

## Sub-jurisdictions

| Level | Legislature | Coverage |
|-------|------------|----------|
| National | Staten-Generaal | All via wetten.overheid.nl |
| Aruba | Staten van Aruba | Separate legal system, not indexed |
| Curacao | Staten van Curacao | Separate legal system, not indexed |
| Sint Maarten | Parliament | Separate legal system, not indexed |

## Access Notes

- **License:** CC0 for wetten.overheid.nl and Rechtspraak.nl.
- **ECLI:** Extensive use of European Case Law Identifier.
- **Rate limits:** 10 req/s for Rechtspraak.nl.

## How to Contribute

Priority: Eerste Kamer, Raad van State, Caribbean territories, AFM, ACM. Create directories under `sources/NL/[SourceName]/`.
