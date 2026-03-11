# Germany — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Germany is a **federal republic** with a **civil law** system and **16 Bundeslander** (federal states), each with its own constitution, legislature, executive, and judiciary. Federal law takes precedence in areas of concurrent competence. The Basic Law (Grundgesetz) is the supreme constitutional document.

**Court hierarchy (federal):** Five supreme courts (BGH civil/criminal, BVerwG admin, BFH finance, BAG labor, BSG social) plus the Bundesverfassungsgericht (BVerfG, Constitutional Court).

Germany provides consolidated federal legislation through gesetze-im-internet.de (CC0) and parliamentary tracking via the DIP Bundestag API.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| BGBl | legislation | Yes | Errors | 5934 | 0 | *Has errors* |
| BGH | case_law | Yes | Never run | - | 0 | Untested |
| BSG | case_law | Yes | Never run | - | 0 | Untested |
| BVerfG | case_law | Yes | Never run | - | 0 | Untested |
| BVerwG | unknown | Yes | Never run | - | 0 | Untested |
| Bayern | unknown | Yes | Never run | - | 12 | Untested (has samples) |
| Brandenburg | legislation, regulation | Yes | Never run | - | 0 | Untested |
| Bremen | legislation, regulation | Yes | Never run | - | 0 | Untested |
| Bundesrat | legislation | Yes | Never run | - | 0 | Untested |
| Bundestag | legislation | Yes | Never run | - | 0 | Untested |
| NRW | unknown | Yes | Never run | - | 15 | Untested (has samples) |
| OpenLegalData | unknown | Yes | Never run | - | 14 | Untested (has samples) |
| Sachsen | legislation | Yes | Never run | - | 0 | Untested |

**13 sources total:** 1 with errors, 3 untested (has samples), 9 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|------------------------|
| gesetze-im-internet.de | Consolidated legislation | All current federal laws (BMJV) | Yes | Bulk XML (CC0) | Excellent |
| BVerfG | Constitutional court | Federal Constitutional Court decisions | Yes | HTML scraping | Good — since 1998 |
| DIP (Bundestag) | Parliamentary | Legislative documentation and tracking | Yes | REST API (API key) | Good |
| **BGH** | Supreme civil/criminal court | Bundesgerichtshof | **No** | juris.de (partly free) | Moderate |
| **BVerwG** | Supreme admin court | Bundesverwaltungsgericht | **No** | bverwg.de | Moderate |
| **BFH** | Supreme finance court | Bundesfinanzhof | **No** | bundesfinanzhof.de | Moderate |
| **BAG** | Supreme labor court | Bundesarbeitsgericht | **No** | bundesarbeitsgericht.de | Moderate |
| **BSG** | Supreme social court | Bundessozialgericht | **No** | bsg.bund.de | Moderate |
| **BGBl** | Official gazette | Federal Law Gazette (free since 2023) | **No** | bgbl.de | Good — newly free |
| **Landesrecht** | State legislation | 16 state legislation portals | **No** | Various | Variable by state |
| **BaFin** | Financial regulator | Federal Financial Supervisory Authority | **No** | bafin.de | Publicly available |
| **Bundeskartellamt** | Competition authority | Federal Cartel Office | **No** | bundeskartellamt.de | Searchable database |
| **BfDI** | Data protection | Federal Data Protection Commissioner | **No** | bfdi.bund.de | Reports available |
| **BNetzA** | Network regulation | Federal Network Agency | **No** | bundesnetzagentur.de | Decisions published |

## Consolidated Legislation vs. Official Journal

Germany has **both**: consolidated legislation (gesetze-im-internet.de, CC0) and the Bundesgesetzblatt (BGBl, free since 2023). We index consolidated legislation as the primary source.

## Sub-jurisdictions

Germany's **16 Bundeslander** each have full legislative competence in non-federal areas. **No state legislation is currently indexed** — this is a significant gap.

| Bundesland | Legislature | Legal Portal |
|-----------|------------|-------------|
| Baden-Wurttemberg | Landtag | landesrecht-bw.de |
| Bayern | Landtag | gesetze-bayern.de |
| Berlin | Abgeordnetenhaus | gesetze.berlin.de |
| Brandenburg | Landtag | bravors.brandenburg.de |
| Bremen | Burgerschaft | transparenz.bremen.de |
| Hamburg | Burgerschaft | landesrecht-hamburg.de |
| Hessen | Landtag | rv.hessenrecht.hessen.de |
| Mecklenburg-Vorpommern | Landtag | landesrecht-mv.de |
| Niedersachsen | Landtag | voris.niedersachsen.de |
| Nordrhein-Westfalen | Landtag | recht.nrw.de |
| Rheinland-Pfalz | Landtag | landesrecht.rlp.de |
| Saarland | Landtag | sl.juris.de |
| Sachsen | Landtag | revosax.sachsen.de |
| Sachsen-Anhalt | Landtag | landesrecht.sachsen-anhalt.de |
| Schleswig-Holstein | Landtag | gesetze-rechtsprechung.sh.juris.de |
| Thuringen | Landtag | landesrecht.thueringen.de |

## Regulatory & Administrative Authorities

| Authority | Domain | Indexed? |
|-----------|--------|----------|
| BaFin | Financial supervision | No |
| Bundeskartellamt | Competition / antitrust | No |
| BfDI | Data protection | No |
| BNetzA | Telecom, energy, postal, rail | No |

## Access Notes

- **gesetze-im-internet.de:** Bulk XML download, CC0 license. Excellent quality.
- **DIP Bundestag:** REST API, API key on request. Comprehensive legislative tracking.
- **BGBl:** Free since 2023 at bgbl.de.
- **Language:** German.

## How to Contribute

Priority: federal supreme courts (BGH, BVerwG, BFH, BAG, BSG), BGBl, state legislation, BaFin, Bundeskartellamt. Create directories under `sources/DE/[SourceName]/`.
