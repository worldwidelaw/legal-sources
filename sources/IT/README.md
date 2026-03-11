# Italy — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

Italy is a **civil law** country organized as a **regional state** with **20 regions**, 5 with special autonomous status. Regions have legislative competence in areas defined by the Constitution (Article 117).

**Court hierarchy:** Tribunali -> Corti d'appello -> **Corte di Cassazione**; TAR -> **Consiglio di Stato**; **Corte Costituzionale**; **Corte dei Conti**.

Italy publishes consolidated legislation through **Normattiva** (normattiva.it) and the official gazette **Gazzetta Ufficiale**. Italy uses the NIR (Norme in Rete) XML standard.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| Camera | legislation | Yes | Never run | - | 0 | Untested |
| CassazioneCivile | case_law | Yes | OK | 12 | 12 | **Working** |
| ConsiglioDiStato | case_law | Yes | Never run | - | 0 | Untested |
| CorteCostituzionale | unknown | Yes | OK | 12 | 12 | **Working** |
| EmiliaRomagna | legislation | Yes | OK | 12 | 12 | **Working** |
| GazzettaUfficiale | legislation | Yes | OK | 7 | 7 | **Working** |
| Lazio | legislation | Yes | OK | 12 | 12 | **Working** |
| Lombardia | legislation | Yes | OK | 12 | 12 | **Working** |
| Normattiva | legislation | Yes | OK | 12 | 12 | **Working** |
| Piemonte | legislation | Yes | OK (0 records) | 0 | 0 | Runs OK, no samples |
| Senato | legislation | Yes | OK | 12 | 12 | **Working** |
| Toscana | legislation | Yes | Never run | - | 0 | Untested |
| Veneto | legislation | Yes | OK | 12 | 12 | **Working** |

**13 sources total:** 9 working, 1 runs OK (no samples), 3 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|------------------------|
| Normattiva | Consolidated legislation | Multi-temporal consolidated acts via URN/ELI | Yes | normattiva.it | Good |
| Gazzetta Ufficiale | Official gazette | All official publications | Yes | gazzettaufficiale.it | Good |
| Corte Costituzionale | Constitutional court | Decisions since 1956 | Yes | cortecostituzionale.it | Good |
| Corte di Cassazione | Supreme court | Civil and criminal | Yes | italgiure.giustizia.it | Moderate |
| Consiglio di Stato | Supreme admin court | Administrative jurisprudence | Yes | giustizia-amministrativa.it | Good |
| **TAR** | Regional admin courts | First-instance admin decisions | **No** | giustizia-amministrativa.it | Available on same platform |
| **Corte dei Conti** | Court of Auditors | Public finance, pension decisions | **No** | corteconti.it | Published on website |
| **Regional legislation** | 20 regional legislatures | State legislation | **No** | Various regional portals | Variable |
| **CONSOB** | Financial regulator | Securities market authority | **No** | consob.it | Available |
| **AGCM** | Competition authority | Antitrust, consumer protection | **No** | agcm.it | In weekly bulletin |
| **Garante Privacy** | Data protection | GDPR enforcement | **No** | garanteprivacy.it | Published |
| **AGCOM** | Telecom/media regulator | Communications authority | **No** | agcom.it | Available |
| **Banca d'Italia** | Central bank | Banking supervision | **No** | bancaditalia.it | Circulars available |

## Consolidated Legislation vs. Official Journal

Italy has **both**: Normattiva (multi-temporal consolidated acts with URN:NIR identifiers and ELI support) and the Gazzetta Ufficiale (official gazette). We index both.

## Sub-jurisdictions

5 regions with **special autonomy**: Valle d'Aosta (bilingual IT/FR), Trentino-Alto Adige/Sudtirol (trilingual), Friuli Venezia Giulia, Sardegna, Sicilia. 15 ordinary regions have limited legislative competence. **No regional legislation is currently indexed.**

## Regulatory & Administrative Authorities

| Authority | Domain | Indexed? |
|-----------|--------|----------|
| CONSOB | Securities, financial markets | No |
| AGCM | Competition, consumer protection | No |
| Garante Privacy | Data protection (GDPR) | No |
| AGCOM | Telecommunications, media | No |
| Banca d'Italia | Banking supervision | No |
| ARERA | Energy, water, waste | No |
| ANAC | Anti-corruption, public procurement | No |

## Access Notes

- **Normattiva:** Uses URN:NIR and ELI identifiers. Multi-temporal navigation.
- **NIR standard:** Norme in Rete XML, predecessor/parallel to Akoma Ntoso.
- **Language:** Italian. Bilingual in some regions.

## How to Contribute

Priority: TAR decisions, Corte dei Conti, CONSOB, AGCM, regional legislation. Create directories under `sources/IT/[SourceName]/`.
