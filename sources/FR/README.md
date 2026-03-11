# France — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

France operates under a **civil law system**, rooted in the Napoleonic codification tradition. French law is highly codified, with **78+ active codes** covering virtually every area of law. France is a **unitary state** — there are no sub-jurisdictions with independent legislative power, though overseas territories have some specificities.

**Court hierarchy:**
- **Judicial courts (ordre judiciaire):** Tribunaux judiciaires -> Cours d'appel -> **Cour de cassation**
- **Administrative courts (ordre administratif):** Tribunaux administratifs -> Cours administratives d'appel -> **Conseil d'Etat**
- **Conseil constitutionnel:** Constitutional review (QPC since 2010, a priori review since 1958)

The DILA provides bulk open data under the Etalab open license. The PISTE platform provides authenticated API access to Legifrance and Judilibre.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| AMF | case_law, doctrine | Yes | Never run | - | 0 | Untested |
| AMF_Doctrine | doctrine, regulation | Yes | Never run | - | 12 | Untested (has samples) |
| AMF_Sanctions | case_law, enforcement | Yes | Never run | - | 12 | Untested (has samples) |
| AssembleeNationale | unknown | Yes | OK | 12 | 12 | **Working** |
| AutoriteConcurrence | case_law | Yes | Never run | - | 0 | Untested |
| CADA | doctrine | Yes | Never run | - | 0 | Untested |
| CASS | case_law | Yes | OK | 15 | 15 | **Working** |
| CNIL | doctrine, case_law | Yes | Never run | - | 0 | Untested |
| ConseilConstitutionnel | case_law | Yes | Never run | - | 0 | Untested |
| ConventionsCollectives | doctrine, collective_agreements | Yes | OK | 15 | 15 | **Working** |
| CouncilState | case_law | Yes | OK | 15 | 15 | **Working** |
| JournalOfficiel | legislation | Yes | OK | 12 | 15 | **Working** |
| Judilibre | case_law | Yes | OK | 15 | 15 | **Working** |
| LegifranceCodes | legislation | Yes | OK | 16 | 16 | **Working** |
| Senat | legislation | Yes | OK | 12 | 12 | **Working** |

**15 sources total:** 8 working, 2 untested (has samples), 5 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|------------------------|
| Legifrance (LEGI) | Consolidated legislation | All 78+ codes, consolidated daily | Yes | PISTE API (OAuth2) | Free but quota-limited |
| Journal Officiel (JORF) | Official gazette | Laws, decrees, arretes, avis | Yes | DILA open data + PISTE | Freely available in bulk XML |
| Cour de cassation (CASS) | Supreme judicial court | Civil, criminal, social, commercial | Yes | DILA bulk open data | Full decisions since ~2000 |
| Judilibre | Judicial case law | All judicial courts, pseudonymized | Yes | PISTE API (OAuth2) | Requires PISTE credentials |
| Conseil d'Etat (JADE) | Administrative case law | CE + CAA + TA decisions | Yes | DILA bulk open data | Comprehensive |
| Conseil constitutionnel | Constitutional decisions | DC, QPC, LP, elections | Yes | DILA bulk open data | Complete since 1958 |
| KALI | Collective agreements | Conventions collectives in force | Yes | DILA bulk open data | Comprehensive |
| AMF | Financial regulator | Market regulation, approvals | Yes | amf-france.org | Web scraping |
| CNIL | Data protection authority | Deliberations, sanctions, guidelines | Yes | DILA bulk open data | Well-structured |
| CADA | FOI commission | Avis on access to admin documents | Yes | cada.fr | Web access |
| Autorite de la concurrence | Competition authority | Merger decisions, antitrust | Yes | Web | Decisions publicly available |
| Assemblee nationale | Lower chamber | Bills, reports, debates | Yes | Open data | API and open data portal |
| Senat | Upper chamber | Bills, reports, debates, amendments | Yes | Open data | API and open data portal |
| **ARCEP** | Telecom regulator | Telecom/postal regulation | **No** | arcep.fr | Publicly available |
| **ARCOM (ex-CSA)** | Media regulator | Audiovisual/digital regulation | **No** | arcom.fr | Publicly available |
| **ASN** | Nuclear safety | Nuclear safety decisions | **No** | asn.fr | Publicly available |
| **HAS** | Health authority | Medical guidelines, evaluations | **No** | has-sante.fr | Rich corpus |
| **Cour des comptes** | Audit court | Public finance audit reports | **No** | ccomptes.fr | Reports freely available |
| **Defenseur des droits** | Ombudsman | Human rights, discrimination | **No** | defenseurdesdroits.fr | Publicly available |
| **ACPR** | Banking/insurance supervision | Prudential supervision | **No** | acpr.banque-france.fr | Important financial regulator |

## Consolidated Legislation vs. Official Journal

France maintains **both**: consolidated legislation (LEGI via Legifrance, updated daily) and the Official Journal (JORF, publishing new acts as enacted). Dual coverage enables both current law lookup and legislative history tracing.

## Sub-jurisdictions

France is a **unitary state** with no sub-jurisdictions with legislative competence. Overseas collectivities (New Caledonia, French Polynesia) have varying legislative autonomy. New Caledonia has "lois du pays" in certain domains. Overseas-specific legislation is partially captured through JORF/Legifrance.

## Regulatory & Administrative Authorities

### Not Indexed (Gap Analysis)

| Authority | Domain | Priority |
|-----------|--------|----------|
| ARCEP | Telecommunications | High |
| ACPR | Banking/insurance supervision | High |
| ARCOM | Media, digital platforms | Medium |
| HAS | Health regulation | Medium |
| Defenseur des droits | Human rights | Medium |
| CRE | Energy regulation | Medium |

## Access Notes

- **DILA Open Data:** Bulk XML/JSON at data.gouv.fr under Etalab open license. Covers CASS, JADE, CONSTIT, CNIL, KALI, LEGI, JORF.
- **PISTE (piste.gouv.fr):** OAuth2 API gateway for Legifrance and Judilibre. Free registration.
- **License:** Licence Ouverte 2.0 (CC-BY compatible). Court decisions are public domain.

## How to Contribute

To add a new source, create a directory under `sources/FR/[SourceName]/` following the project conventions and update this README.
