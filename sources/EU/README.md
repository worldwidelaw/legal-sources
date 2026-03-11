# European Union — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

The European Union (EU) produces a vast body of law — regulations, directives, decisions, and treaties — that is directly applicable or must be transposed across all 27 member states. EU law takes precedence over conflicting national law (primacy principle). The EU institutions — the European Parliament, the Council, and the European Commission — produce primary and secondary legislation published in the Official Journal of the European Union (OJEU) and made available through EUR-Lex, the official EU legal database.

The Court of Justice of the European Union (CJEU), sitting in Luxembourg, comprises two courts: the Court of Justice (ECJ) and the General Court (formerly the Court of First Instance). The CJEU interprets EU law and ensures its uniform application across member states. Its decisions are binding and have generated an extensive body of case law accessible through the CURIA database.

EUR-Lex provides consolidated versions of EU legislation, integrating all amendments into a single document. While the Official Journal remains the legally authentic publication, consolidated texts from EUR-Lex are the standard reference for practitioners. EU legislation uses the CELEX numbering system for unique document identification and supports the European Legislation Identifier (ELI) standard.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| EUR-Lex | legislation, case_law | Yes | Never run | - | 0 | Untested |
| curia | case_law | Yes | Never run | - | 0 | Untested |

**2 sources total:** 2 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| EUR-Lex (CELLAR/SPARQL) | Legislation + Treaties | Full EU legal corpus: regulations, directives, decisions, recommendations, opinions, treaties, international agreements, consolidated texts | Yes | SPARQL endpoint (no auth), CC BY 4.0 | Easy — well-documented SPARQL, bulk downloads available |
| CURIA | Case Law | CJEU (Court of Justice + General Court) — judgments, orders, opinions of Advocates General | Yes | SPARQL endpoint (no auth) | Easy — structured metadata via SPARQL, full text via EUR-Lex HTML |
| DG COMP Decisions | Administrative Decisions | European Commission competition law decisions (antitrust, mergers, state aid) | No | EC competition case search / scraping | Moderate — structured search available but no bulk API |
| ECB Legal Framework | Legislation + Decisions | ECB regulations, guidelines, decisions, opinions | No | ECB website / EUR-Lex (partially) | Moderate — published on ECB site, some in EUR-Lex |
| ESMA Decisions & Guidelines | Regulatory Decisions | Securities and markets authority binding decisions and guidelines | No | ESMA website | Hard — no API, documents scattered across website |
| EBA Decisions & Guidelines | Regulatory Decisions | Banking authority binding decisions and guidelines | No | EBA website | Hard — no API, PDF-heavy |
| EIOPA Decisions & Guidelines | Regulatory Decisions | Insurance and pensions authority decisions | No | EIOPA website | Hard — no API, PDF-heavy |
| EDPS Decisions | Data Protection Decisions | European Data Protection Supervisor formal decisions | No | EDPS website | Hard — limited structured data |
| EUIPO Decisions | IP Decisions | EU Intellectual Property Office board of appeal decisions | No | EUIPO eSearch Case Law | Moderate — search interface available |
| CPVO Decisions | IP Decisions | Community Plant Variety Office board of appeal decisions | No | CPVO website | Hard — very limited digital availability |
| Venice Commission Opinions (EU-related) | Advisory Opinions | Venice Commission opinions on EU member state matters | No | Venice Commission website | Moderate — JSON API available via CoE/HUDOC |
| European Parliament Legislative Observatory | Legislative Tracking | Procedure files tracking EU legislative process | No | OEIL website / API | Moderate — API exists but limited documentation |
| TED (Tenders Electronic Daily) | Public Procurement | EU public procurement notices and contract awards | No | TED API / Bulk download | Easy — open API and bulk XML available |

## Consolidated Legislation vs. Official Journal

The EU publishes both the Official Journal of the European Union (OJEU) and consolidated versions of legislation through EUR-Lex. The OJEU is the only legally authentic publication — an act enters into force based on its OJEU publication date. However, for practical legal research, the consolidated versions are far more useful: they integrate the original act with all subsequent amendments, corrigenda, and adaptations into a single document.

EUR-Lex provides consolidated texts produced by the Publications Office of the EU. These are marked with the disclaimer that they have no legal value and are provided for documentation purposes only. Nevertheless, they are the standard reference for practitioners and are maintained with high fidelity.

**Our strategy:** We index consolidated versions from EUR-Lex as the primary reference. For acts where no consolidated version exists (e.g., acts that have never been amended, or very recent amendments not yet consolidated), we fall back to the individual OJEU publication. The CELEX number system allows us to track the relationship between original acts, amendments, and consolidated versions.

## Sub-jurisdictions

The EU is not a federal state, and its member states retain their own complete legal systems. EU law applies uniformly across all 27 member states but is implemented and enforced through national legal systems. There are no EU "sub-jurisdictions" in the traditional sense, but several important distinctions exist:

- **Eurozone (20 member states):** Additional monetary and financial regulations from the ECB and the Single Supervisory Mechanism
- **Schengen Area (27 states, including non-EU):** Specific border and visa regulations
- **Enhanced cooperation:** Some EU acts apply only to participating member states (e.g., the Unified Patent Court, applicable to 17 member states)
- **Overseas territories:** Varied application of EU law depending on constitutional status (outermost regions vs. overseas countries and territories)

National transposition of EU directives creates 27 different national implementations, which are tracked by EUR-Lex through National Transposition Measures but are indexed under each member state's own sources, not under the EU heading.

## Regulatory & Administrative Authorities

| Authority | Abbreviation | Domain | Decisions Indexed? |
|-----------|-------------|--------|-------------------|
| European Commission — DG Competition | DG COMP | Antitrust, mergers, state aid | No |
| European Commission — DG Trade | DG TRADE | Trade defense (anti-dumping, countervailing duties) | No |
| European Central Bank | ECB | Monetary policy, banking supervision (SSM) | No |
| European Securities and Markets Authority | ESMA | Securities regulation, CRA supervision | No |
| European Banking Authority | EBA | Banking regulation and supervision | No |
| European Insurance and Occupational Pensions Authority | EIOPA | Insurance and pensions regulation | No |
| European Data Protection Supervisor | EDPS | EU institution data protection | No |
| European Data Protection Board | EDPB | Cross-border data protection coordination | No |
| EU Intellectual Property Office | EUIPO | Trade marks and designs | No |
| Community Plant Variety Office | CPVO | Plant variety rights | No |
| European Chemicals Agency | ECHA | Chemical substance regulation (REACH) | No |
| European Medicines Agency | EMA | Pharmaceutical authorization | No |
| European Aviation Safety Agency | EASA | Aviation safety certification | No |
| Single Resolution Board | SRB | Bank resolution in the Banking Union | No |

## Access Notes

### EUR-Lex
- **SPARQL endpoint:** `https://publications.europa.eu/webapi/rdf/sparql` (CELLAR)
- **Authentication:** None required
- **Rate limits:** No formal rate limits documented, but aggressive crawling may be throttled
- **Bulk download:** Available via CELLAR for metadata; full text available as HTML, PDF, or FORMEX (XML)
- **Languages:** All 24 official EU languages available; we primarily index French and English
- **CELEX identifiers:** Unique document IDs following a structured numbering system (sector + year + number)

### CURIA
- **SPARQL endpoint:** Available for structured case metadata
- **Full text:** Retrieved from EUR-Lex HTML pages linked from CURIA results
- **Authentication:** None required
- **Rate limits:** Moderate — the SPARQL endpoint is responsive but full text retrieval via HTML should be paced
- **Document format:** HTML for full text, structured metadata via SPARQL/XML
- **ECLI identifiers:** European Case Law Identifier used for unique case referencing (e.g., `ECLI:EU:C:2023:1`)

## How to Contribute

To add or update a source, create a new directory under `sources/EU/[SourceName]/` with:
- `config.yaml` — source configuration
- `bootstrap.py` — data fetcher
- `status.yaml` — run status (auto-generated)
- `sample/` — sample records

Then update this README to reflect the new coverage.
