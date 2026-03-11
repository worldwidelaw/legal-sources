# Council of Europe — Legal Data Sources

> **Last updated:** 2026-02-21

## Overview

The Council of Europe (CoE) is an international organization founded in 1949, distinct from the European Union, with 46 member states (following Russia's expulsion in 2022). Its primary mission is the protection of human rights, democracy, and the rule of law in Europe. The CoE is not a legislature in the traditional sense — it produces treaties (conventions and protocols), which member states may ratify, and its organs issue resolutions, recommendations, and opinions that carry varying degrees of legal authority.

The cornerstone instrument is the European Convention on Human Rights (ECHR), adopted in 1950, along with its 16 protocols. The Convention established the European Court of Human Rights (ECtHR), sitting in Strasbourg, which adjudicates individual and inter-state applications alleging violations of the Convention rights. The ECtHR's judgments are binding on the respondent state and have generated a vast body of human rights case law that profoundly influences domestic legal systems across Europe.

Beyond the ECtHR, the Council of Europe's legal output includes: the Venice Commission (European Commission for Democracy through Law), which issues advisory opinions on constitutional matters; the Committee of Ministers, which adopts resolutions supervising the execution of ECtHR judgments and issues recommendations to member states; the Parliamentary Assembly, which adopts resolutions and recommendations; and the Commissioner for Human Rights, who issues reports and opinions. The CoE also administers over 200 treaties covering criminal law (e.g., Budapest Convention on Cybercrime), data protection (Convention 108+), cultural heritage, social rights (European Social Charter), and more.

## Sources in This Repository

| Source | Type | Script | Last Run | Fetched | Samples | Diagnosis |
|--------|------|--------|----------|---------|---------|-----------|
| HUDOC | case_law | Yes | Never run | - | 0 | Untested |

**1 sources total:** 1 untested.

## Exhaustive Source Inventory

| Source | Type | Description | Indexed? | Access | Availability Diagnosis |
|--------|------|-------------|----------|--------|----------------------|
| HUDOC | Case Law | ECtHR judgments, decisions, advisory opinions, communicated cases | Yes | JSON API (no auth) | Easy — well-structured JSON API, no authentication |
| HUDOC-EXEC | Execution Monitoring | Committee of Ministers decisions on execution of ECtHR judgments | No | Web interface / JSON API | Moderate — API similar to HUDOC, less documented |
| Venice Commission Opinions | Advisory Opinions | Constitutional law opinions on legislation and institutional reforms | No | Venice Commission website / CODICES | Moderate — CODICES has a search interface but no public bulk API |
| CODICES | Constitutional Case Law | Database of constitutional court decisions from CoE member states | No | Venice Commission CODICES portal | Hard — no public API, web search interface only |
| European Social Charter (ECSR) | Quasi-judicial Decisions | ECSR decisions on collective complaints and conclusions on state reports | No | HUDOC for Social Charter (separate instance) | Moderate — separate HUDOC instance exists |
| GRECO Reports | Evaluation Reports | Group of States against Corruption evaluation and compliance reports | No | GRECO website | Hard — PDF documents, no API |
| CPT Reports | Monitoring Reports | European Committee for the Prevention of Torture visit reports | No | CPT website | Hard — PDF documents, no API |
| Commissioner for Human Rights | Reports & Opinions | Country reports, thematic reports, opinions, letters | No | Commissioner website | Hard — scattered PDF documents |
| Committee of Ministers | Resolutions & Recommendations | Recommendations to member states and resolutions | No | CoE website | Moderate — search interface available but no bulk API |
| Parliamentary Assembly | Resolutions & Recommendations | PACE resolutions, recommendations, and opinions | No | PACE website | Moderate — search interface available but no bulk API |
| CoE Treaty Office | Treaties | Full text of 200+ CoE conventions, protocols, ratification chart | No | CoE Treaty Office website | Moderate — structured website with treaty texts |
| CEPEJ Reports | Judicial Statistics | European Commission for the Efficiency of Justice reports | No | CEPEJ website / CEPEJ-STAT | Moderate — CEPEJ-STAT offers some structured data |

## Consolidated Legislation vs. Official Journal

This distinction does not apply to the Council of Europe. The CoE is not a legislature and does not publish an official journal comparable to the EU's OJEU or a national gazette. The legal instruments are: treaties (static texts), ECtHR case law (the living body of law via HUDOC), and Committee of Ministers resolutions/recommendations (individual acts).

**Our strategy:** We index ECtHR case law from HUDOC as the primary and most legally significant source.

## Sub-jurisdictions

The Council of Europe does not have sub-jurisdictions. It is an international organization whose 46 member states are sovereign nations with their own legal systems. Case law is organized by respondent state.

## Regulatory & Administrative Authorities

| Body | Abbreviation | Domain | Output Indexed? |
|------|-------------|--------|----------------|
| European Court of Human Rights | ECtHR | Human rights adjudication | Yes (via HUDOC) |
| Committee of Ministers | CM | Execution supervision, recommendations | No |
| Venice Commission | CDL | Constitutional democracy, rule of law | No |
| European Committee of Social Rights | ECSR | Social and economic rights | No |
| GRECO | GRECO | Anti-corruption monitoring | No |
| CPT | CPT | Detention conditions monitoring | No |
| Commissioner for Human Rights | CommHR | Human rights promotion | No |
| CEPEJ | CEPEJ | Judicial system evaluation | No |
| ECRI | ECRI | Anti-discrimination monitoring | No |

## Access Notes

### HUDOC
- **API endpoint:** `https://hudoc.echr.coe.int/app/query/results`
- **Authentication:** None required
- **Format:** JSON response; full text available as HTML or PDF
- **Filtering:** By respondent state, article violated, importance level, document type, date range, keyword
- **Languages:** English and French (official Court languages)
- **Identifiers:** Application numbers (e.g., 12345/06) and ECLI identifiers

## How to Contribute

To add or update a source, create a new directory under `sources/CoE/[SourceName]/` with:
- `config.yaml` — source configuration
- `bootstrap.py` — data fetcher
- `status.yaml` — run status (auto-generated)
- `sample/` — sample records

Then update this README to reflect the new coverage.
