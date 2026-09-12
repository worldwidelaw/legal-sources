# ST/TribunalContas — Tribunal de Contas de São Tomé e Príncipe

The Court of Audit of São Tomé e Príncipe, the country's supreme audit institution.
This is **São Tomé e Príncipe's first complete source** in Legal Data Hunter, and its
first case law of any kind (`ST/DRE` and `ST/Legislation` are both blocked).

- **Site:** <https://www.tcontas.st/>
- **Language:** Portuguese
- **Corpus:** ~155 documents (2012 → present; the bulk from 2020 onward)
- **Format:** born-digital PDFs — no OCR required

## What it covers

| Section | `_type` | Content |
|---|---|---|
| `/acordaos-2025`, `/acordaos-2026` | `case_law` | Acórdãos of the 1st Section — prior-visa (*fiscalização prévia*) grants and refusals on public contracts and public-service appointments |
| `/decisoes*` | `case_law` | Decisões of the 1st and 2nd Sections *(currently empty upstream)* |
| `/instrucoes`, `/resolu`, `/deliberacoes` | `legislation` | The Court's own normative acts — instruções on account rendering, resoluções, deliberações |
| `/parecercge` | `doctrine` | Relatório e Parecer on the Conta Geral do Estado (2017–2023) |
| `/auditoria-20xx` | `doctrine` | Compliance, financial and performance audit reports |
| `/vic20xx`, `/vecc` | `doctrine` | Verificação interna/externa de contas — account verification of ministries, embassies, state enterprises and regulators |
| `/relatorios-atividades`, `/p-estrategicos`, `/manuais-1` | `doctrine` | Annual activity reports, strategic plans, audit manuals |

## How the data is fetched

`tcontas.st` is a **Wix** site. There is no REST/JSON API, no SPARQL endpoint, no ELI
implementation, and São Tomé e Príncipe has no open-data portal.

The access path used is the site's own **sitemap index** (`/sitemap.xml`), which exposes
one `dynamic-<collection>_p_<uuid>_0_5000-sitemap.xml` per Wix Data collection plus
`pages-sitemap.xml` for the static pages. Every page server-side-renders its repeater, so
a single page carries every item of its collection: one `div.wixui-repeater__item` per
document holding the publication date, the title and the anchor to the PDF.

### Two traps worth knowing

1. **The PDFs are not on `tcontas.st`.** They are served from
   `https://<site-uuid>.usrfiles.com/ugd/27dc02_<hash>.pdf`. A naive `href="*.pdf"` scrape
   against the site's own domain finds zero documents and exits cleanly, so `discover()`
   raises a `RuntimeError` rather than reporting an empty corpus.
2. **Documents repeat across pages.** A dynamic *detail* page renders its whole collection,
   and `/fr/` mirrors duplicate every Portuguese original. Everything is deduped on the
   `27dc02_<hash>` PDF id, which handles both.

The index is interleaved across `case_law` / `legislation` / `doctrine` so that a sample —
or a crawl that dies early — still spans all three types instead of returning 10
consecutive account verifications from one year.

### Known upstream gaps

`/decisoes1ra`, `/decisoes2ra`, `/acordaos2ra`, `/legislacoes`, `/vic`, `/vec`, `/c-rap`,
`/c-inss` and `/contas-de-gerência` are live but **empty** — the Court has not published
anything under them. That is an upstream publication gap, not a parser miss. Acórdãos of
the 2nd Section, decisões, and the pareceres on the RAP and INSS accounts will appear here
once the Court posts them.

## Usage

```bash
python bootstrap.py test              # connectivity + one live PDF extraction
python bootstrap.py discover          # print the document index by section, no downloads
python bootstrap.py bootstrap --sample
python bootstrap.py bootstrap         # full pull
python bootstrap.py update            # new documents only
```

`fetch_updates(since)` filters on the item's own publication date as rendered in the
repeater — for this site that date *is* when the PDF became available to us. Items whose
date fails to parse are always yielded, so an upstream formatting change cannot silently
freeze the corpus.

## License

> ⚠️ **Commercial use restricted — license unverified.** Treat as all-rights-reserved
> until the Court confirms otherwise.

[Tribunal de Contas de São Tomé e Príncipe](https://www.tcontas.st/) — no terms-of-use,
copyright or reuse page exists anywhere on the site. The only statement is the footer:
*"Copyright 2025 © - Tribunal de Contas de São Tomé e Príncipe. Todos os direitos
reservados."* São Tomé e Príncipe has no statutory open-data or public-sector-information
regime, and no explicit public-domain carve-out for official texts was found. Flagged
rather than assumed open; attribution to the Tribunal de Contas is required in any case.
