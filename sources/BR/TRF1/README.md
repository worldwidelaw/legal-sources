# BR/TRF1 — Brazil TRF-1 Federal Regional Court (1st Region)

**Source:** [https://www.trf1.jus.br/](https://www.trf1.jus.br/)
**Portal:** [https://portal.trf1.jus.br/pesquisadocumentos/](https://portal.trf1.jus.br/pesquisadocumentos/)
**Data types:** case_law

TRF-1 covers 14 judicial sections: Acre, Amapá, Amazonas, Bahia, Distrito
Federal, Goiás, Mato Grosso, Maranhão, Minas Gerais, Pará, Piauí, Rondônia,
Roraima and Tocantins.

## Access

PrimeFaces/JSF form POSTed to `index.jsf`, with AJAX pagination at 5 rows per
page. Full text is embedded in each row's `ExtClipboard` widget, so there is no
per-document fetch — the listing walk *is* the download.

### The 10,000-row cap

Every query's result set is capped at 10,000 rows server-side. This is a per
query cap, not the size of the corpus: even Roraima, the smallest section,
reports `rowCount:10000` for an unfiltered search.

The crawl therefore slices **section × document type** (5 types: Acórdão,
Decisão, Decisão de Antecipação de Tutela, Decisão Liminar, Sentença), because
each slice gets its own 10,000-row window. Searching `tipoDocumento="Todos"`
instead returns a single truncated window that in practice is almost entirely
Sentenças, which is why Acórdãos and Decisões were nearly absent from earlier
crawls. Acre alone reaches ~24,000 documents when sliced, against the 10,000 an
unsliced query exposes.

Two slices per section still hit the cap (Decisão and Sentença report exactly
10,000). Those are genuinely truncated upstream, and the crawl logs a WARNING
naming the slice so the hole is visible rather than silent. Slicing further —
by year or by subject — would be the way to reach past it.

## Incremental refresh

`fetch_updates` compares against a seen-key checkpoint at
`data/trf1_checkpoint.json` (gitignored), not against a date.

That is forced by the portal, not preferred: the search form exposes no date
field, the results table has no date column, and its columns are not sortable.
There is no way to ask for "documents since X" and no ordering that would let a
walk stop once it is past a cutoff.

This does not shorten the walk, and does not claim to — the listing still has to
be paged through because that is where the text lives. What it fixes is
re-emission: the previous implementation handed the entire corpus back to the
loader on every refresh, so a refresh was indistinguishable from a first crawl
(issue #1502). A refresh now emits only documents no previous run has emitted,
which makes a zero-record refresh a real signal.

## Record keys

`_id` is `BR-TRF1-{process_number}-{type_code}`, where `type_code` is one of
`ac`, `dec`, `dat`, `lim`, `sen`.

The document type is part of the key because one process number carries several
documents over its life — a Sentença at first instance, an Acórdão on appeal,
interlocutory Decisões along the way. Keying on the process number alone
collapsed them onto a single row and kept whichever the crawl reached first.

> ⚠️ This changed the key scheme. Rows ingested before this change use the
> unsuffixed `BR-TRF1-{process_number}` form and will not be updated in place by
> a re-crawl; they need deleting once the re-crawl lands.

## Known data-quality limits

- **`date` is best-effort.** There is no date field in the listing, so it is
  scraped from the decision text as the first `DD/MM/YYYY` match. That is
  sometimes a date cited *within* the decision rather than the decision's own
  date. Treat it as approximate; a small fraction of records have no date.
- **Older documents are OCR of scanned paper** and the text quality reflects
  that. `clean_ocr_text()` repairs the doubled-character artifact the portal's
  OCR produces, but cannot recover genuinely illegible passages.

## License

[Open government data](https://dados.gov.br)
