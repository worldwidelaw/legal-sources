# DO/CongresoRD — Dominican Republic Legislation (Consultoría Jurídica)

**Source:** [https://www.consultoria.gov.do/consulta/](https://www.consultoria.gov.do/consulta/)
**Data types:** legislation
**Document types:** Leyes (~12,500), Decretos, Reglamentos, Resoluciones, Varios

## Access

The Consultoría Jurídica portal is an ASP.NET app: `bootstrap.py` acquires the CSRF
token, posts the per-type search, parses the result table, then pulls each document's
metadata and its PDF from
`/Consulta/Home/FileManagement?documentId={id}&managementType=1`.

## Text quality — what the cleaner fixes and what it can't

The PDFs are scans of the *Gaceta Oficial* carrying a **publisher-supplied OCR text
layer**. `pdf_clean.py` repairs the structural damage (issue #1413):

| Defect | Handling |
|---|---|
| Same pages repeated up to 8× in one PDF | Exact page dedup on normalized text. Ley 1927 (Impuesto sobre la Renta) is served as 416 pages holding 65 distinct ones: 1,024,793 → 135,182 chars, with all 126 `Artículo N` numbers preserved. |
| Same physical page scanned twice with slightly different OCR | Near-duplicate dedup at 85 % shared word-trigrams. The threshold is high on purpose — statute pages share a lot of boilerplate. |
| Two-column pages read across the gutter | Blocks are split at a detected gutter and emitted left column first, then right, instead of y-then-x. |
| Apparent "article-order scramble" | A symptom of the repeats, not of shuffled pages: once deduped, first-appearance order tracks the printed page numbers. |
| `10s`→`los`, `a1`→`al`, `naci6n`→`nación` … | A short table of confusions anchored on tokens that cannot occur in Spanish. `1a` is deliberately **not** rewritten to `la`, because this corpus writes ordinals as `Sección 1a.` for `1ª`. |

**Not fixed:** the remaining character-level OCR damage (`de h j u e s t s sobre la
Rents`, `CAPITULB I`, `Articulo 15141`). That is baked into the upstream text layer and
would need a genuine re-OCR of the page images — which needs a `tesseract` binary that is
not available in the build environment. Documents whose text layer is unusable fall back
to the shared `extract_pdf_markdown` (opendataloader/pdfplumber/OCR) path.

Regression-checked on 8 random Leyes: undamaged documents retain ~99 % of their raw text
(the remainder is whitespace normalization and blank-page drops).

## Usage

```bash
python bootstrap.py test-api
python bootstrap.py bootstrap --sample
python bootstrap.py bootstrap
```

## License

Open government data — [https://www.consultoria.gov.do/consulta/](https://www.consultoria.gov.do/consulta/)
