# INTL/IACtHR-Advisory — Inter-American Court of Human Rights: Advisory Opinions

Advisory Opinions (**Opiniones Consultivas**, Series A) of the Inter-American
Court of Human Rights (Corte IDH), the autonomous judicial organ of the
Organization of American States (OAS).

Advisory opinions interpret the American Convention on Human Rights and other
inter-American human-rights treaties at the request of OAS member states and
organs. They are a distinct series from the Court's contentious case judgments
(Series C, covered by **INTL/IACtHR**).

- **Corpus:** 32 opinions, OC-1/82 through OC-32/25 (e.g. OC-5/85 on freedom of
  expression, OC-32/25 on the Climate Emergency and Human Rights).
- **Language:** Spanish (born-digital PDFs with a text layer — no OCR needed).
- **Type:** `case_law`.

## How it works

1. `POST https://corteidh.or.cr/get_jurisprudencia_search_tipo.cfm` with
   `nId_Tipo_Jurisprudencia=OC`, `nId_estado_NUM=T`, `lang=es` returns an HTML
   fragment listing all Advisory Opinions.
2. For each result, the main Spanish PDF (`docs/opiniones/seriea_{NN}_esp.pdf`)
   is selected; `resumen_*` summaries and `votos/vsa_*` separate votes are
   excluded.
3. The PDF is downloaded and its full text extracted via the shared
   `common.pdf_extract` extractor.
4. The OC number and issue date are parsed from the PDF header
   (`OPINIÓN CONSULTIVA OC-N/YY DE(L) DD DE MES DE YYYY`).

## Usage

```bash
python bootstrap.py test              # connectivity + listing count
python bootstrap.py bootstrap --sample
python bootstrap.py bootstrap         # full pull
```

## License

> ⚠️ **Commercial use restricted.** See terms below.

[CC BY-NC-ND 3.0](https://creativecommons.org/licenses/by-nc-nd/3.0/) — matches
the sibling **INTL/IACtHR** source. Official Corte IDH jurisprudence;
attribution required, non-commercial, no derivatives.
