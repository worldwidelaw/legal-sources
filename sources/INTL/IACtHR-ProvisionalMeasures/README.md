# INTL/IACtHR-ProvisionalMeasures — Inter-American Court of Human Rights: Provisional Measures

Provisional-measures resolutions (**Resoluciones de Medidas Provisionales**) of
the Inter-American Court of Human Rights (Corte IDH), the autonomous judicial
organ of the Organization of American States (OAS).

Under Article 63(2) of the American Convention on Human Rights, the Court orders
provisional measures in cases of extreme gravity and urgency to avoid
irreparable harm to persons, and issues binding resolutions granting,
monitoring, expanding, or lifting those measures.

This is a distinct series from the Court's contentious judgments (Series C,
**INTL/IACtHR**) and its advisory opinions (Series A, **INTL/IACtHR-Advisory**).

- **Corpus:** ~779 resolutions.
- **Language:** Spanish (born-digital PDFs with a text layer — no OCR needed).
- **Type:** `case_law`.

## How it works

1. `POST https://corteidh.or.cr/get_jurisprudencia_search_tipo.cfm` with
   `nId_Tipo_Jurisprudencia=MP`, `nId_estado_NUM=T`, `lang=es` returns an HTML
   fragment listing all resolutions.
2. For each result, the main born-digital document under `docs/medidas/` is
   selected (PDF preferred, DOCX fallback); `votos/vsa_*` separate votes are
   excluded.
3. The document is downloaded and its full text extracted (PDFs via the shared
   `common.pdf_extract` extractor).
4. The issue date is parsed from the resolution header
   (`... DE DD DE MES DE YYYY`).

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
