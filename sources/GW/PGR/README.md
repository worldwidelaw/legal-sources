# GW/PGR — Procuradoria-Geral da República da Guiné-Bissau

Full text of the statutory texts published by Guinea-Bissau's Attorney General's Office at [pgr.gw](https://pgr.gw/).

**Coverage:** ~24 full-text documents — the Civil Code, the Codes of Civil and of Labour Procedure, the organic laws of the Public Prosecution Service and of the courts, the 2010 security-sector reform package (Leis 07–09/2010, Decretos 05 and 20/2010), the anti-trafficking (Lei 12/2011) and terrorism-financing laws, OHADA uniform acts, CPLP judicial-cooperation conventions, and the *Suplemento ao Boletim Oficial n.º 32/2018* in full (435K chars).
**Language:** Portuguese
**Data type:** legislation

Additive to [GW/AssembleiaNacional](../AssembleiaNacional/), which covers only the texts published by parliament (Constitution, Estatuto dos Deputados, Lei da Cidadania) — the codes and the Boletim Oficial supplement do not overlap.

## Access

Open WordPress REST, no auth, no captcha, no geo-block:

```
GET https://pgr.gw/wp-json/wp/v2/media?per_page=100&mime_type=application/pdf
```

Returns one page (`X-WP-Total: 41`); each item carries `source_url` pointing at a born-digital PDF, which is downloaded and extracted with the shared `common/pdf_extract` backends.

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # ~15 sample documents
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap-fast       # Full pull (VPS)
```

## Gotchas

- The media library holds a few uploads left over from site testing (`teste`, `PDF Scanner 090924 5.22.10`); they are filtered out by title.
- Several texts were uploaded twice under slightly different titles. Records de-duplicate on a hash of the extracted text, so the second upload of an identical text is dropped rather than ingested twice.
- ~14 of the 38 uploads are scans with no text layer (e.g. `GuineBissau.LeiOrganicaMP`, 25 pages, 0 extractable chars). With no OCR backend they cannot yield full text, so they are skipped, logged and counted rather than ingested as empty records.
- **The WordPress upload date (2024-08) is not the date of the law.** The enactment date is parsed from the Portuguese date line in the head of the document, and accepted only when its year agrees with the year encoded in the title — otherwise the first date in the head belongs to a *cited* older instrument (which is how the 2007 terrorism-financing law first came out dated 1973). Where the title carries no year the latest head date wins; where the body date is unusable the title year is used with `date_is_approximate: true`.
- A few titles carry a symbol-font artefact from the upload (a Private Use Area codepoint standing in for the slash); it is restored to `/`.

## License

> ⚠️ **Commercial use restricted.** See note below.

[No terms-of-use page](https://pgr.gw/) — the site footer asserts "Copyrights © PGR Guiné-Bissau — Todos os Direitos Reservados". The content is state statutory law (official texts, which are not normally copyrightable), but the reserved-rights claim is flagged per the project's err-on-the-side-of-flagging rule.
