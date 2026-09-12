# BG/ConsolidatedLegislation — Bulgaria Consolidated Legislation (Кодекси и закони)

Full consolidated (in-force) text of Bulgarian normative acts: codes (кодекси),
laws (закони), regulations (наредби, правилници, постановления, инструкции,
тарифи), Bulgarian-language EU directives and regulations, and double-taxation
treaties (СИДДО).

## Why this source exists

The Bulgarian State Gazette (`BG/StateGazette`, dv.parliament.bg) publishes
**individual gazette items** — a new act, or a discrete amendment as promulgated —
but not the **consolidated** in-force text of a code or law. A user researching,
e.g., the survivor's-pension provisions of the Social Insurance Code (Кодекс за
социално осигуряване, КСО) cannot reconstruct the framework from scattered
amendment items (issue #1190). This source indexes the consolidated text so
statutory-framework research is possible.

## Data

- **~816 acts** across 12 categories (laws, codes, regulations, EU directives/
  regulations in Bulgarian, tax treaties, accounting standards, other).
- Full text per act (tens of thousands to >1M characters), with amendment
  history annotated inline (`изм. - ДВ, бр. N от YYYY`).
- Language: Bulgarian. Type: `legislation`.

## Access

Plain HTTP GET, no JavaScript/CAPTCHA/auth:

1. Category index: `https://kik-info.com/normativna-baza/{category}/` lists each
   act as a leaf link `/normativna-baza/{category}/{slug}/`.
2. Each act page carries the consolidated text in a `<div class="nb-doc">`
   container (h1 = act title, then the article body). Extracted directly from
   HTML — no PDF, no OCR.

```bash
python bootstrap.py test-api           # connectivity + extraction test
python bootstrap.py bootstrap --sample # ~12 samples
python bootstrap.py bootstrap          # full pull (all acts)
python bootstrap.py bootstrap-fast     # alias for full pull (VPS wrapper)
```

## License

[Public Domain — Bulgarian normative acts](https://lex.bg/laws/ldoc/2135399680) — the
Bulgarian Copyright Act (ЗАПСП, Art. 4) excludes normative and official acts of
state bodies from copyright protection. The consolidated statutory text is
public domain. It is sourced here from the free legal base of **kik-info.com**, a
reachable aggregator used only as a host for the public-domain text (the same
publisher already used as the index for `BG/NAP-TaxDoctrine`). Commercial use of
the underlying law is unrestricted.
