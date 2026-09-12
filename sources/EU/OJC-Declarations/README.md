# EU/OJC-Declarations — OJ C-series Declarations & Joint Statements

Declarations, joint statements and statements of the EU institutions entered in the
Official Journal **C series** ("Information and Notices"), catalogued by the EU
Publications Office (CELLAR / EUR-Lex) under CELEX **sector 3, document class `C`**
(`3{YYYY}C{NNNN}`, e.g. `32026C03482` = a Joint Statement by the European Parliament,
the Council and the Commission; `32013C1220(02)` = "Declarations of the Commission").

## What this covers

The `C` descriptor gathers the soft-law / interpretative declarations of the EU
institutions — chiefly the **Council** and the **Commission**, alone or jointly, plus
the interinstitutional joint statements of the European Parliament, Council and
Commission:

- declarations entered in the Council minutes on the interpretation/application of an
  adopted act,
- Commission statements on the implementation of a regulation or directive,
- joint statements on the annual budgetary procedure,
- statements on the exercise of delegated / implementing powers.

Classified as **doctrine** (they are not the binding enacting text).

## Distinct & additive to EU/EUR-Lex

EU/EUR-Lex enumerates sector 3 by the ordinary binding resource-types only (REG / DIR /
DEC and their implementing/delegated variants) and therefore never pulls the `C`
declarations. `C` is a different CELEX letter from every other descriptor, so there is no
`_id` collision — in particular it is distinct from **EU/OJC-Acts** (descriptor `Y`, the
OJ C-series *acts* such as resolutions and conclusions).

## How it works

1. Enumerate every act via the public CELLAR SPARQL endpoint (CELEX matching
   `^3[0-9]{4}C[0-9]`). The corpus is ~170 rows — a single paged sweep suffices.
2. Fetch full text from CELLAR via HTTP content negotiation: OJ/Formex **xHTML** for
   modern acts, an **OJ HTML** manifestation via the language-suffixed CELEX (`.ENG`)
   for older ones, and a born-digital **PDF** stream (PyMuPDF) for the remainder. All
   are served by CELLAR, which bypasses the eur-lex.europa.eu AWS-WAF that
   202-challenges datacenter IPs, so it is fleet-safe.
3. Normalize to the standard schema.

## Usage

```bash
python3 sources/EU/OJC-Declarations/bootstrap.py test              # probe SPARQL + one full text
python3 sources/EU/OJC-Declarations/bootstrap.py bootstrap --sample  # 15 sample records
python3 sources/EU/OJC-Declarations/bootstrap.py bootstrap         # full corpus
```

Requires `PyMuPDF` (`fitz`) for the PDF fallback branch.

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse permitted, including for commercial purposes; attribution to the source is requested.
