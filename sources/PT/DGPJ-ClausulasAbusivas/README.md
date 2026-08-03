# PT/DGPJ-ClausulasAbusivas — Portuguese Unfair Contract Terms case law

Court decisions from the **Cláusulas Abusivas** (Unfair/Abusive Contract Terms)
database of the **Direção-Geral da Política de Justiça (DGPJ)**, the policy
directorate of the Portuguese Ministry of Justice. The database is published on
[dgsi.pt](https://www.dgsi.pt/jdgpj.nsf) as the Lotus Domino database
`jdgpj.nsf`.

Each record is a court ruling that declares one or more standard/general
contract terms **abusive and null** under:

- **DL 446/85** (Regime Jurídico das Cláusulas Contratuais Gerais — the
  Portuguese unfair standard contract terms statute), and
- Portuguese consumer protection law.

Records capture the operative decision text (which clauses were struck down and
the legal grounds), the contract type, the parties, the first-instance court and
section, the decision date, and legal descriptors.

## Coverage

- ~392 decisions (single database).
- Language: Portuguese.
- Distinct from `PT/DGSI` (Courts of Appeal + administrative courts),
  `PT/STA`, and `PT/SupremeCourt` — this is the DGPJ's curated unfair-terms
  jurisprudence collection.

## Access

- **JSON enumeration:** `/jdgpj.nsf/Por+Ano?ReadViewEntries&Start=N&Count=N&OutputFormat=JSON`
- **Full document:** `/jdgpj.nsf/0/{unid}?OpenDocument&ExpandSection=1`
- Lotus Notes / Domino backend, ISO-8859-1 encoded HTML.
- No auth, no CAPTCHA. Full text is extracted from the `Texto das Cláusulas
  Abusivas` / `Texto Integral` value cells.

## Usage

```bash
python bootstrap.py test               # connectivity check
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # full pull
python bootstrap.py update             # incremental
```

## License

[Open Government Data (Portugal)](https://dados.gov.pt) — Portuguese court
decisions published as public government data on dgsi.pt (Ministério da Justiça).
Commercial use permitted.
