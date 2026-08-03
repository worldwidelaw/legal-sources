# EU/CoR-Opinions — European Committee of the Regions Opinions

Full-text advisory opinions of the **European Committee of the Regions (CoR)**,
the EU's assembly of regional and local representatives. The CoR adopts
advisory opinions on EU legislative proposals (Commission/Council/Parliament
referrals, own-initiative and outlook opinions). These are advisory documents
on EU legislation → classified as **doctrine**.

## Data source & method

Every adopted CoR opinion is published in the **Official Journal C series** and
therefore stored, with full text, in the EU Publications Office repository
(**CELLAR**), addressable by a CELEX number of the form `5{YYYY}AR{NNNN}`
(sector `5` = EESC/CoR acts; `AR` = *avis CdR* / CoR opinion).

1. **Enumerate** via the public CELLAR SPARQL endpoint
   (`http://publications.europa.eu/webapi/rdf/sparql`), matching CELEX
   `^5[0-9]{4}AR`, retrieving CELEX + document date + English title
   (paged with `LIMIT`/`OFFSET`). ~1,096 opinions total.
2. **Full text** via CELLAR HTTP content negotiation:
   ```
   GET http://publications.europa.eu/resource/celex/{CELEX}
   Accept: application/xhtml+xml
   Accept-Language: en
   ```
   This serves the OJ Formex/xHTML body and **bypasses the eur-lex.europa.eu
   AWS-WAF** that returns HTTP 202 JS-challenges to datacenter IPs.

> This is the sibling of `EU/EESC-Opinions`, which uses the same CELLAR pattern
> with the `AE` (EESC) CELEX code instead of `AR` (CoR). The public
> cor.europa.eu listing is a JavaScript SPA; CELLAR is the authoritative,
> anonymous, datacenter-friendly full-text source for the same corpus.

## Usage

```bash
python3 sources/EU/CoR-Opinions/bootstrap.py test              # connectivity + counts
python3 sources/EU/CoR-Opinions/bootstrap.py bootstrap --sample   # 15 samples
python3 sources/EU/CoR-Opinions/bootstrap.py bootstrap         # full corpus
python3 sources/EU/CoR-Opinions/bootstrap.py update            # incremental
```

## License

[EU institutional reuse (Decision 2011/833/EU)](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — Official Journal text is freely reusable, commercial use permitted, attribution requested. Public-domain-equivalent for OJ content.
