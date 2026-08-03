# EU/EESC-Opinions — European Economic and Social Committee Opinions

Full-text advisory opinions of the **European Economic and Social Committee
(EESC)**. The EESC adopts ~160–190 opinions, information reports and
resolutions a year on EU legislative proposals (Council/Commission/Parliament
referrals and own-initiative work). These are advisory documents on EU
legislation → classified as **doctrine**.

## Data source & method

Every adopted EESC opinion is published in the **Official Journal C series** and
therefore stored, with full text, in the EU Publications Office repository
(**CELLAR**), addressable by a CELEX number of the form `5{YYYY}AE{NNNN}`
(sector `5` = EESC/CoR acts; `AE` = *avis CESE* / EESC opinion).

1. **Enumerate** via the public CELLAR SPARQL endpoint
   (`http://publications.europa.eu/webapi/rdf/sparql`), matching CELEX
   `^5[0-9]{4}AE`, retrieving CELEX + document date + English title
   (paged with `LIMIT`/`OFFSET`). ~3,650 opinions total.
2. **Full text** via CELLAR HTTP content negotiation:
   ```
   GET http://publications.europa.eu/resource/celex/{CELEX}
   Accept: application/xhtml+xml
   Accept-Language: en
   ```
   This serves the OJ Formex/xHTML body and **bypasses the eur-lex.europa.eu
   AWS-WAF** that returns HTTP 202 JS-challenges to datacenter IPs.

> The public eesc.europa.eu listing is an Angular "DM Search v5.5.1" SPA whose
> enumeration XHR is Azure-AD gated (`dm-api.eesc.europa.eu`). CELLAR is the
> authoritative, anonymous, datacenter-friendly full-text source for the same
> corpus.

## Usage

```bash
python3 sources/EU/EESC-Opinions/bootstrap.py test            # connectivity + counts
python3 sources/EU/EESC-Opinions/bootstrap.py bootstrap --sample   # 15 samples
python3 sources/EU/EESC-Opinions/bootstrap.py bootstrap        # full corpus
python3 sources/EU/EESC-Opinions/bootstrap.py update           # incremental
```

## License

[EU institutional reuse (Decision 2011/833/EU)](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — Official Journal text is freely reusable, commercial use permitted, attribution requested. Public-domain-equivalent for OJ content.
