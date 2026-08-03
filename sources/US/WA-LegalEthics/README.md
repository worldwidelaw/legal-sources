# US/WA-LegalEthics — Washington State Bar Association (WSBA): Advisory Opinions

**Advisory Opinions** issued by the **Washington State Bar Association's
Committee on Professional Ethics** (and its predecessor RPC Committee). Each
opinion interprets the Washington Rules of Professional Conduct (RPCs) and
advises WSBA members on their ethical obligations.

- **Type:** `doctrine` (official interpretation of the attorney-conduct rules)
- **Jurisdiction:** US-WA (Washington)
- **Corpus:** ~1,700 opinions (two numbering schemes: sequential + year-based)
- **Full text:** yes — born-digital HTML print views (no OCR)

## Source & access

The WSBA publishes every opinion, in full, on its Advisory Opinions portal
(`ao.wsba.org`). Each opinion has a clean, printable HTML view keyed by an
internal id:

`https://ao.wsba.org/print.aspx?ID={id}`

The page carries labelled fields (**Advisory Opinion**, **Year Issued**,
**RPC(s)**, **Subject**) followed by the opinion body and a standard disclaimer.
The portal's own listing is a stateful ASP.NET WebForms search, so the corpus is
enumerated by **walking the contiguous internal id space** (1 … ~1750). Ids past
the ceiling return an empty stub (~50 chars) and are skipped; the walk stops
after 60 consecutive empties.

Two numbering schemes appear across the corpus: older sequential numbers (e.g.
`835`, `1120`, some with a `W` withdrawn/variant suffix) and newer year-based
numbers (e.g. `201601` = 2016; the leading dash on the print view is stripped).

No JavaScript execution, no CAPTCHA, no auth.

## Distinct from other Washington sources

- **US/WA-EthicsOpinions** — Washington State Executive Ethics Board (advises
  *public officials* under RCW 42.52). This source is the *State Bar* advising
  *lawyers* on the Rules of Professional Conduct.
- Also distinct from Washington Attorney General opinions.

Seventh source in the state-bar attorney-ethics vein after US/NC-, US/AZ-,
US/TX-, US/UT-, US/VA-, US/CA-LegalEthics.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
```

## License

[Public Domain — U.S. Government / State Official Record](https://www.law.cornell.edu/uscode/text/17/105) — Advisory Opinions of the Washington State Bar Association's Committee on Professional Ethics are official public records of a Washington regulatory body (the WSBA operates under the authority of the Washington Supreme Court), published on ao.wsba.org for public use with no copyright restriction. Commercial use permitted.
