# US/AK-OAH — Alaska Office of Administrative Hearings (Decisions)

Final decisions of the **Alaska Office of Administrative Hearings (OAH)**,
Alaska's centralized independent administrative tribunal and, by statute
(AS 44.64), the state's **tax court of general jurisdiction**. A single
administrative law judge / hearing officer hears a contested case between a
person and a State agency and issues a final decision that resolves that
specific case — this is **case_law**.

OAH hears, among others:

- **State tax appeals** (corporate income tax, oil & gas production tax,
  fisheries taxes) — OAH is the statutory tax court
- **Permanent Fund Dividend** eligibility disputes
- **Medicaid** and **public assistance** appeals
- **Professional licensing** and many other agency matters

`AS 44.64.070` requires OAH to make its final decisions available online in
an electronic database.

## Data type

`case_law` — each record is a full OAH final decision with its complete text.

## Access

No JavaScript, no CAPTCHA, no auth. Every decision renders as server-side
HTML at a deterministic, sequential URL:

```
https://aws.state.ak.us/OAH/Decision/Display?rec={N}
```

`{N}` is a sequential integer record id (observed live up to ~6000+). The
scraper walks `rec = 1..7000`, strips the site chrome from each page, and
keeps pages whose body contains an OAH decision marker
(`OFFICE OF ADMINISTRATIVE HEARINGS` / `OAH No.`) and ≥ 400 characters.
Gaps / empty / non-decision responses are expected and skipped.

- **Case number**: `OAH No. YY-NNNN-XXX` (the suffix encodes the category —
  `TAX`, `PFD`, `MED`, `DHS`, …)
- **Date**: parsed from the decision body (`Month D, YYYY`)

## Vantage note

`aws.state.ak.us` (158.145.75.84) TCP-times-out on `:443` from many
datacenter build vantages (and some WebFetch egress). **Build / launch from
a VPS or residential vantage** that can reach `aws.state.ak.us`; confirm
full text there before marking complete.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample decisions
python bootstrap.py bootstrap            # full pull (rec 1..7000)
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## License

[Public Domain — US Government Work](https://www.law.cornell.edu/uscode/text/17/105) —
final decisions of the Alaska Office of Administrative Hearings are official
state-government works in the public domain under the government-edicts
doctrine. `AS 44.64.070` mandates their online publication. Commercial use
permitted; no attribution required.
