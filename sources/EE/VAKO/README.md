# EE/VAKO — Estonia, Public Procurement Review Committee decisions

Full text of the decisions (*otsused*) of the Estonian **Riigihangete
vaidlustuskomisjon** (VAKO, Public Procurement Review Committee), the
administrative review body that hears challenges against contracting
authorities' procurement decisions.

- **Register:** <https://fin.ee/riigihanked-riigiabi-osalused/riigihanked/vaidlustusmenetlus>
- **Type:** `case_law`
- **Language:** Estonian
- **Coverage:** 83 decisions, 2017 → 2026 — small procurements (*väikehange*)
  and mini-competitions under framework agreements (*minikonkurss*).

## How it works

The Ministry of Finance page is server-rendered and carries three tables. Two
are decision registers with six columns — procedure type, decision date,
decision number, subject of the challenge, outcome, and a link to the decision
PDF — so every metadata field comes from the register and only the body text is
read out of the born-digital PDF. The third table lists the Committee's annual
statistical reports and is skipped (identified by the absence of an
`Otsuse nr` header column), since those are not decisions.

## Scope caveat

This is the **curated subset the Ministry publishes**, not the complete VAKO
corpus. The full set lives in the Riigihangete register SPA, whose
dispute-search API is auth-gated: `POST
https://riigihanked.riik.ee/rhr/api/public/v1/search/disputes` returns **401**
across three payload shapes with session cookies (re-verified 2026-08-02).
Expanding coverage requires credentials for that register.

## Gotchas

- Estonian diacritics in the PDF file names are percent-encoded. The exact
  `href` bytes from the HTML are fetched unchanged — re-typing the name under a
  different Unicode normalisation 404s.
- Older rows carry no procurement reference number, so their decision-number
  cell reads `55-22/-`. Records de-duplicate on the decision number, falling
  back to the file path.
- If the index yields no decision rows the run raises instead of reporting an
  empty corpus, so a future block or layout change fails loud rather than
  silently ingesting only the committed samples.

## Usage

```bash
python bootstrap.py test-api             # connectivity + one full-text extraction
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull (83 decisions)
python bootstrap.py bootstrap-fast       # high-throughput full pull (fleet)
python bootstrap.py updates --since 2026-01-01
```

## License

[Estonian public information — Public Information Act (*Avaliku teabe seadus*)](https://www.riigiteataja.ee/en/eli/522032023003/consolide)
— decisions of the Public Procurement Review Committee are official acts of an
Estonian public authority, published by the Ministry of Finance for
unrestricted public access. The Estonian Copyright Act (§ 5) excludes acts of
public authorities from copyright protection. No registration and no
reuse-restricting terms are imposed; commercial use is permitted.
