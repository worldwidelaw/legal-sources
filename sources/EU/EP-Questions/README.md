# EU/EP-Questions — European Parliament Parliamentary Questions

**Source:** [European Parliament Open Data Portal](https://data.europarl.europa.eu/api/v2/parliamentary-questions)
**Data types:** doctrine

Members of the European Parliament put thousands of **written questions** to the
Commission, the Council and the European Central Bank every year (Rules of
Procedure, Rule 144 and its predecessors). Each question — together with the
institution's official written answer — is an EU document published by the
Parliament. They form a very large, continuously growing full-text corpus of EU
regulatory and policy Q&A (well over 100 000 documents from the 7th parliamentary
term onward).

This source is distinct from **EU/EuroParl**, which covers the Parliament's
*adopted texts* (resolutions, legislative positions) via the `/adopted-texts`
endpoint. EP-Questions uses the separate `/parliamentary-questions` endpoint;
there is no document overlap, and the loader dedups on `_id` regardless.

## Why not CELLAR?

Parliamentary questions carry CELEX numbers of the form `9{YYYY}E{NNNN}`
(sector 9 = parliamentary questions), but those works are **metadata-only** in
CELLAR — every content-negotiation manifestation (`/resource/celex/{CELEX}`,
language-suffixed, xHTML, PDF) returns 404. The full text lives only in the
European Parliament Open Data Portal.

## How it works

1. Enumerate questions by year (the portal covers term 8 onward, ~2014-present):
   `GET /api/v2/parliamentary-questions?year={YYYY}&offset={N}&limit=100`,
   returning work stubs (`identifier` = e.g. `E-10-2024-001357`).
2. For each work, fetch its detail record:
   `GET /api/v2/parliamentary-questions/{identifier}?language=en`, which embeds
   the English expression's PDF manifestation *and* — via `inverse_answers_to` —
   the answer document's PDF manifestation, both as `is_exemplified_by`
   distribution paths.
3. Download each PDF from `https://data.europarl.europa.eu/{path}` and extract
   text with PyMuPDF. The question text and every answer are concatenated into a
   single `QUESTION … --- ANSWER --- …` record.
4. Normalize to the standard schema (doctrine).

`data.europarl.europa.eu` serves both the JSON API and the PDF distributions
directly (redirecting to `redmapl3.europarl.europa.eu` media) and is **not**
behind the AWS-WAF that 202-challenges the `www.europarl.europa.eu` doceo pages,
so it is fleet-safe.

## Usage

```bash
python3 sources/EU/EP-Questions/bootstrap.py test            # probe API + one full Q&A
python3 sources/EU/EP-Questions/bootstrap.py bootstrap --sample
python3 sources/EU/EP-Questions/bootstrap.py bootstrap-fast  # full corpus → data/records.jsonl
```

## License

[EU institutional reuse (Decision 2011/833/EU)](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — European Parliament documents published via the EP Open Data Portal are reusable under the Commission's reuse policy and the Portal's open reuse terms. Attribution required; commercial use permitted.
