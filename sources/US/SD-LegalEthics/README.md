# US/SD-LegalEthics — State Bar of South Dakota — Ethics Opinions

Full text of the **Ethics Opinions** issued by the **State Bar of South Dakota's
Ethics Committee**. Each opinion applies the **South Dakota Rules of Professional
Conduct** to a stated question to advise **lawyers** on the ethics of
contemplated conduct — i.e. legal-ethics **doctrine** (advisory).

- **Publisher:** State Bar of South Dakota (the state's integrated/unified bar)
- **Type:** `doctrine`
- **Jurisdiction:** `US-SD`
- **Series:** per-year numbered `{year}-{N}` (e.g. `1995-01`, `2006-01`,
  `2020-07`, `2025-01`; a few carry a trailing letter, e.g. `2000-05a`)
- **Coverage:** ~145 opinions listed 1986–present; the born-digital text-layer
  opinions (~1995–present) are captured in full.

## How it works

1. **Discovery** — a single public index page,
   `https://www.statebarofsouthdakota.com/ethics-opinions/`, links every opinion.
   The anchor **text** is `"{number}: {subject} (Rules …)"`; the href is either
   an on-site URL `/{a}_{b}_{slug}/` (which serves the opinion **PDF** directly)
   or a born-digital PDF on the Bar's Azure CDN
   (`growthzonecmsprodeastus.azureedge.net/.../{number}.pdf`). Both resolve to a
   `%PDF`.
2. **Full text** — each PDF is extracted with **PyMuPDF (fitz)**, **no OCR**.
   The newer opinions are born-digital; the oldest (1986–1994, listed as a bare
   number with no subject) are scanned images with no text layer → they yield no
   text and are skipped (`<150`-char guard). A handful of 2021–2023 opinions
   live only behind the members' GrowthZone portal (login) and are skipped.
3. **Number** — the `{year}-{N}` token, sequence zero-padded to two digits
   (`2006-1` → `2006-01`), trailing letter preserved.
4. **Date** — an in-body `Month DD, YYYY` date within ±1 year of the number's
   year when present, else `YYYY-01-01`.

Distinct from `US/SD-Courts` and `US/SD-Legislation`.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction check
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — U.S. government edict.

The State Bar of South Dakota is South Dakota's **integrated (unified/mandatory)
bar** under SDCL ch. 16-17, operating under the authority of the South Dakota
Supreme Court to regulate the legal profession. Its ethics opinions authoritatively
interpret the South Dakota Rules of Professional Conduct and are treated as public
domain under the 17 U.S.C. § 105 government-edicts rationale, consistent with the
other integrated state-bar legal-ethics sources. Published free to the public on
`statebarofsouthdakota.com` with no login, paywall, or terms prohibiting reuse.
**Commercial use permitted.**
