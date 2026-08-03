# US/MI-LegalEthics — State Bar of Michigan — Ethics Opinions (Professional & Judicial)

Full text of the ethics opinions issued by the **State Bar of Michigan**'s
Standing Committees on **Professional Ethics** and on **Judicial Ethics**. Each
opinion interprets the Michigan Rules of Professional Conduct (MRPC) or the
Michigan Code of Judicial Conduct in response to a stated fact situation and
advises lawyers or judges whether the described conduct is proper — this is the
Committees' official written interpretation of the conduct rules = **doctrine**.

The online archive is one continuous corpus combining six opinion series:

| Series | Committee | Kind |
|--------|-----------|------|
| `R`  | Professional Ethics | formal (post-Oct-1988) |
| `RI` | Professional Ethics | informal (post-Oct-1988, current — to RI-394) |
| `C`  | Professional Ethics | formal (pre-1988 legacy) |
| `CI` | Professional Ethics | informal (pre-1988 legacy) |
| `J`  | Judicial Ethics | formal |
| `JI` | Judicial Ethics | informal (current) |

Coverage spans **C-210 (1972) through RI-394 (February 2026)** — roughly
**1,280 opinions**.

## Access & recipe

`michbar.org` is a DNN / DotNetNuke ASP.NET site whose "Ethics Opinion Search"
box is JavaScript-driven, but the box is **bypassed entirely**: every opinion
detail page renders at a stable internal-primary-key route

```
https://www.michbar.org/opinions/ethics/numbered_opinions?OpinionID={id}&Type=6&Index=A
```

(the SEO alias `/opinions/ethics/{prefix}-{NNN}` 302-redirects here). The
`OpinionID` space is **contiguous** `1 .. ~1281`, so enumerating it in order
walks the whole corpus. This is important because the per-series opinion
*numbers* are **not** contiguous (e.g. `CI-389` is absent online but `CI-1188`
is present) — enumerate by `OpinionID`, never by series number.

Each opinion body renders inside
`div#dnn_ctr14174_EthicsOpinionsSearchDetail_divEOWholeOpinion` as born-digital
HTML: `{NUMBER} {DATE} SYLLABUS ... References: ... TEXT ...`. A missing or
superseded id renders only the `SBM - State Bar of Michigan` chrome header
(~27 chars) and is skipped. The opinion number (`(RI|CI|JI|R|C|J)-N`) and its
issue date are parsed from the body; dates handle the modern
`November 25, 1991`, the legacy comma form `August, 1979`, and `July 1972`.

No OCR, CAPTCHA, or authentication. A 1 req/s browser-UA `requests` session is
used.

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
python bootstrap.py bootstrap-fast      # alias for the full pull (VPS wrapper)
```

## Output schema

`_id`, `_source` (`US/MI-LegalEthics`), `_type` (`doctrine`), `_fetched_at`,
`opinion_number`, `issuer`, `title`, `text` (full opinion body), `date`
(ISO 8601), `url`, `jurisdiction` (`US-MI`).

## Distinct from

- **US/MI-MERC** — Michigan Employment Relations Commission (labor board).
- **US/MI-Courts**, **US/MI-Legislation** — Michigan courts / statutes.

This is the state **bar**'s attorney/judicial ethics advisory-opinion series.

## License

[Public Domain (State Bar of Michigan Ethics Opinions)](https://www.michbar.org/opinions/ethics/search)
— State Bar of Michigan ethics opinions are published free to the public on
michbar.org as an educational service interpreting the Michigan Rules of
Professional Conduct and the Code of Judicial Conduct. They are advisory (no
weight of law) and carry no copyright restriction or paywall on the opinion
text. No login or terms prohibiting reuse. Commercial use permitted.
