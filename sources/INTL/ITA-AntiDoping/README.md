# INTL/ITA-AntiDoping — International Testing Agency (ITA) Anti-Doping Rule Violations

The [International Testing Agency](https://ita.sport/) (ITA) is the independent,
not-for-profit foundation (based in Lausanne) that manages anti-doping programmes
on behalf of the International Olympic Committee and roughly 70 international
federations and major-event organisers. Its Legal Affairs Department conducts the
**results management** of anti-doping rule violations (ADRVs) — from the initial
review stage through first-instance hearing panels (including the CAS Anti-Doping
Division) and appeals before the Court of Arbitration for Sport — in accordance
with the World Anti-Doping Code and the ITA's Public Disclosure Policy.

This source captures the full text of the ITA's publicly disclosed **anti-doping
rule violation cases**: for each athlete or support person, the reasoned public
report describing the prohibited substance/method, the regulatory framework, the
ADRV finding, the means of resolution, and the sanction (period of ineligibility,
disqualifications, etc.).

## Access method

The [Anti-Doping Rule Violations page](https://ita.sport/anti-doping-rule-violations/)
is a single server-rendered HTML table. Each case is a `<tr class="accordion sanction">`
row (Athlete, Nationality, Sport, Sanction, Status) followed by a `<tr class="collapse">`
detail panel carrying structured fields (Individual Type, ADRV article, Violation
Date, Ineligibility, Results Management Authority, Disqualification, Means of
Resolution) and a **READ MORE** link to the case's full reasoned news article at
`https://ita.sport/news/{slug}/`.

`bootstrap.py`:
1. parses every case row and its detail panel from the listing table,
2. fetches the linked case article (1.5 s rate limit),
3. extracts the clean article body and prepends a structured metadata header so
   the record captures both the reasoning and the case particulars.

No authentication, no WAF, reachable from any IP. ~160 cases with reasoned
articles are currently listed; the page is updated as new ITA cases are disclosed.

## License

> ⚠️ **Commercial use restricted.** ITA copyright; content published under the
> ITA Public Disclosure Policy with no open licence.

[ITA Terms of Use (All Rights Reserved)](https://ita.sport/) — attribution
expected; commercial use restricted.
