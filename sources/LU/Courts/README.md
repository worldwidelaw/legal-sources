# LU/Courts — Luxembourg Courts Decisions

**Source:** [https://justice.public.lu/](https://justice.public.lu/)
**Data types:** case_law

Pseudonymised full-text decisions from the Luxembourg judicial and
administrative courts.

## Access paths

`justice.public.lu` moved every `/fr/jurisprudence/*` listing page behind a
FriendlyCaptcha V2 challenge (requests are 302'd to
`/challenge.html?return=...`, which returns a 1.5 KB stub). Only search-engine
crawler user-agents are exempted, and we do not impersonate those. Both halves
of the corpus are therefore read from access paths the publisher serves
without a challenge:

| Half | Path | Volume |
|---|---|---|
| Judicial courts | [data.public.lu](https://data.public.lu/) bulk open data — the *Administration judiciaire* organisation publishes one dataset per court/chamber/matter, each holding yearly ZIP archives of the decision PDFs | 95 datasets, ~1,500 ZIPs, ~4.6 GB |
| Administrative courts | `ja.public.lu/{bucket}/{role}.pdf` (and `{role}C.pdf` for Cour administrative appeals) — served directly, only directory indexes are 403 | rôle numbers ~12,000–55,000, ~50 % dense |

`cour-de-cassation-1` is excluded from the judicial sweep because the Cour de
Cassation is already covered by `LU/SupremeCourt`.

Yearly ZIPs are streamed to a temp file and text-extracted member by member,
so peak disk/memory stays at one archive. Completed resources and the last
swept rôle number are checkpointed to `data/checkpoint.json`, so a restarted
fleet run resumes rather than re-appending records.

## Usage

```bash
python bootstrap.py test               # connectivity / corpus-size check
python bootstrap.py bootstrap          # 15 sample records -> sample/
python bootstrap.py bootstrap-fast     # full corpus -> data/records.jsonl
python bootstrap.py update             # datasets changed since a given date
```

## License

[Creative Commons Zero (CC0 1.0)](https://data.public.lu/fr/datasets/justice-de-paix-luxembourg-bail-1/) —
published as open data by the Administration judiciaire on data.public.lu
under the portal's open licence. Commercial use permitted.
