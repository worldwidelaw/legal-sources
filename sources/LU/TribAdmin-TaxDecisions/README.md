# LU/TribAdmin-TaxDecisions — Luxembourg Administrative Tribunal Tax Decisions

**Source:** [https://ja.public.lu](https://ja.public.lu) (decision PDFs) —
listing at [justice.public.lu](https://justice.public.lu/fr/jurisprudence/juridictions-administratives.html)
**Data types:** case_law
**Language:** French

Fiscal (tax) judgments of the Luxembourg **Tribunal administratif** and
**Cour administrative** — direct tax assessments, corporate/wealth tax, IP
regime, holding companies, exchange-of-information injunctions, VAT appeals.

## Access path

The searchable listing (`?r=f/ja_subject_type/fiscal`) is behind a
FriendlyCaptcha proof-of-work challenge since ~2026: every request to
`/fr/jurisprudence/juridictions-administratives.html` is redirected to
`/challenge.html` (verified 2026-08-03, issue #1357). The facet is therefore
not reachable from a script.

The decision PDFs are served unauthenticated and unchallenged from
`ja.public.lu` under a deterministic roll-number scheme:

```
https://ja.public.lu/{folder}/{roll}{suffix}.pdf

folder = "1-15000"          for roll <= 15000
         "15001-20000", "20001-25000", … 5000-blocks above that
suffix = ""                 Tribunal administratif judgment
         "C"                Cour administrative (appeal) judgment
         a / A / b / Ca / aC / C2 / CA   continuation or rectifying rulings
```

`bootstrap.py` sweeps the roll-number space newest-first (HEAD probes,
8 workers), downloads each hit, extracts the text with `common/pdf_extract`,
and keeps the decisions that are fiscal on the strength of their own text —
markers only present in Luxembourg tax litigation (Administration des
contributions directes / de l'enregistrement, loi générale des impôts
(Abgabenordnung), bulletins d'impôt, L.I.R., §§ AO). Progress is checkpointed
to `data/checkpoint.json` so fleet re-runs advance monotonically.

## Commands

```bash
python3 bootstrap.py test               # probe the roll-number scheme
python3 bootstrap.py bootstrap          # 15 samples -> sample/
python3 bootstrap.py bootstrap-fast     # full corpus -> data/records.jsonl
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Luxembourg public
sector information, attribution required.
