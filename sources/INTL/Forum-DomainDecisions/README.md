# INTL/Forum-DomainDecisions — Forum (National Arbitration Forum) Domain Dispute Decisions

Panel decisions of **Forum** (forumadr.com, legacy **adrforum.com**), formerly the
**National Arbitration Forum (NAF)**. Forum is an ICANN-accredited UDRP provider —
the second largest after WIPO — and also administers URS, the U.S. usTLD policies
(usDRP/usRS), the Canadian CDRP, and numerous registry-specific dispute policies.

- **Type:** case_law (domain-name dispute arbitration)
- **Coverage:** 43,000+ UDRP decisions (1999–present) plus URS, USDRP, CDRP and
  other rulesets — full text for each.
- **Auth:** none

## How it works

1. `GET https://webapi.adrforum.com/api/SearchDecisions/GetRulesets`
   → list of dispute ruleset codes (`UDRP`, `URS`, `USDRP`, `CDRP`, `eUDRP`, …).
2. `POST https://webapi.adrforum.com/api/SearchDecisions/DoStandardSearch`
   with `{"ruleset": "UDRP", …}` → the complete case list for that ruleset. Each
   row carries `caseId`, `caseNumber`, `domains`, `caseName`, `ruleset`, `status`,
   `decisionDate` and a `url` to the full-text decision document.
3. `GET` each decision `url` (e.g. `https://www.adrforum.com/DomainDecisions/92016.htm`)
   → the HTML decision is stripped to clean plain text.

Rate limited to ~1 request / 1.2 s.

## Usage

```bash
python bootstrap.py test                 # connectivity check
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull -> data/records.jsonl
python bootstrap.py bootstrap-fast       # alias for full pull (pipeline)
```

## Record schema

`_id`, `_source`, `_type`, `_fetched_at`, `title`, `text` (full decision),
`date`, `url`, `case_number`, `ruleset`, `domain_names`, `decision_result`,
`submission_date`.

## License

[Custom terms — publicly published decisions](https://www.adrforum.com/terms-of-use) —
Forum publishes its domain-name dispute decisions openly on adrforum.com as a public
record, in accordance with ICANN's UDRP Rules (paragraph 4(j)), which require
providers to publish the full decision on a publicly accessible website. Commercial
use permitted; attribution requested. No login required.
