# CA/CanLII-Extended — CanLII Extended Keyword Browse

Extended coverage of the CanLII database network, complementing the existing `CA/A2AJ` source.

## What this adds

The existing `CA/A2AJ` source provides CanLII coverage primarily through the A2AJ API namespace.
This source adds:

1. **All 400+ CanLII databases** — including specialised provincial tribunals not in A2AJ
2. **Expanded keyword set** — adds French-language terms, provincial statutes by name,
   riparian/drainage/tort vocabulary, and Indigenous water rights terms
3. **Superior court deep browse** — provincial superior courts (ONSC, QCCS, BCSC, ABKB, etc.)
   contain water law decisions where "water" appears in the body, not the title

## API behaviour — important notes

Two undocumented behaviours discovered during bulk collection (see issues #74, #75):

| Issue | Behaviour | Workaround |
|-------|-----------|------------|
| Key name | `caseBrowse` endpoint returns `caseDatabases` key, not `databases` | `data.get('caseDatabases', data.get('databases', []))` |
| Missing date | Browse listing omits `decisionDate` | Parse year from citation string leading digits |

## Setup

```bash
export CANLII_API_KEY=your_free_key   # register at developer.canlii.org
python bootstrap.py bootstrap          # Full browse across all courts
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py test               # API connectivity check
python bootstrap.py update             # Incremental (checks recent citations)
```

## Coverage (validated 2016–2026)

| Court | Cases found |
|-------|-------------|
| QCCS (Quebec Superior Court) | 1,331 |
| ONSC (Ontario Superior Court) | 579 |
| QCCA (Quebec Court of Appeal) | 322 |
| BCSC (BC Supreme Court) | 232 |
| ABKB (Alberta King's Bench) | 137 |
| ONCA (Ontario Court of Appeal) | 129 |
| + 50+ additional courts/tribunals | ~100 |

## Data source

Validated as part of the
[Global Water Law Judicial Decisions Dataset](https://github.com/jrklaus8/water-law-dataset)
(DOI: [10.5281/zenodo.19836413](https://doi.org/10.5281/zenodo.19836413)).
