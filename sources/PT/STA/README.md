# PT/STA — Portuguese Supreme Administrative Court

Source for case law from the **Supremo Tribunal Administrativo (STA)** — the highest
court in Portugal for administrative and tax matters.

## Data Source

- **URL**: https://www.dgsi.pt/jsta.nsf
- **Database**: DGSI (Direção-Geral dos Serviços de Informática do Ministério da Justiça)
- **Platform**: Lotus Notes/Domino
- **License**: [Open Government Data](https://dados.gov.pt) (public, free for reuse)
- **Language**: Portuguese

## Coverage

- **Administrative contentious** (Secção do Contencioso Administrativo): since 1950
- **Tax and customs contentious** (Secção do Contencioso Tributário): since 1963
- **Full text available**: from 2002-01-09 onwards only
- **Total entries in the view**: 90,047
- **Usable records with full text**: ~35,005 (view positions 1-35,005)

> **Note**: The view is sorted by decision date descending and full text stops abruptly
> at 2002-01-09 (position 35,005). Positions 35,006-90,047 are 2001-12-24 and older and
> carry no `Texto Integral` at all, so the crawl stops after 400 consecutive text-less
> documents rather than paginating through 55,000 metadata-only entries for hours.

## Case Types

The STA handles appeals in:

- Administrative law (public sector employment, contracts, urban planning)
- Tax law (IRS, IRC, IVA, customs duties)
- Social security disputes
- Regulatory matters

## Data Fields

| Field | Description |
|-------|-------------|
| `case_number` | Process number (e.g., "0230/25.2BECTB.SA1") |
| `date` | Decision date |
| `section` | Court section (Administrative or Tax) |
| `rapporteur` | Judge rapporteur |
| `summary` | Case summary (Sumário) |
| `text` | Full decision text (Texto Integral) |
| `descriptors` | Legal keywords/topics |
| `conventional_number` | Internal document number |
| `appellant` | Appellant party |
| `appellee` | Appellee party |
| `voting` | Voting result (e.g., "UNANIMIDADE") |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12 by default)
python bootstrap.py bootstrap --sample

# Full bootstrap (~35,000 full-text decisions), resuming from the checkpoint
python bootstrap.py bootstrap

# Same, under the name the VPS fleet invokes
python bootstrap.py bootstrap-fast

# Ignore the checkpoint and walk the view from the top
python bootstrap.py bootstrap --restart

# Incremental update (recent decisions)
python bootstrap.py update
```

## Technical Notes

- The DGSI database uses Lotus Notes/Domino.
- Enumeration goes through the Domino view API:
  `/jsta.nsf/Por+Ano?ReadViewEntries&Start=N&Count=M&OutputFormat=JSON`, which returns
  each entry's UNID, ISO decision date, case number and rapporteur, plus the
  authoritative `@toplevelentries` total.
- The HTML listing (`?OpenDatabase&Start=N`) is **not** used: its page size is not
  fixed (108 view positions on page 1, 99 afterwards), so the previous hardcoded
  `PAGE_SIZE = 121` skipped ~20 entries per page — measured at **17.4% of the corpus**
  silently dropped (issue #1461).
- Pagination advances to `last returned @position + 1`; the view interleaves category
  rows, so positions are not contiguous with the number of documents returned.
- Full text requires `?OpenDocument&ExpandSection=1`.
- Detail pages are fetched 5 at a time, in view order.
- Progress is checkpointed to `data/sta_checkpoint.json` after every page, so a torn-down
  fleet slot resumes at its last position instead of re-walking from the top.
- Character encoding is ISO-8859-1 (Latin-1).
- Dates: the view's `DATAAC` column is ISO and authoritative. The detail page renders the
  date according to `Accept-Language` — `14-07-2026` (DD-MM-YYYY) for `pt-PT`,
  `07/14/2026` (MM/DD/YYYY) otherwise — and is only used as a fallback.
- Rate limiting: 1.5 seconds between requests.

## License

[Open Government Data](https://dados.gov.pt) — Portuguese court decisions, public and free for reuse.

## Related Sources

- **PT/SupremeCourt**: Supremo Tribunal de Justiça (civil/criminal)
- **PT/ConstitutionalCourt**: Tribunal Constitucional
- **PT/DiarioRepublica**: Official Gazette (legislation)
