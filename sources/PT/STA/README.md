# PT/STA — Portuguese Supreme Administrative Court

Source for case law from the **Supremo Tribunal Administrativo (STA)** — the highest
court in Portugal for administrative and tax matters.

## Data Source

- **URL**: https://www.dgsi.pt/jsta.nsf
- **Database**: DGSI (Direção-Geral dos Serviços de Informática do Ministério da Justiça)
- **Platform**: Lotus Notes/Domino
- **License**: Public (open government data)
- **Language**: Portuguese

## Coverage

- **Administrative contentious** (Secção do Contencioso Administrativo): since 1950
- **Tax and customs contentious** (Secção do Contencioso Tributário): since 1963
- **Full text available**: from 2002 onwards
- **Total decisions**: ~89,300

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

# Full bootstrap (all ~89,300 decisions)
python bootstrap.py bootstrap

# Incremental update (recent decisions)
python bootstrap.py update
```

## Technical Notes

- The DGSI database uses Lotus Notes/Domino
- Pagination is offset-based: `?OpenDatabase&Start=N`
- Full text requires `?OpenDocument&ExpandSection=1`
- Character encoding is ISO-8859-1 (Latin-1)
- Rate limiting: 1.5 seconds between requests

## Related Sources

- **PT/SupremeCourt**: Supremo Tribunal de Justiça (civil/criminal)
- **PT/ConstitutionalCourt**: Tribunal Constitucional
- **PT/DiarioRepublica**: Official Gazette (legislation)
