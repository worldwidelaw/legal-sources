# UG/LawsAfricaActs — Uganda Acts (ULII / Laws.Africa)

**Source**: Uganda Legal Information Institute (ULII)
**URL**: https://ulii.org/en/legislation/
**Data types**: Legislation
**Auth**: None
**License**: Laws.Africa / Open access

## Overview

Ugandan legislation from ULII, powered by Laws.Africa. ~625 acts including
principal acts, ordinances, decrees, and statutory instruments. Full text
in Akoma Ntoso HTML format.

## Data Access

Paginated listing at `https://ulii.org/en/legislation/?page={N}` (~10 pages).
Individual acts at `https://ulii.org/en/akn/ug/act/{year}/{num}/eng@{date}`.
Full text embedded as `<la-akoma-ntoso>` element in each page.

5-second crawl delay required per robots.txt.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # Full pull (~625 acts, slow)
python bootstrap.py test                 # Connectivity test
```

## License

[Laws.Africa Open Access](https://ulii.org/) — Ugandan legislation published by ULII / Laws.Africa under open access terms.
