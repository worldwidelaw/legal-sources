# SI/AVK - Slovenian Competition Protection Agency

## Overview

This source collects published decisions from the official website of the
Slovenian Competition Protection Agency (Javna agencija Republike Slovenije za
varstvo konkurence, AVK).

- Website: https://www.varstvo-konkurence.si
- Data type: case law / competition authority decisions
- Authentication: none
- Language: Slovenian
- License: Slovenian open government data / CC BY 4.0

## Access

AVK publishes decision lists for restrictive practices and merger control. The
lists link to official decision excerpt pages under
`/ostali-dokumenti/arhiv-odlocb/odlocba.../`. Some entries also link full PDFs,
but the HTML excerpt pages are used as the stable, low-cost route for samples.

Local Python certificate validation rejects this site in the current
environment, so the scraper disables TLS verification for this source and
documents that in `config.yaml`. This is not an anti-bot bypass.

## Output

Each record includes:

| Field | Description |
|-------|-------------|
| `_id` | Stable hash based on AVK case number or official URL |
| `_source` | `SI/AVK` |
| `_type` | `case_law` |
| `title` | Decision title or first operative sentence |
| `text` | Official decision excerpt text |
| `date` | Publication or decision date |
| `url` | Official AVK detail page |
| `case_number` | AVK case number where available |
| `category` | Restrictive practices or merger control |
| `pdf_url` | Linked full decision PDF when present |

## Usage

```bash
python runner.py sample SI/AVK
python sources/SI/AVK/bootstrap.py bootstrap --sample --sample-size 10
python sources/SI/AVK/bootstrap.py update
```

## Limitations

The site is HTML-first and does not expose a public API for decisions. The
scraper therefore follows the official decision lists and extracts the published
operative excerpts from detail pages.
