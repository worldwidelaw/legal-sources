# GE/SupremeCourt-Decisions — Georgia Supreme Court Decisions

## Overview
Fetches case law from Georgia's Supreme Court via jQuery AJAX endpoints.

- **~86,226 decisions** with full text in Georgian
- **3 chambers** (palatas): Administrative (0), Civil (1), Criminal (2)

## API Details
The website uses AJAX requests with `X-Requested-With: XMLHttpRequest` header.

### Endpoints
| Endpoint | Description |
|----------|-------------|
| `/ka/getCases?palata={id}&page={n}` | Paginated case listings (HTML) |
| `/fullcase/{id}/{palata}` | Full decision text (HTML) |
| `/caseGetCategory` | Case categories |

### Palata IDs
| ID | Chamber | Decisions |
|----|---------|-----------|
| 0 | Administrative | ~26,841 |
| 1 | Civil | ~32,790 |
| 2 | Criminal | ~26,595 |

## Full Text
Full decision text is fetched via the `/fullcase/{id}/{palata}` AJAX endpoint.
HTML is stripped to plain text. Typical length: 7,000-45,000 characters.

## Usage
```bash
python bootstrap.py test-api             # Test connectivity
python bootstrap.py bootstrap --sample   # Fetch sample data
python bootstrap.py bootstrap            # Full data pull
python bootstrap.py update 2026-01-01    # Incremental update
```
