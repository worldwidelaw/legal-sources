# ME/SupremeCourt - Montenegro Supreme Court (Vrhovni sud)

## Overview

Case law from the Supreme Court of Montenegro (Vrhovni sud Crne Gore).

- **Source**: https://sudovi.me/vrhs/odluke/
- **Coverage**: 48,700+ decisions from all court departments
- **Language**: Montenegrin
- **Update frequency**: Daily

## Data Access

Uses the official sudovi.me REST API:

- `POST /api/search/decisions` - Search for decisions
- `GET /api/decision/{id}` - Get full text of a decision

### API Parameters

Search endpoint accepts:
- `courtCode`: Court identifier (vrhs = Supreme Court)
- `start`: Pagination offset
- `rows`: Number of results per page
- `caseType`: Filter by case type
- `department`: Filter by court department
- `year`: Filter by case year

## Court Departments

The Supreme Court has multiple departments (odjeljenje):
- Krivično odjeljenje (Criminal Department)
- Građansko odjeljenje (Civil Department)
- Upravno odjeljenje (Administrative Department)

## Schema

Each normalized record contains:

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (ME-VRHS-{dbid}) |
| `title` | Court name + decision type + case number |
| `text` | Full text of the decision (cleaned HTML) |
| `date` | Decision date (datum_vijecanja) |
| `case_number` | Case reference (e.g., Kr-S 1/2011) |
| `court` | Court name |
| `department` | Court department |
| `decision_type` | Type of decision (Presuda, Rješenje) |
| `case_type` | Case category |
| `url` | Link to decision on sudovi.me |

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Fetch all records (outputs JSON lines)
python3 bootstrap.py fetch

# Fetch limited number
python3 bootstrap.py fetch --limit 100
```

## License

Court decisions are public documents under Montenegrin law.
