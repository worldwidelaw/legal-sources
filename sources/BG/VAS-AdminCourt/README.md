# BG/VAS-AdminCourt — Bulgarian Supreme Administrative Court

## Source

- **Name**: Върховен административен съд (Supreme Administrative Court)
- **URL**: https://ecase.justice.bg
- **Type**: case_law
- **Auth**: none
- **Coverage**: 2010–present (~63,500 cases)

## Method

Uses the EPEP (Unified Portal for Electronic Justice) JSON API:

1. **Search cases**: POST `/Case/LoadData` with CourtId=113 (VAS)
2. **Get acts**: POST `/Case/ActsLoadData` with case GID
3. **Full text**: GET `/case/preview?type=7&gid={act_gid}` → find download link → GET `/api/file/download/{uuid}`

Full text files are UTF-16 encoded HTML.

## Fields

| Field | Description |
|-------|-------------|
| case_gid | Unique case identifier |
| act_gid | Unique act/decision identifier |
| title | Act type + case number |
| text | Full decision text |
| date | Decision date (ISO 8601) |
| case_kind | Type of case |
| act_type | Type of act (Решение, Определение) |
| judges | Panel members |
| plaintiff/defendant | Parties |

## License

[Open Government Data](https://data.egov.bg/) — Bulgarian judicial decisions are publicly available for reuse.
