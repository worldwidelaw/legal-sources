# INTL/CAC-UDRP — Czech Arbitration Court Domain Name Dispute Decisions

Panel decisions of the **Arbitration Court attached to the Czech Chamber of
Commerce and the Agricultural Chamber of the Czech Republic** (the "Czech
Arbitration Court" / **CAC**, operating as **adr.eu**).

CAC is:
- an **ICANN-accredited UDRP provider** for generic top-level domain (gTLD)
  disputes, and
- the dispute-resolution provider **designated by the European Commission**
  for **`.eu` / `.ею`** domain name disputes (.eu ADR).

The public decisions portal at <https://udrp.adr.eu/decisions/list> publishes
**~6,000 panel decisions** with complete legal reasoning (full HTML text).

## What this source collects

- `_type`: `case_law`
- Full text of each panel decision (Factual Background, Identification of
  Rights, Parties' Contentions, Rights / Confusing Similarity / Bad Faith
  analysis, Principal Reasons, and the operative outcome).
- Metadata: case number, process (UDRP or .eu ADR), disputed domain(s),
  complainant, respondent, panelist, decision/publication date, result.

## How it works

1. **Enumerate** the server-rendered grid:
   `/decisions/list?grid-perPage=100&grid-page=N` (~61 pages). Each
   `<tr data-id="...">` row carries the decision id and row metadata.
2. **Fetch** each decision: `/decisions/detail?id={id}`. The
   `<section class="content">` block holds the full decision text; the
   "Date of Panel Decision" line gives the ISO decision date.
3. Text is cleaned with BeautifulSoup. Requests are rate-limited to ~1 / 1.5s.

## Usage

```bash
python bootstrap.py test                 # connectivity check
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull -> data/records.jsonl
python bootstrap.py bootstrap-fast       # alias for full pull (pipeline)
```

## License

[Custom terms — publicly published decisions](https://udrp.adr.eu/) — CAC
publishes its UDRP and `.eu` ADR panel decisions openly as a public record of
the proceedings it administers under ICANN's UDRP and the EU `.eu` ADR rules.
No login is required to read decisions. Attribution to the Czech Arbitration
Court is appropriate.
