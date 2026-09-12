# EU/JC-Documents — EU Joint Communications & Joint Reports

Joint documents issued by the **High Representative of the Union for Foreign
Affairs and Security Policy together with the European Commission** — e.g.
*"JOINT COMMUNICATION TO THE EUROPEAN PARLIAMENT AND THE COUNCIL …"* and
*"JOINT REPORT …"*. They articulate the Union's external-action strategy:
foreign, security and defence policy, the neighbourhood and enlargement,
cyber and hybrid threats, trade-defence, and human-rights reporting. Each has a
CELEX number of the form `5{YYYY}JC{NNNN}` (sector 5 = preparatory acts,
descriptor `JC`). The series runs from ~2011 (creation of the EEAS under the
Lisbon Treaty) to the present (~330 documents).

## How it works

1. **Enumerate** every JC work via the public CELLAR SPARQL endpoint
   (`http://publications.europa.eu/webapi/rdf/sparql`), filtering CELEX on
   `^5[0-9]{4}JC`. The whole descriptor is only a few hundred works, so a single
   un-scoped offset sweep stays well under the ~10K OFFSET ceiling.
2. **Fetch full text** via CELLAR content negotiation on the bare CELEX
   (`/resource/celex/{CELEX}`):
   - `Accept: application/xhtml+xml` → OJ/Formex xHTML body (older documents);
   - fall back to `Accept: application/pdf` → a direct born-digital PDF or a
     `300 Multiple-Choice` listing of PDF streams (recent documents), extracted
     with PyMuPDF (`fitz`).
3. **Normalize** to the standard `doctrine` schema.

Using CELLAR (publications.europa.eu) rather than the EUR-Lex portal bypasses
the AWS-WAF that 202-challenges datacenter IPs, so the scraper is fleet-safe.

Distinct from **EU/COM-Documents** (sector-5 `PC`/`DC`), **EU/EUR-Lex** (enacted
sector-3 REG/DIR/DEC) and **EU/EuroParl** (EP adopted texts) — no descriptor
overlap. This source is the `JC` "future sibling" flagged in EU/COM-Documents.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py update               # Incremental (loader dedups on CELEX _id)
python bootstrap.py test                 # Quick connectivity check
```

Requires `PyMuPDF` (`fitz`) for the born-digital PDF branch.

## License

[EU institutional reuse — Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — documents published by the Publications Office are reusable; attribution to the source is requested. Commercial use permitted.
