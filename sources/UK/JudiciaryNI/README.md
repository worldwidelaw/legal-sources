# UK/JudiciaryNI — Judiciary of Northern Ireland (Judicial Decisions)

Full-text judicial decisions of the **Judiciary of Northern Ireland**, published
by the Lady Chief Justice's Office / Northern Ireland Courts and Tribunals
Service on <https://www.judiciaryni.uk/judicial-decisions>.

This fills a genuine jurisdiction gap: `UK/CaseLaw` and `UK/FindCaseLaw` cover
**England & Wales** (plus reserved UK-wide tribunals), and our Scotland sources
(e.g. `UK/ScotHousingChamber`) cover **Scotland** — but the **Northern Ireland
(GB-NIR)** superior courts and reserved NI tribunals were previously uncovered.

## Coverage

~7,700 full-text decisions (2001–present), across these `type` listings:

| Category | Court / Tribunal | approx. |
|----------|------------------|---------|
| `judgments-118` | Court of Appeal, High Court (KB/QB, Chancery, Family), Crown Court | ~6,600 |
| `summary-judgment-114` | Summary judgments | ~310 |
| `ni-valuation-tribunal-decisions-112` | Northern Ireland Valuation Tribunal | ~370 |
| `lands-tribunal-decisions-109` | Lands Tribunal for Northern Ireland | ~300 |
| `charity-tribunal-decisions-117` | Charity Tribunal for Northern Ireland | ~106 |
| `care-tribunal-decisions-106` | Care Tribunal for Northern Ireland | ~43 |
| `ni-health-and-safety-tribunal-116` | NI Health and Safety Tribunal | ~2 |

## How it works

1. For each category, page the Drupal card listing
   `/judicial-decisions/type/{slug}?page=N` (0-indexed, 20 cards/page) until a
   page returns no `<article class="search-result decisions">` cards.
2. Each card yields the detail-page slug, title, neutral citation
   (e.g. `[2026] NIKB 32`), decision date (`<time datetime>`) and judge.
3. Fetch the detail page and extract the born-digital decision PDF link under
   `/files/judiciaryni/…pdf`.
4. Download and extract full text with **PyMuPDF (fitz)** — born-digital, no OCR
   needed. Records are deduplicated by detail slug (a decision may appear under
   more than one listing).

## Usage

```bash
python bootstrap.py bootstrap --sample   # sample records for validation
python bootstrap.py bootstrap            # full pull
python bootstrap.py update               # incremental (recent decisions)
python bootstrap.py test                 # connectivity test
```

Note: on macOS the system Python's LibreSSL can be finicky with this host — use a
Homebrew Python (OpenSSL) build if you hit TLS handshake errors.

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — Crown copyright; commercial use permitted with attribution.

The site's [Crown copyright notice](https://www.judiciaryni.uk/crown-copyright)
states: *"The material featured on this website is subject to Crown copyright
protection and licensed for use under the Open Government Licence unless
otherwise indicated."*
