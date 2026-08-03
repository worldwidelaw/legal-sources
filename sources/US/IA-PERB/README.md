# US/IA-PERB — Iowa Public Employment Relations Board (PERB) Decisions

Full text of the decisions, orders, and neutral (fact-finding /
interest-arbitration) awards of the **Iowa Public Employment Relations Board
(PERB)**, the independent state agency that administers Iowa's public-sector
collective-bargaining law (Iowa Code chapter 20). PERB and its administrative
law judges decide prohibited-practice complaints, bargaining-unit determination
and representation / certification cases, declaratory-order petitions,
negotiability disputes, and state-employee grievance appeals. The corpus also
includes the Iowa District Court and appellate rulings on judicial review of
PERB orders. Each numbered decision/order resolves a specific contested case =
**case_law**.

## Build recipe

The PERB website (`iowaperb.iowa.gov`) was retired — the domain now
301-redirects to `eab.iowa.gov`, and the live searchable decision database moved
to `iowa-superb.iowa.gov`, which is a **Blazor Server (SignalR websocket)**
application that cannot be enumerated without a full browser.

However, the entire born-digital decision corpus — ~3,000 decision/order/award
PDFs — was crawled and preserved by the **Internet Archive Wayback Machine**
under the stable Drupal file path:

```
https://iowaperb.iowa.gov/sites/default/files/{filename}.pdf
```

The scraper:

1. Enumerates the preserved PDFs via the **Wayback CDX API**
   (`filter=statuscode:200`, `filter=mimetype:application/pdf`,
   `collapse=urlkey`).
2. Excludes non-decision support material (blank forms, presentations,
   election-result spreadsheets, voter-list images, guides) by path/filename.
3. Downloads each preserved PDF via the `/web/{timestamp}id_/{url}` raw-replay
   endpoint.
4. Extracts full text with `common.pdf_extract` (born-digital text layer; the
   shared helper falls back to tesseract OCR for the older scanned awards).
5. Applies a **content gate** keeping only records whose body reads like a real
   adjudicative decision (a PERB or court caption **and** an adjudication
   indicator such as "IN THE MATTER OF", a case number, "DECISION AND ORDER",
   an arbitration award, etc.).
6. Parses the case number and decision date from the body.

No auth, no CAPTCHA, no JS challenge — the Wayback Machine serves the preserved
bytes.

## Commands

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~3,000 PDFs)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Output fields

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `record_id`,
`case_number`, `issuer`, `title`, `text` (full decision body), `url`
(original `iowaperb.iowa.gov` PDF path), `archive_url` (Wayback raw-replay),
`date` (ISO 8601), `jurisdiction` (`US-IA`).

## License

[Public Domain (U.S. state government edict)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Iowa Public Employment Relations Board are official works of Iowa state government (edicts of a government agency) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially. Retrieved via the Internet Archive Wayback Machine's preservation of the official `iowaperb.iowa.gov` corpus.
