# FJ/Laws — Laws of Fiji (laws.gov.fj)

**Source:** [https://www.laws.gov.fj/](https://www.laws.gov.fj/)
**Data types:** legislation
**Publisher:** Office of the Attorney-General of Fiji

The official consolidated legislation of Fiji: principal acts, subsidiary
legislation (Legal Notices), the laws as originally published, the omitted and
repealed law volumes, and the 2013 Constitution.

## Access

laws.gov.fj is an Angular single-page app served by an unauthenticated JSON API
at `/api`. The scraper uses that API rather than the retired server-rendered
pages (`/acts/actlist/{A-Z}`, `/Acts/DisplayAct/{id}`, `/Acts/ViewSection/{id}`),
which now 404 — see issue #1355.

| Endpoint | Purpose |
|----------|---------|
| `/api/get_all_acts` | index of consolidated principal acts (390) |
| `/api/get_act_by_id/{ActId}` | nested section tree, one `LegalId` per node |
| `/api/retrieve_html/{LegalId}` | that node's HTML body |
| `/api/toc_lawsaspublished` | index of as-published PDFs by year (1,867) |
| `/api/show_pdf_lawsaspublished/{Id}` | that PDF, base64 in JSON |
| `/api/toc_omittedrepealed` | omitted/repealed volumes (8) |
| `/api/show_pdf_omittedrepealed/{Id}` | that volume, base64 in JSON |
| `/api/retrieve_constitution` | the 2013 Constitution, base64 in JSON |

A consolidated act's full text is assembled by walking its section tree in
pre-order and concatenating each node's `section_html`: parent nodes carry the
part/chapter heading, leaves carry the body. PDFs are decoded from base64 and
run through the shared extractor (PyMuPDF/pdfplumber).

Completed document keys are checkpointed in `data/checkpoint.json` so a
restarted run resumes instead of re-appending from the top.

## Usage

```bash
python bootstrap.py test                 # inventory + one assembled act
python bootstrap.py bootstrap --sample   # 15 samples into sample/
python bootstrap.py bootstrap --full     # full pull to data/records.jsonl
python bootstrap.py bootstrap-fast       # full pull, concurrent
```

## License

Official Fijian legislation — published by the Office of the Attorney-General of Fiji. Laws are public acts of the Republic of Fiji. Free public access; no formal open data license published.
