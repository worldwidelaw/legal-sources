# IE/CBI-Enforcement — Central Bank of Ireland — Enforcement Actions / Settlement Notices

The [Central Bank of Ireland](https://www.centralbank.ie/) (An Banc Ceannais na
hÉireann) is the financial-services regulator and central bank of Ireland. Under
the **Administrative Sanctions Procedure** (Part IIIC of the Central Bank Act
1942, as amended) it investigates and sanctions regulated firms and individuals
for breaches of financial-services law.

Each concluded enforcement action is published in full as a **Settlement Notice**
/ **Public Statement relating to Enforcement Action**, setting out the
contraventions found, the reprimand and/or disqualification imposed, and the
monetary penalty. From 19 April 2023, sanctions imposed under admissions-based
settlements must additionally be confirmed by the High Court. These reasoned,
public determinations are quasi-judicial administrative **case law**.

- **Data type:** case_law
- **Coverage:** ~140 enforcement notices (2004–present)
- **Full text:** yes — born-digital PDFs, text-layer extraction (no OCR)

## Access

The full index of every notice is embedded in the server-rendered
[enforcement-actions listing page](https://www.centralbank.ie/news-media/legal-notices/enforcement-actions)
as a JavaScript data array of objects:

```
{ "type": "pdf", "date": "DD/MM/YYYY",
  "documentName": decodeTitle("..."),
  "url": decodeTitle("/docs/default-source/.../<slug>.pdf?sfvrsn=...") }
```

No pagination, no API key. Each `url` points to a born-digital PDF under
`/docs/default-source/news-and-media/legal-notices/settlement-agreements/`,
extracted via the shared PDF extractor.

## Usage

```bash
python bootstrap.py test               # connectivity + one-doc extraction check
python bootstrap.py bootstrap --sample # ~12 sample documents
python bootstrap.py bootstrap          # full pull (all notices)
python bootstrap.py update             # incremental (recent notices)
```

## License

[CC BY 4.0](https://www.centralbank.ie/fns/re-use-of-public-sector-information) —
the Central Bank of Ireland re-uses public sector information under the Irish PSI
Licence (Creative Commons Attribution 4.0 International, per DPER Circular
12/2016). Attribution to the Central Bank of Ireland is required. Commercial use
permitted. Third-party material is excepted from the licence.
