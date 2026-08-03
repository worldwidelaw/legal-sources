# US/MI-PSC — Michigan Public Service Commission Orders (E-Dockets)

Full text of **Orders** issued by the Michigan Public Service Commission
(MPSC) resolving utility dockets — electric, natural-gas, water,
telecommunications and video/cable rate cases, certificate/approval
applications, complaints and rulemakings. Each Order is an administrative
adjudication of a specific case by the Commission = **case_law**.

## Source

The MPSC E-Dockets system is a **public Salesforce Experience Cloud
community** at `https://mi-psc.my.site.com/s/`. All docket filings are stored
as `Filing__c` records; the substantive Commission decisions are the
`Filing_Type__c == "Order"` filings (~13K of ~180K total filings).

## How it works

1. **Discovery** — the community exposes a public SEO sitemap index at
   `/s/sitemap.xml`, which links nine `Filing__c` shards
   (`/s/sitemap-filing__c-{N}.xml`, ~20K filing URLs each).
2. **Metadata** — for each filing record id we replay the community's own
   guest Aura endpoint (`POST /s/sfsites/aura`,
   `RecordUiController.getRecordsWithFields`, batched 50 at a time,
   `aura.token="null"`). This returns `Filing_Type__c`, `Public__c`,
   `File_Link_Internal__c` (the Salesforce ContentVersion download URL),
   `File_Date__c` and the filing `Name` (e.g. `U-21000-0003`, whose stem
   `U-21000` is the case number). We keep only public Orders with a document
   link. The Aura `fwuid`/app-load id are parsed live from the home page and
   re-parsed on a 401 (they rotate on redeploy).
3. **Full text** — `normalize()` downloads the born-digital Order PDF from the
   guest-accessible shepherd endpoint
   (`/sfc/servlet.shepherd/version/download/{versionId}?operationContext=S1`)
   and extracts text with PyMuPDF (Tesseract OCR fallback for image-only
   scans).

`fetch_all()` checkpoints its scan position (sitemap shard + batch offset) to
`data/mi_psc_checkpoint.json` so fleet reruns advance monotonically over the
~180K-filing scan instead of restarting.

## Usage

```bash
python bootstrap.py test-api            # connectivity + one full-text Order
python bootstrap.py bootstrap --sample  # ~12 sample Orders
python bootstrap.py bootstrap-fast      # full pull (VPS, checkpointed)
```

## License

[U.S. Government Work — state edict / public domain](https://www.law.cornell.edu/uscode/text/17/105) — Orders of a U.S. state administrative agency are government edicts and are not subject to copyright. Commercial use permitted; no attribution required.
