# US/KS-CorpCommission — Kansas Corporation Commission (KCC) Orders

Full text of **Orders** issued by the Kansas Corporation Commission (KCC)
adjudicating utility, energy (oil & gas conservation) and transportation
dockets across the **electric**, **natural-gas**, **telecom**, **water**,
**oil-&-gas** and **motor-carrier** industries. Each Order is an administrative
adjudication of a specific docket by the Commission = `case_law`.

## Source

- **Official site:** https://kcc-connect.kcc.ks.gov/s/ ("KCC Connect")
- **Access:** public Salesforce Experience Cloud community + public SpringCM
  document downloads, no auth.

## How it works

1. **Discovery:** the community publishes an SEO sitemap `/s/sitemap.xml` →
   per-object shards `/s/sitemap-order__c-{N}.xml` (~20K `Order__c` record URLs
   each, ~60,000–80,000 Orders total). Each URL is `/s/order/{id}/{slug}`.
2. **Metadata:** the community's own **guest Aura endpoint**
   (`POST /s/sfsites/aura`, `getRecordsWithFields`, batched 50, `aura.token="null"`)
   returns `Name` (order number), `Docket__c` (an `<a>` whose text is the docket
   number), `OrderDate__c`, `Title__c`, `Type__c`,
   `Publicly_Available__c`/`DocumentAccess__c` and **`PreviewUrl__c`** — a public
   SpringCM `DownloadPdf` URL for the born-digital Order PDF.
3. **Full text:** `normalize()` downloads the Order PDF from `PreviewUrl__c` and
   extracts the text with PyMuPDF (Tesseract OCR fallback for rare image-only
   scans).

`fetch_all()` walks the order shards and **checkpoints** shard + batch offset to
`data/ks_cc_checkpoint.json` so fleet reruns advance monotonically over the
full corpus. The Aura `fwuid`/app id are parsed live and re-parsed on 401/403
(they rotate on community redeploy).

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap-fast       # Full pull (VPS)
```

## License

[Public Domain (US Government Work — Kansas)](https://www.law.cornell.edu/uscode/text/17/105) — Kansas Corporation Commission Orders are official state government edicts in the public domain. Commercial use permitted; no attribution required.
