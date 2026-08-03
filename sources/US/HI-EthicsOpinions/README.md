# US/HI-EthicsOpinions — Hawaii State Ethics Commission Advisory Opinions

Full text of the opinions and resolutions of the **Hawaii State Ethics
Commission (HSEC)**, issued under the State Ethics Code — Hawai‘i Revised
Statutes (HRS) **chapter 84** (conflict-of-interest, gifts, financial-
disclosure and lobbying provisions).

- **Advisory Opinions** (`AO{YYYY}-{N}`) and **Informal Advisory Opinions**
  (`IAO{YYYY}-{NN}`) interpret the ethics code → `doctrine`.
- **Settlements/Resolutions of Charge** (`ROC{YYYY}-{N}`) and **of
  Investigation** (`ROI{YYYY}-{N}`) dispose of a specific enforcement matter →
  `case_law`.

## Source & access

The opinions live in the Commission's **Salesforce Experience Cloud** portal at
`hawaiiethics.my.site.com` as records of the custom object `Ethics_Advice__c`.

1. **Enumeration (zero-fetch):** the public sitemap
   `https://hawaiiethics.my.site.com/public/s/sitemap-ethics_advice__c-1.xml`
   lists all ~909 record URLs of the form
   `/public/s/ethics-advice/{18charRecordId}/{slug}`.
2. **Metadata + full-text link:** the record page is an Aura/LWC shell that
   returns only a "Loading…" skeleton to plain HTTP. The record data is read by
   replaying the Aura `getRecord` action
   (`DetailController/ACTION$getRecord`) against `/public/s/sfsites/aura` as a
   guest. The returned record carries `Name`, `Advice_Type__c`,
   `Date_Issued__c`, `Public_Keywords__c` and **`Advice_URL__c`** — a direct
   link to the born-digital opinion PDF on `files.hawaii.gov`. The `fwuid` +
   app markup id needed for `aura.context` are parsed live from a record page
   each run (they rotate when Salesforce redeploys the site).
3. **Full text:** the `Advice_URL__c` PDF is downloaded and extracted (clean
   born-digital text layer; OCR fallback for older scans).

No CAPTCHA, no auth, no browser/JS engine required.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~909 records)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
opinions and resolutions of the Hawaii State Ethics Commission are official
public records of the State of Hawaii, published for public use with no
copyright restriction. Commercial use permitted.
