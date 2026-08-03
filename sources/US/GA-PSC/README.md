# US/GA-PSC — Georgia Public Service Commission Orders

Full text of **Orders** issued by the **Georgia Public Service Commission
(GPSC)** adjudicating utility dockets — rate cases, certificate /
territorial-service transfers, Integrated Resource Plans (IRP), Universal
Service Fund disbursements, complaints and other proceedings across the
electric, natural-gas, telecommunications and transportation industries.

Each Order is an administrative adjudication of a specific docket by the
Commission → **case_law**.

## Source

- **Publisher:** Georgia Public Service Commission
- **Portal:** https://psc.ga.gov/facts-advanced-search/
- **API:** `https://services.psc.ga.gov/api/v1/External/Public` (public, no auth;
  Swagger at `/swagger/v1/swagger.json`)

## How it works

1. **Discovery + metadata:** `POST /Post/DocumentFilingsFilter` with
   `documentDescription="ORDER"` and a `filingDateFrom`/`filingDateTo` window
   returns document filings whose description contains "ORDER". Pagination is by
   `startIndex` (1-based offset) / `pageSize` against `totalCount`. Only
   Commission-issued Orders (companyName contains `GPSC`) are kept. Each row
   carries `documentId`, `docketId` (== the public GPSC docket number),
   `description` and `filedDate` (= the issue date).
2. **Attachment ids:** the server-rendered document detail page
   `/search/facts-document/?documentId={id}` lists the attachment download links
   (`.../Get/Document/DownloadFile/{documentId}/{attachmentId}`).
3. **Full text:** each attachment is downloaded and its text extracted. GPSC
   Orders are **born-digital** Microsoft Word (`.docx`) documents (and sometimes
   PDFs) with a real text layer — no OCR needed for the common case. Text from
   all attachments is de-duplicated (an Order is frequently attached as both a
   `.docx` and an identical `.pdf`).

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
python bootstrap.py bootstrap            # Full pull (all Orders)
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— Georgia Public Service Commission Orders are official U.S. state government
edicts, in the public domain. Commercial use permitted; no attribution required.
