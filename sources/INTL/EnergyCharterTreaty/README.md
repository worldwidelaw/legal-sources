# INTL/EnergyCharterTreaty — Energy Charter Treaty Investment Arbitration Documents

Full-text legal documents published by the **Energy Charter Secretariat** for
investment-arbitration cases brought under **Article 26 of the Energy Charter
Treaty (ECT)**.

The Secretariat is aware of ~150 ECT investment-arbitration cases, but parties are
not obliged to notify it and most case documents sit at ICSID / PCA / SCC. For a
curated set of **landmark disputes** the Secretariat itself hosts, in full, the
core legal instruments:

- **Nykomb Synergetics v. Latvia** (SCC 118/2001)
- **Plama Consortium v. Bulgaria** (ICSID ARB/03/24)
- **Petrobart v. Kyrgyz Republic** (SCC 126/2003) — incl. Svea Court of Appeal judgment
- **Yukos Universal / Hulley Enterprises / Veteran Petroleum v. Russian Federation**
  (PCA AA 226/227/228) — incl. Hague District Court & Court of Appeal judgments
- **Kardassopoulos v. Georgia** (ICSID ARB/05/18)
- **Amto v. Ukraine** (SCC 080/2005)

Documents include arbitral awards, decisions on jurisdiction, provisional-measures
orders, and the national-court set-aside / enforcement judgments.

## Data access

- **Source page:** https://www.energychartertreaty.org/cases/list-of-cases/ (plain
  server HTML, no login, no WAF, reachable from any IP).
- The page links every document PDF under
  `/fileadmin/DocumentsMedia/Cases/<case>/…pdf`. The scraper parses these anchors,
  drops the per-case flowchart/summary PDFs, downloads each document and extracts
  its full text via the shared OOM-hardened PDF extractor.
- Per-case flowcharts and statistics charts are excluded (not legal documents).
- A small number of older scanned PDFs have no text layer (OCR not enabled) and are
  skipped.

## Usage

```bash
python bootstrap.py test               # list parsed document entries
python bootstrap.py bootstrap --sample # fetch sample records
python bootstrap.py bootstrap          # full pull (~25 documents)
```

## Output schema

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `title`, `text` (full
document text), `date`, `url`, `case_name`, `document_type`.

## License

> ⚠️ **Commercial use restricted.** The Secretariat's terms of use prohibit using
> the materials to promote any organisation, company, individual or commercial
> product/service, or in a way that suggests Energy Charter Secretariat endorsement.

[Energy Charter Secretariat Terms of Use](https://www.energychartertreaty.org/terms-of-use)
— reproduction authorised provided the source is acknowledged. © Energy Charter
Secretariat. The underlying arbitral awards and national-court judgments are
official legal/judicial documents.
