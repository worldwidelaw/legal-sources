# US/IN-CADDNAR — Indiana Natural Resources Commission (CADDNAR Decisions)

Full-text administrative decisions of the **Indiana Natural Resources
Commission (NRC)**, collected in **CADDNAR** ("Cite As Decisions of the
Department of Natural Resources / Natural Resources Commission"), the
NRC's official reporter. An Administrative Law Judge of the **Office of
Administrative Law Proceedings (OALP)** hears an administrative appeal of
a Department of Natural Resources action and issues Findings of Fact,
Conclusions of Law and a Final Order resolving that specific contested
case — each is **case_law**.

Typical subject matter: floodway / construction-in-a-floodway permits
under the Flood Control Act, surface-coal-mining reclamation,
"public freshwater lake" disputes, oil & gas, fish & wildlife, boating,
state-park and cemetery matters.

- **Publisher:** Indiana Office of Administrative Law Proceedings (OALP) / Natural Resources Commission
- **Coverage:** CADDNAR Volumes 1–17, 1977–present (~751 decisions)
- **Type:** `case_law`
- **Jurisdiction:** US-IN (Indiana)

Sibling to **US/IN-OEA** (Indiana Office of Environmental Adjudication /
IDEM final orders) — same publisher (OALP) but a distinct tribunal and
corpus. This source is the "FOLLOW-UP: sibling DNR/CADDNAR" flagged in
the IN-OEA note.

## Access

No JavaScript, no CAPTCHA, no auth; `in.gov` is reachable from datacenter
IPs.

1. The **CADDNAR Citation Index** PDF enumerates every decision as a
   table row (`CAUSE#  CAPTION  YEAR  ALJ  VOLUME  CITE  LAST-PG`):
   `https://www.in.gov/nrc/files/caddnar_index.pdf`
   (~751 rows, Volumes 1–17; born-digital text-layer PDF).
2. Each decision's full text is a standalone born-digital HTML document
   (a Word-to-HTML export) at
   `https://www.in.gov/oalp/files/decisions/dnr/{cause}.v{vol}.html`
   where `{cause}` is the lower-cased cause number (e.g. `77-003w`) and
   `{vol}` the CADDNAR volume number. Some files are served as `.htm`
   (Apache content negotiation returns HTTP 300 for the `.html` name);
   `.htm` variants are Windows-1252 encoded.

### Strategy

1. Download + extract the Citation Index PDF; parse every row into
   `{cause, caption, year, alj, volume}`. Dedup by `(cause, volume)`.
2. For each: fetch `{cause}.v{vol}.html`, falling back to `.htm` on a
   non-200 (content-negotiation 300); decode `utf-8` → `cp1252`.
3. Strip HTML, slice from the `CADDNAR [CITE:` citation marker (drops the
   Word-export preamble), parse the citation + decision date from the
   body, and normalize into the `case_law` schema.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (~751 decisions)
python bootstrap.py bootstrap --sample   # Fetch ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity test
```

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Indiana Natural Resources Commission (CADDNAR) are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
