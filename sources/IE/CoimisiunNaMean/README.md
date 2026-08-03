# IE/CoimisiunNaMean — Coimisiún na Meán (Ireland's media regulator)

Broadcasting & Video-on-Demand **complaint decisions** of **Coimisiún na Meán**,
the statutory media regulator of Ireland.

Coimisiún na Meán was established on 15 March 2023 under the Online Safety and
Media Regulation Act 2022, succeeding the Broadcasting Authority of Ireland
(BAI). Among its statutory functions it adjudicates complaints made by the
public about content on broadcasters and audiovisual on-demand media services
under the **Broadcasting Act 2009** (as amended) and the associated media
service codes/rules. The Commission is required to publish notice of each
complaint decision. Each published decision — the Commission's determination
and, where a complaint is referred for investigation, the Authorised Person's
decision — is a quasi-judicial adjudication (**case_law**).

## What this scraper collects

Every published complaint decision, with its full field set assembled into the
record's `text`:

- Case reference (`CAS-NNNNN`)
- Programme and broadcast/VOD service
- Date broadcast or accessed
- Statute and/or regulatory code invoked
- Short description of the complaint
- Outcome (Dismissed / Referred / Referred for investigation)
- Commission's decision and decision date
- Authorised Person's decision and date (for referred/investigated complaints)

The decisions are **born-digital HTML** — no OCR or PDF extraction is required.
This is the complete published decision as issued by the Commission.

**Corpus:** ~199 complaint decisions (2023-present plus carried-over
"transitional" complaints inherited from the BAI). Language: English.

## Access

Server-rendered, paginated listing (100 decision cards per page):

- Page 1: `https://www.cnam.ie/general-public/report-complain/something-i-saw-on-tv-on-demand-or-heard-on-radio/complaint-decisions/`
- Page N: `.../complaint-decisions/page/N/`

Each card is an `<h3>CAS-NNNNN</h3>` header followed by a `<dl class="row">` of
`<dt>` label / `<dd>` value pairs. The scraper paginates until a page yields no
cards, parses the field pairs, and builds the full decision text.

## Usage

```bash
python bootstrap.py test               # Connectivity + parse check
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # Full pull (streams to data/records.jsonl)
python bootstrap.py bootstrap-fast     # Runner alias for full pull
python bootstrap.py update             # Re-scan (loader dedups on _id)
```

## Historical BAI decisions

The Commission also republishes the Broadcasting Authority of Ireland's
2019–2022 complaint decisions as bundled yearly PDF compilations (linked from
the same page). Those are a separate, per-year PDF form and are not ingested by
this scraper, which targets the current structured decisions database.

## License

[PSI Licence / CC BY 4.0](https://www.gov.ie/en/help/re-use-of-public-sector-information/) — Irish public sector information re-use framework (default CC BY 4.0). Commercial use permitted; attribution required.
