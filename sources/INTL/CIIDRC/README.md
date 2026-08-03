# INTL/CIIDRC — Canadian International Internet Dispute Resolution Centre

Full-text domain name dispute decisions of the **Canadian International Internet
Dispute Resolution Centre (CIIDRC)**, operated by the Vancouver International
Arbitration Centre (VanIAC, formerly the British Columbia International
Commercial Arbitration Centre). CIIDRC is an ICANN-accredited **UDRP** provider
for generic top-level domains and the **CIRA-approved CDRP** provider for `.ca`
Canadian domain name disputes.

- **Type:** case_law
- **Coverage:** ~660 decisions (2020–present), UDRP (gTLD) and CDRP (`.ca`)
- **Full text:** yes — each decision is a published PDF (typically 10K–50K chars)

## How it works

1. The index page
   `https://ciidrc.org/domain-name-disputes/ciidrc-decisions/` server-renders a
   DataTable of every decision: listing date, case-number link
   (`/my-portal/decisions/?casenumber={INTERNAL_ID}&action=view`), disputed
   domain(s) and status.
2. Each case **view page** exposes structured metadata (case number such as
   `27084-CDRP`, decision status, complainant, respondent, decision date,
   panelists) and a documents table linking the full-text decision PDF under
   `/wp-content/uploads/YYYY/MM/...-Decision.pdf`.
3. The scraper downloads each PDF and extracts the full text.

## Usage

```bash
python bootstrap.py test                 # connectivity test
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull -> data/records.jsonl
python bootstrap.py bootstrap-fast       # alias for full pull (pipeline)
```

## License

[Custom terms — publicly published decisions](https://ciidrc.org/domain-name-disputes/uniform-domain-name-dispute-resolution-policy/) — CIIDRC panel decisions are public records published openly on ciidrc.org pursuant to the UDRP/CDRP Rules (the UDRP requires that the full decision be published over the Internet, save for exceptional redactions). No explicit reuse licence; treated as publicly published decisions, consistent with the project's other domain-dispute sources (INTL/WIPO-UDRP, INTL/CAC-UDRP, INTL/Forum-DomainDecisions, INTL/ADNDRC). Commercial use: permitted.
