# UK/SocialWorkEngland — Social Work England Fitness-to-Practise Hearing Decisions

Full text of the fitness-to-practise hearing determinations of **Social Work
England (SWE)**, the specialist statutory regulator of the ~100,000 social
workers in England (established by the Children and Social Work Act 2017,
operational since December 2019, having taken over regulation from the HCPC).

SWE adjudicators sit in **final hearings**, **interim-order hearings** and
**substantive-order review hearings** held under the Social Workers Regulations
2018. Their final determinations set out the allegation, the panel's reasoned
findings of fact and impairment, and the sanction imposed (removal, suspension,
conditions of practice, warning). These are binding professional-regulator
**case law**, distinct from the other UK professional-regulator tribunals
already covered (UK/HCPTS, UK/GMC, UK/SDT, UK/BTAS).

## Data access

No authentication, no CAPTCHA, no JavaScript required.

- Each concluded hearing has a server-rendered detail page at
  `/umbraco/surface/hearingdetails/details/{id}` (integer hearing id). The page
  carries the registrant's name + registration number, the outcome, notes and
  (for upcoming hearings) the full allegations, plus a *Hearing details* block
  (type / date / location).
- A concluded hearing links its full written determination as one or more
  **Outcome documents** — born-digital PDFs served from
  `/umbraco/surface/hearingdetails/download?docid={docid}&hearingid={id}`. Final
  hearings run ~10–30 pages / 20k–40k characters of reasoned decision. No OCR.
- Old decisions are removed under SWE's publication policy, so the live corpus is
  a **rolling window** of recently published hearings; hearing ids are a sparse
  integer sequence. The scraper enumerates ids over a sliding window (from
  `MIN_ID` upward, the ceiling auto-extending past the last valid id) and skips
  the fixed *Page Not Found* pages.

Records are kept only when at least one Outcome-document PDF yields real text —
i.e. a concluded hearing with a published determination. Upcoming hearings that
carry only a charge sheet and no determination are skipped.

## Usage

```bash
python bootstrap.py test               # connectivity / parser check
python bootstrap.py bootstrap --sample # ~12 sample determinations
python bootstrap.py bootstrap          # full pull
python bootstrap.py bootstrap-fast     # full pull (runner alias)
python bootstrap.py update             # incremental (recent hearing ids)
```

## License

> ⚠️ **Commercial use restricted.** No explicit open licence; Crown copyright
> applies to the underlying records.

[Social Work England website terms and conditions](https://www.socialworkengland.org.uk/privacy/terms-and-conditions/)
— SWE fitness-to-practise decisions are public professional-regulator
adjudication records published under SWE's publication policy. Attribution
expected; commercial re-use conservatively flagged, consistent with the sibling
UK professional-regulator tribunal sources (UK/HCPTS, UK/GMC, UK/SDT, UK/BTAS).
