# INTL/FEI-Tribunal — FEI Tribunal Decisions (international equestrian sport)

Full-text decisions of the **FEI Tribunal**, the independent judicial body of the
**Fédération Équestre Internationale (FEI)**, the world governing body for
equestrian sport (jumping, dressage, eventing, driving, endurance, vaulting,
reining and para-equestrian).

The FEI Tribunal rules on:
- **Equine anti-doping** and controlled-medication / prohibited-substance cases
- **Human anti-doping** cases
- **Consent awards** (equine anti-doping)
- **Other disciplinary matters** — horse abuse, conduct cases, appeals against
  Ground Jury decisions, settlement agreements
- **CAS decisions** in FEI-related appeals

Each case is published openly (for transparency) as a full, redacted decision
PDF containing the substantive findings, the sanction, period of ineligibility,
fines and the full legal reasoning.

## Source

- Hub: <https://inside.fei.org/fei/your-role/athletes/fei-tribunal>
- Decisions are organised by category and year/year-range. Each year sub-page is
  a server-rendered HTML table whose rows link to the decision PDF in
  `/system/files/`.
- The decision date and case reference are encoded in each PDF filename
  (`YYYY.MM.DD_<...case ref...>.pdf`), which the scraper parses for metadata.

## Method

`bootstrap.py`:
1. Reads the FEI Tribunal hub, then each decision category landing page.
2. Discovers the year / year-range sub-pages for every category.
3. Parses each sub-page table for decision PDF links (~478 decisions).
4. Downloads each PDF and extracts the full text (opendataloader → pdfplumber →
   pypdf fallback chain).

```bash
python bootstrap.py test               # discover & list decision PDFs
python bootstrap.py bootstrap --sample # fetch a 12-record sample
python bootstrap.py bootstrap          # full pull
```

## Notes

- Openly published, no login, no API key. `inside.fei.org` WAF-rejects some
  datacenter IPs — build from a normal/residential IP.
- Cases involving minors are not published, per the FEI Anti-Doping and
  Controlled Medication Regulations.

## License

> ⚠️ **Commercial use restricted.** The FEI asserts copyright and does not
> publish these decisions under an open licence.

[FEI Terms of Use / Legal (All Rights Reserved)](https://inside.fei.org/fei/about-fei/legal) —
decisions are published openly for transparency but carry no open licence;
attribution to the FEI is expected. Commercial use is flagged restricted per
project policy (err on the side of flagging).
