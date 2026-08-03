# INTL/ITIA — International Tennis Integrity Agency (ITIA) Sanctions & Decisions

Full-text published sanctions and disciplinary decisions of the **International
Tennis Integrity Agency (ITIA)**, the independent body responsible for
safeguarding the integrity of professional tennis worldwide. The ITIA
administers the **Tennis Anti-Corruption Program (TACP)** and the **Tennis
Anti-Doping Programme (TADP)**.

## Source

- **Listing:** https://www.itia.tennis/news/sanctions/
- **Platform:** Umbraco CMS, server-rendered HTML (no login, no WAF, reachable
  from any IP).
- **Coverage:** ~388 decision articles at `/news/sanctions/{slug}/`
  (reverse-chronological). Each article carries the full substantive findings of
  an Anti-Corruption Hearing Officer (AHO) / Independent Tribunal decision, an
  agreed sanction, or a provisional suspension — the breach(es), the sanction,
  the period of ineligibility, fines, and the reasoning summary. Tribunal cases
  additionally link the full **redacted decision PDF** under `/media/.../*.pdf`,
  which the scraper downloads and extracts in full.

## Strategy

1. Fetch the sanctions listing page and collect every `/news/sanctions/{slug}/`
   article URL.
2. For each article, fetch the detail page and extract the headline
   (`<h1 class="title">`), the body text (`<div class="article__inner">`), and
   the published date (`Published DD Month YYYY`).
3. If the body links a full decision PDF, download it and append the extracted
   text via `common/pdf_extract` (pdfplumber/pypdf fallback).

## Usage

```bash
python bootstrap.py test               # Print parsed listing entries
python bootstrap.py bootstrap --sample # Fetch 12 sample records
python bootstrap.py bootstrap          # Full pull (~388 articles)
```

## Data type

`case_law` — disciplinary tribunal decisions and agreed sanctions.

## License

> ⚠️ **Commercial use restricted.** The ITIA asserts copyright; no open licence
> is offered.

[ITIA Terms of Use](https://www.itia.tennis/) — decisions are published openly
for transparency, but all rights are reserved. Commercial use flagged restricted
per project policy (err on flagging). Attribution required.
