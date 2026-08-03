# INTL/AquaticsIntegrityUnit — Aquatics Integrity Unit (AQIU) Decisions

Full-text disciplinary and integrity decisions of the **Aquatics Integrity Unit
(AQIU)**, the independent body that, since 1 January 2023, handles disciplinary,
ethics, competition-manipulation, safe-sport and anti-doping matters for **World
Aquatics** (formerly FINA) across swimming, water polo, diving, artistic
swimming, open-water swimming and high diving.

> Not to be confused with the **Athletics** Integrity Unit
> (`INTL/AthleticsIntegrityUnit`).

## What it collects

Two complementary corpora, both `case_law`:

1. **AQIU Adjudicatory Body reasoned decisions** — full multi-page reasoned
   rulings (findings on liability, sanction, period of ineligibility and the
   full legal reasoning), including legacy FINA Doping Panel decisions.
   Published as PDFs on the [Suspended Persons](https://aquaticsintegrity.com/suspended-persons/)
   page. Rich full text (typically 15k–50k characters).

2. **AQIU published case outcomes** — the AQIU's official published account of
   each specific sanction (suspensions, anti-doping-rule-violation bans,
   whereabouts-failure bans, reprimands, water-polo match misconduct). Served
   via the site's WordPress REST API (`/wp-json/wp/v2/posts`) with full rendered
   body text. Pure news, anti-doping statistics roundups and administrative
   announcements (workshops, appointments, etc.) are filtered out.

Anti-doping matters are heard on the merits by the **CAS Anti-Doping Division**;
the AQIU publishes the resulting outcome here.

## Access

- No authentication, no WAF, reachable from any IP.
- Reasoned-decision PDFs are discovered by parsing the Suspended Persons page;
  case outcomes via the public WordPress REST API.
- A small number of legacy decision PDFs are image-only scans (no embedded text)
  and are skipped when OCR is unavailable.

## Usage

```bash
python bootstrap.py test               # List discovered PDFs + posts
python bootstrap.py bootstrap --sample # Fetch sample records
python bootstrap.py bootstrap          # Full pull
```

## License

> ⚠️ **Commercial use restricted.** See terms below.

[AQIU / World Aquatics Terms (All Rights Reserved)](https://aquaticsintegrity.com/) —
decisions are published openly for transparency, but World Aquatics / the AQIU
assert copyright with no open licence. Attribution required; commercial use
flagged restricted per project policy.
