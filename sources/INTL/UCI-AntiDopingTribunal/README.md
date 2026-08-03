# INTL/UCI-AntiDopingTribunal — UCI Anti-Doping Tribunal Decisions & CAS Appeal Awards

Decisions of the **UCI Anti-Doping Tribunal** (Tribunal Antidopage de l'UCI), the
independent first-instance body that hears anti-doping rule violation cases in
international cycling under the UCI Anti-Doping Rules and the World Anti-Doping
Code, together with the **Court of Arbitration for Sport (CAS)** awards rendered on
appeal against those decisions. The UCI publishes both in full.

## Source

- **Listing:** https://www.uci.org/uci-anti-doping-tribunal/5JsEGc56ZHWXlcVkHh66d9
- **Type:** case_law
- **Auth:** none
- **Coverage:** 47 UCI Anti-Doping Tribunal first-instance judgments (2015–present)
  + 6 CAS appeal awards = 52 reasoned decisions.

## How it works

The "UCI Anti-Doping Tribunal" page is a Contentful-backed page whose rich-text
body — including a link to every decision PDF — is server-embedded in the HTML as
a unicode-escaped JSON island. `bootstrap.py`:

1. Fetches the listing page and unescapes the embedded rich text.
2. Parses every `<a href="//assets.ctfassets.net/.../<file>.pdf">Title</a>` anchor,
   filtering to `UCI ADT …` judgments and `CAS …` appeal awards (the procedural
   rules document is a regulation and is skipped).
3. Downloads each PDF and extracts full text via the shared OOM-hardened extractor.
4. Parses the decision date (English or French) from the judgment body.

Each PDF is hosted openly on Contentful's asset CDN (`assets.ctfassets.net`) — no
login, no WAF, reachable from any IP.

## Usage

```bash
python bootstrap.py test               # Print the parsed decision list
python bootstrap.py bootstrap --sample # Fetch ~12 sample records
python bootstrap.py bootstrap          # Full pull (52 decisions)
```

## License

> ⚠️ **Commercial use restricted.** See terms below.

[UCI Terms of Use (All Rights Reserved)](https://www.uci.org/disclaimer/6PpA8u6xkUyhTfRkjj9Xqm)
— UCI copyright. Decisions are published in full by the UCI for transparency under
the UCI Anti-Doping Rules; there is no open licence, so commercial use is
restricted. Attribution required.
