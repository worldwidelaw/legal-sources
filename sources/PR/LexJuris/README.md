# PR/LexJuris — Puerto Rico Laws & Jurisprudence

Puerto Rico legislation from [LexJuris.com](https://www.lexjuris.com/), the primary free legal database for Puerto Rico.

## Coverage

- **Years:** 1997–2026 (ongoing)
- **Content:** All laws enacted by the Puerto Rico Legislature (~200+ per year)
- **Language:** Spanish
- **Format:** Full text HTML
- **Total:** ~5,000+ laws

## Data Access

- The master index (`lexleyes.htm`) links one menu page per year. The menu URL
  scheme changed several times (`ley1997/lex1997menu.htm`,
  `Leyes2001/lex2001menu.htm`, `Leyes2024/lexl2024Menu.htm`), so menus are
  discovered from the index rather than templated.
- Law filenames are `lex[l]{YY|YYYY}{NNN}.htm` (2-digit year before 2000).
- Individual law pages are Word exports; the body lives in the `Section1` /
  `Section2` / … divs (pre-2022) or `WordSection1` (2022+). **All** section
  divs must be concatenated — on multi-section pages the first one ends at the
  `DECRÉTASE` enacting formula and the articles follow in the next.
- **Charset:** pages before ~2022 are `windows-1252` (declared in a meta tag);
  2022+ pages are UTF-8 and declare nothing. Decode UTF-8 strictly first and
  fall back to cp1252 — forcing UTF-8 on the older bytes destroys every
  accented character (issue #1410).
- No authentication required
- No robots.txt restrictions
- 2-second crawl delay for politeness

## License

[Public domain (17 USC § 105)](https://www.law.cornell.edu/uscode/text/17/105) — Puerto Rico laws are works of the US government and are not subject to copyright.

Note: LexJuris adds its own copyright notice to their presentation/formatting, but the underlying legislative text is public domain.
