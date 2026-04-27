# BG/SupremeCourt - Bulgarian Supreme Court of Cassation

## Overview
- **Source ID:** BG/SupremeCourt
- **Country:** Bulgaria (BG)
- **Data Type:** Case Law
- **Language:** Bulgarian
- **Coverage:** 2008 onwards (full text decisions)

## Data Source
The scraper fetches case law from the **Supreme Court of Cassation of Bulgaria** (Върховен касационен съд - ВКС).

### Primary Endpoint
- **Domino Database:** `http://domino.vks.bg/bcap/scc/webdata.nsf/`
- Category-based browsing via expandable views
- Full HTML text available for each decision

### Alternative Endpoint
- **JSP System:** `https://www.vks.bg/pregled-akt.jsp`
- Newer system with documents accessible by ID
- ECLI-indexed decisions

## Document Types
- **Решение (Decision):** Final court rulings
- **Определение (Order):** Procedural orders and determinations  
- **Разпореждане (Directive):** Administrative directives
- **Тълкувателно решение (Interpretative Decision):** Binding interpretations

## Chambers
- **Civil Chamber** (Гражданска колегия)
- **Criminal Chamber** (Наказателна колегия)
- **Commercial Chamber** (Търговска колегия)

## Technical Notes
- **Encoding:** Legacy Domino uses Windows-1251; newer JSP system uses UTF-8
- **Rate Limiting:** 1 request/second to respect server load
- **Document Discovery:** Categories 1-18 in the Domino view contain all case law

## ECLI Format
Bulgarian Supreme Court of Cassation uses ECLI format:
```
ECLI:BG:SC001:YYYY:NNNNNNNNNNNN.NNN
```

## License

[Open Government Data](https://data.egov.bg/) — Bulgarian judicial decisions are publicly available for reuse.

## References
- [VKS Official Website](https://www.vks.bg)
- [Domino Case Law Database](http://domino.vks.bg/bcap/scc/webdata.nsf/)
- [National Case Law Portal (legalacts.justice.bg)](https://legalacts.justice.bg/)
