# BR/CARF — Brazilian Tax Appeals Council

**Source**: CARF (Conselho Administrativo de Recursos Fiscais)
**URL**: https://carf.fazenda.gov.br/
**Type**: Tax doctrine (administrative tribunal decisions)
**Volume**: 571,000+ acórdãos (decisions)
**Language**: Portuguese

## Data Access

Uses the public Apache Solr index at:
```
https://acordaos.economia.gov.br/solr/acordaos2/select
```

Standard Solr query parameters: `q`, `start`, `rows`, `fl`, `sort`, `fq`, `wt=json`.

No authentication required.

## Coverage

CARF is the top administrative tax appeals body in Brazil (Ministry of Finance).
Decisions cover all federal tax disputes including:
- IRPJ/IRPF (corporate/individual income tax)
- PIS/COFINS (social contributions)
- CSLL (social contribution on net profit)
- IPI (excise tax)
- Customs duties
- Social security contributions

Three sections with multiple chambers and panels.

## Full Text

Full text is in the `conteudo_txt` Solr field, extracted from PDFs via Apache Tika.
The field is prefixed with Tika metadata; actual content starts after `Conteúdo =>`.
Some PDFs have doubled-character artifacts which are cleaned by the bootstrap script.

## Usage

```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # Fetch 15 sample records
python bootstrap.py bootstrap          # Full pull (571K+ records)
python bootstrap.py update             # Recent 90 days
python bootstrap.py update --since 2025-01-01  # Since specific date
```

## License

[Open Government Data](https://carf.fazenda.gov.br/) — official tax tribunal decisions published by Brazil's Ministry of Finance.
