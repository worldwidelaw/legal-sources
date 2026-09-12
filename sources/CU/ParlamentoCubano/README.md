# CU/ParlamentoCubano

Cuba National Assembly — Laws & Decree-Laws from the Asamblea Nacional del Poder Popular.

Fetches enacted legislation from the "Labor legislativa" section, including the Constitution,
laws (leyes), and decree-laws (decretos-ley) approved since the 2019 Constitution.

51 enacted laws with full text via PDF extraction, dating back to the 2019
Electoral Act.

The site links the Gaceta Oficial issue the law was published in, not the law
on its own, so an issue that promulgated several laws is linked once per law
and each of those links returns the whole issue. Records are therefore cut down
to the one document the link names, using the `GOC-YYYY-NNN-OXX` code the
gazette prints in its summary and again where each document's body starts. The
`gazette_code` field records which document a record was cut to; it is null for
issues that only carry one.

```
python bootstrap.py bootstrap          # Full pull -> data/records.jsonl
python bootstrap.py bootstrap --sample # 15 sample records -> sample/
python bootstrap.py bootstrap-fast     # Alias for the full bootstrap (fleet entry point)
python bootstrap.py test               # Connectivity check
```

## License

[Open Government Data (Cuba)](https://www.parlamentocubano.gob.cu/) — official public legislation texts from Cuba's National Assembly.
