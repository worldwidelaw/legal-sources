# SE/SKV — Swedish Tax Agency (Skatteverket)

**Source:** [https://lagen.nu/dataset/myndfs?rpubl_forfattningssamling=skvfs](https://lagen.nu/dataset/myndfs?rpubl_forfattningssamling=skvfs)
**Data types:** doctrine

## Coverage

The Atom feed (`dcterms_publisher=publisher/skatteverket`) lists 200 entries —
170 SKVFS, 29 RSFS (the predecessor Riksskatteverket collection) and 1 TSFS.
159 of them carry body text; the other 41 (24 RSFS + 17 pre-2007 SKVFS) are
metadata-only stubs on lagen.nu with no document body upstream. Their `Källa`
links point at `www4.skatteverket.se`, which rejects our requests at the WAF,
so there is no fallback text path for those.

## License

[Public domain — Swedish Copyright Act § 9](https://lagen.nu/1960:729#P9) —
Swedish authority regulations carry no copyright. Only the authority text is
extracted; lagen.nu's own annotations and cross-reference lists are stripped.
