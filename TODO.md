( 1. data page skipping -> Unimplemented )
DONE 1.5 Bloom filter -> Unimplememted -> own implementation 
DONE 2. row filtering

( 3. aggregation )
(4. complex aggregation parsing)
5. testing

(6. connection handoff)
(7. tcp instead of http internally)

8. integration

9. Snowset dataset
10. thesios dataset
11. public bi dataset

12. benchmarking




#### Backlog
Compression & Encoding & Dictionary


# Memory Rechnung
- Bloom Filter, min max auf bytes
- 0.5 GB auf 1 TB an Memory verfügbar

# Benchmarking

- how many

- Disk Bandwith ausnutzen!
- extrem Queries: worst (expensive predicate, nix rausfiltert), best (alles rausfiltert, einfaches predicate)



## Optimization
- Metadata: Arc statt Clone
