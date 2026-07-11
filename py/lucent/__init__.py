"""Lucent control plane package.

Modules land with their roadmap tasks:
  dev.py       (M0-T11)  process supervisor + control server
  embedsvc.py  (M0-T6)   gRPC embedding service
  ingest.py    (M0-T7)   corpus fetch, embed+cache, partition, project, load+seal
  bench.py     (M1-T4)   recall/latency harness against the brute-force oracle
  record.py    (M6-T2)   capture replay bundles from a live cluster
"""

__version__ = "0.0.1"
