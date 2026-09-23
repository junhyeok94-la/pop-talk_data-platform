# Movie normalization

`movie_bronze_silver.py` validates preserved raw manifests and produces deterministic
Python movie and box-office rows. It does not access a remote compute service or database.

Raw bytes and checksums, source identity, incomplete coverage and ambiguous matches
remain explicit. PostgreSQL loading will be implemented separately in Phase 1.
The transformer also accepts an explicitly supplied legacy snapshot with a verified checksum.
