# Movie Bronze/Silver transform contract

`movie_bronze_silver.py` is the deterministic policy core for the Databricks
step. It accepts JSON documents already loaded from S3 or a Databricks landing
volume and has no network, Airflow, Spark, or database side effects.

## Input gate

- Operational runs require `DAILY_READY.status=SUCCESS` and `sample=false`.
- `DAILY_READY`, raw `SUCCESS`, and every stage manifest must have the same
  `run_id` and `scope`.
- Every referenced raw envelope must be loaded, and its canonical payload
  SHA-256 must match both the envelope and manifest reference.
- Unexpected, missing, cross-run, and conflicting inputs fail closed.

## Output contract

- `bronze`: one immutable row per referenced raw object. The complete payload
  remains in `payload_json`; no source fields are discarded.
- `silver_movies`: one row per KOFIC detail, keyed by the external string ID.
  KMDb is matched only by normalized exact Korean/English title plus production
  year tolerance. Duplicate candidates are removed by KMDb identity. Truncated
  or equally ranked candidates are never confirmed; the best candidate is kept
  separately as provisional and the row is `REVIEW_REQUIRED`.
- `silver_boxoffice`: daily observations. `audience_accumulated` remains an
  accumulated value and must not be summed across dates.
- `quality`: input/output reconciliation and policy/matching counts.

Rows are not deleted by service eligibility policy. `policy_eligible`,
`policy_exclusion_reasons`, and `policy_version` preserve the decision. Version
1 applies release date 2020+, adult/erotic/documentary, and the legacy company
and director exclusions. Re-release and missing-release-date policy remains
explicitly conservative: missing dates are ineligible rather than guessed.

`content_sha256` uses an explicit business-field allowlist. It excludes
run/object trace metadata, ingestion source, service ID mapping, serving status
observations, eligibility policy, and matching diagnostics. Those change
independently through `policy_sha256`, `matching_sha256`, and
`embedding_input_sha256`. A new observation of unchanged business content
therefore does not trigger a false content update.

`transform_legacy_snapshot` validates the original JSON bytes and adapts the
initial 5,985-row `movies_final.json` snapshot to the same movie schema. The
legacy file has no service ID, so `service_movie_id` remains null; KOFIC IDs
remain strings and form a `kofic:<external-id>` canonical key. Pipe/comma
delimited people, company, country, genre, and keyword fields become arrays.
Service ID mapping must be resolved separately against the serving database.

Run the local contract tests:

```powershell
docker compose exec -T airflow-scheduler python -m unittest discover -s /opt/airflow/tests/pipelines/transforms -t /opt/airflow -v
```
