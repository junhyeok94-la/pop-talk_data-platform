"""Read one completed S3 raw run and execute the pure transform without writes."""
from __future__ import annotations

import argparse
import json
import os
import sys


def load_json(client, bucket: str, key: str):
    return json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--ready-key")
    inputs.add_argument("--legacy-manifest-key")
    args = parser.parse_args()

    root = os.environ.get("POP_TALK_PROJECT_ROOT", "/opt/airflow/modules")
    if root not in sys.path:
        sys.path.insert(0, root)
    import boto3
    from airflow.models import Connection
    from airflow.utils.session import create_session
    from pipelines.transforms.movie_bronze_silver import transform_legacy_snapshot, transform_run

    with create_session() as session:
        connection = session.query(Connection).filter(Connection.conn_id == "pop_talk_aws").one()
        client = boto3.client(
            "s3",
            aws_access_key_id=connection.login,
            aws_secret_access_key=connection.password,
            region_name=connection.extra_dejson.get("region_name", "ap-northeast-2"),
        )
    if args.legacy_manifest_key:
        manifest = load_json(client, args.bucket, args.legacy_manifest_key)
        source_bytes = client.get_object(Bucket=args.bucket, Key=manifest["key"])["Body"].read()
        output = transform_legacy_snapshot(manifest, source_bytes)
        movies = output["silver_movies"]
        result = {
            **output["quality"],
            "service_movie_id_non_null_count": sum(row["service_movie_id"] is not None for row in movies),
            "missing_kofic_id_count": sum(not row["kofic_movie_cd"] for row in movies),
            "missing_title_count": sum(not row["title_ko"] for row in movies),
            "multi_genre_count": sum(len(row["genres"]) > 1 for row in movies),
            "multi_director_count": sum(len(row["directors"]) > 1 for row in movies),
            "multi_actor_count": sum(len(row["actors"]) > 1 for row in movies),
            "multi_company_count": sum(len(row["production_companies"]) > 1 for row in movies),
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return

    ready = load_json(client, args.bucket, args.ready_key)
    success = load_json(client, args.bucket, ready["raw_success_manifest"])
    stages = {name: load_json(client, args.bucket, key)
              for name, key in success["stage_manifests"].items()}
    references = {ref["key"]: ref for stage in stages.values()
                  for ref in stage.get("objects") or []}
    raw = {key: load_json(client, args.bucket, key) for key in sorted(references)}
    output = transform_run(ready, success, stages, raw)
    result = {
        "source_run_id": output["quality"]["source_run_id"],
        "bronze_object_count": len(output["bronze"]),
        "silver_movie_count": len(output["silver_movies"]),
        "silver_boxoffice_count": len(output["silver_boxoffice"]),
        "eligible_count": output["quality"]["eligible_count"],
        "excluded_count": output["quality"]["excluded_count"],
        "kmdb_matched_count": output["quality"]["kmdb_matched_count"],
        "kmdb_review_required_count": output["quality"]["kmdb_review_required_count"],
        "kmdb_truncated_count": output["quality"]["kmdb_truncated_count"],
        "kmdb_ambiguous_count": output["quality"]["kmdb_ambiguous_count"],
        "kmdb_provisional_count": output["quality"]["kmdb_provisional_count"],
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
