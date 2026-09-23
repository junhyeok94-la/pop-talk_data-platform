"""검증된 초기 Exchange를 일일 배치와 같은 STAGING 테이블에 멱등 적재한다."""
from pipelines.transforms.legacy_candidate_snowflake import read_legacy_candidate_exchange
from pipelines.transforms.snowflake_exchange_loader import DDL, ExchangeInput, load_exchange


def load_initial_snapshot(*, s3_client, snowflake_connection, bucket, published,
                          fail_after_movie_merge=False):
    """원문/lineage 검증 후 기존 loader를 재사용한다. 일일 데이터는 삭제하지 않는다."""
    source = read_legacy_candidate_exchange(s3_client, bucket=bucket, ready_key=published["ready_key"])
    expected = (source.source_run_id, source.source_sha256, source.artifact_version,
                source.publication_revision, len(source.movies), 0)
    actual = tuple(published[key] for key in ("source_run_id", "source_sha256", "artifact_version",
                                             "publication_revision", "movie_count", "boxoffice_count"))
    if actual != expected or not source.movies:
        raise RuntimeError("초기 Exchange 결과와 게시 metadata가 다릅니다")
    # 과거 원천의 미상 시각은 NULL 그대로 유지한다. DDL은 적재 트랜잭션 전에 끝낸다.
    with snowflake_connection.cursor() as cursor:
        for statement in (part.strip() for part in DDL.split(";") if part.strip()):
            cursor.execute(statement)
        cursor.execute("ALTER TABLE POP_TALK_DW_DEV.STAGING.MOVIE_OBSERVATIONS_RAW "
                       "ALTER COLUMN SOURCE_OBSERVED_AT DROP NOT NULL")
    exchange = ExchangeInput(source.ready_key, source.source_run_id, source.artifact_version,
                             source.publication_revision, source.bundle_sha256, source.movies, ())
    movie_count, boxoffice_count = load_exchange(snowflake_connection, exchange,
                                                fail_after_movie_merge=fail_after_movie_merge)
    return {"exchange_ready_key": source.ready_key, "source_run_id": source.source_run_id,
            "source_sha256": source.source_sha256, "artifact_version": source.artifact_version,
            "publication_revision": source.publication_revision,
            "movie_count": movie_count, "boxoffice_count": boxoffice_count}
