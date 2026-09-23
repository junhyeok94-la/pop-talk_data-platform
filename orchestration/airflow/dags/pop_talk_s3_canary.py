"""Pop Talk S3 bucket과 데이터 prefix를 읽기 전용으로 점검한다.

객체를 생성·수정·삭제하지 않는다. 성공하면 bucket 접근 가능 여부와 prefix별 객체 수,
최대 5개의 표본 key를 XCom으로 남긴다.
"""
from __future__ import annotations

import pendulum

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task


AWS_CONN_ID = "pop_talk_aws"
BUCKET_NAME = "amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an"
PREFIXES = ("raw/", "manifests/", "exchange/")


@dag(
    dag_id="pop_talk_s3_canary",
    description="Read-only validation of the Pop Talk S3 connection and data prefixes",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 9, tz="UTC"),
    catchup=False,
    tags=["pop-talk", "aws", "s3", "canary"],
)
def s3_canary():
    """bucket 존재 확인 후 Raw/manifest/Exchange prefix를 관찰한다."""

    @task
    def check_bucket() -> str:
        """AWS Connection으로 지정 bucket을 읽을 수 있는지 확인한다."""
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        if not hook.check_for_bucket(BUCKET_NAME):
            raise RuntimeError(f"S3 bucket is not accessible: {BUCKET_NAME}")
        return BUCKET_NAME

    @task(multiple_outputs=False)
    def inspect_prefixes(bucket_name: str) -> dict[str, dict[str, object]]:
        """운영자가 구조를 확인할 수 있도록 prefix별 건수와 작은 표본만 반환한다."""
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        result: dict[str, dict[str, object]] = {}

        for prefix in PREFIXES:
            keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix) or []
            result[prefix] = {
                "object_count": len(keys),
                "sample_keys": keys[:5],
            }

        return result

    inspect_prefixes(check_bucket())


s3_canary()
