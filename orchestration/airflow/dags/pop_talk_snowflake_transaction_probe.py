"""Snowflake 적재 DML이 실패 시 원자적으로 rollback되는지 수동 검증한다.

고정된 probe 행을 movie merge 직후 의도적으로 실패시키고, observation과 ledger가 모두
0건인지 조회한다. 성공 결과는 ``ROLLBACK_CONFIRMED``이며 업무 publication은 만들지 않는다.
"""
import os
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

DEFAULT_PROJECT_ROOT = "/opt/airflow/modules"
SNOWFLAKE_CONN_ID = "pop_talk_snowflake"


@dag(
    dag_id="pop_talk_snowflake_transaction_probe",
    description="의도적 적재 실패 후 Snowflake observation과 ledger rollback 검증",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 9, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["pop-talk", "snowflake", "test"],
)
def transaction_probe():
    """실패 주입과 사후 조회를 한 연결에서 수행해 rollback을 입증한다."""

    @task(multiple_outputs=False)
    def prove_rollback() -> dict[str, object]:
        """movie merge 뒤 실패시키고 관련 행이 한 건도 남지 않았는지 확인한다."""
        root = Path(os.environ.get("POP_TALK_PROJECT_ROOT", DEFAULT_PROJECT_ROOT))
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from pipelines.transforms.snowflake_exchange_loader import (
            ExchangeInput,
            SnowflakeLoadError,
            load_exchange,
        )

        probe_id = "rollback-probe-20260909-v1"
        artifact = "f" * 64
        row = {
            "source_run_id": probe_id,
            "transform_version": artifact,
            "ready_manifest_key": "probe/raw-ready",
            "source_observed_at": "2026-09-09T00:00:00Z",
            "canonical_movie_key": "probe-movie",
        }
        exchange = ExchangeInput(
            "probe/exchange-ready",
            probe_id,
            artifact,
            1,
            "e" * 64,
            (row,),
            (),
        )
        connection = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
        try:
            try:
                load_exchange(connection, exchange, fail_after_movie_merge=True)
            except SnowflakeLoadError as error:
                if "Injected failure" not in str(error):
                    raise
            else:
                raise RuntimeError("Rollback probe did not inject its expected failure")
            with connection.cursor() as cursor:
                cursor.execute("""SELECT
                  (SELECT COUNT(*) FROM POP_TALK_DW_DEV.STAGING.MOVIE_OBSERVATIONS_RAW
                   WHERE SOURCE_RUN_ID=%s AND ARTIFACT_VERSION=%s),
                  (SELECT COUNT(*) FROM POP_TALK_DW_DEV.STAGING.SILVER_EXCHANGE_LOADS
                   WHERE EXCHANGE_READY_KEY=%s)""", (
                    probe_id,
                    artifact,
                    exchange.ready_key,
                ))
                movie_count, ledger_count = cursor.fetchone()
            if (movie_count, ledger_count) != (0, 0):
                raise RuntimeError(
                    "Rollback probe에서 잔여 상태를 발견했습니다: "
                    f"movie={movie_count}, ledger={ledger_count}"
                )
            return {
                "status": "ROLLBACK_CONFIRMED",
                "movie_count": 0,
                "ledger_count": 0,
            }
        finally:
            connection.close()

    prove_rollback()


transaction_probe()
