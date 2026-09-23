"""단일 DW DAG의 입력 고정, dbt 실행, PostgreSQL 게시. 실행 상태는 Airflow가 관리한다."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

from pipelines.orchestration.service_sync import sync_service
from pipelines.orchestration.dbt_deployment import validate_deployment
from pipelines.orchestration.dbt_relation_bindings import relation_parts
from pipelines.paths import DBT_DEPLOYMENT_ROOT

MODEL_GROUPS = {
    "staging": ("stg_successful_exchange_publications", "stg_movie_observations", "stg_boxoffice_observations"),
    "dw": ("dim_movie", "fct_boxoffice_daily"),
    "mart": ("mart_boxoffice_daily", "mart_data_quality"),
}
SOURCE_NAMES = ("movie_observations_raw", "boxoffice_observations_raw", "silver_exchange_loads")
ARTIFACT_ROOT = Path("/opt/airflow/dbt-attempt-artifacts/dw-pipeline")


def deployment_for(identity):
    """태스크 실행 중 current 포인터가 바뀌어도 DAG에 고정된 배포본을 사용한다."""
    return validate_deployment(DBT_DEPLOYMENT_ROOT, identity["deployment_id"],
                               expected_manifest_sha256=identity["manifest_sha256"])


def prepare_inputs(snowflake, identity, run_id):
    """동일 서버 시점의 source clone과 실행별 출력 이름만 XCom으로 전달한다."""
    deployment = deployment_for(identity)
    with snowflake.cursor() as cursor:
        cursor.execute("""SELECT CURRENT_TIMESTAMP(), EXCHANGE_READY_KEY FROM
            POP_TALK_DW_DEV.STAGING.SILVER_EXCHANGE_LOADS WHERE STATUS='SUCCESS'
            ORDER BY COMPLETED_AT DESC, EXCHANGE_READY_KEY DESC LIMIT 1""")
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("성공한 원천 적재가 필요합니다")
        cutoff, manifest_key = row
        # 이전 실행을 재시도해도 새로운 실행의 DW 테이블을 덮어쓰지 않는다.
        token = hashlib.sha256(f"{run_id}:{cutoff.isoformat()}".encode()).hexdigest()[:24].upper()
        bindings = {}
        cursor.execute("CREATE SCHEMA IF NOT EXISTS POP_TALK_DW_DEV.CANDIDATE")
        for name in SOURCE_NAMES:
            relation = f"POP_TALK_DW_DEV.CANDIDATE.INPUT_{token}_{name.upper()}"
            cursor.execute(f"CREATE TABLE {relation} CLONE POP_TALK_DW_DEV.STAGING.{name.upper()} "
                           "AT (TIMESTAMP => %s::TIMESTAMP_TZ)", (cutoff.isoformat(),))
            bindings[f"source.pop_talk_dw.pop_talk_staging.{name}"] = relation_parts(relation)
    for model_id in deployment.model_unique_ids:
        name = model_id.rsplit(".", 1)[1]
        bindings[model_id] = relation_parts(f"POP_TALK_DW_DEV.CANDIDATE.DW_{token}_{name.upper()}")
    return {"identity": identity, "token": token, "cutoff": cutoff.isoformat(),
            "manifest_key": manifest_key, "bindings": bindings}


def run_dbt(inputs, stage):
    """선택 모델을 독립 디렉터리에서 실행하고 실제 성공 결과를 검증한다."""
    deployment = deployment_for(inputs["identity"])
    selected = MODEL_GROUPS.get(stage, (stage,))
    if stage != "test" and not set(selected).issubset({n for names in MODEL_GROUPS.values() for n in names}):
        raise ValueError("알 수 없는 DW 단계")
    directory = ARTIFACT_ROOT / inputs["token"] / stage
    project = directory / "project"
    shutil.copytree(deployment.project_path, project, dirs_exist_ok=True)
    result_path = directory / "target" / "run_results.json"
    result_path.unlink(missing_ok=True)
    command = ["/opt/dbt-venv/bin/dbt", "test" if stage == "test" else "run",
               "--project-dir", str(project), "--profiles-dir", str(project),
               "--target-path", str(directory / "target"), "--log-path", str(directory / "logs"),
               "--no-partial-parse", "--vars", json.dumps({"pop_talk_relations": inputs["bindings"]})]
    if stage != "test":
        command += ["--select", *selected]
    # Airflow 전용 PYTHONPATH가 dbt 플러그인 탐색에 섞이지 않도록 격리한다.
    environment = {k: v for k, v in os.environ.items() if k != "PYTHONPATH" and not k.startswith("DBT_")}
    with (directory / "console.log").open("w", encoding="utf-8") as log:
        result = subprocess.run(command, env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=1800)
    if result.returncode:
        raise RuntimeError(f"dbt {stage} 실패: {directory / 'console.log'}")
    results = json.loads(result_path.read_text(encoding="utf-8"))["results"]
    expected = set(deployment.test_unique_ids if stage == "test" else
                   (f"model.pop_talk_dw.{name}" for name in selected))
    status = "pass" if stage == "test" else "success"
    if {r["unique_id"] for r in results} != expected or any(r["status"] != status for r in results):
        raise RuntimeError(f"dbt {stage} 실행 결과가 예상 모델/검사와 다릅니다")
    return {"stage": stage, "passed": len(results)}


def publish_serving(inputs, snowflake, postgres, attempt_id):
    """검사 완료한 영화·흥행을 dev 서비스 테이블에 원자적으로 반영한다."""
    deployment = deployment_for(inputs["identity"])
    rows = []
    with snowflake.cursor() as cursor:
        for name in ("dim_movie", "fct_boxoffice_daily", "mart_data_quality"):
            parts = inputs["bindings"][f"model.pop_talk_dw.{name}"]
            relation = ".".join(parts[key] for key in ("database", "schema", "identifier"))
            relation_parts(relation)
            cursor.execute(f"SELECT * FROM {relation}")
            names = [column[0] for column in cursor.description]
            rows.append([dict(zip(names, row)) for row in cursor.fetchall()])
    movies, facts, quality = rows
    quality.sort(key=lambda row: row["EXCHANGE_READY_KEY"])
    if not quality or any(row["MOVIE_COUNT_DIFFERENCE"] or row["BOXOFFICE_COUNT_DIFFERENCE"] for row in quality):
        raise RuntimeError("DW 품질 대사가 성공하지 않았습니다")
    return sync_service(postgres, movies, facts, cutoff=inputs["cutoff"],
                        run_id=attempt_id, model_version=deployment.model_version)
