#!/usr/bin/env python3
"""신규 모델 단위 DAG 전용 exact dbt 실행기.

기존 전체 DAG는 ``pop-talk-dbt-legacy``를 계속 사용한다. 이 entrypoint는 JSON contract와
PostgreSQL reservation이 없으면 dbt를 시작하지 않는다.
"""
from __future__ import annotations

import hashlib
import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable, Mapping

import psycopg

DEFAULT_PROJECT_ROOT = "/opt/airflow/modules"
DEFAULT_ARTIFACT_ROOT = "/opt/airflow/dbt-attempt-artifacts"
REAL_DBT_EXECUTABLE = "/opt/dbt-venv/bin/dbt"
CONTRACT_ENV = "POP_TALK_DBT_STRICT_CONTRACT"

SAFE_INHERITED_ENV = {
    "HOME",
    "LANG",
    "LC_ALL",
    "PATH",
    "PYTHONIOENCODING",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
}
SAFE_AUTH_ENV = {
    "SNOWFLAKE_AUTHENTICATOR",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
    "SNOWFLAKE_TOKEN",
}
FORBIDDEN_OPTIONS = {
    "--defer",
    "--defer-state",
    "--exclude",
    "--favor-state",
    "--indirect-selection",
    "--log-format-file",
    "--log-level-file",
    "--log-path",
    "--no-defer",
    "--partial-parse",
    "--selector",
    "--state",
    "--target-path",
    "--vars",
    "--write-json",
}


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"필수 strict dbt 환경변수가 없습니다: {name}")
    return value


VALUE_OPTIONS = {"--select", "--project-dir", "--profiles-dir", "--profile", "--target"}
FLAG_OPTIONS = {"--no-partial-parse"}


def _parse_allowlisted_arguments(arguments: list[str]) -> tuple[str, dict[str, list[str]], set[str]]:
    """Cosmos argv 전체를 소비한다. 알 수 없는 option과 남는 bare token은 즉시 거부한다."""
    command: str | None = None
    options: dict[str, list[str]] = {}
    flags: set[str] = set()
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument in {"run", "test"}:
            if command is not None:
                raise RuntimeError("dbt command는 정확히 하나만 필요합니다")
            command = argument
            index += 1
            continue
        name, separator, inline = argument.partition("=")
        if name in FLAG_OPTIONS:
            if separator or name in flags:
                raise RuntimeError(f"dbt flag가 중복됐거나 값을 가집니다: {name}")
            flags.add(name)
            index += 1
            continue
        if name not in VALUE_OPTIONS:
            raise RuntimeError(f"허용하지 않는 dbt 인자입니다: {argument}")
        if name in options:
            raise RuntimeError(f"dbt 인자 {name}은 정확히 한 번만 허용됩니다")
        values: list[str] = []
        if separator:
            if not inline:
                raise RuntimeError(f"dbt 인자에 {name} 값이 없습니다")
            values.extend(shlex.split(inline) if name == "--select" else [inline])
            index += 1
        else:
            index += 1
            while index < len(arguments) and not arguments[index].startswith("-"):
                values.extend(
                    shlex.split(arguments[index]) if name == "--select" else [arguments[index]]
                )
                index += 1
                if name != "--select":
                    break
        if not values:
            raise RuntimeError(f"dbt 인자에 {name} 값이 없습니다")
        if name != "--select" and len(values) != 1:
            raise RuntimeError(f"dbt 인자 {name} 값은 하나여야 합니다")
        options[name] = values
    if command is None:
        raise RuntimeError("dbt command는 정확히 하나만 필요합니다")
    return command, options, flags


def prepare_strict_argv(
    arguments: list[str],
    *,
    contract: Any,
    deployment: Any,
    project_directory: Path,
) -> list[str]:
    """Cosmos command를 exact contract와 대사하고 고정 deployment copy를 사용한다."""
    expected_command = "run" if contract.invocation_kind == "MODEL" else "test"
    if contract.invocation_kind == "EMPTY_TEST_SET":
        if arguments:
            raise RuntimeError("EMPTY_TEST_SET은 dbt argv를 가질 수 없습니다")
        return []
    command, options, flags = _parse_allowlisted_arguments(arguments)
    if command != expected_command:
        raise RuntimeError("dbt command가 invocation kind와 다릅니다")
    required = VALUE_OPTIONS
    if set(options) != required:
        raise RuntimeError("strict dbt 필수 인자 집합이 정확하지 않습니다")
    selectors = tuple(options["--select"])
    if selectors != contract.expected_selectors:
        raise RuntimeError("dbt selector가 exact contract와 다릅니다")
    if any(any(token in selector for token in ("+", "*", "@", "tag:", "path:")) for selector in selectors):
        raise RuntimeError("graph/wildcard selector는 strict 실행에서 금지됩니다")
    profile = options["--profile"][0]
    target = options["--target"][0]
    if profile != deployment.profile_name or target != deployment.target_name:
        raise RuntimeError("dbt profile/target이 deployment와 다릅니다")
    if project_directory.exists():
        raise RuntimeError("strict invocation project 디렉터리가 이미 존재합니다")
    project_directory.mkdir(parents=True)
    shutil.copytree(deployment.project_path, project_directory, dirs_exist_ok=True)
    fixed = str(project_directory.resolve())
    relation_vars = json.loads(contract.relation_vars_json)
    if relation_vars:
        from pipelines.orchestration.dbt_relation_bindings import relation_parts

        if set(relation_vars) != {"pop_talk_relations"}:
            raise RuntimeError("지원하지 않는 relation vars입니다")
        bindings = relation_vars["pop_talk_relations"]
        if contract.model_unique_id not in bindings:
            raise RuntimeError("실행 모델의 출력 relation이 없습니다")
        for bound in bindings.values():
            if set(bound) != {"database", "schema", "identifier"} or relation_parts(
                ".".join(bound[key] for key in ("database", "schema", "identifier"))
            ) != bound:
                raise RuntimeError("잘못된 relation binding입니다")
    prepared = [command]
    if "--no-partial-parse" in flags:
        prepared.append("--no-partial-parse")
    prepared.extend(["--select", *selectors])
    prepared.extend(
        ["--project-dir", fixed, "--profiles-dir", fixed,
         "--profile", profile, "--target", target]
    )
    if relation_vars:
        # Cosmos의 vars는 허용하지 않는다. 원장과 대사한 계약 값만 dbt에 전달한다.
        prepared.extend(["--vars", contract.relation_vars_json])
    return prepared


def sanitized_dbt_environment(
    source: Mapping[str, str], *, target_path: Path, log_path: Path
) -> dict[str, str]:
    """인증·OS 최소값만 보존하고 dbt 실행 의미는 wrapper 값으로 고정한다."""
    result = {
        key: value
        for key, value in source.items()
        if key in SAFE_INHERITED_ENV or key in SAFE_AUTH_ENV
    }
    result.update(
        {
            "DBT_DEFER": "false",
            "DBT_INDIRECT_SELECTION": "empty",
            "DBT_LOG_FORMAT_FILE": "json",
            "DBT_LOG_LEVEL_FILE": "debug",
            "DBT_LOG_PATH": str(log_path),
            "DBT_PARTIAL_PARSE": "false",
            "DBT_TARGET_PATH": str(target_path),
            "DBT_WRITE_JSON": "true",
        }
    )
    return result


def _assert_no_symlink_components(path: Path, root: Path) -> None:
    resolved_root = root.resolve(strict=True)
    current = root
    if current.is_symlink():
        raise RuntimeError("artifact root symlink는 허용되지 않습니다")
    for part in path.relative_to(root).parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise RuntimeError("artifact 경로에 symlink가 있습니다")
    resolved = path.resolve(strict=False)
    if resolved != resolved_root and resolved_root not in resolved.parents:
        raise RuntimeError("artifact 경로가 허용 root를 벗어났습니다")


def invocation_directory(contract: Any) -> Path:
    configured_root = Path(contract.artifact_root)
    allowed_root = Path(os.environ.get("POP_TALK_DBT_ARTIFACT_ROOT", DEFAULT_ARTIFACT_ROOT))
    if configured_root.resolve(strict=True) != allowed_root.resolve(strict=True):
        raise RuntimeError("contract artifact root가 고정 allowlist와 다릅니다")
    path = configured_root.joinpath(
        contract.build_id,
        str(contract.attempt_no),
        str(contract.fence_token),
        contract.invocation_kind.lower(),
        contract.orchestration_invocation_id,
    )
    _assert_no_symlink_components(path, configured_root)
    return path


def _digest_json(value: Any) -> str:
    from pipelines.orchestration.model_generation_contract import canonical_json

    return hashlib.sha256(canonical_json(value)).hexdigest()


def environment_receipt_digest(environment: Mapping[str, str]) -> str:
    """실행 의미는 봉인하되 인증 비밀 자체는 digest 입력에도 넣지 않는다."""
    safe_view = {
        key: ("<present>" if key in SAFE_AUTH_ENV else value)
        for key, value in environment.items()
    }
    return _digest_json(safe_view)


def seal_receipt(path: Path, body: Mapping[str, Any]) -> None:
    """같은 filesystem에서 exclusive temp + fsync + rename으로 receipt를 봉인한다."""
    if path.exists():
        raise RuntimeError("기존 invocation receipt를 덮어쓸 수 없습니다")
    temporary = path.with_name(path.name + ".tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
    try:
        raw = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        os.write(descriptor, raw)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def run_supervised(
    command: list[str],
    *,
    environment: Mapping[str, str],
    heartbeat: Callable[[], float],
    heartbeat_interval_seconds: float,
    lease_deadline_seconds: float,
    grace_seconds: float,
    popen_factory: Callable[..., subprocess.Popen[Any]] = subprocess.Popen,
) -> int:
    """dbt child를 poll하며 lease와 Airflow 취소 signal을 함께 감독한다."""
    if not (0 < heartbeat_interval_seconds < lease_deadline_seconds) or grace_seconds < 0:
        raise ValueError("heartbeat interval/deadline/grace 설정이 올바르지 않습니다")
    process: subprocess.Popen[Any] | None = None
    cancelled = threading.Event()
    heartbeat_results: Queue[tuple[float | None, BaseException | None]] = Queue()
    heartbeat_pending = False
    next_heartbeat = time.monotonic() + heartbeat_interval_seconds
    lease_deadline = time.monotonic() + lease_deadline_seconds

    def forward(signum: int, _frame: Any) -> None:
        cancelled.set()
        if process is not None and process.poll() is None:
            process.send_signal(signum)

    previous: dict[int, Any] = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, forward)
    try:
        process = popen_factory(command, env=dict(environment))
        if cancelled.is_set() and process.poll() is None:
            process.terminate()
        failure: BaseException | None = None
        while process.poll() is None or heartbeat_pending:
            now = time.monotonic()
            if cancelled.is_set():
                failure = RuntimeError("dbt task 취소 signal을 받았습니다")
                break
            if now >= lease_deadline:
                failure = RuntimeError("마지막 성공 heartbeat의 안전 deadline을 넘었습니다")
                break
            if process.poll() is None and not heartbeat_pending and now >= next_heartbeat:
                heartbeat_pending = True

                def invoke_heartbeat() -> None:
                    try:
                        heartbeat_results.put((float(heartbeat()), None))
                    except BaseException as error:
                        heartbeat_results.put((None, error))

                threading.Thread(target=invoke_heartbeat, daemon=True).start()
            try:
                remaining, heartbeat_error = heartbeat_results.get_nowait()
            except Empty:
                pass
            else:
                heartbeat_pending = False
                if heartbeat_error is not None:
                    failure = heartbeat_error
                    break
                if remaining is None or remaining <= 0:
                    failure = RuntimeError("heartbeat가 유효한 남은 lease를 반환하지 않았습니다")
                    break
                completed_at = time.monotonic()
                lease_deadline = completed_at + remaining
                next_heartbeat = completed_at + heartbeat_interval_seconds
            time.sleep(min(0.1, heartbeat_interval_seconds))
        if cancelled.is_set() and failure is None:
            failure = RuntimeError("dbt task 취소 signal을 받았습니다")
        if failure is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=grace_seconds)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            raise RuntimeError("dbt supervisor가 실행을 중단했습니다") from failure
        exit_code = int(process.wait())
        if cancelled.is_set():
            raise RuntimeError("dbt task 취소 signal을 받았습니다")
        return exit_code
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def _connect_control() -> psycopg.Connection:
    return psycopg.connect(
        host=_required_environment("POP_TALK_CONTROL_POSTGRES_HOST"),
        port=int(os.environ.get("POP_TALK_CONTROL_POSTGRES_PORT", "5432")),
        dbname=_required_environment("POP_TALK_CONTROL_POSTGRES_DB"),
        user=_required_environment("POP_TALK_CONTROL_POSTGRES_USER"),
        password=_required_environment("POP_TALK_CONTROL_POSTGRES_PASSWORD"),
        connect_timeout=5,
        options="-c statement_timeout=5000 -c lock_timeout=3000",
    )


def _load_sealed_artifact(path: Path) -> Any:
    from pipelines.orchestration.dbt_invocation_contract import (
        DbtInvocationArtifact,
        read_regular_file_nofollow,
    )

    body = json.loads(read_regular_file_nofollow(path).decode("utf-8"))
    body["expected_unique_ids"] = tuple(body["expected_unique_ids"])
    body["executed_unique_ids"] = tuple(body["executed_unique_ids"])
    return DbtInvocationArtifact(**body)


def validate_recovery_artifact(
    *, contract: Any, artifact: Any, directory: Path, manifest: Mapping[str, Any]
) -> None:
    """FILE_SEALED 복구 전에 receipt와 원본 dbt 증거를 다시 대사한다."""
    from pipelines.orchestration.dbt_invocation_contract import validate_run_results

    if (
        artifact.orchestration_invocation_id != contract.orchestration_invocation_id
        or artifact.build_id != contract.build_id
        or artifact.attempt_no != contract.attempt_no
        or artifact.fence_token != contract.fence_token
        or artifact.cohort_manifest_id != contract.cohort_manifest_id
        or artifact.deployment_id != contract.deployment_id
        or artifact.model_unique_id != contract.model_unique_id
        or artifact.invocation_kind != contract.invocation_kind
        or artifact.expected_unique_ids != contract.expected_unique_ids
        or artifact.artifact_path != str(directory)
    ):
        raise RuntimeError("복구 artifact identity가 exact contract와 다릅니다")
    if contract.invocation_kind == "EMPTY_TEST_SET":
        if (
            artifact.status != "EMPTY_TEST_SET"
            or artifact.dbt_native_invocation_id is not None
            or artifact.run_results_sha256 is not None
            or artifact.executed_unique_ids
        ):
            raise RuntimeError("EMPTY_TEST_SET 복구 artifact가 올바르지 않습니다")
        return
    native_id, executed_ids, digest = validate_run_results(
        contract=contract,
        run_results_path=directory / "target" / "run_results.json",
        log_path=directory / "logs" / "dbt.log",
        manifest=manifest,
    )
    if (
        artifact.status != "SUCCEEDED"
        or artifact.dbt_native_invocation_id != native_id
        or artifact.executed_unique_ids != executed_ids
        or artifact.run_results_sha256 != digest
    ):
        raise RuntimeError("복구 receipt와 dbt 원본 증거가 다릅니다")


def main() -> None:
    project_root = Path(os.environ.get("POP_TALK_PROJECT_ROOT", DEFAULT_PROJECT_ROOT))
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from pipelines.paths import DBT_DEPLOYMENT_ROOT

    from pipelines.orchestration.dbt_deployment import validate_deployment
    from pipelines.orchestration.dbt_invocation_contract import (
        DbtInvocationArtifact,
        contract_from_json,
        validate_run_results,
    )
    from pipelines.orchestration.postgres_control_store import PostgresControlStore

    contract = contract_from_json(_required_environment(CONTRACT_ENV))
    deployment = validate_deployment(
        DBT_DEPLOYMENT_ROOT,
        contract.deployment_id,
        expected_manifest_sha256=contract.manifest_sha256,
        expected_model_version=contract.model_version,
    )
    manifest = json.loads(deployment.manifest_path.read_text(encoding="utf-8"))
    directory = invocation_directory(contract)
    receipt_path = directory / "invocation_receipt.json"
    with _connect_control() as connection:
        store = PostgresControlStore(connection)
        reservation, owns_execution = store.reserve_dbt_invocation(contract)
        if not owns_execution:
            if reservation.state == "COMPLETED":
                if not receipt_path.is_file():
                    store.mark_dbt_artifact_missing(contract)
                    raise RuntimeError("DB 완료 증거의 artifact 파일이 없어 복구가 필요합니다")
                file_artifact = _load_sealed_artifact(receipt_path)
                database_artifact = store.load_dbt_invocation_artifact(contract)
                if database_artifact is None or file_artifact != database_artifact:
                    raise RuntimeError("DB 완료 증거와 봉인 artifact 파일이 다릅니다")
                return
            if reservation.state in {"RESERVED", "RUNNING", "FILE_SEALED"} and receipt_path.is_file():
                artifact = _load_sealed_artifact(receipt_path)
                validate_recovery_artifact(
                    contract=contract,
                    artifact=artifact,
                    directory=directory,
                    manifest=manifest,
                )
                if reservation.state != "FILE_SEALED":
                    store.set_dbt_invocation_state(
                        contract,
                        expected_state=reservation.state,
                        new_state="FILE_SEALED",
                        artifact_path=str(directory),
                        dbt_native_invocation_id=artifact.dbt_native_invocation_id,
                    )
                store.record_dbt_invocation_artifact(contract, artifact)
                return
            raise RuntimeError("동일 invocation은 이미 실행 중이거나 재실행할 수 없습니다")

        local_state = "RESERVED"
        try:
            directory.mkdir(parents=True, exist_ok=False)
            target_path = directory / "target"
            log_path = directory / "logs"
            project_path = directory / "project"
            target_path.mkdir()
            log_path.mkdir()
            arguments = prepare_strict_argv(
                list(sys.argv[1:]),
                contract=contract,
                deployment=deployment,
                project_directory=project_path,
            )
            environment = sanitized_dbt_environment(os.environ, target_path=target_path, log_path=log_path)
            argv = [REAL_DBT_EXECUTABLE, *arguments]
            argv_sha256 = _digest_json(argv)
            environment_sha256 = environment_receipt_digest(environment)
            if contract.invocation_kind == "EMPTY_TEST_SET":
                artifact = DbtInvocationArtifact(
                    contract.orchestration_invocation_id, None, contract.build_id,
                    contract.attempt_no, contract.fence_token, contract.cohort_manifest_id,
                    contract.deployment_id, contract.model_unique_id, contract.invocation_kind,
                    (), (), None, argv_sha256, environment_sha256, str(directory), "EMPTY_TEST_SET",
                )
            else:
                store.set_dbt_invocation_state(contract, expected_state="RESERVED", new_state="RUNNING")
                local_state = "RUNNING"

                lease_safety_seconds = float(
                    os.environ.get("POP_TALK_DBT_LEASE_SAFETY_SECONDS", "15")
                )

                def remaining_seconds(expires_at: datetime) -> float:
                    return (expires_at - datetime.now(timezone.utc)).total_seconds() - lease_safety_seconds

                initial_remaining = remaining_seconds(
                    store.current_dbt_invocation_lease_expires_at(contract)
                )
                if initial_remaining <= 0:
                    raise RuntimeError("dbt 시작 전 claim lease 안전 시간이 부족합니다")

                def heartbeat() -> float:
                    with _connect_control() as heartbeat_connection:
                        expires_at = PostgresControlStore(heartbeat_connection).heartbeat_dbt_invocation(
                            contract, lease_seconds=int(os.environ.get("POP_TALK_DBT_LEASE_SECONDS", "300"))
                        )
                    return remaining_seconds(expires_at)

                exit_code = run_supervised(
                    argv,
                    environment=environment,
                    heartbeat=heartbeat,
                    heartbeat_interval_seconds=float(os.environ.get("POP_TALK_DBT_HEARTBEAT_SECONDS", "30")),
                    lease_deadline_seconds=initial_remaining,
                    grace_seconds=float(os.environ.get("POP_TALK_DBT_TERMINATION_GRACE_SECONDS", "15")),
                )
                if exit_code != 0:
                    raise RuntimeError(f"dbt가 실패했습니다: exit code {exit_code}")
                native_id, executed_ids, run_results_sha256 = validate_run_results(
                    contract=contract,
                    run_results_path=target_path / "run_results.json",
                    log_path=log_path / "dbt.log",
                    manifest=manifest,
                )
                artifact = DbtInvocationArtifact(
                    contract.orchestration_invocation_id, native_id, contract.build_id,
                    contract.attempt_no, contract.fence_token, contract.cohort_manifest_id,
                    contract.deployment_id, contract.model_unique_id, contract.invocation_kind,
                    contract.expected_unique_ids, executed_ids, run_results_sha256,
                    argv_sha256, environment_sha256, str(directory), "SUCCEEDED",
                )
            seal_receipt(receipt_path, artifact.body)
            store.set_dbt_invocation_state(
                contract,
                expected_state=local_state,
                new_state="FILE_SEALED",
                artifact_path=str(directory),
                dbt_native_invocation_id=artifact.dbt_native_invocation_id,
            )
            local_state = "FILE_SEALED"
            store.record_dbt_invocation_artifact(contract, artifact)
            local_state = "COMPLETED"
        except BaseException as error:
            try:
                if local_state == "RESERVED":
                    store.set_dbt_invocation_state(
                        contract, expected_state="RESERVED", new_state="ABANDONED",
                        failure_reason=type(error).__name__,
                    )
                elif local_state == "RUNNING":
                    store.set_dbt_invocation_state(
                        contract, expected_state="RUNNING", new_state="FAILED",
                        failure_reason=type(error).__name__,
                    )
            except BaseException:
                pass
            raise


if __name__ == "__main__":
    main()
