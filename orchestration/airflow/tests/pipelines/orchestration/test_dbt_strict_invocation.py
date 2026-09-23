"""strict dbt contract, artifact 대사와 supervisor 경계를 검증한다."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from dataclasses import replace
from pathlib import Path
from pipelines.paths import DBT_DEPLOYMENT_ROOT
from types import SimpleNamespace
from unittest.mock import patch

from pipelines.orchestration.dbt_invocation_contract import (
    DbtInvocationContractError,
    _native_ids_from_json_log,
    contract_from_json,
    create_dbt_invocation_contract,
    read_regular_file_nofollow,
    validate_run_results,
)
from scripts.dbt_strict_runner import (
    prepare_strict_argv,
    run_supervised,
    sanitized_dbt_environment,
    seal_receipt,
    validate_recovery_artifact,
)


class DbtStrictInvocationTests(unittest.TestCase):
    @staticmethod
    def _contract(root: Path, kind="MODEL"):
        model_id = "model.pop_talk_dw.dw.dim_movie"
        if kind == "MODEL":
            ids = (model_id,)
            selectors = ("fqn:pop_talk_dw.dw.dim_movie",)
        elif kind == "OWNED_TESTS":
            ids = ("test.pop_talk_dw.not_null_dim_movie_movie_id.abc",)
            selectors = ("fqn:pop_talk_dw.dw.not_null_dim_movie_movie_id",)
        else:
            ids = selectors = ()
        return create_dbt_invocation_contract(
            invocation_kind=kind,
            deployment_id="a" * 64,
            manifest_sha256="b" * 64,
            model_version="c" * 64,
            plan_id="plan-1",
            cohort_manifest_id="cohort-1",
            build_id="build-1",
            attempt_no=1,
            fence_token=2,
            claim_owner="worker-1",
            model_unique_id=model_id,
            expected_unique_ids=ids,
            expected_selectors=selectors,
            artifact_root=str(root),
        )

    def test_contract_is_content_addressed_and_tamper_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            contract = self._contract(Path(directory))
            loaded = contract_from_json(json.dumps(contract.body))
            self.assertEqual(loaded, contract)
            tampered = contract.body
            tampered["attempt_no"] = 2
            with self.assertRaises(DbtInvocationContractError):
                contract_from_json(json.dumps(tampered))

    def test_sanitized_environment_removes_dbt_state_and_forces_json_log(self) -> None:
        environment = sanitized_dbt_environment(
            {
                "PATH": "/bin",
                "HOME": "/home/airflow",
                "DBT_STATE": "/stale",
                "DBT_DEFER": "true",
                "DBT_LOG_FORMAT_FILE": "debug",
                "UNRELATED_SECRET": "must-not-pass",
                "SNOWFLAKE_AUTHENTICATOR": "externalbrowser",
            },
            target_path=Path("/artifacts/target"),
            log_path=Path("/artifacts/logs"),
        )
        self.assertNotIn("DBT_STATE", environment)
        self.assertNotIn("UNRELATED_SECRET", environment)
        self.assertEqual(environment["DBT_DEFER"], "false")
        self.assertEqual(environment["DBT_INDIRECT_SELECTION"], "empty")
        self.assertEqual(environment["DBT_LOG_FORMAT_FILE"], "json")
        self.assertEqual(environment["DBT_LOG_LEVEL_FILE"], "debug")

    def test_exact_argv_and_caller_override(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            (source / "dbt_project.yml").write_text("name: pop_talk_dw\n", encoding="utf-8")
            deployment = SimpleNamespace(
                project_path=source, profile_name="pop_talk_dw", target_name="dev"
            )
            contract = self._contract(root)
            arguments = [
                "run", "--select", contract.expected_selectors[0],
                "--project-dir", "/tmp/cosmos", "--profiles-dir", "/tmp/cosmos",
                "--profile", "pop_talk_dw", "--target", "dev",
            ]
            prepared = prepare_strict_argv(
                arguments,
                contract=contract,
                deployment=deployment,
                project_directory=root / "invocation-project",
            )
            self.assertIn(str((root / "invocation-project").resolve()), prepared)
            with self.assertRaises(RuntimeError):
                prepare_strict_argv(
                    [*arguments, "--log-format-file", "debug"],
                    contract=contract,
                    deployment=deployment,
                    project_directory=root / "other-project",
                )

    def test_bare_extra_selector_and_relation_vars_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            (source / "dbt_project.yml").write_text("name: pop_talk_dw\n", encoding="utf-8")
            deployment = SimpleNamespace(
                project_path=source, profile_name="pop_talk_dw", target_name="dev"
            )
            contract = self._contract(root)
            base = [
                "run", "--select", contract.expected_selectors[0],
                "--project-dir", "/tmp/cosmos", "--profiles-dir", "/tmp/cosmos",
                "--profile", "pop_talk_dw", "--target", "dev",
            ]
            with self.assertRaises(RuntimeError):
                prepare_strict_argv(
                    [*base[:3], "fqn:pop_talk_dw.dw.extra", *base[3:]],
                    contract=contract,
                    deployment=deployment,
                    project_directory=root / "extra-selector-project",
                )
            with self.assertRaises(RuntimeError):
                prepare_strict_argv(
                    base,
                    contract=replace(contract, relation_vars_json='{"cutoff":"x"}'),
                    deployment=deployment,
                    project_directory=root / "relation-vars-project",
                )

    def test_owned_test_selectors_accept_cosmos_space_join_only_as_exact_set(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model_id = "model.pop_talk_dw.dw.dim_movie"
            selectors = ("fqn:pop_talk_dw.dw.test_a", "fqn:pop_talk_dw.dw.test_b")
            contract = create_dbt_invocation_contract(
                invocation_kind="OWNED_TESTS",
                deployment_id="a" * 64,
                manifest_sha256="b" * 64,
                model_version="c" * 64,
                plan_id="plan-1",
                cohort_manifest_id="cohort-1",
                build_id="build-1",
                attempt_no=1,
                fence_token=2,
                claim_owner="worker-1",
                model_unique_id=model_id,
                expected_unique_ids=("test.p.a", "test.p.b"),
                expected_selectors=selectors,
                artifact_root=str(root),
            )
            source = root / "source"
            source.mkdir()
            (source / "dbt_project.yml").write_text("name: pop_talk_dw\n", encoding="utf-8")
            deployment = SimpleNamespace(
                project_path=source, profile_name="pop_talk_dw", target_name="dev"
            )
            base = [
                "test", "--select", " ".join(selectors),
                "--project-dir", "/tmp/cosmos", "--profiles-dir", "/tmp/cosmos",
                "--profile", "pop_talk_dw", "--target", "dev",
            ]
            prepare_strict_argv(
                base,
                contract=contract,
                deployment=deployment,
                project_directory=root / "project",
            )
            with self.assertRaises(RuntimeError):
                prepare_strict_argv(
                    [*base[:-6], "--select", "fqn:pop_talk_dw.dw.extra", *base[-6:]],
                    contract=contract,
                    deployment=deployment,
                    project_directory=root / "other-project",
                )

    def test_run_results_uses_manifest_type_and_independent_native_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contract = self._contract(root)
            native_id = str(uuid.uuid4())
            run_results = root / "run_results.json"
            log = root / "dbt.log"
            run_results.write_text(
                json.dumps(
                    {
                        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json", "invocation_id": native_id},
                        "results": [{"unique_id": contract.model_unique_id, "status": "success"}],
                    }
                ),
                encoding="utf-8",
            )
            log.write_text(json.dumps({"info": {"invocation_id": native_id}}) + "\n", encoding="utf-8")
            observed, executed, digest = validate_run_results(
                contract=contract,
                run_results_path=run_results,
                log_path=log,
                manifest={"nodes": {contract.model_unique_id: {"resource_type": "model"}}},
            )
            self.assertEqual(observed, native_id)
            self.assertEqual(executed, contract.expected_unique_ids)
            self.assertEqual(len(digest), 64)
            log.write_text(json.dumps({"info": {"invocation_id": str(uuid.uuid4())}}), encoding="utf-8")
            with self.assertRaises(DbtInvocationContractError):
                validate_run_results(
                    contract=contract,
                    run_results_path=run_results,
                    log_path=log,
                    manifest={"nodes": {contract.model_unique_id: {"resource_type": "model"}}},
                )

    def test_file_sealed_recovery_revalidates_original_dbt_evidence(self) -> None:
        from pipelines.orchestration.dbt_invocation_contract import DbtInvocationArtifact

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contract = self._contract(root)
            invocation_dir = root / "invocation"
            target = invocation_dir / "target"
            logs = invocation_dir / "logs"
            target.mkdir(parents=True)
            logs.mkdir()
            native_id = str(uuid.uuid4())
            body = {
                "metadata": {"invocation_id": native_id},
                "results": [{"unique_id": contract.model_unique_id, "status": "success"}],
            }
            raw = json.dumps(body).encode("utf-8")
            (target / "run_results.json").write_bytes(raw)
            (logs / "dbt.log").write_text(
                json.dumps({"info": {"invocation_id": native_id}}), encoding="utf-8"
            )
            artifact = DbtInvocationArtifact(
                contract.orchestration_invocation_id, native_id, contract.build_id,
                contract.attempt_no, contract.fence_token, contract.cohort_manifest_id,
                contract.deployment_id, contract.model_unique_id, contract.invocation_kind,
                contract.expected_unique_ids, contract.expected_unique_ids,
                __import__("hashlib").sha256(raw).hexdigest(), "a" * 64, "b" * 64,
                str(invocation_dir), "SUCCEEDED",
            )
            validate_recovery_artifact(
                contract=contract,
                artifact=artifact,
                directory=invocation_dir,
                manifest={"nodes": {contract.model_unique_id: {"resource_type": "model"}}},
            )
            body["results"][0]["status"] = "error"
            (target / "run_results.json").write_text(json.dumps(body), encoding="utf-8")
            with self.assertRaises(DbtInvocationContractError):
                validate_recovery_artifact(
                    contract=contract,
                    artifact=artifact,
                    directory=invocation_dir,
                    manifest={"nodes": {contract.model_unique_id: {"resource_type": "model"}}},
                )

    @unittest.skipUnless(hasattr(os, "symlink"), "symlink 지원 환경에서만 실행")
    def test_nofollow_reader_rejects_parent_directory_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old = root / "old"
            old.mkdir()
            (old / "evidence.json").write_text("old evidence", encoding="utf-8")
            linked_parent = root / "logs"
            linked_parent.symlink_to(old, target_is_directory=True)
            with self.assertRaises(DbtInvocationContractError):
                read_regular_file_nofollow(linked_parent / "evidence.json")

    @unittest.skipUnless(
        Path("/opt/dbt-venv/bin/python").exists(),
        "Airflow dbt 1.10 runtime에서만 실행",
    )
    def test_installed_dbt_serializes_v6_without_resource_type(self) -> None:
        """설치본 schema serializer로 만든 v6 artifact를 strict 검증기가 직접 읽는다."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contract = self._contract(root)
            program = """
import json
from dbt.artifacts.schemas.run.v5.run import RunResultsArtifact
from dbt_common.invocation import get_invocation_id, reset_invocation_id
reset_invocation_id()
invocation_id = str(get_invocation_id())
body = {
  'metadata': {
    'dbt_schema_version': 'https://schemas.getdbt.com/dbt/run-results/v6.json',
    'dbt_version': '1.10.11',
    'invocation_id': invocation_id,
  },
  'results': [{
    'status': 'success', 'timing': [], 'thread_id': 'main',
    'execution_time': 0.0, 'adapter_response': {}, 'message': None,
    'failures': None, 'unique_id': 'model.pop_talk_dw.dw.dim_movie',
    'compiled': True, 'compiled_code': 'select 1', 'relation_name': 'DIM_MOVIE'
  }],
  'elapsed_time': 0.0,
  'args': {}
}
print(json.dumps(RunResultsArtifact.from_dict(body).to_dict(omit_none=False)))
"""
            completed = subprocess.run(
                ["/opt/dbt-venv/bin/python", "-c", program],
                check=True,
                capture_output=True,
                text=True,
            )
            body = json.loads(completed.stdout)
            self.assertEqual(
                body["metadata"]["dbt_schema_version"],
                "https://schemas.getdbt.com/dbt/run-results/v6.json",
            )
            self.assertNotIn("resource_type", body["results"][0])
            run_results = root / "run_results.json"
            run_results.write_text(json.dumps(body), encoding="utf-8")
            native_id = body["metadata"]["invocation_id"]
            log = root / "dbt.log"
            log.write_text(json.dumps({"info": {"invocation_id": native_id}}), encoding="utf-8")
            observed, executed, _ = validate_run_results(
                contract=contract,
                run_results_path=run_results,
                log_path=log,
                manifest={"nodes": {contract.model_unique_id: {"resource_type": "model"}}},
            )
            self.assertEqual(observed, native_id)
            self.assertEqual(executed, contract.expected_unique_ids)

    @unittest.skipUnless(
        Path("/opt/dbt-venv/bin/dbt").exists(),
        "Airflow dbt 1.10 runtime에서만 실행",
    )
    def test_installed_dbt_writes_one_native_id_to_json_file_log(self) -> None:
        project_root = (Path(__file__).resolve().parents[3] / "modules")
        from pipelines.orchestration.dbt_deployment import load_current_deployment

        deployment = load_current_deployment(DBT_DEPLOYMENT_ROOT)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = root / "project"
            import shutil

            shutil.copytree(deployment.project_path, project)
            target = root / "target"
            logs = root / "logs"
            environment = sanitized_dbt_environment(
                os.environ, target_path=target, log_path=logs
            )
            completed = subprocess.run(
                [
                    "/opt/dbt-venv/bin/dbt", "parse", "--no-partial-parse",
                    "--project-dir", str(project), "--profiles-dir", str(project),
                    "--profile", deployment.profile_name, "--target", deployment.target_name,
                ],
                env=environment,
                check=False,
            )
            self.assertEqual(completed.returncode, 0)
            native_ids = _native_ids_from_json_log(logs / "dbt.log")
            self.assertEqual(len(native_ids), 1)

    @unittest.skipUnless(
        Path("/home/airflow/.local/lib/python3.12/site-packages/cosmos").exists(),
        "Airflow Cosmos 1.15 runtime에서만 실행",
    )
    def test_installed_cosmos_full_command_is_accepted_by_strict_parser(self) -> None:
        """Cosmos run_command가 임시 project/profile flag를 붙인 실제 명령을 대사한다."""
        from cosmos.config import ProfileConfig
        from cosmos.operators.local import DbtRunLocalOperator
        from pipelines.orchestration.dbt_deployment import load_current_deployment
        from pipelines.orchestration.dbt_model_registry import (
            CURRENT_TEST_OWNER_OVERRIDES,
            build_model_dag_registry,
        )

        project_root = (Path(__file__).resolve().parents[3] / "modules")
        deployment = load_current_deployment(DBT_DEPLOYMENT_ROOT)
        manifest = json.loads(deployment.manifest_path.read_text(encoding="utf-8"))
        registry = build_model_dag_registry(
            manifest,
            deployment_id=deployment.deployment_id,
            explicit_owner_by_test_name=CURRENT_TEST_OWNER_OVERRIDES,
        )
        spec = registry.models[0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contract = create_dbt_invocation_contract(
                invocation_kind="MODEL",
                deployment_id=deployment.deployment_id,
                manifest_sha256=deployment.manifest_sha256,
                model_version=deployment.model_version,
                plan_id="plan-smoke",
                cohort_manifest_id="cohort-smoke",
                build_id="build-smoke",
                attempt_no=1,
                fence_token=1,
                claim_owner="smoke",
                model_unique_id=spec.model_unique_id,
                expected_unique_ids=(spec.model_unique_id,),
                expected_selectors=(spec.selector,),
                artifact_root=str(root),
            )
            operator = DbtRunLocalOperator(
                task_id="strict_cosmos_smoke",
                project_dir=str(deployment.project_path),
                profile_config=ProfileConfig(
                    profile_name=deployment.profile_name,
                    target_name=deployment.target_name,
                    profiles_yml_filepath=str(deployment.project_path / "profiles.yml"),
                ),
                select=spec.selector,
                install_deps=False,
                partial_parse=False,
                indirect_selection=None,
                emit_datasets=False,
                append_env=False,
                env={"POP_TALK_DBT_STRICT_CONTRACT": json.dumps(contract.body)},
                dbt_executable_path="/usr/local/bin/pop-talk-dbt-strict",
                should_store_compiled_sql=False,
            )
            captured: dict[str, object] = {}

            class CapturedCommand(RuntimeError):
                pass

            def capture(_operator, *, command, env, cwd, context):
                captured.update(command=command, env=env, cwd=cwd)
                raise CapturedCommand

            with patch.object(DbtRunLocalOperator, "invoke_dbt", new=capture):
                with self.assertRaises(CapturedCommand):
                    operator.build_and_run_cmd({"run_id": "strict-smoke"})
            command = list(captured["command"])
            self.assertEqual(command[0], "/usr/local/bin/pop-talk-dbt-strict")
            prepared = prepare_strict_argv(
                command[1:],
                contract=contract,
                deployment=deployment,
                project_directory=root / "strict-project",
            )
            self.assertIn("--no-partial-parse", prepared)
            child_env = sanitized_dbt_environment(
                captured["env"],
                target_path=root / "target",
                log_path=root / "logs",
            )
            self.assertNotIn("POP_TALK_DBT_STRICT_CONTRACT", child_env)
            self.assertEqual(child_env["DBT_LOG_FORMAT_FILE"], "json")

    def test_receipt_is_exclusive_and_supervisor_stops_after_heartbeat_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "receipt.json"
            seal_receipt(receipt, {"ok": True})
            with self.assertRaises(RuntimeError):
                seal_receipt(receipt, {"ok": True})

            def failed_heartbeat():
                raise TimeoutError("database timeout")

            with self.assertRaises(RuntimeError):
                run_supervised(
                    [sys.executable, "-c", "import time; time.sleep(30)"],
                    environment=os.environ,
                    heartbeat=failed_heartbeat,
                    heartbeat_interval_seconds=0.01,
                    lease_deadline_seconds=1,
                    grace_seconds=0.1,
                )

    def test_supervisor_deadline_is_not_blocked_by_slow_heartbeat(self) -> None:
        def stuck_heartbeat() -> float:
            time.sleep(1)
            return 10

        started = time.monotonic()
        with self.assertRaises(RuntimeError):
            run_supervised(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                environment=os.environ,
                heartbeat=stuck_heartbeat,
                heartbeat_interval_seconds=0.01,
                lease_deadline_seconds=0.05,
                grace_seconds=0.01,
            )
        self.assertLess(time.monotonic() - started, 0.5)

    def test_supervisor_cancellation_latch_wins_over_zero_exit_race(self) -> None:
        installed: dict[int, object] = {}

        class ExitedProcess:
            def poll(self):
                return 0

            def wait(self, timeout=None):
                return 0

            def send_signal(self, _signum):
                pass

            def terminate(self):
                pass

            def kill(self):
                pass

        def fake_signal(signum, handler):
            previous = installed.get(signum, signal.SIG_DFL)
            installed[signum] = handler
            return previous

        def launch_after_cancel(*_args, **_kwargs):
            installed[signal.SIGTERM](signal.SIGTERM, None)
            return ExitedProcess()

        import signal
        with patch("scripts.dbt_strict_runner.signal.signal", side_effect=fake_signal):
            with self.assertRaises(RuntimeError):
                run_supervised(
                    ["fake-dbt"],
                    environment={},
                    heartbeat=lambda: 10,
                    heartbeat_interval_seconds=1,
                    lease_deadline_seconds=10,
                    grace_seconds=0,
                    popen_factory=launch_after_cancel,
                )


if __name__ == "__main__":
    unittest.main()
