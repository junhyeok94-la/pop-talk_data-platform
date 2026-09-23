"""Idempotent installation: Airflow migration, Workbench migration, bootstrap DAGs."""

import argparse
import json
import os
from pathlib import Path
import subprocess


def ensure_pool(name, capacity):
    from airflow.models.pool import Pool
    from airflow.utils.session import create_session

    with create_session() as session:
        pool = session.query(Pool).filter(Pool.pool == name).one_or_none()
        if pool is None:
            session.add(Pool(pool=name, slots=capacity, description="Model Lab external execution", include_deferred=True))
        elif pool.slots != capacity or not pool.include_deferred:
            raise RuntimeError(f"Existing pool {name} differs from its Model Lab configuration; reconcile it in Airflow before retrying.")


def publish(filename, source):
    from airflow.configuration import conf

    folder = Path(conf.get("core", "dags_folder")) / "workbench_managed"
    if folder.is_symlink():
        raise RuntimeError("Managed DAG directory must not be a symlink")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / filename
    temporary = path.with_suffix(".pending")
    if path.is_symlink() or temporary.is_symlink():
        raise RuntimeError("Managed DAG file must not be a symlink")
    if not path.exists() or path.read_text(encoding="utf-8") != source:
        temporary.write_text(source, encoding="utf-8")
        temporary.replace(path)


def bootstrap(config):
    from airflow_workbench.model_lab.contracts import Endpoint
    from airflow_workbench.model_lab.store import LabStore
    from airflow_workbench.model_lab import environments
    from airflow_workbench.model_lab.evaluation_dag import source

    if set(config) - {"endpoints", "environments"}:
        raise ValueError("Unknown bootstrap settings")
    # Validate all inputs before writing any endpoint/environment records.
    endpoints = [Endpoint.model_validate(s) for s in config.get("endpoints", [])]
    targets = [environments.Environment.model_validate(s) for s in config.get("environments", [])]
    if any(s.version != 0 for s in [*endpoints, *targets]):
        raise ValueError("Bootstrap definitions must use version 0")
    store = LabStore()
    known = {r["id"]: Endpoint.model_validate(r["spec"]) for r in store.list("_global", "endpoint")}
    merged = {s.id: s for s in endpoints} | known  # Existing installation settings win.
    dag_ids = [s.dag_id() for s in merged.values()]
    if len(dag_ids) != len(set(dag_ids)):
        raise ValueError("Endpoint IDs produce duplicate DAG IDs")
    for requested in endpoints:
        spec = merged[requested.id]
        ensure_pool(spec.pool, spec.capacity)
        if spec.id not in known:
            spec = spec.model_copy(update={"version": 1})
            store.put("_global", "endpoint", {"name": spec.name, "spec": spec.model_dump()}, key=spec.id, expected=0)
        publish("evaluation_" + spec.id + ".py", source(spec))
        print("Evaluation DAG:", spec.dag_id())
    existing = {s.id: s for s in environments.configured()}
    for requested in targets:
        spec = existing.get(requested.id, requested)
        ensure_pool(spec.pool, spec.capacity)
        if spec.id not in existing:
            spec = environments.save(spec)
        publish(spec.id + ".py", environments.dag_source(spec))
        print("Worker DAGs:", ", ".join(spec.dag_ids().values()))


def initialize_identity():
    from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder
    from airflow_workbench.model_lab.contracts import Endpoint
    from airflow_workbench.model_lab.store import LabStore

    with get_application_builder() as builder:
        sm = builder.sm
        access = sm.add_role("WorkbenchAccess")
        for permission in ("can_read", "menu_access"):
            sm.add_permission_to_role(access, sm.create_permission(permission, "Plugins"))
        runner = sm.add_role("ModelLabRunner")
        sm.add_permission_to_role(runner, sm.create_permission("can_create", "DAG Runs"))
        for record in LabStore().list("_global", "endpoint"):
            dag_id = Endpoint.model_validate(record["spec"]).dag_id()
            sm.add_permission_to_role(runner, sm.create_permission("can_edit", "DAG:" + dag_id))
        username = os.environ.get("AIRFLOW_ADMIN_USERNAME", "admin")
        if not sm.find_user(username=username):
            password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "")
            if len(password) < 16:
                raise ValueError("Set AIRFLOW_ADMIN_PASSWORD to at least 16 characters")
            if not sm.add_user(username, "Workbench", "Administrator", "admin@example.invalid", sm.find_role("Admin"), password):
                raise RuntimeError("Initial administrator creation failed")
            print("Created initial administrator (password is in local .env).")
        else:
            print("Existing administrator account preserved.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="/opt/workbench/config/bootstrap.json")
    parser.add_argument("--bootstrap-only", action="store_true", help="Register a further environment after the initial migration")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    if not args.bootstrap_only:
        subprocess.run(["airflow", "db", "migrate"], check=True)
        from airflow_workbench.shared.metadata import migrate
        migrate()
    bootstrap(config)
    if not args.bootstrap_only:
        initialize_identity()
    print("Workbench initialization completed. New DAGs are paused until enabled in Airflow.")


if __name__ == "__main__":
    main()
