"""Persistence for evaluations and presets created before project workflows."""

import json
import time
import uuid
from airflow_workbench.shared.database import Store as Database, Conflict


class Store(Database):
    def save_preset(self, value):
        preset_id = uuid.uuid4().hex
        with self.connection() as db:
            db.execute(
                "INSERT INTO documents VALUES (?, ?, 1)",
                (f"preset:{preset_id}", json.dumps({"id": preset_id, **value})),
            )
        return {"id": preset_id, **value}

    def presets(self):
        with self.connection() as db:
            return [
                json.loads(row[0])
                for row in db.execute(
                    "SELECT value FROM documents WHERE key LIKE 'preset:%' ORDER BY key DESC LIMIT 200"
                )
            ]

    def start_experiment(self, kind, config, actor):
        experiment_id = uuid.uuid4().hex
        now = time.time()
        with self.connection() as db:
            db.lock("ollama")
            row = db.execute(
                "SELECT owner, expires FROM leases WHERE name='ollama'"
            ).fetchone()
            if row and row["expires"] > now:
                raise Conflict(
                    "다른 모델 실험이 실행 중입니다. 완료 후 다시 시도하세요."
                )
            if row:
                db.execute(
                    "UPDATE experiments SET status='interrupted', finished=?, error=? WHERE id=? AND status='running'",
                    (
                        now,
                        "서버 중단 또는 실행 시간 초과로 결과를 확인할 수 없습니다.",
                        row["owner"],
                    ),
                )
            db.execute(
                "INSERT INTO leases VALUES ('ollama', ?, ?) ON CONFLICT(name) DO UPDATE SET owner=excluded.owner,expires=excluded.expires",
                (experiment_id, now + 1260),
            )
            db.execute(
                "INSERT INTO experiments VALUES (?, ?, ?, ?, ?, NULL, 'running', ?, NULL, NULL)",
                (experiment_id, kind, config["name"], actor, now, json.dumps(config)),
            )
        return experiment_id

    def finish_experiment(self, experiment_id, result=None, error=None, release=True):
        with self.connection() as db:
            db.execute(
                "UPDATE experiments SET status=?, finished=?, result=?, error=? WHERE id=? AND status='running'",
                (
                    "failed" if error else "success",
                    time.time(),
                    json.dumps(result) if result is not None else None,
                    error,
                    experiment_id,
                ),
            )
            if release:
                db.execute("DELETE FROM leases WHERE owner=?", (experiment_id,))

    def experiments(self, hours=None):
        with self.connection() as db:
            now = time.time()
            db.execute(
                "UPDATE experiments SET status='interrupted', finished=?, error=? WHERE status='running' AND id IN (SELECT owner FROM leases WHERE expires<=?)",
                (now, "서버 중단 또는 실행 시간 초과", now),
            )
            rows = db.execute(
                "SELECT id,kind,name,actor,created,finished,status,error FROM experiments WHERE created >= ? ORDER BY created DESC LIMIT 100",
                (now - hours * 3600 if hours else 0,),
            ).fetchall()
        return [dict(row) for row in rows]

    def experiment(self, experiment_id):
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM experiments WHERE id=?", (experiment_id,)
            ).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["config"] = json.loads(value["config"])
        value["result"] = json.loads(value["result"]) if value["result"] else None
        return value
