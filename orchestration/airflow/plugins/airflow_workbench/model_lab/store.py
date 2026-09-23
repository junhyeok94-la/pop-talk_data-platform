"""Project-scoped records in the existing Airflow PostgreSQL database."""

import json
import time
import uuid
from airflow_workbench.shared import metadata
from airflow_workbench.shared.database import Store, Conflict, NotFound


class LabStore(Store):
    def get(self, project, kind, key):
        with self.connection() as db:
            row = db.execute(
                "SELECT value FROM lab_records WHERE project=? AND kind=? AND id=?",
                (project, kind, key),
            ).fetchone()
        if row is None:
            raise NotFound("기록이 없거나 이 프로젝트에 속하지 않습니다.")
        return json.loads(row[0])

    def list(self, project, kind):
        with self.connection() as db:
            return [
                json.loads(r[0])
                for r in db.execute(
                    "SELECT value FROM lab_records WHERE project=? AND kind=? ORDER BY created DESC LIMIT 500",
                    (project, kind),
                )
            ]

    def put(self, project, kind, value, *, key=None, expected=None):
        key = key or uuid.uuid4().hex
        with self.connection() as db:
            db.lock("lab:" + project + ":" + kind)
            row = db.execute(
                "SELECT value,version FROM lab_records WHERE project=? AND kind=? AND id=?",
                (project, kind, key),
            ).fetchone()
            previous = json.loads(row[0]) if row else None
            version = row[1] if row else 0
            if expected is not None and expected != version:
                raise Conflict("다른 창에서 변경되었습니다. 다시 불러오세요.")
            result = {
                **value,
                "id": key,
                "version": version + 1,
                "created": previous["created"] if previous else time.time(),
                "updated": time.time(),
            }
            db.execute(
                "INSERT INTO lab_records(project,kind,id,version,created,value) VALUES (?,?,?,?,?,?) ON CONFLICT(project,kind,id) DO UPDATE SET version=excluded.version,value=excluded.value",
                (
                    project,
                    kind,
                    key,
                    result["version"],
                    result["created"],
                    json.dumps(result, ensure_ascii=False, allow_nan=False),
                ),
            )
        return result

    def patch(self, project, kind, key, changes):
        old = self.get(project, kind, key)
        return self.put(
            project, kind, {**old, **changes}, key=key, expected=old["version"]
        )

    def request(self, project, value, *, request_spec=None, dag_conf=None):
        from airflow_workbench.model_lab.contracts import digest

        value = {**value, "request_sha256": digest(request_spec), "dag_conf": dag_conf}
        key = uuid.UUID(value["request_id"]).hex
        try:
            old = self.get(project, "run", key)
        except NotFound:
            return self.put(project, "run", value, key=key, expected=0)
        if old["input_sha256"] != value["input_sha256"]:
            raise Conflict(
                "같은 요청 ID에 다른 입력이 있습니다. 새 실행으로 제출하세요."
            )
        return old
