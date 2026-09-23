"""Compatibility persistence for pre-Studio dashboards."""

import json
from airflow_workbench.dashboard.database import Store as Database, Conflict

DEFAULT_DASHBOARD = {
    "title": "Operations overview",
    "version": 0,
    "hours": 24,
    "dag_filter": "",
    "refresh_seconds": 60,
    "panels": [
        {"id": "runs", "title": "DAG 실행", "source": "run_count", "width": 4},
        {
            "id": "success",
            "title": "완료 실행 성공률",
            "source": "success_rate",
            "width": 4,
        },
        {"id": "models", "title": "로컬 모델", "source": "model_count", "width": 4},
        {
            "id": "duration",
            "title": "최근 실행 소요시간",
            "source": "run_duration",
            "width": 8,
        },
        {"id": "states", "title": "실행 상태 분포", "source": "run_states", "width": 4},
        {
            "id": "pipeline",
            "title": "파이프라인 실행 이력",
            "source": "dag_runs",
            "width": 12,
        },
        {
            "id": "experiments",
            "title": "모델 실험 이력",
            "source": "experiments",
            "width": 12,
        },
    ],
}


class Store(Database):
    def dashboard(self):
        with self.connection() as db:
            row = db.execute(
                "SELECT value FROM documents WHERE key='dashboard'"
            ).fetchone()
        return json.loads(row[0]) if row else json.loads(json.dumps(DEFAULT_DASHBOARD))

    def save_dashboard(self, value):
        with self.connection() as db:
            db.lock("dashboard")
            row = db.execute(
                "SELECT version FROM documents WHERE key='dashboard'"
            ).fetchone()
            version = row[0] if row else 0
            if value["version"] != version:
                raise Conflict(
                    "다른 사용자가 수정했습니다. 새로 불러온 뒤 다시 저장하세요."
                )
            value = {**value, "version": version + 1}
            db.execute(
                "INSERT INTO documents VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,version=excluded.version",
                ("dashboard", json.dumps(value), value["version"]),
            )
        return value
