"""Connection discovery for the settings UI. Never return credentials or URLs."""

import os
import re
from urllib.parse import urlsplit

LABELS = {
    "environment": "환경변수",
    "metadata": "Airflow Connections",
    "secrets_backend": "외부 Secrets Backend",
    "missing": "등록 확인 필요",
    "unavailable": "조회 실패",
    "legacy": "기본 Ollama 환경 설정",
}


def describe(connection_id):
    if not connection_id:
        return {"connection_id": "", "source": "legacy", "source_label": LABELS["legacy"], "resolved": True, "valid": True}
    from airflow.configuration import ensure_secrets_loaded
    from airflow.secrets.environment_variables import EnvironmentVariablesBackend
    from airflow.secrets.metastore import MetastoreBackend

    failed = False
    # Follow Airflow's backend precedence, including external-secret overrides.
    for backend in ensure_secrets_loaded():
        try:
            conn = backend.get_connection(conn_id=connection_id)
        except Exception:
            failed = True
            continue
        if conn is None:
            continue
        source = "environment" if isinstance(backend, EnvironmentVariablesBackend) else "metadata" if isinstance(backend, MetastoreBackend) else "secrets_backend"
        try:
            url = urlsplit(conn.host or "")
            valid = bool(url.scheme in {"http", "https"} and url.hostname and not (url.username or url.password or url.query or url.fragment))
        except ValueError:
            valid = False
        return {"connection_id": connection_id, "source": source, "source_label": LABELS[source], "resolved": True, "valid": valid}
    source = "unavailable" if failed else "missing"
    return {"connection_id": connection_id, "source": source, "source_label": LABELS[source], "resolved": False, "valid": False}


def environment_ids():
    return sorted({name[len("AIRFLOW_CONN_"):].lower() for name in os.environ if name.startswith("AIRFLOW_CONN_") and re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", name[len("AIRFLOW_CONN_"):])})
