"""Airflow 3.1+ 내부 메뉴에 프로젝트 독립적인 Workbench를 등록한다."""

from airflow.plugins_manager import AirflowPlugin

from airflow_workbench.app import app


class WorkbenchPlugin(AirflowPlugin):
    name = "airflow_workbench"
    fastapi_apps = [
        {"app": app, "url_prefix": "/workbench", "name": "Airflow Workbench"}
    ]
    react_apps = [
        {
            "name": "내 대시보드",
            "bundle_url": "workbench/static/shared/host.js",
            "destination": "nav",
            "url_route": "workbench-dashboard",
            "category": "workbench",
            "nav_top_level": True,
            "icon": "/workbench/static/dashboard.svg",
        },
        {
            "name": "운영 모니터링",
            "bundle_url": "workbench/static/shared/host.js",
            "destination": "nav",
            "url_route": "workbench-monitoring",
            "category": "workbench",
            "nav_top_level": True,
            "icon": "/workbench/static/monitoring/monitoring.svg",
        },
        {
            "name": "Model Lab",
            "bundle_url": "workbench/static/shared/host.js",
            "destination": "nav",
            "url_route": "workbench-models",
            "category": "workbench",
            "nav_top_level": True,
            "icon": "/workbench/static/models.svg",
        },
    ]
