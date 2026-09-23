"""Stable owner namespace derived exclusively from the authenticated Airflow user."""

import hashlib
import json
from fastapi import HTTPException
from airflow_workbench.shared import auth


def owner_id(user):
    subject = user.get_id()
    if not isinstance(subject, str) or not subject.strip():
        raise HTTPException(401, "Airflow 계정 식별자를 확인할 수 없습니다.")
    cls = type(auth.get_auth_manager())
    return hashlib.sha256(
        json.dumps([f"{cls.__module__}.{cls.__qualname__}", subject]).encode()
    ).hexdigest()
