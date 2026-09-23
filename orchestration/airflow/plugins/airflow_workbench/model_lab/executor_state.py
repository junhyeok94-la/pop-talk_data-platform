"""Fixed executor operations; only this API process touches PostgreSQL.

One HTTP environment represents one worker process. Its existing Connection
password authenticates callbacks. No SQL, credentials, or other environments'
records are exposed to that worker.
"""

import hashlib
import hmac
import json
import os
import secrets
import time
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from airflow_workbench.model_lab import environments
from airflow_workbench.shared import metadata
from airflow_workbench.model_lab import worker_client


def database():
    return metadata.connection(
        os.environ.get("AIRFLOW_WORKBENCH_DB_SCHEMA", "workbench")
    )


def executor_identity(request: Request):
    try:
        spec = environments.resolve(request.headers.get("x-workbench-executor", ""))
        if not request.headers.get("x-workbench-executor") or spec.backend != "http":
            raise ValueError("executor required")
        _, token = worker_client.connection(spec.connection_id)
        if len(token) < 32 or not hmac.compare_digest(
            request.headers.get("authorization", ""), "Bearer " + token
        ):
            raise ValueError("invalid token")
    except Exception:
        raise HTTPException(401, "Executor authentication failed") from None
    return spec.id


class CreateJob(BaseModel):
    model_config = ConfigDict(extra="forbid")
    key: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,100}$")
    request: str = Field(max_length=200000)


class Transition(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal[
        "running",
        "cancelling",
        "cancelled",
        "interrupted",
        "rejected",
        "timeout",
        "failed",
        "success",
    ]
    result: dict | None = None
    error: str | None = Field(default=None, max_length=4000)


def public(row):
    return {
        **{k: row[k] for k in ("id", "key", "status", "created", "updated", "error")},
        "result": json.loads(row["result"]) if row["result"] else None,
    }


def lookup(db, env, job_id):
    row = db.execute(
        "SELECT * FROM executor_jobs WHERE environment_id=? AND (id=? OR key=?)",
        (env, job_id, job_id),
    ).fetchone()
    if not row:
        raise HTTPException(404, "작업이 없습니다.")
    return row


app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/jobs/{job_id}")
def get_job(job_id: str, env=Depends(executor_identity)):
    with database() as db:
        return public(lookup(db, env, job_id))


@app.get("/jobs")
def list_jobs(env=Depends(executor_identity)):
    with database() as db:
        return [
            public(r)
            for r in db.execute(
                "SELECT * FROM executor_jobs WHERE environment_id=? ORDER BY created DESC LIMIT 50",
                (env,),
            )
        ]


@app.post("/jobs")
def create_job(body: CreateJob, env=Depends(executor_identity)):
    try:
        if json.loads(body.request)["key"] != body.key:
            raise ValueError("key mismatch")
    except (ValueError, KeyError, TypeError):
        raise HTTPException(422, "Invalid job request") from None
    digest = hashlib.sha256(body.request.encode()).hexdigest()
    with database() as db:
        db.lock("executor:" + env)
        row = db.execute(
            "SELECT * FROM executor_jobs WHERE environment_id=? AND key=?",
            (env, body.key),
        ).fetchone()
        if row:
            if row["hash"] != digest:
                raise HTTPException(409, "같은 key의 입력이 다릅니다.")
            return {"created": False, "job": public(row)}
        busy(db, env)
        job_id, now = secrets.token_hex(16), time.time()
        db.execute(
            "INSERT INTO executor_jobs(environment_id,id,key,hash,status,created,updated,request) VALUES (?,?,?,?,'queued',?,?,?)",
            (env, job_id, body.key, digest, now, now, body.request),
        )
        return {"created": True, "job": public(lookup(db, env, job_id))}


def busy(db, env):
    if (
        db.execute(
            "SELECT 1 FROM executor_jobs WHERE environment_id=? AND status IN ('queued','running','cancelling')",
            (env,),
        ).fetchone()
        or db.execute(
            "SELECT 1 FROM executor_reservations WHERE environment_id=? AND expires>?",
            (env, time.time()),
        ).fetchone()
    ):
        raise HTTPException(409, "GPU가 학습 또는 다른 실험에 예약되어 있습니다.")


@app.post("/jobs/{job_id}/transition")
def transition(job_id: str, body: Transition, env=Depends(executor_identity)):
    with database() as db:
        db.lock("executor:" + env)
        row = lookup(db, env, job_id)
        allowed = {
            "queued": {
                "running",
                "cancelling",
                "cancelled",
                "interrupted",
                "rejected",
                "failed",
            },
            "running": {
                "cancelling",
                "cancelled",
                "interrupted",
                "timeout",
                "failed",
                "success",
            },
            "cancelling": {"cancelled", "interrupted"},
        }
        changed = body.status in allowed.get(row["status"], set())
        if changed:
            db.execute(
                "UPDATE executor_jobs SET status=?,updated=?,result=?,error=? WHERE environment_id=? AND id=?",
                (
                    body.status,
                    time.time(),
                    json.dumps(body.result) if body.result is not None else None,
                    body.error,
                    env,
                    row["id"],
                ),
            )
        return {"changed": changed, "job": public(lookup(db, env, row["id"]))}


@app.post("/recover")
def recover(env=Depends(executor_identity)):
    with database() as db:
        db.lock("executor:" + env)
        count = db.execute(
            "UPDATE executor_jobs SET status='interrupted',error='워커 재시작',updated=? WHERE environment_id=? AND status IN ('queued','running','cancelling')",
            (time.time(), env),
        ).rowcount
        return {"interrupted": count}


@app.get("/admission")
def admission(env=Depends(executor_identity)):
    with database() as db:
        active = db.execute(
            "SELECT id FROM executor_jobs WHERE environment_id=? AND status IN ('queued','running','cancelling')",
            (env,),
        ).fetchone()
        reserved = db.execute(
            "SELECT max(expires) AS expires FROM executor_reservations WHERE environment_id=? AND expires>?",
            (env, time.time()),
        ).fetchone()
        return {
            "active_job": active["id"] if active else None,
            "inference_reserved_until": reserved["expires"] if reserved else None,
        }


@app.post("/reservations")
def reserve(env=Depends(executor_identity)):
    with database() as db:
        db.lock("executor:" + env)
        db.execute(
            "DELETE FROM executor_reservations WHERE environment_id=? AND expires<=?",
            (env, time.time()),
        )
        busy(db, env)
        token = secrets.token_hex(16)
        db.execute(
            "INSERT INTO executor_reservations VALUES (?,?,?)",
            (env, token, time.time() + 1260),
        )
        return {"id": token}


@app.post("/reservations/{reservation_id}/release")
def release(reservation_id: str, env=Depends(executor_identity)):
    with database() as db:
        db.execute(
            "DELETE FROM executor_reservations WHERE environment_id=? AND id=?",
            (env, reservation_id),
        )
    return {"released": True}


@app.post("/reservations/{reservation_id}/renew")
def renew(reservation_id: str, env=Depends(executor_identity)):
    with database() as db:
        db.lock("executor:" + env)
        changed = db.execute(
            "UPDATE executor_reservations SET expires=? WHERE environment_id=? AND id=? AND expires>?",
            (time.time() + 1260, env, reservation_id, time.time()),
        ).rowcount
        if not changed:
            raise HTTPException(
                409, "추론 예약이 만료되었습니다. 실행기 상태를 확인하세요."
            )
    return {"renewed": True}
