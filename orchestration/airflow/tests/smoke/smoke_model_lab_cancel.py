"""외부 작업 취소가 deferred Airflow 태스크에 전달되고 pool을 해제하는지 검사."""
import json
import os
import time
import uuid
from pathlib import Path
import httpx


def main():
    c=httpx.Client(base_url="http://127.0.0.1:8080",timeout=20,trust_env=False)
    auth=c.post("/auth/token",json={"username":os.environ["_AIRFLOW_WWW_USER_USERNAME"],"password":os.environ["_AIRFLOW_WWW_USER_PASSWORD"]})
    auth.raise_for_status()
    c.headers.update({"Authorization":"Bearer "+auth.json()["access_token"],"X-Workbench-Request":"1"})
    def api(method,path,data=None):
        r=c.request(method,path,json=data)
        assert r.is_success,(path,r.status_code,r.text[:1500])
        return r.json()
    run=api("POST","/workbench/api/mlops/diagnostic",{"request_id":str(uuid.uuid4())})
    path=f"/api/v2/dags/{run['dag_id']}/dagRuns/{run['dag_run_id']}"
    print("Cancellation propagation test queued",flush=True)
    deadline=time.monotonic()+100
    cancelled=None
    while time.monotonic()<deadline:
        ti=api("GET",path+"/taskInstances")["task_instances"]
        if any(t["state"]=="deferred" for t in ti):
            status=api("GET","/workbench/api/mlops/status")
            job=status["worker"]["active_job"]
            assert job,"external job finished before cancellation test"
            assert status["pool"]["occupied_slots"]==1
            cancelled=api("POST","/workbench/api/mlops/jobs/"+job+"/cancel")
            assert cancelled["status"]=="cancelled"
            break
        time.sleep(.5)
    assert cancelled,"did not observe deferred state"
    while time.monotonic()<deadline:
        state=api("GET",path)["state"]
        if state in {"failed","success"}:break
        time.sleep(1)
    assert state=="failed",state
    pool=api("GET","/api/v2/pools/model_lab_gpu")
    assert pool["occupied_slots"]==0,pool
    result={"dag_id":run["dag_id"],"dag_run_id":run["dag_run_id"],"job_id":cancelled["id"],"job_status":cancelled["status"],"airflow_status":state,"pool_occupied_after_cancel":pool["occupied_slots"],"checked_at":time.time()}
    Path("/opt/airflow/workbench/qa-cancel-v2.json").write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps(result,indent=2),flush=True)


if __name__=="__main__":main()
