# Airflow Asset 통합 검증 정리 및 최종 상태

## 검토 판정

운영형 다중 mapping 보완은
`docs/qa/airflow-asset-trigger-integration-final-review-2026-09-10.md`에서 APPROVED를
받았다. 기존 P2는 종료됐으며 새로운 P0/P1은 없다.

## 임시 리소스 정리

- `orchestration/airflow/dags/pop_talk_asset_batch_probe.py` 삭제
- 임시 producer/success/failure DAG metadata 삭제
- 검증용 DagRun, task instance와 XCom은 DAG 삭제에 포함해 정리
- 검증 문서와 검토 보고서는 학습·감사 이력으로 보존

정리 후 metadata DB에서 probe DAG 0건, AssetDagRunQueue 0건을 확인했다.
`pop_talk_movie_raw_daily`와 `pop_talk_movie_databricks_daily`는 모두 pause 상태다.

## 최종 검증

- Airflow DAG import error: 0
- DAG 구조 계약: 10개 통과
- pipeline 단위 테스트: 52개 통과

기존 검사 기록의 9개보다 하나 늘어난 이유는 Model Lab에서 생성한
`model_lab_eval_default_ollama` 평가 DAG다. 이 DAG는 기존 환경별 학습 DAG와 같은
`model-lab` 태그를 쓰지만 `RemoteEvaluationOperator`라 `environment` 필드가 없다.

`scripts/check_airflow_dag_contracts.py`를 유형별로 분리해 다음을 각각 검증하도록 보완했다.

- 환경 DAG: 기존 `ExternalModelJobOperator`/`PortableTrainingPodOperator` 계약 유지
- 평가 DAG: endpoint fingerprint, DAG id, capacity, pause, tag, pool, timeout,
  `RemoteEvaluationOperator` 및 serialization 계약 검증

따라서 새 평가 DAG를 무조건 허용한 것이 아니라 해당 유형의 독립적인 승인 baseline을
추가했다.

## 승인 범위

검증 결과는 단일 READY 실제 클라우드 end-to-end와 다중 READY의 실제 Airflow
scheduler/XCom/mapping/계약 동작을 승인한다. 다중 READY에서의 클라우드 호출은 프로브로
대체했으므로 실제 복수 입력 클라우드 실행이나 외부 서비스 재시도까지 검증한 것으로
확대하지 않는다.
