# Asset 통합 검증 정리 검토

판정: APPROVED. 검사기 유형 분리와 임시 리소스 정리를 수용한다. 이번 범위에서 추가 수정이 필요한 결함은 발견하지 않았다. 이전 Asset 통합 최종 승인은 유지한다.

## 검사기 검토

`scripts/check_airflow_dag_contracts.py`와 Model Lab의 `lab_dag.py`, `lab_contracts.py`를 비교했다. 현재 작업 경로는 Git 저장소가 아니므로 과거 revision과의 diff 대신 현재 구현의 계약을 검토했다.

환경 관리 DAG는 Environment 및 backend별 operator 계약으로, 평가 관리 DAG는 RemoteEvaluationOperator와 Endpoint 계약으로 검사한다. 관리 DAG 전체가 두 분류에 포함되어야 한다는 assertion이 있어 알 수 없는 operator를 단순히 건너뛰지 않는다. 평가 DAG의 id, capacity, fingerprint 태그, pool, timeout 등은 실제 생성 코드와 일치한다. 기존 영화 파이프라인의 태스크/edge, ALL_SUCCESS, mapping 동시성, 재시도, Cosmos 및 직렬화 관련 검사는 현재 검사기에 남아 있다.

실제 scheduler 컨테이너에서 검사기를 재실행하여 10개 DAG 구조 계약 통과를 확인했다. 검사기 자체의 기존 동시성·retry 변조 거부 검사도 함께 통과했다.

추가로 평가 DAG 객체의 메모리 상태만 변경하여 max_active_runs=17, 잘못된 pool, execution_timeout 제거, config fingerprint 태그 제거를 각각 시도했다. 네 경우 모두 AssertionError로 거부됐고 각 검사 후 객체 값을 복원했다. 운영 DAG 설정이나 파일은 변경하지 않았다.

직렬화 검사는 round-trip 성공과 복원된 pool 일치를 확인하는 범위다. 평가 실행 전체 또는 endpoint의 모든 필드가 직렬화되어 보존되는지를 검증했다는 의미로 확대하지 않는다. 평가 DAG의 외부 실행은 이번 검토 대상이 아니다.

## 정리 상태 독립 확인

Airflow metadata를 SELECT로 확인했다.

| 확인 항목 | 결과 |
| --- | --- |
| probe dag | 0건 |
| probe dag_run | 0건 |
| probe task_instance | 0건 |
| probe xcom | 0건 |
| asset_dag_run_queue 전체 | 0건 |
| import_error | 0건 |
| 운영 Raw/Main is_paused | 모두 True |

로컬 DAG 디렉터리의 `*asset_batch_probe*` 파일도 없었다. 현재 DagBag은 임시 프로브 없이 10개 DAG를 정상 파싱했다. 기존 실행 증거는 이전 독립 검토 보고서에 남아 있으며, 이번에는 삭제된 과거 XCom을 다시 검증한 것으로 취급하지 않는다.

## 범위

이번 재검토는 구조 검사기, 관련 생성 코드, 정리 상태 확인에 한정했다. 정리 문서의 pipeline unittest 52개 통과는 구현 작업의 실행 보고이며 이번에 다시 실행하지 않았다. 새 외부 작업 실행, 업무 데이터 쓰기 또는 업무 코드 수정은 수행하지 않았다.
