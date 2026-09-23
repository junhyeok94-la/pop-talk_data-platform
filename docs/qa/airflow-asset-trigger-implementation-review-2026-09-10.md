# Asset 자동 연결 구현 검토

판정: CHANGES REQUESTED. P0/P1 발견 없음. 아래 P2 두 건을 보완한 뒤 재검토한다. 외부 통합 실행 및 구현 코드 수정은 하지 않았다.

## P2 — resolver 성공 전에 노트북 배포가 실행될 수 있음

근거: orchestration/airflow/dags/pop_talk_movie_databricks_daily.py의 마지막 의존 관계 선언 및 resolve_ready_inputs/identify_gold_build 연결.

실제 Airflow DagBag에서 root는 resolve_ready_inputs와 identify_gold_build 두 개이며 deploy_notebook의 upstream은 identify_gold_build뿐이다. 따라서 잘못된 event로 resolver가 실패해도 identify→deploy가 독립 실행되어 Databricks workspace mkdir/import가 발생할 수 있다. 승인한 resolve→identify→deploy 및 ‘잘못된 입력은 외부 변경 전에 실패’ 계약과 다르다. 현재 구조 검사는 이 그래프도 통과한다.

수정: ready_inputs >> gold_identity를 명시하거나 동등한 선행 관계를 추가하고, resolver가 모든 외부 쓰기의 ancestor인지 구조 검사한다. 잘못된 provenance/payload와 빈 event에서는 deploy가 upstream_failed가 되는 부정 검증을 포함한다. identify의 literal op_kwargs 보존은 유지한다.

## P2 — barrier가 READY별 연결 대신 독립 집합만 비교함

근거: pipelines/orchestration/asset_contract.py의 confirm_loaded_batch.

raw_run_id 집합, ready_key 집합, exchange_ready_key 집합을 따로 비교하므로 행 간 매칭이 바뀌어도 모두 통과한다. 독립 재현에서 stage의 a→ready-b / b→ready-a와 load의 a→exchange-b / b→exchange-a를 동시에 전달했는데 input_count=2로 승인됐다. 현재 테스트는 순서가 바뀐 정상 행과 누락만 검사하므로 이 사례를 잡지 못한다. downstream 기존 검증이 존재하더라도, mapped lineage 혼합을 검출하도록 추가한 barrier 자체의 보장은 충족하지 않는다.

수정: raw_run_id를 key로 입력/stage/load를 각각 index하고 각 ID별 READY key, 기대 Exchange key, artifact/revision을 직접 대조한다. bucket은 입력 고정 bucket 검증과 실제 단계의 사용 경계를 명확히 한다. map 결과 순서 변경은 허용하되 ID 간 key 교환, 한 ID의 다른 key, artifact/revision 불일치는 거부하는 테스트를 추가한다. ID/건수 중복 검사도 유지한다.

## 정상 확인 사항

- 실제 Airflow 3.3.1 DagBag import 오류 없음, 현재 구조 검사 9개 DAG 통과. 변경 범위 orchestration 테스트 19개 통과.
- SDK TriggeringAssetEventsAccessor는 Asset별 AssetEventDagRunReferenceResult 목록을 반환한다. source_dag_run은 실제 source DAG/run으로 조회하는 cached property다. 현재 정상 outlet event의 provenance 접근 방향은 맞다. 출처 없는 event는 None일 수 있으므로 명시적 ValueError로 정규화하면 오류 설명이 더 명확해진다.
- producer는 validate_run 성공 후 event extra를 기록하며 실패/skip attempt에서 임의 update를 만들지 않는다. 성공 후 clear 중복은 논리 dedup으로 처리한다.
- resolver는 triggering events 전체를 사용하고 manual만 Param을 사용한다. lineage 충돌 거부·결정적 정렬·map 상한 처리 방향은 타당하다.
- stage는 단일 READY 목록으로 expand, Databricks는 완성 json/token 묶음으로 expand_kwargs한다. Cartesian product 및 mapped json 내 Jinja 사용은 없다.
- 네 mapped 외부 태스크에 max_active_tis_per_dag=1이 적용된다. prepare_transform_requests는 전체 stage 결과를 모으므로 stage 실패 시 submit 이전에 차단된다.
- transformed→published의 ALL_SUCCESS 의존성은 mapped TaskGroup별 depth-first가 아닌 단계 전체 성공 경계로 해석한다. 일부 원격 작업 실패 시 Exchange를 일괄 보류하는 것은 안전한 범위이며 ‘성공한 index가 즉시 다음 단계로 진행’한다고 확대해 설명하지 않는다.
- confirm_loaded_batch는 unmapped ALL_SUCCESS이고 성공 후 Cosmos/PG가 한 번만 실행된다. identify literal identity, Cosmos env 및 게시 전 검증은 유지된다.

## 재검토 후 통합 검증

실제 2개 READY, 완료 순서 역전, 한 mapped 태스크 실패/skip, 성공 load 재사용 및 1회 게시를 검증한다. 이번에는 DAG 파싱·설치 SDK 소스 확인·순수 함수 재현과 단위 검사만 수행했으며 Asset event 발행, DAG trigger, 클라우드 DB 쓰기는 하지 않았다.
