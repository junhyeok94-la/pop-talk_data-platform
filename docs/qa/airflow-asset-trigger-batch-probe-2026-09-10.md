# Airflow Asset 다중 입력 격리 통합 검증

## 목적과 범위

통합 검토에서 남은 P2인 다음 동작을 실제 Airflow 3.3.1 scheduler, Asset event,
XCom, dynamic task mapping 경로로 검증했다.

- 한 DagRun에 들어온 서로 다른 A/B 입력을 map index 0/1로 실행하는가?
- 같은 A가 중복되어도 논리 입력 하나로 축약되는가?
- 모든 mapped 입력이 성공해야 batch barrier가 열리는가?
- 한 index가 실패하면 barrier와 후속 단일 실행이 차단되는가?

검증 DAG는 Airflow metadata와 XCom만 사용했다. AWS, Databricks, Snowflake,
PostgreSQL 및 업무 데이터에는 쓰지 않았다.

## 이벤트 누적 방법

운영 `pop_talk_movie_raw_daily`와 `pop_talk_movie_databricks_daily`가 pause이고 실행 중인
DagRun이 없음을 먼저 확인했다. 그 뒤 scheduler를 잠시 중지하고 임시 producer의
`emit_a_first → emit_b → emit_a_duplicate`를 실행했다. 소비자 두 개는 unpause 상태였으며,
AssetDagRunQueue에 두 소비자 항목이 생긴 것을 확인한 다음 scheduler를 다시 시작했다.

검증 시점의 최근 Asset event는 id 4~9로 `A/B/A/A/B/A`였다. scheduler가 만든 각 소비자
DagRun은 event id 4, 5, 6, 7, 즉 `A/B/A/A` 네 건을 한 번에 소비했다. 이는 서로 다른 두
논리 입력과 A 중복을 같은 DagRun에서 처리한 실제 누적 사례다.

## 성공 소비자 결과

- DAG: `pop_talk_asset_batch_probe_success`
- Run: `asset_triggered__2026-09-10T04:01:07.230180+00:00_lEpHpP0Z`
- 최종 상태: `success`
- `resolve` XCom: `['A', 'B']`
- `echo[0]` XCom: `A`
- `echo[1]` XCom: `B`
- `barrier` XCom: `{'input_count': 2, 'logical_ids': ['A', 'B']}`

입력 event 네 건 중 중복 A가 제거되어 두 map index만 생성됐고, 두 index가 모두 성공한 뒤
barrier가 정확히 2개 결과를 검증해 한 번 성공했다. 운영 DAG에서는 이 barrier 뒤에만
Cosmos dbt와 PostgreSQL 게시가 있으므로 batch 단위 후속 실행의 1회 경계가 확인된다.

## 일부 실패 소비자 결과

- DAG: `pop_talk_asset_batch_probe_failure`
- Run: `asset_triggered__2026-09-10T04:01:07.230186+00:00_0ooniURE`
- 최종 상태: `failed`
- `resolve` XCom: `['A', 'B']`
- `fail_b[0]`: `success`, XCom `A`
- `fail_b[1]`: `failed`
- `barrier`: `upstream_failed`, 반환 XCom 없음

B index 하나만 실패하자 Airflow의 mapped dependency가 barrier를 실행하지 않았다. 따라서
운영 DAG에서 barrier 뒤에 있는 Cosmos dbt와 PostgreSQL 게시 역시 실행될 수 없다.

## 결론

단위 helper가 아니라 실제 scheduler가 생성한 Asset-triggered DagRun에서 중복 축약,
2개 dynamic mapping, 성공 batch barrier, 일부 실패 차단을 모두 확인했다. 프로브는 증거
수집 후 삭제하며 운영 Raw/Main DAG는 계속 pause 상태로 유지한다.

## 운영형 다단계 mapping 보완

첫 재검토에서 단순 mapped task가 운영 Main의 다단계 그래프를 충분히 재현하지 않는다는
P2가 남았다. 이에 같은 임시 DAG를 다음 운영형 구조로 확장하고 다시 실행했다.

`resolve → identify/deploy → stage[2] → prepare → classic operator[2] →`
`Exchange[2] → load[2] → 실제 confirm_loaded_batch → dbt 대체 → PG 대체`

`resolve_ready_inputs`와 `confirm_loaded_batch`는
`pipelines.orchestration.asset_contract`의 실제 helper를 사용했다. event가 제공하는 A/B 식별자는
실제 READY 계약 모양의 fixture로 변환했고, 외부 단계 구현만 XCom 반환으로 대체했다.

### 성공 run

- Run: `asset_triggered__2026-09-10T04:16:48.047892+00:00_uVW4o3WZ`
- 소비 event: id 8/9/10, 즉 `B/A/A`
- 최종 상태: `success`
- resolve 결과: A/B의 서로 다른 24자리 raw_run_id와 READY key 두 건
- `stage_bundle`, classic `transform_bronze_silver`, `publish_exchange`,
  `load_snowflake`: 각각 map index 0/1 성공
- index 0: A READY → `probe-transform-aaaaaaaa...` token → A Exchange key → A load
- index 1: B READY → `probe-transform-bbbbbbbb...` token → B Exchange key → B load
- 실제 `confirm_loaded_batch`: `input_count=2`, A/B raw_run_id, artifact
  `probe-artifact-v1`, revision 1로 성공
- `dbt_gold_substitute`: unmapped task instance 1개, `calls=1`, `input_count=2`
- `postgres_substitute`: unmapped task instance 1개, `calls=1`, `input_count=2`

따라서 중복 A는 READY 한 건으로 축약됐고 A/B가 각 map index에서 READY, token, Exchange,
load까지 섞이지 않은 채 실제 batch 계약을 통과했다. batch 뒤 후속 작업은 입력 수와 무관하게
각각 한 번만 실행됐다.

### B transform 실패 run

- Run: `asset_triggered__2026-09-10T04:16:48.047886+00:00_JkJqrXaN`
- 소비 event: id 8/9/10, 즉 `B/A/A`
- 최종 상태: `failed`
- A/B stage와 `prepare_transform_requests`: 성공
- classic `transform_bronze_silver[0]` A: 성공
- classic `transform_bronze_silver[1]` B: 의도된 실패
- `publish_exchange`, `load_snowflake`, `confirm_loaded_batch`, dbt 대체, PG 대체:
  모두 `upstream_failed`, `try_number=0`, 반환 XCom 없음

이는 운영 DAG와 같은 의존성에서 mapped 외부 변환 하나가 실패하면 Exchange 이후의 모든
게시 경계와 단일 Gold/serving 실행이 시작되지 않는다는 것을 입증한다.

검증 후 producer와 두 consumer 및 운영 Raw/Main이 모두 pause인 것을 metadata DB에서
확인했다. 검토가 끝나면 임시 DAG 파일과 세 probe DAG metadata를 제거한다.
