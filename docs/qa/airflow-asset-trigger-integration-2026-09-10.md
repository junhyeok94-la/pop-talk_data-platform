# Airflow Asset 자동 연결 통합 검증

## 최종 결과

Raw 수집 완료 → Asset event → Main 자동 DagRun → Databricks Bronze/Silver → S3 Exchange →
Snowflake load → Cosmos dbt Gold → PostgreSQL serving을 실제 클라우드 연결로 끝까지 검증했다.
정상 full 입력 2건의 Main DagRun이 순차 성공했으며 마지막 활성 serving은 영화 118건,
박스오피스 80건이다.

검증 종료 후 `pop_talk_movie_raw_daily`와 `pop_talk_movie_databricks_daily`는 모두 pause,
Main의 queued Asset event는 0건이다.

## 1. sample 안전장치와 실패 전파

- Raw run: `manual__asset_acceptance_20260910T1221`
- Params: `days_back=1`, `max_movies=10`, `kmdb_max_pages=1`
- Asset event id: 1
- raw_run_id: `4679c311db8e8ef3a3a161a2`
- 자동 Main run:
  `asset_triggered__2026-09-10T03:24:35.338905+00:00_RYJQ5tYy`
- 결과: 의도된 실패

Raw는 성공했고 Asset-triggered Main의 `resolve_ready_inputs`도 성공했다. 그러나 후보를 10건으로
절단한 READY는 `sample=true`라 `stage_bundle`의 운영 변환 계약이
`Sample READY cannot be consumed by the operational transform`으로 거부했다. 1회 재시도 후
Main은 실패했고 Exchange, Snowflake, dbt 8개 task, PostgreSQL은 모두
`upstream_failed`였다. 검증용 일부 표본이 serving 전체를 교체하지 못하는 안전장치와 실패
전파를 확인했다.

## 2. full 일일 Raw와 첫 정상 자동 실행

Raw를 통합 검증 중 잠시 unpause했을 때 오늘의 정기 run도 생성됐다.

- Raw run: `scheduled__2026-09-09T18:00:00+00:00`
- raw_run_id: `816aa86088f7d8adb2d2b4ca`
- Asset event id: 2
- 자동 Main run:
  `asset_triggered__2026-09-10T03:30:40.183618+00:00_Jby6VmlA`
- 결과: SUCCESS

`max_movies=0`의 완전한 일일 후보 범위였고 mapped map index 0의 stage, Databricks,
Exchange, Snowflake와 batch barrier가 성공했다. 이후 Cosmos 모델 7개와 project test,
PostgreSQL 게시까지 성공했다.

## 3. 대기 event와 두 번째 정상 자동 실행

수동 full run은 정기 run이 이미 진행 중이라 queued였으나, 정기 run 종료 직후 시작됐다.
실행 중 DagRun을 강제 실패 처리하면 외부 수집과 metadata가 어긋날 수 있어 중단하지 않고
완료시켰다.

- Raw run: `manual__asset_acceptance_full_20260910T1228`
- Params: `days_back=1`, `max_movies=0`, `kmdb_max_pages=1`
- Asset event id: 3
- 자동 Main run:
  `asset_triggered__2026-09-10T03:34:14.188250+00:00_kDWJz6bc`
- 결과: SUCCESS

첫 Main이 실행 중일 때 event 3은 queue에 1건으로 대기했고, 첫 run 종료 뒤 두 번째 Main의
태스크가 시작됐다. 두 번째 run의 19개 정적/mapped task가 모두 성공했다.

- `confirm_loaded_batch`: SUCCESS
- Cosmos model task: 7개 SUCCESS
- dbt project test: `PASS=43 WARN=0 ERROR=0 SKIP=0 TOTAL=43`
- `publish_postgres`: SUCCESS

## 4. 최종 PostgreSQL serving 대사

`dw_serving.active_publications_v3`와 `dataset_publications_v3`, current view를 직접 조회했다.

| 항목 | 값 |
| --- | --- |
| dataset | `movie_gold` |
| publication_id | `4948661c16ac9a71cda398e7756b8e74e2e3e1d6f632765ee2c6f465812b15d6` |
| generation | 23 |
| movie_count / current view | 118 / 118 |
| boxoffice_count / current view | 80 / 80 |
| quality_count | 5 |
| activated_at | `2026-09-10 03:43:37.439415+00` |

## 5. 운영 관찰

- Airflow pause는 새 정기 DagRun만이 아니라 현재 run의 아직 시작하지 않은 태스크 scheduling도
  멈춘다. 통합 실행 중 Raw를 너무 일찍 다시 pause하자 후속 태스크가 정지했고, 완료까지
  unpause를 유지해야 했다.
- 일일 cron 시간이 이미 지난 상태에서 Raw를 unpause하면 최신 scheduled run이 즉시 생성될
  수 있다. 운영 전환 절차는 이 동작을 예상해야 한다.
- `max_active_runs=1`은 두 번째 Asset DagRun 레코드 생성을 막는 것이 아니라 첫 run이 끝날
  때까지 그 태스크 시작을 지연시켰다. 두 run의 외부 task 실행은 겹치지 않았다.
- sample READY 실패는 정상 운영 보호 동작이다. 실제 end-to-end 검증에는 후보 절단이 없는
  작은 날짜 범위를 사용해야 한다.

## 6. 정적/단위 검증

- DagBag import error 0, 9개 DAG 구조 계약 통과
- pipeline 전체 단위 테스트 52개 통과
- Asset 계약 집중 테스트 7개 통과
- Raw/Main 최종 pause, queued Asset event 0

## 7. 통합 검토 요청

1. 위 세 event와 세 자동 Main 결과가 정상/실패 경로를 충분히 입증하는가?
2. pause/unpause 중 생긴 scheduled run과 뒤이은 queued event의 처리 기록이 완전한가?
3. Snowflake/dbt/PostgreSQL의 성공 및 최종 건수 대사가 충분한가?
4. 1차 운영 전환 전에 추가로 필요한 복수 event 단일 DagRun 또는 재시도 검증이 있는가?

## 8. 다중 READY 격리 보완 검증

최초 검토에서 요청한 다중 입력 증거는
`docs/qa/airflow-asset-trigger-batch-probe-2026-09-10.md`에 보완했다. 실제 Airflow
scheduler가 `A/B/A/A` Asset event를 한 DagRun에 연결했고 resolver는 `A/B` 두 입력으로
축약했다. 성공 소비자는 map index 0/1과 barrier가 성공했으며, 실패 소비자는 B index 실패로
barrier가 `upstream_failed`가 됐다. 외부 서비스와 업무 데이터에는 쓰지 않았다.
