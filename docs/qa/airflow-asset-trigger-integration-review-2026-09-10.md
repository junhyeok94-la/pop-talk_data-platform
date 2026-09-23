# Asset 자동 연결 통합 검토

판정: CHANGES REQUESTED — 단일 READY 자동 연결은 수용한다. 다중 READY batch 통합 증빙 P2 한 건이 남아 전체 자동 연결의 통합 승인은 보류한다. P0/P1 발견 없음. 추가 외부 실행이나 업무 코드 수정은 하지 않았다.

## P2 — 두 개의 단일 입력 run으로 복수 이벤트 단일 DagRun을 검증할 수 없음

근거: 통합 문서 §2/§3 및 두 정상 run의 confirm_loaded_batch/attempt=1.log:16.

두 정상 run은 각각 input_count=1이며 네 mapped 외부 태스크는 모두 map_index=0만 실행됐다. 이는 Asset 자동 기동과 DagRun 간 직렬화를 입증하지만, 이전 설계 P1을 해결하기 위해 도입한 한 DagRun의 여러 READY, map index 0/1 대응, 성공/실패 혼합, batch barrier 및 1회 dbt/PG 동작을 검증하지 않는다.

보완안: 격리된 통합 검증으로 서로 다른 A/B 이벤트를 한 DagRun에 연결하고 input_count=2 및 네 단계 index별 READY/token/Exchange/load 대응을 확인한다. 성공 시 Cosmos/PG가 각각 한 번 실행되는지, 하나의 index 실패/skip 시 barrier와 dbt/PG가 차단되는지, 동일 READY 중복은 한 입력으로 축약되는지 확인한다. 실제 scheduler/XCom/mapping 경로를 사용하는 무외부쓰기 대체 태스크도 우선 활용할 수 있다. 순수 helper 단위 테스트만으로 scheduler의 mapped 의존성을 대체하지 않는다. 외부 쓰기 실행은 이 검토에서 수행하지 않았다.

## 독립 확인 결과

Airflow metadata DB, 태스크 로그 및 PostgreSQL을 SELECT로 대조했다.

- sample run RYJQ5tYy: resolve/identify/deploy 성공, stage index 0은 두 attempt 후 failed. 로그에 Sample READY cannot be consumed by the operational transform 확인. transform/Exchange/load/barrier/Cosmos/PG 후속은 upstream_failed, 실행 횟수 0. ‘모든 downstream’은 stage 이후를 의미하며 선행 노트북 배포까지 무변경이었다는 뜻은 아니다.
- 첫 full run Jby6VmlA: 19개 태스크 모두 success. READY raw_run_id=816aa86088f7d8adb2d2b4ca. project_test 로그 120행 PASS=43, WARN/ERROR/SKIP=0.
- 두 번째 full run kDWJz6bc: 19개 태스크 모두 success. READY raw_run_id=2de2f361597fb1060c97b10a. project_test 로그 120행 PASS=43, WARN/ERROR/SKIP=0.
- 첫 full run 태스크 실행 구간 UTC 03:30:40.646037 ~ 03:37:51.357399, 두 번째 UTC 03:37:53.669776 ~ 03:43:37.864957. 두 run의 태스크 실행은 겹치지 않는다.
- Raw metadata는 sample 수동 run, scheduled__2026-09-09T18:00:00+00:00, full 수동 run 모두 success를 나타낸다. 정기 run 실제 시작은 9월 10일 03:24:36 UTC다. run ID의 논리 시각과 실제 시작 시각을 구분해야 한다.
- PG active publication=4948661c16ac9a71cda398e7756b8e74e2e3e1d6f632765ee2c6f465812b15d6, generation=23, movie_count=118, boxoffice_count=80, quality_count=5, activated_at=2026-09-10 03:43:37.439415 UTC. current view count는 각각 118/80으로 일치한다.
- Raw/Main의 현재 is_paused=True, Main AssetDagRunQueue=0 확인.

## pause 설명과 검증 한계

문서 §2는 ‘pause 중에도 새 scheduled run을 생성했다’가 아니라 일시 unpause 시 최신 정기 run이 생성됐다고 설명하므로 관찰과 모순되지 않는다. 완료된 metadata만으로 과거 pause 토글의 정확한 시점까지 복원하지는 않았다. 현재 pause 상태는 직접 확인했다.

두 번째 run의 run_after=03:34:14 UTC와 start_date=03:37:53 UTC를 확인했으며 태스크 시작 지연/비중첩은 입증됐다. run_after 자체를 DagRun 레코드 INSERT 시각으로 간주해서는 안 된다. 중간 queue=1 관찰은 작성자 기록이며 이번 사후 조회는 최종 queue=0을 확인한다.

기존 코드 검토 승인은 유지한다. 이번 결과로 단일 이벤트 운영 경로를 수용하되, 승인된 복수 이벤트 처리 범위까지 통합 완료라고 확대하지 않는다. 위 P2 증빙 후 최종 재검토한다.
