# Asset 자동 연결 통합 재검토

판정: CHANGES REQUESTED — 보완 프로브의 결과는 수용한다. 기존 코드 검토 및 단일 READY 운영 경로 승인은 유지한다. 기존 P2 중 운영 DAG와 동등한 다단계 mapping 검증은 남아 있다. 새로운 P0/P1 결함은 발견하지 않았다.

## 독립 확인한 보완 결과

프로브 소스, 통합 문서 및 Airflow metadata의 consumed_asset_events, task_instance, XCom을 대조했다. 이번 재검토는 읽기 조회로 진행했으며 클라우드 실행이나 업무 데이터 쓰기는 수행하지 않았다.

- 성공 run: `asset_triggered__2026-09-10T04:01:07.230180+00:00_lEpHpP0Z` (`pop_talk_asset_batch_probe_success`).
- 실패 run: `asset_triggered__2026-09-10T04:01:07.230186+00:00_0ooniURE` (`pop_talk_asset_batch_probe_failure`).
- 두 run 모두 event 4/5/6/7, 즉 A/B/A/A를 실제 소비했다. resolve XCom은 모두 `["A", "B"]`였다.
- 성공 run의 echo map index 0/1은 각각 A/B로 성공했다. unmapped barrier는 한 번 성공했고 XCom은 `{"input_count": 2, "logical_ids": ["A", "B"]}`였다.
- 실패 run의 fail_b index 0은 성공, index 1은 실패했다. barrier는 `upstream_failed`, try_number=0이며 반환 XCom이 없었다.
- 조회 시 운영 Raw/Main은 모두 paused 상태였다.

따라서 실제 scheduler의 복수 이벤트 결합, 프로브 resolver의 중복 축약, 두 mapped instance 실행 및 단일 단계 실패의 barrier 전파에 관한 증빙 부족은 해소됐다.

## 남은 P2 — 프로브가 운영 다단계 mapping 경로를 재현하지 않음

근거: `orchestration/airflow/dags/_pop_talk_asset_batch_probe.py`의 성공/실패 consumer와 `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py`의 191–316행, 461행.

프로브는 독립 resolver → echo/fail_b 한 단계 → 독립 barrier만 실행한다. 운영 경로의 stage 결과 집계 → prepare_transform_requests → classic operator expand_kwargs → stage 결과를 다시 사용하는 publish_exchange → load_snowflake → 실제 confirm_loaded_batch는 실행하지 않는다. 운영 resolver와 barrier helper도 사용하지 않는다. 따라서 READY/token/Exchange/load가 A/B별로 끝까지 대응하는지, 한 입력 실패 시 운영 의존성이 모든 후속 단계를 차단하는지에 관한 통합 증거를 이 프로브로 대체할 수 없다. 이전 두 운영 성공 run은 여전히 각각 input_count=1이다.

보완은 외부 서비스 호출 없이 가능하다. 운영 DAG와 동일한 mapping/집계/의존성 구조를 보존하고 외부 I/O만 대체한 격리 DAG에서 A/B/A를 처리한다. 실제 계약 helper에 맞는 fixture로 index별 raw_run_id, READY key, transform token, Exchange key, load 결과를 확인하고, 성공 시 barrier 뒤 dbt/PG 대체 태스크가 각각 한 번 실행되는지 확인한다. B 실패 시 barrier 및 게시 대체 태스크의 미실행도 확인한다. 이렇게 하면 추가 클라우드 쓰기 없이 기존 P2의 남은 범위를 닫을 수 있다.

## 판정 범위

이번 보완에서 실제 구현 오류가 새로 재현된 것은 아니다. 남은 항목은 승인 대상인 복수 READY 운영 흐름과 통합 검증 그래프의 차이다. 단일 READY end-to-end 성공, 기존 구조/단위 검증, 이번 scheduler 프로브를 각각 수용하되 이들을 운영 다중 READY end-to-end 검증 완료로 확대하지 않는다. 재시도/skip 경로 역시 이번 프로브에서 실행하지 않았다.
