# Asset 자동 연결 통합 최종 검토

판정: APPROVED — 기존 P2의 남은 운영형 다단계 mapping 통합 증빙을 수용하고 종료한다. 이번 검토에서 추가 수정이 필요한 결함은 발견하지 않았다. 기존 코드 검토와 단일 READY 실제 운영 경로 승인도 유지한다.

## 검토 방법

`pop_talk_asset_batch_probe.py`를 운영 `pop_talk_movie_databricks_daily.py` 및 실제 `asset_contract.py`와 비교했다. Airflow metadata에서 DagRun, consumed_asset_events, task_instance, 반환 XCom을 읽기 조회하여 아래 결과를 독립 확인했다. 업무 코드 변경, 새 DAG 실행, 외부 서비스 쓰기는 수행하지 않았다.

프로브는 실제 resolve_ready_inputs/confirm_loaded_batch helper를 사용한다. stage mapping → 결과 집계 → classic operator expand_kwargs → stage 결과 기반 Exchange mapping → load mapping → 실제 batch barrier → unmapped 후속 단계라는 운영 의존성 구조를 보존한다. 이전 단일 echo 프로브에서 빠졌던 다단계 집계와 입력 대응을 이번 검증이 보완한다.

## 성공 경로

- DAG: `pop_talk_asset_batch_probe_success`
- Run: `asset_triggered__2026-09-10T04:16:48.047892+00:00_uVW4o3WZ`
- 실제 소비 이벤트: 8/9/10, B/A/A. resolver 결과는 중복 제거·정렬된 A/B 두 READY다.
- stage_bundle, transform_bronze_silver, publish_exchange, load_snowflake는 각각 index 0/1이 success, try_number=1이었다.
- XCom에서 index 0은 `aaaaaaaaaaaaaaaaaaaaaaaa`, index 1은 `bbbbbbbbbbbbbbbbbbbbbbbb`였다. 각 READY key, prepare 요청의 staged_result, transform 반환 token, Exchange key, load source_run_id가 같은 입력을 유지했다.
- 실제 confirm_loaded_batch는 input_count=2, A/B raw_run_ids, artifact_version=probe-artifact-v1, publication_revision=1을 반환했다.
- dbt_gold_substitute와 postgres_substitute는 각각 map_index=-1인 태스크 인스턴스 하나가 try_number=1로 성공했다. 각 반환값은 calls=1, input_count=2였다. 실행 횟수는 반환값뿐 아니라 태스크 인스턴스와 시도 횟수로 확인했다.

## 일부 실패 경로

- DAG: `pop_talk_asset_batch_probe_failure`
- Run: `asset_triggered__2026-09-10T04:16:48.047886+00:00_JkJqrXaN`
- 동일 이벤트 8/9/10을 소비했고 실제 resolver는 A/B 두 READY를 반환했다.
- A/B stage와 prepare는 성공했다. transform index 0은 성공, index 1은 failed, try_number=1이었다.
- publish_exchange와 load_snowflake는 map_index=-1의 미확장 상태에서 upstream_failed, try_number=0이었다. 두 index가 실행 후 실패한 것이 아니라 후속 실행 자체가 차단된 것이다.
- confirm_loaded_batch, dbt/PG 대체 태스크 역시 upstream_failed, try_number=0이며 반환 XCom이 없었다.

따라서 운영과 같은 의존성 구조에서 한 변환 입력 실패가 Exchange 이후 및 Gold/serving 게시 경계로 전파되는 것을 확인했다.

## 승인 범위와 검증 한계

기존 단일 READY 실제 end-to-end 결과, 기존 구조/계약 검토, 이번 복수 입력 운영형 scheduler/XCom 검증을 함께 근거로 Asset 자동 연결 통합을 승인한다.

이번 프로브의 classic operator는 DatabricksSubmitRunOperator 대신 PythonOperator이며, source provenance는 실제 생산자 metadata를 추출하는 대신 계약 fixture로 구성한다. token도 fixture이고 dbt/PG는 대체 태스크다. 따라서 이번 결과를 복수 READY의 실제 클라우드 변환·트랜잭션·원격 재시도 검증으로 해석하지 않는다. retry/skip 사례도 이번 실행 범위 밖이다. 이는 기존 P2에서 허용한 무외부쓰기 보완 방식의 명시적 범위이며 추가 차단 사유는 아니다.

조회 시 운영 Raw/Main 및 프로브 producer/consumer 세 DAG가 모두 paused였다. 임시 프로브 파일과 metadata 정리는 구현 작업에서 증빙 보존 후 진행할 수 있다. 이 검토에서는 삭제하거나 운영 스케줄을 변경하지 않았다.
