# dbt Gold 재검토

판정: 현재 단일 DAG의 Gold 생성 범위 승인. 이전 P1 종료, 추가 차단 finding 없음.

- dim_movie와 fct_boxoffice_daily가 동일 observation_recency_order 매크로를 사용함을 확인.
- 순서는 source_observed_at → publication_revision → publication_completed_at → source_run_id/artifact_version이다. 승인된 upstream 우선순위와 일치한다.
- assert_revision_precedes_completion_time은 동일 원천 시각에서 rev1 완료03:00/rev2 완료02:00 fixture를 같은 매크로로 정렬하고 rev2가 아니면 실패한다.
- 검토자는 실제 매크로 본문을 추출해 SQLite 메모리 실행으로 revision2 선택을 직접 재확인했다. 원격 Gold는 재생성하지 않았다.
- 구현 작업이 보고한 Snowflake build PASS50(7 models+43 tests)은 원격 실행 근거로 구분한다.

다음 reverse ETL 단계 진행 가능. 실제 서비스 게시 전에는 성공 build의 고정 결과 범위, 기존 서비스 PK 연결, 승인/노출 상태 보존, 변경분 I/U 및 실패 시 게시 원자성을 별도 검토한다. 이번 승인은 그 단계까지 포함하지 않는다.
