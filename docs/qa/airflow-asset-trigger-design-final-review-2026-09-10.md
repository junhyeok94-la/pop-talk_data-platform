# Asset 자동 연결 설계 최종 검토

판정: APPROVED — 구현 착수 가능한 설계. 이전 P1/P2 모두 설계 수준에서 종결하며 추가 차단 사항 없음.

## 최종 반영 확인

- stage_bundle / Databricks submit / publish_exchange / load_snowflake 각각 max_active_tis_per_dag=1을 명시했다. 같은 단계의 mapped 작업이 동시에 외부 쓰기를 수행하지 않으며 기존 Cosmos 1-slot pool도 유지한다.
- Databricks json과 idempotency_token을 READY별 완성된 dict로 묶어 expand_kwargs에 전달한다. 독립 expand 축의 Cartesian product 및 mapped 값 안의 Jinja 조회를 금지했다.
- confirm_loaded_batch가 입력/stage/load identity 집합과 건수를 대사하는 ALL_SUCCESS barrier이며, 성공 후 Cosmos와 PostgreSQL 게시를 각각 한 번 수행한다.
- 복수 이벤트 전체 처리, 논리 중복 제거/충돌 거부, max_map_length 초과 복구, queue와 처리 완료 구분, backlog 및 revision/attempt 운영 규칙이 유지됐다.

## 구현 검토 기준

각 단계의 1개 제한은 서로 다른 단계 전체를 하나의 전역 직렬 실행으로 만드는 것은 아니다. 서로 다른 시스템 단계 간 겹침은 허용하되, 동일 외부 단계의 제한이 실제 mapped 태스크에 적용되는지 확인한다.

barrier에서는 raw_run_id 집합/건수뿐 아니라 단계 간 bucket/READY key/artifact/revision 연결도 검증해 같은 run_id를 가진 잘못된 결과로 대체되지 않도록 한다. 기존 stage/load에서 사용하는 run_id/source_run_id와 resolver의 raw_run_id 필드 대응을 명확히 정의한다. 이는 설계의 READY identity 대사를 구체화하는 구현 기준이다.

앞선 재검토에서 확인한 설치 환경의 expand_kwargs 지원은 그대로 유효하다. 실제 복수 입력, 완료 순서 역전, 한 index 실패/skip, 성공 load 재사용, 최종 게시 한 번의 동작은 구현 및 통합 단계에서 증빙한다. identify의 고정 op_kwargs/graph identity와 외부 쓰기 선행 조건도 보존해야 한다.

이번에는 변경된 설계 문서만 재검토했다. DAG/업무 코드 수정, DAG 등록·실행, 외부 데이터 변경은 하지 않았다. 승인은 설계와 구현 착수 범위이며 구현·통합 완료 승인은 별도다.
