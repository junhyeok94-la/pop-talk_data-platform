# Databricks 일일 단계 3차 재검토

판정: 현재 단일 Airflow DAG를 통한 직렬 실행 범위에서 승인. 2차 지적 P1/P2 종료. 추가 차단 finding 없음.

## 직접 확인

- stage_bundle(deployed)로 배포 결과를 받아, 읽은 artifact 버전을 비교한 뒤에만 S3/Volume staging을 시작한다. submit은 이 배포 경로와 staging 결과를 소비한다. 중간 소스 변경의 버전 혼합 경로 차단 확인.
- publication_revision은 양의 정수 파라미터/widget이며 submit token과 ledger MERGE key 및 replay 조건에 포함된다.
- READY/revision에 다른 artifact가 있으면 replay나 RUNNING 쓰기 전에 거부한다. 실패한 artifact에 이미 할당된 revision도 재사용하지 않는다.
- current view는 성공 ledger만 조인하며 원천 관측 시각, publication revision, 완료 시각 순으로 정렬한다. 동일 원천의 낮은 revision이 늦게 성공해도 높은 revision을 밀어내지 않는다.
- 관측은 raw run/artifact별 불변 데이터로 재사용하고 revision/attempt는 ledger에서 관리한다. 따라서 view join에 attempt/revision을 관측 키로 추가하지 않은 것은 이 구조에서 타당하다.
- 관측 병합 및 persisted count 검증 이후에 SUCCESS를 기록한다.
- 변환/bridge 테스트 25개 재실행 통과. 배포 후 소스 변경 차단 및 revision 우선순위 회귀 포함.

## 승인 범위와 검증 한계

이 승인은 현재 max_active_runs=1인 DAG를 통해 실행하는 일일 Bronze/Silver 단계에 대한 코드 검토 승인이다. 원격 실패 주입이나 모든 Delta 병행 실행을 검증했다는 의미는 아니다. 구현 작업이 제공한 원격 성공 기록과 별개로 검토자는 로컬 컨테이너 테스트와 코드 경로를 재확인했다.

revision 소유권 검사는 조회 후 기록 방식이며 분산 원자적 claim은 아니다. 다른 DAG/직접 Jobs 호출로 별도 artifact를 병행 제출하도록 실행 경로를 늘릴 때는 READY/revision 예약의 원자성 또는 별도 single-writer 조정이 필요하다. 현재 단일 직렬 DAG 범위의 차단 사항으로 보지 않는다.

공개 view 정의 변경은 모든 버전이 공유하는 스키마 변경이므로 향후 view 계약을 바꿀 때는 실행 중인 구버전과 호환성을 검토한다. 다음 Snowflake/서비스 DB 단계에서는 공개 revision 고정, KMDb 기존 확정값 보존, 서비스 승인 상태 보존 및 변경분 게시를 별도 검증한다.
