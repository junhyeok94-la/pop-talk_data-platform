# Airflow Asset 자동 연결 설계 검토

판정: CHANGES REQUESTED. 고정 Asset URI + 실행별 extra, 수동 입력 분리, resolve 태스크 방향은 타당하다. 다만 이벤트 누적과 실패 후 복구 계약을 먼저 보완해야 한다. P0 없음. 구현은 하지 않았다.

## P1 — 복수 이벤트 거부만으로는 Raw 자동 처리의 완전성을 보장하지 못함

근거: 설계 59행 및 94-95행.

설치된 Airflow 3.3.1 SchedulerJobRunner._create_dag_runs_asset_triggered를 직접 읽었다. scheduler는 이전 asset DagRun 이후 시간 창의 AssetEvent 목록을 조회해 한 DagRun의 consumed_asset_events에 연결하고, 같은 트랜잭션에서 해당 AssetDagRunQueue 행을 삭제한다. 소비 태스크의 성공을 기다린 뒤 acknowledge하는 작업 큐가 아니다. 단일 고정 Asset이라도 scheduler가 처리하기 전에 여러 업데이트가 쌓이면 복수 event가 같은 run에 연결될 수 있다.

따라서 정상 Raw A/B가 누적되면 resolver는 2건 이상이라 실패하고, 재시도도 동일 triggering events를 받아 실패한다. 자동으로 A/B 각각에 대한 재실행이 생기지 않으며 이후 새 event C가 과거 실패 입력을 다시 포함한다고 보장할 수 없다. max_active_runs=1과 하위 저장소 멱등성은 이 누락을 해결하지 않는다. 잘못된 입력을 처리하지 않는 안전성은 있지만 자동 처리의 진행성과 완전성이 빠져 있다.

필요한 수정:

- 먼저 event를 bucket/ready key/raw_run_id의 논리 입력 기준으로 중복 제거한다. 동일 입력 중복 event와 서로 다른 Raw 실행 여러 건을 구분한다.
- 서로 다른 입력 여러 건을 빠짐없이 처리하는 전략을 명시한다. 예: Asset을 기동 신호로 사용하고 별도 처리 원장에서 미완료 READY를 추적하거나, 제한된 batch 입력을 모두 처리하는 구조. 현재 단일 READY DAG를 유지하려면 실패 run의 모든 입력을 보존해 각각 수동 복구하고 완료를 대사하는 절차까지 설계한다. 이 경우 자동 연결의 승인 범위도 그 운영 제한으로 명시한다.
- queued event 제거 전 해당 event/READY의 처리 또는 의도적 제외가 기록돼 있어야 한다. 큐 삭제를 Raw 처리 완료로 취급하지 않는다.
- 초기 Asset schedule 등록 전 backlog는 catchup=False에서 제외될 수 있으므로 기존 READY 초기 연결 범위와 재처리 대상을 명시한다.
- 단일 정상 이벤트 외에 pause/재개 뒤 서로 다른 2건, scheduler 지연 중 2건, 동일 READY 중복 2건, resolver 실패 후 새 이벤트, 큐 삭제 후 복구 시나리오를 검증한다.

공식 자료: [Airflow 3.3.1 Asset-aware scheduling](https://airflow.apache.org/docs/apache-airflow/3.3.1/authoring-and-scheduling/asset-scheduling.html). 문서는 triggering_asset_events가 Asset별 event 목록이며 처리 정책은 작성자가 결정한다고 설명한다. 큐 삭제 시점과 단일 Asset 시간 창 조회는 설치된 3.3.1 scheduler 소스로 추가 확인했다.

## P2 — 생산 성공 이벤트를 exactly-once로 서술하지 않도록 보완

근거: 설계 47-48행.

실패/skip attempt가 update를 만들지 않는 것과 동일 논리 Raw에 이벤트가 항상 하나뿐이라는 보장은 다르다. 성공한 validate_daily를 clear 후 다시 성공시키거나 같은 READY를 재발행하면 중복 알림이 가능하다. 기존 설계도 중복 이벤트를 언급하지만 resolver는 논리 중복을 처리하기 전에 개수만으로 실패시키도록 되어 있다.

필요한 수정: ‘성공시에만 발행하되 소비자는 중복을 허용’으로 설명하고 event ID와 논리 READY identity를 구분한다. payload 자체의 source_dag_id/source_run_id 문자열뿐 아니라 가능한 실제 event provenance와 READY 내용의 raw_run_id/collection_date도 대사한다. 현재 생산 태스크의 실패→재시도→성공, 성공 후 clear→재성공을 구분해 검증한다. 각 attempt의 본문에서 validate_run 성공 이후 metadata를 설정하는 방향은 맞다.

## 수용 가능한 부분 및 구현 확인사항

- 고정 스트림 URI와 event extra의 동적 실제 S3 key는 적절하다. URI가 실제 파일이라는 가정 없이 제어 이벤트 식별자로 사용한다. 비밀값/본문 제외도 맞다.
- ASSET_TRIGGERED는 해당 run의 triggering_asset_events만 사용하고 MANUAL만 Param을 허용하는 분기가 맞다. 입력 오류 시 최신 이벤트나 기본 Param으로 fallback하지 않는다.
- resolve_ready_input을 별도 태스크로 두고 검증 실패 시 외부 쓰기 전에 중단하는 구조는 명료하다. multiple_outputs=False와 작은 JSON 반환을 유지한다.
- 현재 stage_bundle의 Param 읽기 및 notebook의 params.ready_manifest_key Jinja를 둘 다 resolved XCom으로 변경해야 한다. token 생성도 이 동일 key를 사용해야 한다.
- resolve→identify→deploy와 resolve→stage 데이터 의존성을 보존하고, identify의 고정 op_kwargs/graph identity가 resolver 구현 때문에 runtime current 조회로 되돌아가지 않도록 구조 검사한다.
- 자동 run의 revision/processing_attempt 정책을 명시한다. 새 READY는 기본 1로 시작할 수 있지만 이미 성공/실패한 READY의 코드 변경 재처리는 기존 revision/attempt 운영 규칙을 따라야 한다. 반복 이벤트만으로 임의 승격하지 않는다.
- pause는 신규 자동 실행 운영 제어이며 진행 중/이미 생성된 run의 취소나 데이터 처리 완료 증거가 아니다. 승인 전에는 두 DAG pause 및 신규 외부 실행 금지 상태를 유지한다.

이벤트 누적/복구 정책을 정한 뒤 설계 재검토가 가능하다. 기존 Cosmos 및 저장소 통합 승인 범위는 변경하지 않는다.
