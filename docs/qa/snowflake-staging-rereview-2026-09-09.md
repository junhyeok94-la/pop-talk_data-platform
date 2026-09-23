# Snowflake STAGING 재검토

판정: 현재 단일 DAG 직렬 실행 범위 승인. 이전 P1/P2 종료. 추가 차단 finding 없음.

## 확인 결과

- 영구/임시 테이블 DDL과 executemany 입력 준비가 모두 BEGIN 이전에 위치한다. BEGIN 이후 대상 conflict 검사, ledger/관측 DML, persisted 대사 및 SUCCESS만 수행한다.
- 성공 replay도 verify_persisted를 호출해 실제 관측 건수와 incoming 업무 키/저장 hash를 대사한다. 정상 최초 처리와 같은 검증 경로다.
- 검토자 가짜 연결 검사: 정상 replay 성공, 저장 행 누락 거부, 저장 hash 불일치 거부, 예외 시 rollback 호출 확인. 실행된 명령에서 BEGIN 이후 CREATE 부재 확인.
- 단위 테스트 33개 재실행 통과.
- transaction probe는 기존 run과 다른 identity에 movie MERGE를 시도한 직후 예외를 발생시키고 rollback 후 같은 연결에서 movie/ledger 모두 0임을 요구한다. 예외 발생만으로 성공 판정하지 않는다. 제공된 실제 probe 기록과 코드 구조가 부합한다.
- publication revision은 immutable observation의 최초 값 대신 SUCCESS ledger와 run/artifact로 결합해 선택하도록 후속 계약에 명시됐다.

## 검증 범위와 기록 보완

검토자는 원격 probe를 재실행하지 않았다. 실제 0/0 결과는 구현 작업의 실행 기록이며 이번 직접 검증은 코드와 메모리 가짜 연결 및 단위 테스트다. 여러 독립 실행의 경쟁 또는 외부 작업이 PAYLOAD와 저장 hash를 따로 수정하는 상황까지 검증했다는 의미는 아니다.

기존 QA의 최초 SQL 컴파일 오류에 대한 'transaction rollback 후 수정'은 rollback 호출과 완전 원복을 구분해 읽어야 한다. 실제 원자성 근거는 수정 후 격리 probe의 0/0 검증이다. 이 설명은 과거 실행의 원복을 소급 보장하지 않는다.

다음 dbt 단계 진행 가능. 성공 ledger 기반 revision 선택, 원천 관측 시각 우선순위, 빈 실행 및 같은 artifact 재게시를 dbt 모델 계약에서 검증한다.
