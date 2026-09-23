# DAG 개발 표준 1차 검토

판정: 수정 후 재검토. DAG 소스는 변경하지 않았다.

## P2 — 신규 작성 규칙과 기존 동작 보존 규칙의 우선순위가 불명확

위치: DAG_DEVELOPMENT_STANDARD.md §4(90-96), §10, §13(214-215).

§4는 기본값을 소량/무변경, 실패 주입 이름을 test_*, 비밀값을 Connection/secret backend로 제한한다. 기존 daily raw는 일일 후보 전체를 기본으로 하고 현재 운영 DAG에는 snowflake_fail_after_movie_merge/postgres_fail_before_activation Params와 런타임 POP_TALK_POSTGRES_PASSWORD 환경 경로가 있다. 이들을 일괄 적용하면 단순 리팩터링에서 Params 계약이나 자격 증명 공급 경로가 변경된다. §13의 동작 보존과 충돌하며 기본값 변경은 예정된 일일 수집 범위도 바꾼다.

수정 권고: 신규 DAG 기본 규칙과 기존 DAG의 호환성 예외를 분리한다. 기존 Params 이름/type/default, Connection ID와 secret 주입 방식, 일정/시각 계산, pause/catchup을 이번 단계에서는 보존한다. 변경은 별도 migration으로 다룬다. 정규 일일 수집은 검증된 운영 범위가 기본일 수 있고, 소량 기본은 수동 검증/신규 위험 작업에 적용한다고 명시한다. 보안 결함 발견은 별도 수정 과제로 분리하되 비밀값 노출 금지는 항상 적용한다.

## P2 — 코드 추출 시 artifact identity와 배포 중 재실행 보존 절차가 빠짐

위치: §1.1, §3, §6, §11, §13.

현재 프로젝트는 bridge/core/notebook/publisher 파일 바이트로 artifact를 만들고 버전별 불변 landing과 revision 소유권을 사용한다. 공통 runtime loader나 외부 helper로 코드를 옮기면 의미가 같아도 hash가 바뀌거나, 새 helper가 hash에서 빠져 실행 코드를 버전이 대표하지 못할 수 있다. 단순히 S3 key와 멱등성을 보존한다는 문구만으로 적용 방법이 정해지지 않는다.

수정 권고: 리팩터링 후 실행 artifact의 전이적 코드 의존성을 식별자에 포함하거나 배포 패키지 digest로 고정한다. 파일 이동에 따른 새 artifact 발급은 데이터 스키마/키 형식 변경과 구분하고, 기존 불변 key 덮어쓰기·기존 revision 탈취 금지를 명시한다. 진행 중인 run은 고정 코드를 사용하거나 변경 감지 후 안전하게 실패시키며 새 run/revision 전환 절차를 문서화한다. 최초 등록 generation/원천 시각/게시 revision/attempt 토큰 의미는 보존한다.

## 권장 보완 — 구조 테스트와 Airflow 경계

- §11 구조 baseline에 task ID/edge/Params뿐 아니라 trigger_rule, retries/timeouts, max_active_runs/tasks, pool, schedule/catchup/timezone/pause, Jinja 참조 field를 포함한다. 성공 게시 태스크가 upstream 실패를 우회하지 않는지 확인한다.
- §5는 dict 반환 또는 dataclass 직렬화 방식과 multiple_outputs 사용 여부를 명시적으로 정해 기존 XCom key·타입을 보존하도록 보완하면 좋다. 타입 주석 도입만으로 출력 형태가 달라지지 않아야 한다.
- §6에 max_active_runs=1은 해당 DAG 내부 제약이며 다른 DAG/직접 Jobs 호출을 직렬화하지 않는다는 범위를 기록한다. Jobs 응답 유실 재시도는 같은 attempt token, 원격 실패 후 새 처리에는 명시적 새 attempt가 필요하다.
- §6의 DB 규칙에는 Snowflake 임시 DDL도 BEGIN 이전, PostgreSQL active+SUCCESS 같은 transaction, commit 불확실 시 재조회, 저장 key/count/실제 payload 대사를 명시하면 앞선 검토 결과를 재사용할 수 있다.
- §9 shell/Jinja 값은 타입/형식 검증 및 안전한 인자 전달을 요구한다. 사용자 run_id/Param을 shell 문자열에 직접 삽입하지 않도록 한다.

## 적절한 부분

parse-time 외부 I/O 금지, runtime Connection 조회, TaskFlow 제어 정보 전달, provider Operator 사용, 업무 로직 추출, timezone-aware 시작일, 진단 DAG 분리, 변화 규모에 따른 검증은 현재 프로젝트에 적절하다. 문서만으로 명백한 Airflow 3.3.1 API 충돌은 발견하지 못했다. 실행 API의 세부 호환성은 실제 리팩터링의 import/구조 검증에서 확인한다.

docstring의 입력/출력/부작용/재시도 기준은 학습과 코드 리뷰에 충분히 구체적이다. 단일 task의 자원/문서량까지 불필요하게 강제하지 않고 비자명한 부분에 집중한 점도 적절하다.

두 P2를 정리한 뒤 표준 승인과 실제 DAG 리팩터링 검토를 분리해 진행한다.
