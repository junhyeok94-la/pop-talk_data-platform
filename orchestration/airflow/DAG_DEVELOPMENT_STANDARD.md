# Pop Talk Airflow DAG 개발 표준

이 문서는 Pop Talk DW 프로젝트의 Airflow 3.3.1 DAG를 작성하고 검토하는 기준이다.
목표는 단순히 짧은 코드가 아니라, 데이터 흐름과 실패·재실행 의미를 팀원이 코드에서
설명할 수 있는 DAG를 만드는 것이다.

## 1. 핵심 원칙

### 1.1 DAG는 처리 코드가 아니라 실행 지도다

DAG 파일에는 다음 내용만 둔다.

- 언제 무엇을 실행하는지 보여 주는 태스크 정의와 의존 관계
- Connection ID, 저장소 위치, 실행 제한처럼 흐름을 이해하는 데 필요한 설정
- 각 태스크가 런타임 모듈을 호출하는 얇은 어댑터

API 순회, 파일 검증, 체크섬 계산, 변환, SQL 적재, 트랜잭션 게시 같은 업무 로직은
`orchestration/airflow/modules/pipelines/` 아래의 일반 Python 모듈에 둔다. 이 모듈은 Airflow 없이 단위 테스트할 수
있어야 한다.

### 1.2 설명은 코드의 동작보다 계약과 이유를 기록한다

`count += 1`에 “count를 1 증가”라고 적는 주석은 쓰지 않는다. 대신 아래처럼 코드만으로
알기 어려운 내용을 설명한다.

- 입력이 어떤 검증을 통과했다고 가정하는가
- 출력/XCom이 다음 태스크에 무엇을 보장하는가
- S3, 플랫폼 PostgreSQL, 서비스 PostgreSQL 중 어디를 변경하는가
- 재시도할 때 같은 결과를 재사용하는가, 새 실행을 만드는가
- 어떤 실패는 안전하게 재시도할 수 있고 어떤 실패는 새 DAG run이 필요한가

모듈 docstring에는 전체 흐름과 완료 조건을, 비자명한 태스크 docstring에는
입력·출력·부작용·재시도 계약을 적는다. 상세 데이터 계약은 가까운 `orchestration/airflow/modules/pipelines/` 모듈이나
운영 문서에 두고 링크한다.

### 1.3 태스크 경계는 재시도와 관측 경계다

다음 중 하나가 달라질 때 태스크를 나눈다.

- 외부 시스템 또는 자격 증명
- 독립적으로 재시도할 수 있는 부작용
- 운영자가 별도로 확인해야 하는 건수·manifest·publication
- 실행 시간, timeout, pool 또는 실패 대응 방식

함수 한 줄마다 태스크를 만들거나, 반대로 여러 외부 시스템을 한 태스크에서 연속으로
변경하지 않는다.

## 2. 파일 구조와 이름

DAG 파일은 아래 순서를 따른다.

1. 흐름과 완료 조건을 설명하는 모듈 docstring
2. `from __future__ import annotations`
3. 표준 라이브러리, 외부 패키지, Airflow import
4. Connection ID와 고정 설정 상수
5. 순수 helper 또는 DAG 밖 런타임 어댑터
6. `@dag` 함수와 태스크 정의
7. 파일 마지막의 DAG 인스턴스 생성

명명 규칙은 다음과 같다.

- DAG ID: `pop_talk_<도메인>_<주기 또는 목적>`
- 태스크 ID/함수: `동사_대상` (`collect_boxoffice`, `load_staging`)
- Connection ID: `pop_talk_<서비스>`
- 결과 변수: 완료 상태를 나타내는 과거분사 (`staged`, `published`, `loaded`)
- 진단용 DAG에는 `canary`, `probe`, `check` 중 하나를 이름과 tag에 명시

파일마다 따옴표·들여쓰기·줄바꿈 스타일을 통일하고, 한 줄에 여러 의미를 압축하지 않는다.
공통 문자열을 복사하지 말고 이름 있는 상수나 설정 객체로 올린다.

## 3. 파싱 안전성

DAG import 시점에는 다음 작업을 금지한다.

- API, S3, 데이터베이스, Airflow Connection 접속
- 비밀값 또는 실행 시점 환경변수 조회
- 대용량 파일 탐색·해시 계산
- 데이터를 변경하는 코드

외부 연결과 프로젝트 런타임 모듈 import는 태스크 실행 시점에 수행한다. 단,
Airflow 이미지에 설치되어 있고 import 부작용이 없는 provider/operator는 상단에서 import할
수 있다. DAG 파싱은 네트워크 없이 빠르고 결정적이어야 하며 `dags list-import-errors`가
항상 0건이어야 한다.

프로젝트 경로를 태스크마다 직접 `sys.path`에 넣지 않는다. 당장은 하나의 공통 runtime
loader를 사용하고, 장기적으로는 `pipelines`를 이미지에 설치 가능한 패키지로 만든다.

## 4. 설정, Params, 자격 증명

- 신규 DAG의 비밀값은 Airflow Connection 또는 secret backend에 저장한다. 컨테이너
  secret/env로 이미 공급되는 기존 PostgreSQL 자격 증명은 이번 동작 보존 리팩터링에서
  경로를 바꾸지 않는다. 공급 방식 변경은 배포 설정과 함께 별도 migration으로 검토한다.
- 비밀번호, PAT, API key를 Params, XCom, manifest, 예외, 로그에 넣지 않는다.
- bucket, catalog, database, schema, connection ID는 상수 또는 환경별 설정으로 관리한다.
- 사용자 입력은 `Param`의 type, 범위, pattern, description으로 검증한다.
- 신규 수동 검증 또는 위험한 작업의 기본값은 소량 실행이나 무변경처럼 비용과 부작용이
  안전해야 한다. 검증된 정규 일일 수집 DAG는 약속된 일일 운영 범위를 기본값으로 삼을 수
  있다.
- 신규 장애 주입 값은 일반 운영 Params와 섞지 않는 것이 원칙이다. 꼭 필요하면 이름에
  `test_`를 붙이고, 수동 실행 전용·기본값 `false`로 제한하며 문서화한다.

기존 DAG를 동작 보존 리팩터링할 때에는 기존 Params의 이름/type/default, Connection ID와
secret 주입 방식, schedule, 업무 시각 계산, pause/catchup 계약을 우선 보존한다. 위 신규
규칙에 맞추기 위한 인터페이스 변경은 별도 migration으로 다룬다. 단, 실제 비밀 노출 같은
보안 결함은 호환성 예외로 허용하지 않고 즉시 차단한 뒤 영향 범위를 별도 검토한다.

## 5. 태스크 입력과 출력

XCom에는 작은 제어 메타데이터만 전달한다.

- 허용: S3 key, run ID, artifact version, publication ID, 행 수
- 금지: 원천 JSON 본문, DataFrame, 자격 증명, 대용량 key 목록

여러 필드를 전달하는 결과는 `TypedDict` 또는 불변 `dataclass`로 계약을 정의한다.
Airflow 직렬화 경계의 기본 형식은 하나의 JSON 직렬화 가능 dict 반환이며
`multiple_outputs=False` 의미를 유지한다. `multiple_outputs=True`나 dataclass 자체 직렬화를
사용하려면 backend 호환성과 기존 XCom key/type 변화를 구조 테스트로 확인한다. 타입 주석을
도입했다는 이유로 기존 출력 형태를 바꾸지 않는다. 결과에는 후속 단계가 무결성과 동일 실행
여부를 검증할 식별자를 포함한다.

Airflow 3.3.1은 `-> dict[...]` 반환 타입만으로 `multiple_outputs=True`를 추론할 수 있다.
따라서 단일 dict를 반환하는 태스크는 `@task(multiple_outputs=False)`를 명시하고 구조
baseline에서도 이 값을 검사한다.

데이터 의존성이 이미 TaskFlow 인자로 표현되면 동일한 순서를 `>>`로 중복 선언하지 않는다.
데이터 전달이 없는 제어 의존성에만 `>>`를 사용한다.

## 6. 재시도, 멱등성, 동시성

모든 외부 부작용 태스크는 재실행 계약을 가져야 한다.

- S3 Raw는 불변 key와 조건부 쓰기를 사용한다.
- 완료 marker(`SUCCESS`, `READY`)는 데이터와 검증 결과를 모두 쓴 뒤 마지막에 게시한다.
- PostgreSQL 다중 테이블 적재는 하나의 게시 단위로 commit/rollback한다.
- 서비스 게시는 기존 영화 ID·관리자 상태·리뷰를 보존하며 원천 소유 필드만 갱신한다.
- 늦게 끝난 과거 실행이 최신 데이터를 되돌리지 못하도록 revision/generation을 비교한다.

쓰기 DAG는 기본적으로 `max_active_runs=1`을 명시한다. API quota 또는 공유 자원을 보호해야
하면 `max_active_tasks`, pool, concurrency 제한을 함께 사용한다. retry 횟수, 지연,
exponential backoff와 execution timeout은 외부 시스템의 실패 특성에 맞춰 명시한다.
`max_active_runs=1`은 같은 DAG의 run만 제한하며 다른 DAG나 직접 실행한 적재 스크립트까지
직렬화하지 않는다. 여러 진입점이 같은 자원을 변경하면 pool 또는 대상 시스템의 lock/ledger로
보호한다. 실행 결과가 불확실하면 같은 입력 식별자로 처리 이력을 확인한다.

PostgreSQL 서비스 게시에서는 입력 검증, 영화·흥행 갱신과
성공 상태 기록을 같은 트랜잭션으로 처리한다. commit 결과가 불확실하면 같은 식별자로 저장
상태를 재조회하며, key·count뿐 아니라 실제 canonical payload digest도 대사한다.

### 6.1 실행 artifact 전환

실행 artifact 식별자는 notebook/core뿐 아니라 실행 결과에 영향을 주는 공통 helper와 설정의
전이적 코드 의존성을 모두 포함해야 한다. 개별 파일 목록을 유지하기 어렵다면 배포 패키지의
결정적 digest를 사용한다. DAG orchestration 표현만 바뀌고 원격 실행 코드가 바뀌지 않는다면
왜 artifact가 유지되는지 테스트 또는 검토 기록으로 입증한다.

리팩터링 때문에 코드 digest가 달라지면 새 artifact를 발급하되, 이를 데이터 schema나 S3 key
형식의 변경으로 간주하지 않는다. 기존 불변 landing/exchange key를 덮어쓰거나 기존
publication revision을 새 artifact가 차지하면 안 된다. 진행 중인 run은 시작할 때 고정한
artifact를 끝까지 사용하거나 중간 변경을 감지해 안전하게 실패한다. 변경 감지 실패 후에는
새 DAG run과 새 publication revision으로 전환한다. 원천 관측 시각, 최초 등록 generation,
publication revision과 processing attempt/idempotency token의 기존 의미를 보존한다.

## 7. 시간과 스케줄

- 시간대가 필요한 DAG는 `pendulum.datetime(..., tz="Asia/Seoul")`을 사용한다.
- naive `datetime`을 사용하지 않는다.
- `schedule`, `catchup`, 시작일, pause 기본 상태를 항상 명시한다.
- 일일 수집의 업무 날짜는 논리적 data interval에서 계산한다. 수동 실행의 기준 시각이
  다르면 그 규칙을 함수명과 docstring에 설명한다.
- 전체 backfill과 일일 증분은 비용·재시도 계약이 다르므로 별도 DAG로 유지한다.

## 8. 로그, 오류, 관측성

로그와 XCom 결과에는 최소한 다음 식별자를 남긴다.

- Airflow run ID 또는 원천 run ID
- manifest/READY key
- artifact/model/publication version
- 입력·출력·격리 행 수

비밀값과 전체 payload는 출력하지 않는다. 예외 메시지는 작업 단계와 안전한 식별자를
포함하되 HTTP 응답 본문이나 인증 URL을 그대로 포함하지 않는다. 오류를 빈 결과로 바꾸지
않고, “정상 0건”과 “수집/검증 실패”를 구분한다.

### 8.1 Asset 기반 DAG 연결

- Asset URI는 실행마다 바뀌는 파일명이 아니라 팀이 합의한 논리 데이터 스트림을 안정적으로
  식별한다. 실제 manifest key와 run lineage는 event `extra`에 넣는다.
- Asset URI와 event `extra`는 metadata DB에 평문 저장되므로 비밀값과 원천 본문을 넣지 않는다.
- 생산 태스크는 데이터와 READY 검증을 모두 끝낸 뒤 outlet event metadata를 설정한다.
  성공 event는 exactly-once 알림이 아니므로 소비자는 동일 논리 입력의 중복을 허용한다.
- 소비 DAG는 `triggering_asset_events`에서 이번 DagRun을 만든 event를 읽는다. Asset 실행에서
  최신 event나 수동 Param으로 fallback하지 않는다.
- scheduler는 여러 event를 한 DagRun에 묶을 수 있다. 서로 다른 입력을 누락하지 않도록
  batch/mapping 또는 처리 원장 전략을 두고, queue 삭제를 처리 완료 acknowledge로 보지 않는다.
- mapped 외부 쓰기는 `max_active_tis_per_dag`나 pool로 같은 DagRun 안의 동시성을 명시한다.
  모든 map index의 성공과 lineage를 대사하는 barrier 뒤에서만 공통 Gold/serving을 게시한다.

## 9. Operator와 코드 표현

- Python 흐름과 작은 메타데이터 전달은 TaskFlow `@task`를 사용한다.
- Bash처럼 provider가 재시도·상태 추적을 제공하는 작업은 공식 Operator를
  사용한다.
- 긴 SQL과 shell 문자열은 DAG 안에 묻지 않고 `.sql`, 스크립트 또는 테스트 가능한 모듈로
  분리한다.
- shell/Jinja 인자는 허용 형식과 타입을 먼저 검증하고 가능한 한 환경변수 또는 인자 배열처럼
  경계가 분명한 방식으로 전달한다. 사용자 `run_id`나 Param을 검증 없이 shell 명령에 직접
  삽입하지 않는다.
- Jinja/XCom 문자열 참조가 필요한 Operator에는 참조 대상 task ID와 결과 field를 상수화하거나
  가까운 주석으로 계약을 남긴다.
- 외부 클라이언트는 context manager 또는 `try/finally`로 닫는다.

## 10. 진단 DAG 예외

canary/probe/check DAG는 운영 흐름과 분리한다. 작은 단일 목적 때문에 일부 업무 로직을
DAG에 둘 수 있지만 다음 조건을 모두 만족해야 한다.

- 수동 실행 전용이며 기본 pause 상태다.
- 읽기 전용인지, 임시 데이터를 쓰고 rollback하는지 description에 명시한다.
- 고정된 probe 식별자가 실제 업무 key와 충돌하지 않는다.
- 성공 조건을 코드와 docstring에서 바로 확인할 수 있다.
- 운영 DAG가 probe용 실패 주입에 의존하지 않는다.

## 11. 테스트와 검토 게이트

변경 종류에 따라 아래 검증을 수행한다.

1. 일반 Python 업무 로직 단위 테스트
2. DAG import 오류 0건
3. DAG ID, task ID, 의존 관계, Params 기본값을 확인하는 구조 테스트
4. 데이터 계약 변경 시 manifest/JSONL/SQL schema 계약 테스트
5. dbt 변경 시 `dbt build`
6. 외부 연동 변경 시 최소 표본 canary 또는 수동 통합 실행
7. 게시·재시도 변경 시 동일 입력 2회, 중간 실패, stale 실행 시나리오

구조 baseline에는 DAG/task ID와 edge 외에도 `trigger_rule`, retries/timeouts,
`max_active_runs`, `max_active_tasks`, pool, schedule/catchup/timezone/pause, Params의
name/type/default, Operator의 Jinja 참조 task/field를 포함한다. 최종 성공 게시가 upstream
실패를 우회할 수 있는 trigger rule인지도 검사한다.

리뷰어는 스타일보다 먼저 다음 질문에 답한다.

- DAG 파일만 읽어 전체 흐름과 완료 조건을 설명할 수 있는가?
- 각 외부 쓰기가 실패하거나 재시도될 때 데이터가 일관적인가?
- 코드 변경 중인 artifact와 실제 실행된 artifact를 혼동하지 않는가?
- 원천부터 serving까지 lineage 식별자가 이어지는가?
- 자격 증명과 대량 데이터가 XCom/로그에 노출되지 않는가?

## 12. 완료 정의

DAG 변경은 다음을 모두 만족해야 완료다.

- 모듈 docstring에 목적, 흐름, 완료 조건이 있다.
- 비자명한 태스크에 입력·출력·부작용·재시도 의미가 설명되어 있다.
- DAG는 orchestration에 집중하고 업무 로직은 단위 테스트 가능한 모듈에 있다.
- Params, 시간대, 동시성, retry, timeout이 명시되어 있다.
- XCom은 작은 제어 정보만 포함한다.
- import/구조/단위 테스트가 통과한다.
- 변경한 외부 부작용에 비례한 통합 검증 증빙이 있다.
- 관련 운영 문서와 실행 예제가 현재 코드와 일치한다.

## 13. 기존 DAG 적용 순서

1. `pop_talk_movie_raw_daily`, `pop_talk_movie_raw_initial`: 공통 runtime 생성과 수집 단계
   설명을 통일한다.
2. `pop_talk_environment_check`, `pop_talk_s3_canary`: 진단 DAG 예외 기준과 시간대·pause 기준을 맞춘다.
3. PostgreSQL STG·DW/Mart DAG는 Phase 1에서 새로 구현한다.

리팩터링은 데이터 계약, S3 key, task ID, schedule과 멱등성 의미를 바꾸지 않는 동작 보존
변경으로 진행한다. 기존 DAG에는 §4의 호환성 우선순위를 적용한다. 실행 코드를 추출·이동하면
§6.1에 따라 artifact 의존성과 진행 중 run 전환을 먼저 검증한다. 의미 변경이 필요하면 별도
설계·migration으로 검토한다.
