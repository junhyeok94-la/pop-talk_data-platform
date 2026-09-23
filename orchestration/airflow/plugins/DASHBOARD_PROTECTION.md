# 대시보드 운영 보호

개인 대시보드는 배치 실행보다 낮은 우선순위로 조회를 받아들인다. 부하가 늘어나면 새 집계를
포기하고, 가능하면 같은 조회 조건의 마지막 결과를 보여준다. PostgreSQL/OS 스케줄러에
실행 우선순위를 설정하는 기능은 아니다. 공유 서버에서 영향이 0이라고 보장하지 않는다.

## 요청 흐름

1. 플러그인 진입부에서 대시보드 요청 수·속도·혼잡 상태를 검사한다.
2. 현재 로그인과 Plugins/DAG 권한을 확인한다. 권한 검사를 캐시로 생략하지 않는다.
3. 별도의 작은 DB 연결 풀로 설정·담당 범위·캐시를 읽는다.
4. 캐시가 만료됐으면 DB 혼잡을 검사하고 전역 집계 슬롯과 갱신 예산을 확보한다.
5. 한도가 찼으면 반복 조회하며 기다리지 않는다. 같은 권한·조건의 5분 이내 결과를
   `stale=true`로 반환하거나 HTTP 503과 `Retry-After`를 반환한다.
6. 정상 상태에서만 짧은 원본 집계를 수행한다. 여러 패널은 한 SQL의 CTE를 공유한다.

이미 혼잡을 감지한 프로세스는 냉각 시간 동안 설정·캐시 확인도 새로 시작하지 않는다.
이때 브라우저는 이미 표시한 결과와 기준 시각을 유지한다. 서버가 반환하는 오래된 캐시의
상한은 5분이고, 브라우저에 이미 표시된 결과는 경고와 함께 더 오래 남을 수 있다.
필터 변경 또는 권한 오류가 확인되면 이전 조건의 결과는 비운다.

## 기본 한도

환경 변수는 **API 서버 프로세스에** 설정한다. 아래 이름 앞에 `AIRFLOW_WORKBENCH_`를 붙인다.
숫자 범위는 `dashboard/protection.py`의 `Policy`에서 검증한다.

| 변수 | 기본값 | 의미 |
|---|---:|---|
| `DASHBOARD_REQUESTS` | 4 | 프로세스당 진행 중인 대시보드 HTTP 요청 상한 |
| `DASHBOARD_RATE` | 4 | 프로세스당 초당 요청 토큰, 순간 여유는 요청 상한의 2배 |
| `DASHBOARD_CONNECTIONS` | 2 | 프로세스당 대시보드 DB 연결, 추가 연결 0, 풀 대기 50ms |
| `DASHBOARD_QUERY_MS` | 1000 | 대시보드 트랜잭션의 SQL 한 문장 실행 제한 |
| `DASHBOARD_COMPUTE_GAP` | 2 | 같은 Workbench 스키마에서 새로운 집계 시작 간 최소 초 간격 |
| `DASHBOARD_COOLDOWN` | 30 | 혼잡 후 새 조회를 중단하는 초 |
| `DASHBOARD_DB_ACTIVE` | 8 | 같은 DB의 대시보드 외 활성 세션이 이 수 이상이면 중단 |
| `DASHBOARD_DB_CONNECTION_RATIO` | 0.75 | 서버 연결 수 / max_connections 기준 |
| `DASHBOARD_PROBE_MS` | 200 | DB 활동 카운터 조회 자체가 느릴 때의 기준 |
| `DASHBOARD_LOOP_LAG_MS` | 100 | API 이벤트 루프 지연 기준, 250ms 간격 관찰 |
| `DASHBOARD_STALE_SECONDS` | 300 | 서버가 같은 조건의 이전 결과를 재사용할 수 있는 최대 나이 |

변수와 별개로 전체 프로세스 합계 **원본 집계 1개**, 전역 lease 10초, 프로세스당
대시보드 작업 스레드 4개를 사용한다. 무제한 작업 큐를 만들지 않는다. 클라이언트 연결이
끊겨도 실행 중인 스레드의 자리가 바로 반환되지 않는다. 비동기 SQL/실험 보드는 전체 집계를
3초로 제한하고 취소된 I/O가 끝날 때까지 전역 lease가 남도록 한다.

쿼리는 lock timeout 100ms, idle-in-transaction timeout 2.5초, 병렬 쿼리 워커 0,
work_mem 4MB를 적용한다. work_mem은 연산별 설정이며 전체 메모리 상한은 아니다.
원본 이력 집계는 읽기 전용이다. 설정·캐시 쓰기는 별도의 짧은 트랜잭션이다.
Connection 설정을 쓰는 직접 SQL 소스도 연결 2초·SQL 1초와 읽기 전용 조건을 적용한다.

## 혼잡 감지와 복구

- 캐시 miss 시 프로세스당 최대 5초에 한 번 `pg_stat_activity`의 카운터만 읽는다.
  쿼리 본문·자격 증명은 읽거나 기록하지 않는다. 다른 세션의 Lock 대기가 있으면 중단한다.
- DB 연결/SQL 시간 초과, API 이벤트 루프 지연도 새 조회를 중단시킨다.
- DB 부하 검사의 I/O 동안 API 진입부의 잠금을 잡지 않는다. 느린 보호 검사 자체가
  API 이벤트 루프를 막지 않도록 별도 작업 스레드에서 처리한다.
- 냉각 시간이 지난 뒤 5초 이상 떨어진 정상 샘플 2회가 확인되어야 혼잡 상태를 해제한다.
- API 로그의 `airflow_workbench.dashboard.protection`에 중단 원인 변경과 재개를 기록한다.
- 브라우저는 갱신 보류 안내와 마지막 기준 시각을 표시한다. 자동 갱신은 0~15% 시간
  분산을 적용한다. 일반 갱신은 계정 설정을 따르고, 보류 응답은 서버가 안내한 시점 이후
  재시도한다. 자동 갱신을 끈 계정은 켜지지 않는다. 수동 버튼도 대기 중에는 비활성화한다.
- 새 화면을 여는 단계에서 제한된 경우에는 안내와 대기 후 사용 가능한 다시 불러오기
  버튼을 표시한다. 화면의 편집 저장도 같은 요청 한도를 따르므로 실패 메시지를 확인한다.

## 보호 범위와 한계

보호 대상은 담당 범위·목록·집계, 차트·추세의 설정/미리보기/조회, 기존 대시보드 조회 API다.
별도 분석의 SQL·이전 실험 소스도 전역 집계 예산을 사용한다. 명시적인 DAG 재실행·상태 변경,
native Airflow API, Model Lab 작업, 실행기의 상태 콜백에는 이 대시보드 차단을 적용하지 않는다.
명시적 작업은 기존 권한·최신 상태 확인을 계속 거친다.

별도 풀은 Airflow의 기존 DB URL·SSL 연결 옵션·`create_metadata_engine` 사용자 정의 인증
팩토리를 재사용한다. DB 사용자·비밀번호·테이블·인덱스·마이그레이션 버전을 추가하지 않는다.
기존 `workbench.leases`에 집계 간격 제어용 한 행을 사용한다. Airflow의 기본 연결 풀과
트랜잭션 설정을 수정하지 않는다. **네이티브 인증/권한 검사 자체의 DB 연결은 Auth Manager의
기존 경로를 사용한다.** 따라서 전체 요청의 모든 SQL이 별도 풀을 쓴다고 해석하면 안 된다.

연결 수·스레드·HTTP 제한은 API 프로세스당 값이다. API 프로세스 4개라면 대시보드 전용
연결은 최대 8개가 더해진다. 집계 슬롯·시작 간격·캐시 공간만 같은 Workbench 스키마 전체에서
공유한다. DB pooler 사용 시 서버 활동 카운터의 의미와 권한을 배포 환경에서 확인해야 한다.
사용자 정의 DB 팩토리가 전달받은 풀 설정을 무시하면 한도를 유지할 수 없다.

원격 DB의 CPU·스토리지 I/O 사용률을 직접 측정하지 않는다. 활성 세션이 적지만 비싼 쿼리가
있는 경우, 감지 전의 부하, 이미 시작한 SQL, 인증 비용과 Python의 CPU 사용은 남는다.
같은 API 프로세스가 core/execution을 모두 호스팅하므로 이 구성이 강한 자원 격리를 제공하지는 않는다.

엄격한 배치 SLO가 필요하면 배포 단계에서 Airflow의 `api-server --apps core`와
`--apps execution`을 분리하고 CPU/메모리·DB 연결 예산을 따로 배정한다. Task SDK의 execution
URL과 프록시 라우팅도 함께 검증해야 한다. 이는 추가 대시보드 제품 설치와 관계없는 Airflow
자체 배포 구성이다. DB에서도 조회/실행 부하의 물리적 격리가 필요하면 복제 지연과 권한을
검토한 별도 읽기 경로가 필요하며, 현재 플러그인에는 자동 적용하지 않는다.

참고: [Airflow API 서버 CLI](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#api-server),
[스케줄러 DB 성능 고려사항](https://airflow.apache.org/docs/apache-airflow/3.3.1/administration-and-deployment/scheduler.html).

## 검증과 배포

`orchestration/airflow/tests/test_workbench_protection.py`는 격리 PostgreSQL 스키마에서 연결 고갈·SQL 취소·실제
활성 DB 세션·혼잡 복구·캐시 조건 분리·취소된 스레드·API 진입부 잠금·직접 SQL 예산을 확인한다.
`orchestration/airflow/tests/test_workbench_refresh.cjs`는 Retry-After와 자동 갱신 꺼짐·수동 갱신 제한을 검증한다.
`orchestration/airflow/tests/benchmarks/benchmark_workbench_protection.py`는 50,000행, 50개 조건의 1,000 요청과 작은 쓰기의
공존을 측정한다. 임시 스키마를 정리하며 업무 DAG를 생성·수정하지 않는다.
`orchestration/airflow/tests/smoke/smoke_workbench_protection.py`는 실제 JWT/저장 설정을 사용해 새 Airflow 앱 프로세스에서
정상 조회와 혼잡 상태의 native API 접근을 검증한다. live 서버에 혼잡 상태를 주입하지 않는다.

운영 승인 기준은 실제 DAG 수·권한이 다른 사용자·집계 기간·동시 사용량으로 정한다.
플러그인 없는 기준선과 대시보드 사용 시의 스케줄 지연, Task SDK heartbeat 오류율, native API
p95/p99, DB CPU/I/O/연결/Lock 대기를 비교해야 한다. 짧은 합성 테스트는 그 검증을 대체하지 않는다.
현재 버전의 로컬 보호 검증 기록은 `docs/qa/workbench-protection-2026-09-10.md`에 남긴다.
