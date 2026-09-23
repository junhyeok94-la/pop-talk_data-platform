# 대시보드 집계·캐시 검증

Airflow 3.3.1, 기존 PostgreSQL 메타 DB, 2026-09-10 로컬 실행.

## 결과

- 기존 Python 회귀 테스트 95개 + 새 성능·권한·집계 테스트 10개 = **105개 통과**.
- 테마 JavaScript 테스트 **5개 통과**, 변경된 Studio JS 문법 검사 통과.
- 마이그레이션 v5로 `workbench.dashboard_cache` 추가 후 API 서버 재시작·인증 요청 정상.
- Airflow 왼쪽 대시보드 메뉴에서 개인 보드 v9·7패널 확인. 전체 44건, 성공률 77.3%, 실패 10건과 실행 상세 표·차트 표시 정상. 개인 보드 구성은 수정하지 않음.
- 패널의 데이터 기준 시각 표시 확인.

## 측정

| 조건 | 첫 동시 조회 p95 | 캐시 재조회 p95 |
|---|---:|---:|
| 격리 PostgreSQL 50,003건·7패널·20개 동시 요청, 테스트 권한 객체 | 400.4ms | 16.1ms |
| 현재 Airflow HTTP·실제 인증·44건·7패널·동일 계정 20개 동시 요청 | 295.6ms | 146.6ms |

격리 테스트의 40회 요청에서 원본 집계 SQL은 1회 실행됐다. 실제 HTTP 테스트는 첫 묶음 19/20, 재조회 묶음 20/20이 캐시 또는 진행 중인 집계 결과를 재사용했다. 표는 같은 부하에 대한 변경 전후 비교가 아니라, 변경 후 서로 다른 두 실행 조건이다.

장시간 부하·스케줄러 자원 경합·여러 실제 계정·브라우저 렌더링 시간을 측정한 결과는 아니다. 별도 회귀 테스트에서 DAG 권한 변경·빈 권한·전체 5천 건 초과 집계·프로세스 간 lease 경쟁과 복구·결과 덮어쓰기 방지·캐시 용량 제한을 확인했다.

- [격리 데이터 측정 JSON](../../.local/qa/docs/workbench-performance-fixture-2026-09-10.json)
- [실제 HTTP 측정 JSON](../../.local/qa/docs/workbench-performance-http-2026-09-10.json)
- [구조·적용 범위·재현 방법](../../orchestration/airflow/plugins/DASHBOARD_PERFORMANCE.md)

![Airflow 내장 대시보드](images/workbench-aggregate-dashboard-2026-09-10.png)
