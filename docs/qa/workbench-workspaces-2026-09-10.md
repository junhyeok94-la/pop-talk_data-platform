# 내 대시보드와 운영 모니터링 검증

환경: Airflow 3.3.1, 현재 Airflow PostgreSQL, 로컬 Docker, Airflow에 로그인한 브라우저.

## 적용 범위

- 왼쪽 메뉴를 **내 대시보드 / 운영 모니터링 / Model Lab**으로 제공한다.
- 내 대시보드의 기본 화면은 계정별 담당 DAG·태그·기간·장기 실행 기준을 저장한다.
  기존 Studio는 **차트·추세**로 연결하고 기존 보드 v9를 유지한 채 표시했다.
- 실패 Run의 대상 미리보기·현재 상태 재검사·native Clear·실행 상태 추적을 제공한다.
- 운영 모니터링은 관리자가 등록한 URL을 iframe으로 표시한다. 최종 등록 URL은 없다.
- 새 설정과 확인 기록은 기존 PostgreSQL `workbench` 스키마에 저장한다. 마이그레이션 6 적용 완료.

## 자동 검증

- Python 회귀 **117개 통과**: 계정·Studio·쿼리·집계·캐시·모듈 경계·Model Lab·저장소·워커·신규 업무 API.
- 개인 업무 집계 정확성 테스트 **1개 추가 통과**: 기간 밖의 실행 중 작업은 포함하고,
  기간 밖 실패와 다른 DAG는 제외한다. 건수·마지막 성공·주의 목록·최신 상태를 실제 격리
  PostgreSQL에서 확인하며 모든 지표가 원본 CTE 하나를 공유하는지 검사한다.
- 테마 테스트 **5개 통과**: Light/Dark/System, 부모 토큰 변경, 차트 확대·범례 유지.
- 신규 12개 업무 API 테스트: 계정 소유권·버전 충돌·DAG 권한, URL 검증, 관리자 설정/일반 조회,
  운영 화면에만 적용되는 CSP, CSRF, native 권한 거절, 매핑 태스크 범위, 만료·다른 사용자 확인 거절,
  같은 확인 ID와 동시 요청 방지, 불명확한 응답 처리, paused/running/Model Lab 차단.

테스트는 `scripts/test_workbench_workflows.py`, `scripts/test_workbench_performance.py`, 기존
회귀 스위트에 있다. Python 테스트는 별도 `wb_test_*` 스키마를 사용하고 정리한다.

## 실제 Airflow 실행

외부 시스템을 호출하지 않는 `workbench_retry_verification` DAG만 실행했다.
schedule=None, 최초 paused, 자동 retry=0이며 태스크 세 개로 구성한다.

| 태스크 | 첫 실행 | 플러그인 재실행 후 |
|---|---|---|
| already_succeeded | success / try 1 | success / try 1, 시작·종료 시각 동일 |
| fail_once | 의도한 failed / try 1 | success / try 2 |
| downstream | upstream_failed / try 0 | success / try 1 |

`workbench_retry_qa_a96c7cd933ea`에서 위 결과, 미리보기의 무변경,
확인 ID 재전송 시 추가 Clear 없음까지 검증했다.
[실행 전후 증거 JSON](../../.local/qa/docs/workbench-workspaces-retry.json).

첫 연동 과정에서 native 태스크 응답의 `dag_run_id` 필드 차이를 발견해 어댑터와 테스트를
수정했다. 남아 있던 `workbench_retry_qa_0f01ae483ab6`은 브라우저에서 직접 미리보기·확인하여
success로 복구했고 화면의 추적 상태도 success가 됐다. 검증 DAG는 다시 paused 상태로 두었다.
기존 프로젝트 DAG를 이 검증에서 재실행하지 않았다.

재현: `PYTHONPATH=/opt/airflow/plugins:/opt/pop-talk-dw/scripts python /opt/pop-talk-dw/scripts/smoke_workbench_workflows.py`.
이 스크립트는 고정된 검증용 DAG만 활성화·실행하며 마지막에 이전 paused 값을 복원한다.

## 브라우저 확인

이 단계에서는 native 화면 링크의 클릭 후 이동까지 검증하지 못했다. 이후 실제 클릭으로
`external_views` iframe의 상위 탐색 제한을 재현하고 수정했다.
[링크·배치·DAG 제어 후속 검증](workbench-navigation-controls-2026-09-10.md)을 참고한다.

- Airflow 왼쪽 메뉴에서 세 플러그인 메뉴로 이동.
- 개인 업무 범위를 검증 태그로 저장하여 해당 DAG만 표시하고, 빈 범위에서는 시작 안내 확인.
  검증 후 원래 업무 태그·기간·기준 값을 복원.
- 실제 실패 대상의 태스크 2개 미리보기 → 사용자 메모 → 확인 → 성공 추적.
- native dialog의 비동기 close 이벤트 때문에 저장 직후 조회가 건너뛰어지던 문제를 수정했다.
  닫힘 처리 후 조회하도록 변경하고 담당 범위 저장 직후 일치/불일치 목록 전환을 확인했다.
- 운영 연결 UI의 이름·URL·설명 저장 → nested iframe에 실제 페이지 표시 → 삭제 → 미등록 복원.
  임시 같은 출처 URL로 렌더링을 검증했으며 가짜 운영 URL을 남기지 않았다.
- 기존 차트 페이지와 Model Lab이 각자 정적 자산으로 동작하는 회귀 검증.

![운영 모니터링 미등록 안내](images/workbench-monitoring-empty.png)
![개인 담당 DAG 화면](images/workbench-personal-dags.png)
![실제 실패 작업 미리보기](images/workbench-retry-preview.png)

## 확인 범위의 한계

등록할 외부 서비스가 없으므로 Grafana·SSO·외부 쿠키·외부 역할 매핑을 실제 환경에서 검증한
것은 아니다. 외부 서비스가 정해지면 그 출처의 iframe/CSP/로그인 조건을 확인해야 한다.
이 변경은 모니터링 서버나 SSO 프록시를 설치하지 않는다.

개인 업무 집계는 현재 Airflow 메타 DB를 사용하며 30초 캐시와 동시에 두 개의 집계 제한을
공유한다. 조회 건수가 적은 실제 실행 검증은 대규모·장시간 부하 시험을 대신하지 않는다.
기존 차트 성능 측정은 [성능 검증 기록](workbench-performance-2026-09-10.md)을 참고한다.
