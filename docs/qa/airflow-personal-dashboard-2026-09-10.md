# Airflow 계정별 Dashboard Studio 검증

2026-09-10 KST. 로컬 Airflow 3.3.1 / FAB Auth Manager / PostgreSQL 17.

## 반영 결과

- 로그인한 계정 ID와 Auth Manager 종류를 기준으로 보드·패널 템플릿·저장 이력을 분리한다.
- 모든 저장/조회는 서버에서 소유자와 ID를 함께 확인한다. 클라이언트 owner 값은 받지 않는다.
- Plugins 권한 사용자는 자신의 구성을 편집할 수 있고, 운영/SQL 실행 권한은 별도로 유지한다.
- 마지막 선택 보드를 서버에 저장해 재접속과 다른 브라우저에서 복원한다.
- 기존 공용 보드·이력·템플릿은 관리자용 보관본으로 유지하고 계정당 한 번 복사할 수 있다.
- 현재 로그인 계정에 공용 보드 2개와 템플릿 1개를 복사했다. 기본 보드 2개와 함께 총 4개다.
  복사본은 새 ID를 가지며 기본 보드/공용 원본을 덮어쓰지 않는다.

## 자동 검사

`scripts/test_workbench_accounts.py`: 9/9 통과.

1. Alice/Bob 독립 기본 보드 및 같은 ID에 대한 독립 저장. 관리자도 타인 개인 보드 미노출.
2. 타 계정 ID 추측, owner 헤더/쿼리/JSON 위조, 존재하지 않는 개인 보드의 이력/설정 접근 거부.
3. 같은 템플릿 ID의 계정별 저장과 조회 분리.
4. 최근 이력 20개 보존·복원·낡은 버전 409, 다른 계정의 이력은 유지.
5. 계정별 마지막 선택, 새 보드 저장과 선택의 원자적 반영, 표시 이름 변경 후 동일 소유자 유지.
6. 개인 편집 사용자에게 SQL 소스 등록·Model Lab 실행·공용 보관본 권한을 부여하지 않음.
7. 비인증 401, Plugins 권한 없는 계정 403, 잘못된 Origin/누락된 CSRF 헤더 403.
8. 같은 버전을 동시에 저장하면 한 요청만 성공하고 다른 요청은 409.
9. 공용 가져오기에서 새 ID/이력/템플릿 복사, 재실행 409, 원본과 다른 사용자 기본 보드 보존.

테스트는 임시 저장소와 가상의 인증된 사용자 객체를 사용한다. 인증 사용자 조회만 대체하며
실제 서버의 권한/소유권 검사 코드는 실행한다. 테스트용 Airflow 계정이나 DB 권한은 만들지 않았다.

기존 `test_workbench_studio.py` 12/12, `test_airflow_workbench.py` 17/17 통과. 합계 38개.
JavaScript `node --check` 통과. Python Black / JS·CSS Prettier 적용.

## 실행 서버 및 브라우저 확인

진행/대기 중 DAG 0건을 확인한 뒤 API 서버만 재시작했다. 서버 기동 완료 후
`scripts/smoke_workbench_studio.py`로 기존 계정의 실제 토큰 인증과 개인 설정 API,
기본 보드 데이터 조회, 정적 번들, 중복 메뉴 없음, 인증/Origin 거부, Model Lab 계약·pool을 확인했다.
새 계정 생성이나 GPU 학습 실행은 하지 않았다.

브라우저에서 Airflow 왼쪽 대시보드 메뉴, `내 대시보드`와 `개인` 표시,
공용 보관본 펼치기/가져오기, 개인 보드 선택/저장, 전체 페이지 재접속을 확인했다.
공용 operations v5에서 복사한 새 ID의 개인 보드를 v6로 저장했고 재접속 후 v6이 표시됐다.
실제 API 재조회에서도 해당 보드가 마지막 선택이며 개인 이력 v5~v1이 남아 있음을 확인했다.
공용 원본 operations는 v5를 유지한다. 브라우저 console error 0건.

- 화면: `airflow-personal-dashboard-2026-09-10.png`
- 인증 API 검증: `airflow-personal-dashboard-validation-2026-09-10.json`
- 실제 저장 결과: `airflow-personal-dashboard-storage-2026-09-10.json`

## 범위

개인 대시보드 설정의 분리이며, 팀 공유/다른 계정에 대한 편집 ACL은 아직 없다.
Model Lab 실험 기록의 공개 범위는 기존 Plugins 권한 체계를 따른다.
Airflow 메타 DB는 PostgreSQL이며, 개인 화면 구성은 기존 플러그인 SQLite 파일에 저장한다.
다중 호스트 운영 시 저장소 전환과 백업 설계가 필요하다.
PostgreSQL 직접 조회용 전용 Connection은 기존과 같이 연결 대기이며 이번 변경에서 만들지 않았다.
