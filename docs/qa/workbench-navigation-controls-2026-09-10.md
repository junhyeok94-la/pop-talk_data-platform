# 담당 DAG 링크·배치·운영 상태 제어 검증

환경: Airflow 3.3.1, 기존 PostgreSQL, 로컬 Docker, 로그인된 Airflow 브라우저.

## 수정 내용

- `external_views`의 iframe sandbox는 `_top` 이동을 허용하지 않았다. 링크의 href가
  있어도 실제 클릭 시 이동하지 않는 문제를 재현했다.
- 공식 `react_apps` 등록과 공유 React/React Router를 사용하는 작은 호스트를 추가했다.
  기능별 화면은 기존 iframe 안에서 동작하며 같은 iframe·같은 출처·허용된 경로의
  이동 요청만 Airflow 라우터로 전달한다. Airflow 코어 파일은 수정하지 않았다.
- 주의 실행과 담당 DAG는 2열, 최근 실행은 아래 2열 목록으로 표시한다.
  iframe 너비 1440px 이상이면 세 카테고리를 같은 행에, 950px 이하이면 한 열에 표시한다.
  긴 DAG 이름은 줄바꿈하고 Run ID는 생략 표시와 전체 값 툴팁을 제공한다.
- DAG 활성화/일시중지 버튼에서 실시간 상태·변경 권한·스케줄을 확인한다.
  확인 후 현재 사용자로 native `PATCH /api/v2/dags/{dag_id}`의 `is_paused`를 변경한다.
  권한 없음, 오래된 확인 상태, 배포되지 않은 DAG, 활성화할 DAG의 파싱 오류를 거절한다.
  Model Lab 관리 DAG는 Model Lab으로 안내한다.

## 자동 검증

- `test_workbench_workflows` + `test_workbench_modules`: **20개 통과**.
  활성화/일시중지 양방향 payload, 현재 사용자 인증 전달, 거절 시 native PATCH 없음,
  조회 이후 권한 회수, 기존 개인 설정·재실행·모듈 경계를 검사했다.
- `test_workbench_navigation.cjs` + `test_workbench_theme.cjs`: **7개 통과**.
  허용된 native 경로, 다른 출처/다른 iframe/API 경로 거절, 배포 basename,
  부모 준비 메시지 확인, Light/Dark/System 연동을 검사했다.
- 수정된 `work.js` 구문 검사 통과.

이 기록은 이번 변경의 대상 테스트 결과다. 이전 전체 Python 검증 기록은
[워크스페이스 검증](workbench-workspaces-2026-09-10.md)에 있다.

## 실제 브라우저와 API 확인

1. 왼쪽 **내 대시보드** 메뉴로 진입하여 새 화면과 1280×720 브라우저에서 2열 배치를 확인했다.
2. 주의 목록의 **실행·로그 열기**를 실제로 클릭했다. 선택한 Run의 Airflow breadcrumb와
   native 실행 화면이 표시되고 플러그인 iframe은 사라졌다. URL 문자열 확인만으로 판정하지 않았다.
3. 담당 DAG 이름을 클릭하여 native DAG 상세 화면으로 이동함을 확인했다.
   차트·추세 → 담당 DAG로 같은 iframe 안에서 돌아온 뒤 **DAG 상세·새 실행**도 클릭하여
   native 상세로 이동함을 확인했다. iframe 문서 교체 이후에도 링크 연결이 유지되었다.
4. **차트·추세**, **Model Lab**, **운영 모니터링**의 기존 화면이 새 호스트에서 로드됨을 확인했다.
5. 외부 호출이 없는 `workbench_retry_verification`만 담당 범위에 잠시 추가했다.
   UI **활성화 확인** 후 API `is_paused=false`, UI **일시중지 확인** 후 `is_paused=true`를 확인했다.
   기존 두 Run은 success로 유지되었고 새 Run은 생성되지 않았다.
6. 임시 개인 설정을 원래 값으로 복원했다. profile v8: 직접 선택 없음, 태그 `pop-talk`,
   최근 168시간, 장기 실행 60분. 기존 보드 v9는 유지했다.
   운영 모니터링 v2는 `screens=[]`이며 검증용 DAG는 paused 상태다.

![담당 DAG 병렬 배치](images/workbench-work-panels-2026-09-10.png)
![링크 클릭 후 native 실행 화면](images/workbench-run-navigation-2026-09-10.png)
![검증용 DAG 활성화 확인](images/workbench-dag-activation-2026-09-10.png)

실제 활성화 시험은 스케줄이 없는 검증용 DAG만 사용했다. 기존 업무 DAG의 상태는
변경하지 않았다. 별도 경로 배포의 basename과 다른 화면 너비의 분기 규칙은 코드/단위 테스트로
확인했으며, 실제 브라우저 실행은 현재 루트 경로와 1280×720 환경에서 수행했다.
외부 운영 URL과 SSO 연동은 이번 시험의 대상이 아니다.
