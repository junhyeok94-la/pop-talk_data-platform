# Model Lab 평가 실행 권한 오류 확인

## 원인

2026-09-10 18:27:15/18:27:33 KST에 `tester` 프로젝트의 평가 요청이 native Airflow `POST /api/v2/dags/model_lab_eval_default_ollama/dagRuns`에서 403으로 거부됐다. DAG·Task·Pool 준비 조회는 성공했다.

실제 계정 `pop-talk-viewer`는 `Viewer + WorkbenchAccess`이며 프로젝트 소유자다. 프로젝트 소유권으로 평가셋·실험을 만들 수 있지만 Viewer에는 DAG edit 및 DAG Run create 권한이 없다. 기존 화면은 프로젝트 권한만 검사해 실행 버튼을 활성화했고, native 거부를 `submission_unknown`으로 기록했다.

## 수정

- bootstrap이 추론 연결별로 현재 Auth Manager의 `POST / RUN / DagDetails(id)` 권한을 반환한다. 실행 버튼은 이 권한과 프로젝트 권한을 모두 확인하고 차단 이유를 표시한다.
- 평가 API는 동일한 DAG 실행 권한을 Run 생성·재시도 전에 검사한다. 실제 native API RBAC도 그대로 적용된다.
- 새로운 제출의 401/403은 `submission_rejected`로 기록하며, 거부된 실행 상세에서는 존재하지 않는 DAG 조회·링크를 제공하지 않는다. 같은 요청 ID로 재시도할 수 있다.
- 통신 장애 또는 앞선 불확실한 제출 이후의 거부는 `submission_unknown`을 유지해 이미 생성됐을 수 있는 DAG를 누락하지 않는다.
- 계정 역할과 기존 실행 이력은 수정하지 않았다. 이전 `88e58af89110482f8e3705c77e33636e` 기록은 기존 상태를 유지한다.

## 검증

`python -m unittest test_model_lab_workflow test_airflow_workbench`: 35개 통과. 무작위 `wb_test_*` PostgreSQL 스키마에서 실행·정리했다. 프로젝트 소유자의 Airflow 실행 권한 부족, 거부 시 native 호출/Run 생성 없음, 인증·권한 거부와 통신 장애 구분, 거부된 요청의 같은 ID 재시도를 검증했다.

`node --check .../model_lab/static/lab.js` 통과. 기존 theme/navigation 테스트 7개 통과.

실제 설치된 Airflow 3.3.1/FAB Auth Manager와 실제 계정 객체를 사용한 읽기 전용 검사에서 Viewer의 `can_trigger`는 false, Admin은 true이며 bootstrap 응답도 일치했다. 실제 평가 DAG는 실행하지 않았다.

Browser 도구의 localhost 접속이 `ERR_BLOCKED_BY_CLIENT`로 차단돼 이번 변경의 시각 검증은 완료하지 못했다.

로컬 `airflow-apiserver` 컨테이너를 재시작해 변경을 반영했다. `Application startup complete`와 컨테이너 `running healthy`를 확인했다. 인증 없이 접근한 Model Lab 문서·정적 파일은 401을 반환하므로 이 요청만으로 로그인 화면 동작을 검증했다고 간주하지 않는다.

## 테스트 계정 설정

Admin으로 로그인하면 기존 프로젝트에서 테스트할 수 있다. Viewer 계정을 유지하며 평가만 허용하려면 별도 역할에 `can edit on DAG:model_lab_eval_default_ollama`와 `can create on DAG Runs`를 추가하고 해당 계정에 부여한다. 기존 Viewer/WorkbenchAccess 역할은 유지한다. `User + WorkbenchAccess`도 실행 가능하지만 다른 DAG 수정·삭제 권한까지 포함한다.
