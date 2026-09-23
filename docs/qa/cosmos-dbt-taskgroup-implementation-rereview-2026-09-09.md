# Cosmos 구현 재검토

판정: APPROVED — 이전 P1 두 건은 코드 검토 범위에서 종결한다. 최소 표본 통합 검증으로 진행 가능한 상태다. 실제 Snowflake/PostgreSQL 전체 실행 완료를 의미하지 않는다. 구현 코드는 수정하지 않았다.

## P1 종결 근거

### 임시 project와 target/log 격리

wrapper는 Cosmos 임시 project 안의 기존 내용을 정리하고 검증된 source를 복사한다. project/profile 옵션 모두 이 복사본을 사용한다. 중복 critical option과 target/log/state 경로 옵션, 알려지지 않은 모델 선택 및 task/selector 불일치를 거부한다.

새 이미지에서 설치된 Cosmos operator의 build_cmd/add_cmd_flags/_generate_dbt_flags로 만든 argv를 prepare_invocation에 전달했다. DB 접근을 피하기 위해 준비된 run 명령을 parse로 바꾸고 select만 제거한 대조 실행에서 exit 0, 임시 project의 target/manifest.json 및 logs/dbt.log 생성을 확인했다. read-only 배포 원본에는 target/log가 생성되지 않았다. 이전 Errno 30 문제는 해소됐다. 이미지 내부 wrapper 바이트가 현재 작업공간 코드와 같은지도 확인했다.

### 그래프와 실행 identity 결합

identify_gold_build는 최초 선행 태스크이며 parse-time identity를 명시적 op_kwargs로 받는다. Cosmos env는 literal graph identity와 identify XCom identity를 따로 전달하고 wrapper는 dbt 실행 전에 두 값의 일치를 요구한다. 모델 selector, command, profile/target 및 task 이름도 고정 manifest 계약과 대조한다. confirm/publish는 같은 XCom 배포를 계속 검증한다.

SerializedDAG 왕복에서 identify op_kwargs와 Cosmos env 보존 검사가 통과했다. 서로 다른 모델 이름/edge를 가진 배포 fixture의 digest 혼합 거부와 graph/locked identity 불일치, 잘못된 모델 선택 거부도 통과했다. 기존의 전역 current 재조회와 무검증 selector 전달 경로가 보완됐다.

## 독립 실행 결과

- 새 pop-talk-airflow:3.3.1-local 일회성 컨테이너, network none, 프로젝트 코드 read-only mount 사용.
- Python 테스트 65개 통과: 수집 17 / transforms 33 / orchestration 12 / PostgreSQL 게시 3.
- DAG 6개 구조 및 부정 회귀 검사 통과, import 오류 0.
- 모델 7개/전체 test gate 1개, 메인 16 tasks의 구조 계약 통과.
- 임시 복사본의 dbt parse 성공과 원본 무변경 확인.

## 승인 범위와 다음 검증

현재의 고정 profile, manifest, 생성 Cosmos argv 및 단일 DAG 직렬 실행 범위를 승인한다. 임의 CLI/환경변수에 대한 범용 보안 실행기를 승인하는 것은 아니다.

다음 통합 실행에서 실제 test run_results의 43 unique_id 대사, PostgreSQL active 게시, 실패·skip 전파를 증빙한다. 실행 중 LocalDagBundle 재로드에 대한 현재 증거는 직렬화 왕복과 identity/selector 부정 검사다. 실제 scheduler/worker가 재구성되는 A/B 전환 시나리오는 아직 독립 실행하지 않았으므로 통합 검증 완료 전에는 진행 중 DAG의 배포 변경을 피한다. 정상 고정 A 계속 실행 또는 혼합 감지 후 실패만 허용하며, B로 묵시적 전환되면 안 된다.

wrapper 상단 설명의 ‘경로 치환’은 본문 구현처럼 ‘임시 project 내용 교체’로 다듬으면 더 정확하다. 승인 차단 사항은 아니다.
