# Cosmos 구현 검토

판정: CHANGES REQUESTED. 아래 P1 두 건 수정 후 재검토가 필요하다. 구현 코드는 변경하지 않았다.

## P1 — wrapper가 임시 target/log 격리를 깨뜨려 실제 실행이 실패함

위치: scripts/dbt_cosmos_runner.py:58-62.

Cosmos 1.15.1의 실제 _generate_dbt_flags는 임시 --project-dir와 profile/target 이름만 추가한다. 별도의 --target-path/--log-path를 생성하지 않는다. run_command는 임시 project를 cwd로 실행하지만 wrapper가 --project-dir를 불변 원본으로 치환하므로 dbt_project.yml의 상대 target-path: target은 불변 원본 아래로 해석된다. compose는 pipelines를 read-only mount한다.

독립 재현: 새 이미지에서 network none, pipelines/scripts read-only로 dbt parse를 Cosmos와 같은 인자로 실행하면 exit 2. log만 /tmp로 지정해 원인을 확인한 결과 OSError [Errno 30] Read-only file system: .../dbt_deployments/bf08.../project/target이 발생했다. 동일 실행에 --target-path /tmp/<task>/target을 추가하면 exit 0이다. 최초 검사에서 log도 명시하지 않은 실행은 출력 없이 exit 2였다.

권고: 검증된 A source를 Cosmos의 해당 태스크 임시 project에 복사한 뒤 그곳에서 실행하거나, wrapper가 target/log를 태스크의 독립 writable 경로로 명시적으로 고정한다. 후자의 경우 Cosmos가 후처리에서 읽는 tmp_project/target의 run_results와도 경로를 일치시킨다. 중복 옵션도 제거/거부하여 마지막 옵션으로 우회되지 않게 한다. 수동으로 target/log를 추가한 parse 성공이 아니라 생성 operator와 동일한 인자·환경으로 성공하는 회귀 검증이 필요하다. 불변 배포 원본에 생성 파일이 남지 않아야 한다.

## P1 — XCom source 고정만으로 serialized graph A / 실행 B 혼합을 막지 못함

위치: orchestration/airflow/dags/pop_talk_movie_databricks_daily.py:234-251, scripts/dbt_cosmos_runner.py:49-62.

identify_gold_build는 파싱 시 고정값을 함수 인자로 받지 않고 DBT_DEPLOYMENT 전역을 실행 시 읽는다. LocalDagBundle이 다시 로드될 때 current가 B라면 기존 A 그래프로 시작한 run에서도 B identity를 반환할 수 있다. 이후 env에는 A 그래프 identity와 비교할 값 없이 이 XCom의 B 값만 전달된다. B source 검증은 통과하지만 기존 run의 모델 집합/edge는 A일 수 있다. 확인 태스크와 PostgreSQL 게시도 B 자체만 재검증하므로 빠진 모델의 기존 Snowflake 테이블을 새 B 버전 결과로 게시할 위험이 있다.

identify 뒤 current가 바뀌는 경우에도 wrapper는 --select, 명령, profile/target 등 그래프에서 나온 인자를 고정 A manifest와 검증하지 않고 전달한다. 독립 mock 검증에서 A manifest에 없는 fqn:pop_talk_dw.MODEL_NOT_IN_FIXED_MANIFEST를 run --select로 넘겨도 os.execv가 호출됨을 확인했다. 테스트의 서로 다른 expected digest 검사와 env 직렬화 확인은 이 실제 혼합 경로를 재현하지 않는다.

권고: run에 실제로 적용된 그래프의 deployment/manifest/model identity를 identify 이전부터 고정하고, identify 및 모든 실행에서 독립적으로 대조한다. Airflow worker 재구성에서도 유지됨을 테스트해야 한다. versioned bundle 또는 배포별 DAG artifact를 사용할 수 없으면 진행 중 배포를 금지하고 혼합을 감지해 실패시키는 명시적 계약이 필요하다. wrapper의 command/select/profile/target도 고정 manifest와 태스크 계약에 대조한다. A parse→B current→identify 실행, A identify→B worker 재로드를 각각 검증하고, 모델 이름/의존 관계가 다른 A/B로 검사한다. 단순 SQL 상수만 다른 fixture로는 그래프 누락을 재현하지 못한다.

## 확인한 정상 부분

- 새 pop-talk-airflow:3.3.1-local 이미지에서 네트워크 없이 DAG 구조 검사 6개와 Python 테스트 60개를 독립 실행해 통과했다. DagBag import 오류 없음.
- AFTER_ALL의 project_test는 select=None이고 전체 모델 완료 경로 뒤에 있다. 독립 singular test 및 다중 부모 test를 포함할 방향은 맞다. 실제 43개 run_results 대사는 아직 수행 전이다.
- 모델 7개/전체 test 1개, ALL_SUCCESS, Cosmos pool 지정, 30분 task timeout, 4시간 dagrun_timeout을 구조 검사로 확인했다. compose init에 전용 pool 1 slot 생성이 정의돼 있다.
- model_version과 deployment_id가 분리되고 PostgreSQL은 XCom이 가리키는 배포를 검증한다. 문제는 이 identity가 실제 실행 그래프에 결합됐는지 여부다.
- wrapper는 shell 문자열을 실행하지 않고 argv + execv를 사용한다. deployment ID 정규식 검사는 일반적인 ../ 경로 입력을 막는다.
- 한글 설명과 runtime 분리는 대체로 명료하다. 다만 현재 A/B 혼합 차단 및 임시 경로 격리 설명은 위 재현 결과에 맞게 수정해야 한다.

## 검증 범위

실행 중인 compose airflow-scheduler에는 Cosmos가 설치되지 않아 새 이미지의 일회성 컨테이너를 사용했다. 기존 서비스를 재시작하거나 외부 Snowflake/PostgreSQL을 변경하지 않았다. 정상 단위/구조 검사 외에 wrapper parse 실패/성공 대조 및 execv mock 부정 입력 검사를 수행했다. 전체 DAG 통합 실행은 미수행이다.
