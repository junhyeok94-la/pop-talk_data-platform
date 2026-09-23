# Legacy candidate notebook/runtime/DAG 검토

## 재검토 최종 판정 — APPROVED (adapter 코드 범위)

기존 P1/P2 세 건의 수정 반영을 확인했다. 아래 CHANGES REQUESTED는 최초 검토 이력이며, 현재 코드 검토의 미해결 차단 지적은 없다. 실제 candidate DAG 실행과 운영 비변경 검증의 완료를 뜻하지 않는다.

독립 검증 결과:

- transforms 47개, orchestration 24개 테스트 모두 통과했다.
- 실제 Airflow 3 DagBag의 import errors는 비어 있고 transform upstream은 정확히 `stage_bundle`이다. 6개 태스크의 선행 흐름을 확인했다.
- 실제 operator의 template rendering을 모형 XCom으로 실행해 notebook archive SHA 전달과 별도 `idempotency_token` 필드의 렌더링 결과를 확인했다.
- 실제 transform source로 만든 정상 fixture bundle을 notebook의 Spark 쓰기 이전 코드에서 재계산해 통과했다. archive 변조는 ZIP 열기 이전에 거부했고, 잘못된 landing은 파일 open 이전에 거부했다. 전체 archive SHA 검증 다음에 exact index identity 검사 및 bundled Python exec가 위치한다.
- Snowflake runtime은 현재 artifact를 비교한 뒤 reader/loader를 호출한다. 신규 mismatch fixture에서 S3 GET과 DB cursor 호출이 없음을 확인했다. DAG가 Snowflake 연결 객체를 여는 시점은 runtime 검사보다 앞이지만, observation 읽기·쓰기/DDL은 검증 뒤에 있으므로 기존 실행 코드 불일치 지적은 해결된 것으로 수용한다.

실제 외부 쓰기는 수행하지 않았다. 아래에 명시한 production 전후 fingerprint, Workspace 실제 재배포, Delta 부분 실패 복구, token 재사용과 실제 notebook replay의 구분, 실건수 대사는 격리 통합 실행의 검증 조건으로 유지한다. 실행 중 source 변경을 피하고 파일 digest 대상과 실행 배포가 일치하는 전제도 유지한다.

## 최초 검토 이력

판정: **CHANGES REQUESTED**. candidate로 분리한 전체 방향은 타당하지만, 실제 실행 전에 아래 세 연결 경계를 수정해야 한다. 실제 DAG나 클라우드 쓰기는 실행하지 않았다.

## 독립 검증

- Airflow 컨테이너에서 transforms 47개, orchestration 22개 unittest 모두 통과했다.
- 실제 DAG 파일을 DagBag으로 파싱했다. import errors는 비어 있고 태스크는 6개다. 그러나 태스크 수와 의존성의 정확성은 다르며 아래 1번이 재현됐다.
- notebook의 Spark 쓰기 이전 부분만 실행했다. 실제 transform source와 fixture bundle을 사용하고 ZIP 읽기를 메모리 모형으로 치환했다. Delta/Spark import와 쓰기는 실행하지 않았다. 아래 2번 반례가 검증 경계를 통과했다.

## 1. [P1] stage와 Databricks 실행 사이 의존성이 없음

위치: `orchestration/airflow/dags/pop_talk_movie_legacy_baseline_candidate.py:133`.

실제 파싱 결과 `transform_candidate_bronze_silver.upstream_task_ids`는 빈 집합이다. `publish_exchange`에는 stage와 transform이 모두 연결돼 있지만 transform 자체는 deploy/stage를 기다리지 않는다. 따라서 시작하자마자 아직 없는 stage/deploy XCom을 Jinja로 참조하며 실패할 수 있다. 재시도 지연에 따라 우연히 성공하는 구조가 된다.

`staged >> transformed`를 명시하고 DagBag 테스트에서 transform의 upstream과 전체 실행 순서를 검증한다. 구현 기록에 적힌 직렬 흐름은 현재 파싱 결과와 다르다. 유효한 stage/deploy XCom을 사용한 template rendering 검사도 추가한다.

## 2. [P1] 원격 notebook이 승인된 archive와 동일한 bundle인지 확인하기 전에 실행·쓰기함

위치: `pipelines/databricks/04_movie_legacy_candidate.py:41`, DAG notebook base_parameters.

stage는 archive SHA를 계산하지만 notebook에는 전달하지 않는다. notebook은 bundle 내부 index에 적힌 document hash만 신뢰하고 bundled Python을 exec한다. index 자체와 archive 전체를 stage의 기대 SHA에 연결하지 않는다. landing도 prefix만 검사한다. publisher의 SHA 검증은 Delta 쓰기가 끝난 뒤이므로 원격 쓰기 전 검증을 대신하지 못한다.

재현: 실제 transform source로 생성한 정상 bundle에서 `bundle_index.json`의 source_sha256/source_run_id만 다른 값으로 바꿨다. 나머지 document bytes/hash와 Silver는 그대로 두었다. notebook의 원격 재계산과 quality 검증을 모두 통과해 Delta 쓰기 직전까지 도달했고, index의 source_run_id와 실제 Silver row의 source_run_id가 달랐다. 이후 코드라면 잘못된 Bronze/ledger lineage로 쓰기를 시작하고 Silver 대사 단계에서야 실패한다.

stage의 archive SHA를 notebook parameter로 전달하고 ZIP 열기·exec 이전에 전체 bytes를 검증한다. 정확한 landing 경로와 manifest/source path/SHA/run 관계도 검증한다. 변조 index·변조 transform·잘못된 landing에 대해 exec/Delta 쓰기 0회를 검사하는 notebook 사전 검증 fixture가 필요하다. 전체 의존성을 원격에 중복 배포할 필요는 없지만, 실행할 transform bytes가 stage에서 승인한 archive에 속한다는 연결은 필수다.

## 3. [P2] Snowflake 단계에서는 현재 실행 코드와 artifact digest 연결이 끊김

위치: `pipelines/orchestration/movie_legacy_candidate_runtime.py:222` 및 DAG `load_snowflake`.

stage/publish는 project source digest를 재계산해 앞 단계 artifact와 비교한다. 반면 load_snowflake는 project_root를 받지 않고 현재 import한 loader를 바로 실행한다. publish 이후 파일이 변경되거나 다른 배포의 worker에서 loader 태스크가 재시도되면, 변경된 Snowflake loader로 이전 artifact_version을 기록할 수 있다. 현재 비교는 READY와 published metadata 사이 비교여서 실행 코드 변경을 감지하지 못한다. 다른 작업에서 코드가 진행 중인 환경에서는 실제 가능한 경계다.

load에도 project_root를 전달해 외부 I/O/DDL 이전 동일 artifact 검사를 적용한다. 또한 digest 계산 대상 bytes와 실제 import되어 실행되는 코드가 같은 배포라는 전제를 고정 배포 또는 실행 소스 검증으로 유지한다. publish 이후 dependency를 바꾼 fixture에서 load가 S3/SQL 호출 없이 거부되는지 확인한다.

## 수용 사항과 실제 실행 검증 범위

- 고정 candidate 테이블/S3 prefix, schedule=None, max_active_runs=1, Asset/dbt/PG 호출 부재는 확인했다. production current를 덮어쓰는 경로는 이번 대상 코드에서 발견하지 않았다.
- artifact의 선언된 프로젝트 의존성 목록은 순수 변환과 전송·publisher·loader·runtime을 포함한다. 제출 테스트는 선언된 파일 변경이 digest를 바꾸는지 확인한다. 원격 archive 연결 및 load 실행 코드 검증은 위 지적대로 추가해야 한다.
- notebook import overwrite=False, 기존 409 응답에서 export byte 비교, 입력 기반 64자리 submit token은 코드상 확인했다. 실제 Workspace API의 기존 경로 응답과 SOURCE export 정규화 여부는 모형 테스트로 입증되지 않았으므로 격리 배포·동일 artifact 재배포에서 확인한다.
- Delta는 테이블별 merge 후 persisted key/hash/count 확인, 마지막 SUCCESS 기록이다. 다중 테이블 원자 transaction은 아니므로 중간 실패 후 부분 적재를 replay로 복구하는 설계다. 현재 신규 runtime 테스트 3개는 Delta 실행/오염/replay를 검증하지 않는다. notebook의 SUCCESS replay 및 중간 실패 복구는 격리 통합 검증이 필요하다.
- 같은 submit token으로 DAG를 다시 실행하면 원격 실행 재사용을 검증한다. notebook 자체의 SUCCESS replay 검증은 별도의 명시적 processing_attempt 증가 등으로 실제 notebook을 다시 실행해 구분해야 한다.
- confirm_candidate는 XCom lineage/count 비교다. Databricks 실물 대사는 notebook에 의존하며 독립적인 세 저장소 fingerprint 비교를 수행하는 태스크는 아니다.

실행 전 production run 부재와 함께 Delta version·key/content hash, production S3 key/ETag metadata, Snowflake observation/Gold content digest, PostgreSQL active publication/content digest를 기준선으로 저장한다. 실행 후 동일 범위와 방식으로 비교하며, 운영 작업이 개입했으면 비변경 증거의 한계를 기록한다. candidate에서는 실제 1/5,985건 및 정책 4,320/1,665건, Exchange 5,985/0, Snowflake 5,985건, 실패 복구와 두 종류 replay를 확인한다. fingerprint 문서 계획만으로 실제 검증 완료라고 표기하지 않는다.
