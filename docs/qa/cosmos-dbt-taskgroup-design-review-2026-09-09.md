# Cosmos 설계 검토

판정: CHANGES REQUESTED. Cosmos 도입 방향은 타당하지만 아래 실행 계약을 설계에 확정한 뒤 구현하는 것이 적절하다. dependency와 DAG는 변경하지 않았다.

## P1 — 기존 dbt 테스트의 실행 범위가 정의되지 않음

설계의 ‘테스트 표현’ 절은 TestBehavior, indirect selection, 여러 부모/부모 없는 테스트 처리 방식을 지정하지 않았다. 현재 assert_mart_boxoffice_preserves_fact_grain.sql은 fact와 mart 두 모델을 참조한다. AFTER_EACH 기본 동작에서는 부모 중 하나만 생성된 시점에 다른 부모를 읽는 테스트가 실행될 수 있다. should_detach_multiple_parents_tests=True 등 전체 부모 완료 의존성이 필요하다.

또한 assert_revision_precedes_completion_time.sql은 ref/source 없이 fixture와 macro만 사용하는 독립 singular test다. 모델별 dbt test 선택만으로 이 테스트 실행을 보장할 수 없다. 1.15.1 구현은 AFTER_EACH에서 모델별 테스트 태스크를 생성하며, 기본적으로 각 dbt test 하나마다 태스크를 만드는 방식도 아니다.

권고: AFTER_EACH + 다중 부모 테스트 분리 + 부모 없는 테스트의 명시적 실행을 정의하거나, 모델별 run 이후 전체 dbt test 게이트를 둔다. UI의 모델별 테스트 묶음과 개별 테스트 표시를 구분한다. 기존 43개 테스트 unique_id와 실제 실행 선택 집합을 대사하고 누락 시 실패시킨다. 독립 테스트 실패, 다중 부모 중 하나 실패, 중간 모델 실패, skipped/upstream_failed에서 publish_postgres가 실행되지 않는지 검증한다. 전체 내부 경로가 ALL_SUCCESS이고 누락된 테스트가 없을 때에만 모든 leaf 성공 게이트가 충분하다.

근거: [Cosmos testing behavior](https://astronomer.github.io/astronomer-cosmos/guides/translate_dbt_to_airflow/testing-behavior.html), [1.15.1 graph implementation](https://raw.githubusercontent.com/astronomer/astronomer-cosmos/astronomer-cosmos-v1.15.1/cosmos/airflow/graph.py).

## P1 — sidecar가 실제 렌더링된 그래프와 연결되지 않음

‘Parsing 방식’의 source digest와 runtime sidecar 비교만으로는 파싱 시 사용한 manifest와 실행 시 manifest가 같음을 입증하지 못한다. 그래프 A를 파싱한 뒤 파일/sidecar/SQL이 B로 교체되면 runtime에서 B끼리 비교해 통과하면서 A의 태스크 집합으로 B 코드를 실행할 수 있다. publish 직전 source digest 비교도 B로 일치하여 빠진 모델을 검출하지 못한다.

권고: manifest 바이트 SHA256, source model_version, dbt/adapter 버전 및 target/config를 하나의 생성 계약으로 묶고 원자적으로 배포한다. 파싱에 사용한 manifest digest/그래프 식별자를 validate_dbt_graph의 고정 인자로 전달해 runtime의 manifest/sidecar/source와 함께 비교한다. 읽은 manifest와 Cosmos가 다시 읽는 manifest 사이 교체도 막도록 digest별 불변 경로 또는 불변 배포 bundle을 사용한다. 태스크 실행 코드도 해당 배포에 고정하고 진행 중 변경은 새 run으로 전환한다. 단순히 실행 직전 파일을 다시 hash하는 것만으로 고정 그래프를 증명하지 않는다.

manifest/sidecar는 model_version 계산 대상 밖에 두어 자기참조 digest를 피한다. A 파싱 → B 배포, manifest만 교체, sidecar만 교체, SQL만 교체, 실행 도중 교체의 부정 테스트를 포함한다. manifest 생성은 별도 명령에서 실행하며 DAG import에서는 로컬 고정 artifact 읽기만 허용한다.

## P2 — 모델별 동시 실행과 timeout의 의미 변경이 빠짐

기존 dbt build는 한 프로세스에서 threads=2로 실행됐다. TaskGroup에서는 DAG 동시 태스크 한도까지 독립 dbt 프로세스가 뜨므로 같은 threads 설정을 보존해도 전체 DB 동시성이 보존되지 않는다. max_active_runs=1은 태스크 병렬성을 제한하지 않는다. 고정 target/log 경로를 그대로 전달하면 동시 프로세스의 manifest/run_results/partial parsing 파일이 충돌할 수도 있다.

권고: Cosmos용 pool 또는 태스크 동시성 예산, 각 subprocess의 threads, 실행별 독립 writable target/log 경로를 명시한다. Cosmos가 만드는 임시 작업 디렉터리 및 artifact 경로를 실제 command 검사로 확인한다. 기존 2시간 timeout은 이제 태스크마다 적용되어 전체 dbt 단계 제한과 달라진다는 점을 문서화하고 필요한 전체 기한을 별도로 정한다. 모델/test 재시도는 전체 build 재실행과 다르므로 부분 성공 후 재시도, downstream 재실행, 게시 전 실패에도 기존 PG active snapshot이 유지되는지 검증한다.

## 버전·인증·파싱 방향에 대한 판단

- 공식 1.15.1 릴리스에 Airflow 3.3 관련 CI/문서 변경이 있으며 이 버전 선택은 합리적이다. 이 프로젝트의 정확한 provider 조합까지 검증됐다는 의미는 아니므로 실제 이미지 pip check와 parser 검증은 필요하다. [공식 릴리스](https://github.com/astronomer/astronomer-cosmos/releases/tag/astronomer-cosmos-v1.15.1)
- Airflow와 dbt를 분리한 LOCAL 실행은 공식 의존성 충돌 회피 방향과 맞다. SUBPROCESS invocation과 /opt/dbt-venv/bin/dbt를 명시하고 Airflow 환경에 dbt extras가 유입되지 않게 한다. Airflow 3.3.1도 설치 제약에 고정하고 두 환경의 pip check/버전 확인을 수행한다. [공식 의존성 안내](https://astronomer.github.io/astronomer-cosmos/guides/dbt_setup/execution-modes-local-conflicts.html)
- 기존 profiles.yml + profile_name/target_name 명시 방식은 타당하다. RSA 개인키는 기존 read-only mount 경로에 유지하고 manifest/sidecar/XCom에 키 내용을 포함하지 않는다. [기존 profile 사용](https://astronomer.github.io/astronomer-cosmos/guides/connect_database/use-your-profiles-yml.html)
- DBT_MANIFEST는 subprocess 없는 파싱에 적절하다. 로컬 경로와 명시적 load mode를 사용하고 파일 부재 시 DBT_LS 등으로 fallback하지 않는다. Asset/OpenLineage 비활성은 실제 설치 버전의 설정과 생성 operator 속성으로 검증한다.

세 항목을 설계에 반영한 뒤 설계 승인과 구현 검토를 진행할 수 있다. 이번에는 설치나 외부 DB 실행을 하지 않았으며, 버전 판단은 공식 자료 및 현재 프로젝트 코드 대조에 근거한다.
