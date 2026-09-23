# 모델 단위 DAG 단계 A 순수 계약 검토

판정: **CHANGES_REQUESTED**. 정상 흐름은 승인 계획을 따르지만, 공개 dataclass를 신뢰하는 경계와 registry 검증에 재현 가능한 누락이 있다. 업무 코드는 수정하지 않았다.

## 독립 검증

구현 기록, 두 모듈과 두 신규 test 파일을 직접 읽었다. Airflow 컨테이너에서 `python -m unittest discover -s pipelines -p "test_*.py"`에 해당하는 전체 suite를 독립 실행해 **114개 모두 통과**했다(0.185초).

current pointer의 실제 deployment `bf08df4b8ce3a5412d7931e5ed391e32d9772b0bb2cc624d70cbf2d5005ba2a8` manifest를 직접 읽고 registry의 **7 models / 43 tests**를 확인했다. 아래 반례는 제출 fixture와 `dataclasses.replace` 및 메모리 manifest만 사용했다. 실제 DAG/클라우드 쓰기는 수행하지 않았다.

## 1. [P1] source/cohort/result의 content ID와 의미를 재검증하지 않아 위조 입력이 통과함

대상: `model_generation_contract.py`의 create_generation_plan, create_model_result, _binding_for_model_parent 및 READY 생성 경계.

재현 결과:

- 정상 g1을 완성한 뒤 source를 `replace(source, content_sha256='9'*64)`로 바꾸고 snapshot_id는 유지했다. changed_resources=[]로 g2를 만들면 변경된 movie staging이 **REUSE**가 된다. 변경 자동 추론은 snapshot_id만 비교하기 때문에 source 본문 변경을 놓친다.
- 정상 source cohort를 `replace(cohort, parent_bindings=())`로 바꾼 뒤 create_model_result에 넘기면 부모가 없는 result가 정상 봉인된다. cohort ID를 재계산하지 않고, plan의 직접 부모 집합과 binding을 대사하지 않는다.
- 정상 staging result를 `replace(result, relation_id='OTHER.RELATION')`로 바꾸고 기존 result ID로 catalog에 넣으면 dim cohort가 그 relation을 그대로 채택한다. result ID와 내용의 일치를 검사하지 않는다.

frozen dataclass는 정상 생성 경로만 강제하지 않으며 외부 저장 adapter의 역직렬화 입력도 같은 문제를 갖는다. create 함수 호출 이력이나 ID 문자열 일치만으로 exact parent 결과임을 보장할 수 없다.

수정: GraphSnapshot/SourceSnapshot/GenerationPlan/Cohort/Result에 canonical ID 재계산과 구조·상호 lineage 검증을 제공하고 공개 소비 경계에서 적용한다. cohort는 plan의 정확한 부모 집합과 SOURCE/RESULT/REUSED_RESULT binding을 대사해야 한다. result는 검증된 cohort 및 해당 plan slot과 연결하고 catalog key=내부 result ID도 강제한다. 단순 ID 재계산만으로 의미상 틀린 부모 집합을 허용하지 않는다. READY extra는 durable result 조회의 힌트이며 별도 권위 증명이 아님을 adapter 계약에 명시한다.

## 2. [P1] scan의 완료 판정은 build_id만 보므로 다른 plan 결과가 정상 작업을 영구 억제할 수 있음

대상: `scan_ready_cohorts`의 completed_builds 및 중복 결과 검사.

정상 staging result의 build_id를 유지하고 plan_id='wrong', generation_id='gen-999999999999'로 바꿔 results에 넣었다. scan 결과에서 STG_MOVIE cohort가 사라졌다. 반면 자식 cohort의 부모 조회는 plan/generation이 달라 이 result를 사용할 수 없다. 결국 부모는 완료 취급하고 자식은 미완료 부모를 기다리는 정체가 생긴다.

완료 판정을 검증된 `(plan_id, generation_id, deployment_id, model_unique_id, build_id)` 전체 일치로 바꾼다. catalog 전체의 같은 slot 상충 result를 먼저 검사하고 동일 result ID의 논리 중복은 멱등 처리한다. 현재 복수 결과 검사는 자식이 해당 부모를 조회할 때만 실행되므로 자식 없는 마지막 모델의 상충 결과는 검출되지 않는다. 잘못된 타 plan 결과·마지막 모델 중복·catalog alias 중복 테스트를 추가한다.

generation_sequence의 전역 유일 예약, plan ID와 build slot의 중복 저장 거부는 미래 durable adapter 책임이다. 그러나 순수 scan에서도 잘못된 결과가 완료를 대신할 수 없도록 해야 한다.

## 3. [P2] 지원하지 않는 dbt resource를 거부하지 않고 그래프에서 누락시킴

대상: `graph_snapshot_from_manifest` 및 registry 생성.

staging의 부모를 `seed.p.input`으로 바꾸고 manifest에 seed node를 추가하면 graph가 정상 생성되며 staging parents는 빈 tuple이 된다. config.materialized='ephemeral'인 모델도 그대로 허용된다. 승인 계획은 ephemeral/seed/snapshot 지원 전 배포를 거부하도록 했으나, 현재 구현은 model/source prefix 필터로 부모를 조용히 버린다.

지원하지 않는 resource/materialization은 graph 생성 시 명시적으로 거부한다. 모든 선언된 model/source/test 부모가 실제 manifest의 올바른 resource인지 확인하고 미지원 부모를 필터로 생략하지 않는다. seed·snapshot 부모, ephemeral, dangling test source/model을 회귀 fixture에 포함한다.

## 4. [P2] 다중 부모 test owner가 모든 입력을 준비할 수 있는지 확인하지 않음

대상: `build_test_ownership`, `build_model_dag_registry`.

현재 배포의 `assert_mart_boxoffice_preserves_fact_grain` owner를 mart에서 fact MODEL_GATE로 바꿔 registry를 만들면 통과한다. fact의 direct parent는 staging뿐이므로 해당 gate에서 아직 실행되지 않은 mart를 읽게 된다. owner가 test 부모 중 하나라는 검사만으로 마지막/quality gate라는 주석의 계약을 보장하지 못한다. 제출 신규 테스트도 서로 독립인 a/b의 다중 test를 b gate에 배정하는 것을 정상으로 인정하지만, b DAG가 a를 기다리는 cohort는 생성하지 않는다.

수정: MODEL_GATE의 test 부모가 owner 자신과 그 실행에서 고정되는 ancestor result 범위 안에 있는지 검사한다. 독립 sibling을 요구하면 해당 test의 모든 부모를 기다리는 명시적 quality gate/cohort를 제공하거나 해당 배정을 거부한다. owner_kind enum과 MODEL/SOURCE resource 종류도 검증한다. SOURCE_GATE는 실제 존재하는 source를 소유해야 한다. 소유권 개수 43뿐 아니라 모든 test 입력이 해당 gate의 exact snapshot vector로 해결되는지 자동 대사한다.

## 수용 사항 및 다음 adapter 경계

정상 factory 입력에서는 source snapshot 변경 추론, graph 후손 REBUILD, g2 dim+g1 fact 재사용 binding, 부모 준비 후 cohort 생성이 작동한다. 현재 manifest의 기본 owner override도 fact-grain 검사를 mart에 배정해 적절하다. 실제 partition_key 비교도 확인했다. 위 지적은 정상 경로를 뒤집는 요구가 아니라 잘못된 입력·향후 graph 변경을 조용히 수용하지 않게 하는 보완이다.

실제 relation 존재/허용 namespace·content digest·test 실행 성공은 순수 manifest constructor가 증명할 수 없으며 adapter가 검증한 뒤 결과를 봉인해야 한다. generation/cutoff 순서, 원자적 slot claim 및 result commit, publication outbox/재발행, partitioned Airflow 실행, Cosmos 선택 결과와 실제 SQL snapshot 고정은 아직 미구현 범위다.

`existing_cohort_ids`는 저장됐다는 사실과 이벤트 발행/소비 완료를 구분하지 않는다. 미래 adapter에서 persist 후 emit 전 실패를 복구하도록 outbox 상태/미발행 재스캔 기준을 명시해야 한다. 이번 순수 scan 테스트를 durable event delivery 검증으로 표현하지 않는다.

위 네 항목 보완 후 재검토한다. 이번 승인은 보류하며 구현 코드는 변경하지 않았다.
