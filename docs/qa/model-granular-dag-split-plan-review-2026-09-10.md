# 모델 단위 DAG 분할 계획 검토

판정: **CHANGES_REQUESTED**. 모델별 실행·재시도와 직접 부모만 기다리는 방향은 수용한다. 그러나 이벤트 식별자와 실제 조회 snapshot을 연결하는 계약이 빠져 있어 현재 계획만으로는 generation 일관성을 보장할 수 없다. 구현이나 실제 DAG 실행 없이 계획과 소스를 검토했다.

## 확인한 현재 구조

계획 원문, `pop_talk_movie_databricks_daily.py`, Raw DAG, dbt SQL/config/tests, PostgreSQL v3 publisher를 직접 확인했다. 현재 배포 pointer가 가리키는 `bf08df4b8ce3a5412d7931e5ed391e32d9772b0bb2cc624d70cbf2d5005ba2a8/manifest.json`도 읽었다. 작업 디렉터리의 오래된 target manifest를 기준으로 삼지 않았다.

실제 7개 모델에서 dim은 movie staging만, fact는 boxoffice staging만 참조한다. boxoffice mart는 dim/fact를 참조한다. 공통 quality는 두 staging, dim/fact, 공통 publication 모델 모두를 참조한다. 따라서 도메인 ledger/quality 분리는 조기 실행에 필요한 변경이다. staging과 mart는 view, dim/fact는 table이다.

## 1. [P1] 동일 generation 이벤트만 확인해도 실제 데이터 snapshot은 고정되지 않음

계획 3·5·6절의 generation 검사와 stale 실행 차단을 물리적 읽기/쓰기 계약으로 구체화해야 한다. 현재 staging은 SUCCESS ledger와 observation의 전체 최신 상태를 읽는 view이며, dim/fact는 전체 관측에서 최신 행을 고른다. 모델 SQL에는 선택한 batch/generation 범위가 없다. mart 역시 변경 가능한 dim/fact의 view다.

반례: dim(g1) Asset 발행 뒤 dim(g2)가 같은 테이블을 교체하고, 늦게 도착한 fact(g1)과 함께 mart(g1)을 실행한다. 이벤트 둘은 g1이어도 SQL은 dim(g2)+fact(g1)을 읽는다. staging view는 새로운 load만으로도 결과가 바뀌므로 같은 DAG의 max_active_runs=1이나 writer pool로 해결되지 않는다. 최종 activation 장벽으로 이미 섞여 계산된 결과를 복구할 수도 없다.

수정: generation별 immutable relation/schema 또는 명시적으로 고정된 입력 snapshot을 택하고, `ref()`/source가 정확한 부모 relation/version을 읽도록 규정한다. READY에 relation 식별자·입력 snapshot 집합·content digest를 연결하고 downstream test/export도 같은 snapshot을 사용한다. 과거 작업은 자기 immutable 결과만 생성하고 current pointer의 단조 증가 조건은 별도 CAS/lock으로 지킨다. g1/g2 branch 교차 완료와 후속 load 중 view 변화 반례를 완료 기준에 넣는다.

## 2. [P1] Airflow Asset AND는 batch별 조인이 아니며 불일치 실패만으로는 진행성이 없음

Airflow는 모든 입력 Asset이 이전 실행 이후 갱신되면 소비 DAG를 예약하며, 한 Asset의 여러 업데이트가 한 소비 실행으로 합쳐질 수 있다. extra의 batch_id를 기준으로 자동 매칭하지 않는다. [Airflow Asset scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html).

반례: A(g1), A(g2), B(g1)이 먼저 도착해 한 run을 만들고 그 run이 g2/g1 불일치로 실패한 뒤 B(g2)만 도착한다. 입력 이벤트를 단순 최신 선택하거나 실패로 끝내면 정상 g2 조합이 다음 실행으로 연결된다는 보장이 없다. 무변경 재실행을 no-op하면서 이벤트도 생략하면 다른 branch가 영구 대기할 수도 있다.

수정: Asset은 wake-up으로 사용하고 durable pending/consumed ledger에서 generation별 전체 직접 부모 조합을 찾는 규칙을 명시한다. 합쳐진 이벤트의 모든 미처리 generation, 역순/중복/재발행, 실패 후 재개, 빈 결과 READY를 처리한다. AND를 유지할 경우 이미 소비된 알림 이후 남은 조합을 다시 깨우는 경로도 명시한다. 또는 직접 부모의 OR wake-up과 ledger readiness gate를 사용한다. 이것은 모델의 실제 부모 조건을 느슨하게 만드는 것이 아니라 알림과 실행 가능성 판단을 분리하는 방식이다. 정상 지연을 매번 운영자 backfill로 해결하는 것을 기본 경로로 두지 않는다.

## 3. [P1] batch·revision·generation의 발급자와 구성원이 정의되지 않음

Raw 하나의 batch_id를 공유한다는 설명만으로 누적 DW snapshot과 최종 publication identity를 결정할 수 없다. 현재 소비 DAG는 한 run에서 여러 Raw READY를 처리할 수 있고, dbt는 누적 관측을 읽는다. 또한 도메인별 독립 재게시를 허용하면서 부모 publication_revision을 무조건 같게 요구하면 movie r2/boxoffice r1 조합의 정당한 수정도 진행할 수 없다.

수정: source batch와 서비스 generation을 구분한다. generation 발급 주체, 순서, required 모델 집합, 데이터 범위/cutoff, 공통 배포 compatibility, 부모별 정확한 revision/version 벡터를 담은 불변 build manifest를 정의한다. 독립 revision을 허용할지 전 branch revision을 같이 올릴지 선택하고 재사용 규칙을 명시한다. 한쪽 무변경/0행/정정, 누적 baseline, 다중 Raw 입력의 경우도 정의한다.

PostgreSQL v3 publisher는 현재 전체 snapshot digest를 알고 generation을 예약한 뒤 영화·박스오피스를 함께 적재한다. 분리 후에는 두 적재 DAG가 공유할 candidate header/ID, component READY와 digest, required component 집합, activation transaction과 stale 거부 순서를 새로 명시해야 한다. 늦게 끝난 과거 데이터가 뒤늦게 더 큰 generation을 예약해 최신으로 취급되지 않도록 순서를 고정한다.

## 4. [P2] 단일 모델 선택만으로 test scope와 registry 대사가 완성되지 않음

현재 `assert_mart_boxoffice_preserves_fact_grain`은 fact와 mart를 함께 참조한다. fact DAG에서 기본 eager test 선택을 사용하면 아직 준비되지 않은 mart까지 검사할 수 있다. 반대로 cautious로만 바꾸면 이 다중 모델 검사가 빠질 수 있다. 부모 없는 `assert_revision_precedes_completion_time`의 소유자도 필요하다. [dbt indirect selection](https://docs.getdbt.com/reference/global-configs/indirect-selection).

수정: 각 test unique_id의 소유 gate와 입력 snapshot 요구를 registry에 명시한다. fact-grain 검사는 mart gate에, 도메인 count 검사는 해당 quality gate에, 부모 없는 계약 검사는 CI/배포 검증 등에 배정한다. 누락과 중복을 자동 대사하고, 설치된 Cosmos/dbt에서 실제 모델 run 1개 및 정확한 test ID 목록을 검사한다. model `depends_on.nodes` 대사는 model뿐 아니라 source 매핑을 처리하며 publication 모델을 별도 DAG로 남길지 inline할지도 확정한다. 향후 ephemeral/seed/snapshot 처리 또는 명시적 거부 규칙도 둔다.

## 5. [P1] pause/unpause만으로 이중 writer 없는 전환을 보장하지 못함

단계 E의 schedule 제거·순차 unpause는 이미 실행/예약된 DagRun, retry/clear, Databricks 원격 작업, 수동 실행을 차단하는 writer fence가 아니다. candidate에서 production으로 바뀌는 dbt source/schema 설정, 기존 합본 ledger 이력 이관, 전환 중 쌓인 Raw 이벤트의 처리 기준도 없다.

수정: cutover watermark와 미처리 Raw 목록을 고정하고 기존 writer를 drain한 뒤 원격 실행 종료까지 확인한다. 전환 epoch/소유 lock 또는 권한 등 실제 쓰기 경계로 구 writer를 차단한다. candidate Asset/registry/source/schema도 production과 격리한다. 기존 observation을 신 ledger로 인정할 검증·bootstrap 규칙, watermark 이후 backlog 재생/중복 제거, 신규 writer 정지 후 rollback 순서를 문서화한다. 신규 production 활성화 직전에도 구 writer가 쓰지 못함을 확인한다. 단순 schedule 변경을 fence로 간주하지 않는다.

## DAG 분할 수준에 대한 의견

업무 모델별 DAG는 사용자가 원하는 장애 격리와 관찰성에 맞으므로 수용한다. helper, 개별 API 호출, test마다 별도 DAG를 추가할 필요는 없다. Raw가 출력 Asset 두 개를 갖는 것은 원칙 1의 명시적 예외로 기록하면 된다. 목표 목록은 Raw 포함 약 17개 DAG이며 publication 모델을 별도로 남기면 더 늘어난다. 공통 factory/registry와 pool을 사용하되, 분리된 Bronze/Silver 실행마다 생기는 원격 기동·bundle 전송·전체 dbt rebuild 비용을 측정하고 SLA에 맞춰 조절한다.

현재 Raw는 movie_list→details→KMDb→전체 validate 순서다. 도메인 READY를 따로 만들 때 실제 후보/상세 수집에 필요한 원천 의존성을 보존해야 한다. 이벤트 이름만 둘로 나누고 공통 완료 함수 뒤에서 함께 발행하면 조기 실행 효과는 없다. 빈 데이터도 성공 READY로 발행하며 실패와 구분한다.

최종 activation에만 서비스 전체 장벽을 두는 방향은 맞다. 다만 그 이전 모든 모델 경계에서도 정확한 부모 snapshot 결합은 필요하다. 위 다섯 계약을 보완한 뒤 계획을 재검토하며, 이번에는 구현하지 않는다.
