# 모델 단위 DAG 분할 계획 재검토

판정: **CHANGES_REQUESTED**. 최신 계획과 이전 검토서를 다시 읽었다. 기존 다섯 지적에 대한 핵심 보완은 수용하지만, generation 생성과 carry-forward에 두 가지 설계 모순이 남아 있다. 계획만 검토했으며 구현·DAG 실행·외부 데이터 변경은 하지 않았다.

## 기존 지적의 반영 상태

| 이전 지적 | 재검토 |
| --- | --- |
| 공유 view/table로 g1/g2 데이터 혼합 | generation별 append-only 결과와 exact parent 필터, 동일 snapshot test/export 계약으로 계획 수준에서 해결 |
| Asset AND와 이벤트 유실/역순 | pending/cohort ledger, 0..N scan과 READY 재발행으로 핵심 반례 해결 |
| batch/revision/generation 혼용 | 식별자 분리·cutoff·revision vector·PG header/CAS 수용. 아래 manifest 생성 시점과 재사용 규칙은 추가 보완 필요 |
| test 선택/소유 누락 | TestBehavior.NONE 및 test unique-ID 단일 소유 registry, source/무부모 test 배정 수용 |
| pause만으로 writer 전환 | drain·원격 실행 종료·writer fence·watermark·rollback 계획 수용 |

Airflow 3.3.1의 PartitionedAssetTimetable 사용 자체는 가능하다. 실제 event의 partition_key를 발행해야 하며 extra에 generation만 넣는 것으로 대체할 수 없다. 기본 IdentityMapper는 generation ID를 그대로 전달한다. 이는 구현 검증 항목이며 새 차단 지적은 아니다. [Airflow 공식 Asset partitions](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html).

## 1. [P1] 최초 불변 generation manifest가 아직 생성되지 않은 부모 digest를 요구함

위치: 계획 3.1절 generation 예약/manifest 필드, 4.0절, 4.5절, 3.5·4.6절 publication 예약.

계획은 source READY 등록 시 generation을 예약하고 불변 manifest에 모든 모델의 직접 부모 relation/digest vector를 넣는다. 그러나 새 generation의 dim/fact digest는 해당 모델 계산 뒤에만 알 수 있다. mart의 부모 digest를 최초 manifest 확정 조건으로 삼으면 부모 모델을 시작할 manifest를 만들 수 없다. 나중에 digest를 채워 넣으면 불변 manifest 계약을 어긴다. 모델별 cohort manifest를 별도로 두었지만 최초 generation manifest에 요구한 필드와의 생명주기가 아직 구분되지 않았다.

publication_id도 content-addressed인데 header를 먼저 예약하도록 되어 있다. 모든 component digest를 알아야 ID가 결정된다면 영화 candidate 적재가 박스오피스 완료를 기다리는 추가 전역 장벽이 된다. 이는 최종 activation에만 서비스 장벽을 둔다는 목표와 충돌한다.

수정:

- 최초 generation plan에는 source snapshot/cutoff, deployment, required graph 및 재사용할 기존 relation만 고정한다. 신규 모델 결과는 예측하지 않고 논리적 build ID로 참조한다.
- 각 모델 성공 뒤 immutable result manifest를 추가한다. 해당 직접 부모 result가 모두 확정됐을 때 모델별 immutable cohort manifest를 생성하고, 최초 plan 및 정확한 부모 result digest를 연결한다. 최초 manifest는 수정하지 않는다.
- PG 사전 header는 generation/plan 기반 ID로 예약하고 최종 content digest는 component가 모인 후 봉인하거나, component를 독립 ID로 먼저 적재하고 content-addressed publication을 마지막에 조립한다. 어느 방식을 택할지 명시한다.
- 완료 기준: boxoffice 결과 digest가 아직 없어도 movie staging→dim→영화 candidate 적재까지 진행 가능해야 한다. 아직 모르는 digest를 placeholder로 확정하거나 기존 불변 manifest를 수정하지 않아야 한다.

## 2. [P1] carry-forward가 직접 부모 변경의 전이 영향을 검사하지 않음

위치: 계획 3.1절의 movie r2/boxoffice r1 component carry-forward 예시 및 3.2·4.5절 cohort 구성.

boxoffice 원천 revision이 그대로인 것과 boxoffice 서비스 component를 재사용할 수 있는 것은 다르다. 현재 `mart_boxoffice_daily`는 fact 외에도 dim_movie에서 title, policy_eligible, release_date, genres를 조인한다. movie r2가 이 값을 바꾸면 fact r1은 재사용 가능하지만 이전 boxoffice mart/component는 새 dim과 일치하지 않는다. 계획은 boxoffice r1의 불변 component를 carry-forward한다고만 명시해 이 경우를 구분하지 않는다.

또한 coordinator는 parent event를 `(generation_id, parent_model)`로 모으는데, 재사용한 fact는 이전 generation 결과다. 새 generation용 carry-forward binding 없이 원래 event만 기다리면 재사용 모델이 새 event를 만들지 않아 cohort가 완성되지 않을 수 있다. 모든 원본 부모 generation이 같다는 검사도 재사용과 충돌한다.

수정:

- source 변경에서 시작해 manifest의 model 의존성 그래프를 따라 영향받는 후손을 재계산한다. 모델 재사용은 deployment/모델 계약과 exact parent result vector가 호환될 때만 허용한다. 단순 도메인 revision 비교로 서비스 component를 재사용하지 않는다.
- 새 generation의 논리적 parent slot과 실제 이전 generation의 immutable result를 연결하는 carry-forward binding을 원장에 기록한다. 이를 cohort readiness 증거로 인정하고 재발행/scan으로 복구 가능하게 한다. 같은 generation은 소비 build의 세대를 의미하며 모든 물리 부모의 원래 generation 값이 같아야 한다는 뜻으로 사용하지 않는다.
- 반례 fixture: movie title/policy 변경·boxoffice source 무변경에서 fact는 재사용하고 mart와 해당 serving component는 재계산한다. 새 event가 없는 재사용 부모도 binding으로 cohort가 완성돼야 한다.

## 구현 단계로 넘길 검증 조건

위 두 항목 외에 기존 방향을 다시 뒤집을 사유는 없다. 다음은 계획에 이미 포함됐거나 구현 시 구체화할 항목이다.

- partition_key 실제 발행, generation별 분리 실행, ledger 등록과 Asset 발행 사이 실패의 재발행 복구를 검증한다. scan은 단순 수동 가능성에 그치지 않고 운영상 실행되는 복구 경로여야 한다.
- append-only partition도 모델 결과 commit 및 SUCCESS/READY 경계가 필요하다. 부분 실패·동시 writer에서 성공으로 노출되는 행 집합이 하나인지 검증한다.
- Cosmos의 고정 graph deployment와 실행 generation deployment가 같아야 한다. 단일 모델 실행, 명시적 test ID 범위, exact parent 필터를 실제 렌더링/컴파일 결과로 확인한다.
- writer fence는 실제 권한 또는 모든 쓰기 경로가 준수하는 소유 epoch로 입증한다. pause나 선언만으로 대체하지 않는다.
- DAG 수 증가는 사용자 요구에 맞지만 cohort 준비는 공통 factory/원장 로직으로 단순화하고, helper·개별 test마다 불필요한 DAG를 추가하지 않는다.

두 설계 항목을 보완한 뒤 계획 승인 여부를 재검토한다. 구현 승인은 별도 단계다.
