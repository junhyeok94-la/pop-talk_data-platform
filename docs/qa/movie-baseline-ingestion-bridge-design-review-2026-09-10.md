# 전체 영화 observation 편입·bridge 설계 선검토

판정: CHANGES REQUESTED. 원문 보존, 서비스 소유권 분리, long-form provenance 방향은 타당하다. 다만 실제 적재와 현재 모델 교체에 앞서 아래 계약을 확정해야 한다. 검토는 설계와 기존 소스 읽기로 진행했으며 업무 코드·데이터를 변경하지 않았다.

## 1. [P1] 서비스에만 보존된 영화 속성과 서비스 전용 영화의 통합 경로가 없음

대상: §3, §4.3, §5, §10.

앞선 대사는 서비스에만 KMDb ID가 있는 194건, 줄거리 194건, 포스터 180건을 확인했다. 이번 snapshot은 ID/상태/hash만 추출하므로 해당 값은 attribute observation에 들어오지 않는다. 서비스가 원천 정보의 최종 정답이 아니라는 원칙은 맞지만, 그 값들을 비교·보존 가능한 관측에서 제외해도 된다는 의미는 아니다. 현재 서비스 테이블을 변경하지 않으므로 즉시 유실되지는 않지만, 새 Gold가 기존 보강값을 보존한다는 후속 계약은 충족하지 못한다.

서비스 전용 10건 중 Gold에도 없는 행은 ‘KOFIC current 연결이 유일하고 유효할 때’만 생성하는 bridge에서 빠질 수 있다. 서비스 ID를 snapshot에 남기는 것과 모든 서비스 영화를 유효한 bridge로 연결하는 것은 다르다.

필수 수정:

- 서비스 보강값을 `SERVICE_PRESERVED` 같은 낮은 신뢰도의 독립 관측으로 필요한 열만 보존할지, 별도 보존 영역과 serving overlay에만 유지할지 선택한다. 서비스 승인/보정 필드와 원천 후보 속성을 구분하고 자동 최우선 원천으로 승격하지 않는다.
- 통합 영화가 아직 없는 service ID는 unresolved 관계로 보존하거나 명시적 정책에 따라 placeholder movie를 만든다. 제외 건수와 이유를 완료 조건에 포함한다.
- ‘Gold 합집합’이 legacy+API만인지 서비스 보존 관측까지인지 명시한다. bridge의 미해결 행을 성공한 연결처럼 세지 않는다.

## 2. [P1] 속성별 현재값 전략이 이전 정상 관측 유지 계약과 일부 충돌함

대상: §5.1, §5.2, §6.

§5.1은 최신 API가 NOT_COLLECTED이면 Legacy로 fallback한다고만 적는다. 예를 들어 API t1의 정상 제목/상영시간 → API t2의 NOT_COLLECTED가 발생하면 먼저 t1의 값을 유지해야 한다. 레거시로 바로 돌아가면 알려진 최신 값이 퇴행한다. ‘최신 API PRESENT 우선’과 fallback의 정확한 후보 순서를 고정해야 한다.

KMDb도 **같은 source identity를 사용하는 것**과 **한 observation 행의 모든 속성을 사용하는 것**은 다르다. 동일 확정 KMDb의 새 응답에서 포스터만 빠지면 이전 같은 entity의 포스터를 유지할 수 있어야 한다. 연결 교체 시에는 이전 entity의 모든 속성을 배제하되 새 entity 안에서는 필드별 PRESENT 선택을 적용해야 한다.

Legacy LEGACY_ACCEPTED와 신규 MATCHED 충돌도 §5.2에서 격리한다고만 되어 있다. 이전 값 유지, 해당 필드 null, 영화 전체 격리 중 어떤 결과를 내는지 정해야 한다. 취소/교체를 판정할 이벤트나 결정 원장도 현재 설계에 없다.

필수 수정: 연결 유효성 → 필드 상태 → 원천 신뢰도/시각 → 동률 처리 순서의 결정표를 작성한다. 최소 fixture는 이전 API PRESENT→최신 미수집, 같은 KMDb의 부분 미수집, 다른 KMDb로 교체, 명시적 연결 취소, legacy/API 충돌, 파싱 오류다. 각 예상 value와 provenance를 고정한다.

## 3. [P1] stable key 발급과 history의 시간·결정 원장이 정의되지 않음

대상: §4.2, §4.3, §6.

stable movie key가 어디서 어떻게 발급되고 유지되는지 없다. 기존 key는 canonical 문자열의 MD5이며, 신규 의미상의 stable key로 전환할 경우 팩트와 서비스 bridge를 어떤 규칙으로 연결할지도 필요하다.

Legacy의 source_observed_at은 null인데 history PK에 valid_from을 사용한다. 원천 관측시각을 valid_from으로 쓰면 키/순서가 없고, dbt 실행시각을 쓰면 rebuild 때마다 이력이 달라진다. ‘적재시각은 현재값 우선순위에 사용하지 않는다’는 원칙과 ‘결정이 시스템에 기록된 시각’은 분리해서 설명해야 한다.

`movie_source_match_observation`은 candidate identity를 grain에 넣으면서 UNMATCHED도 저장한다. 후보가 0개인 경우 행을 만들 수 없다. 규칙 버전·점수·근거도 기존 Silver의 상태/후보 수만으로 복원할 수 없다.

필수 수정:

- 영화 key registry 또는 결정론적 발급 규칙, 재사용 금지, 병합/분리 시 대체 관계를 정의한다.
- immutable decision ID와 결정 기록 시각을 둬 current/history를 재구성할지, 별도의 이력 append 모델을 둘지 선택한다. 원천 effective time과 시스템 decision time을 구분하고 null/동시각/동률 처리를 정한다.
- match evaluation(후보 0개 가능)과 candidate observation을 분리하거나 후보 없음의 별도 행 계약을 둔다. 점수·증거·규칙 버전의 생성 단계와 전달 필드를 명시한다.
- service snapshot에는 snapshot identity와 완결성 표시, 이전 snapshot 대비 행 부재/삭제의 의미가 필요하다. 서비스 상태 관측 이력을 source bridge의 연결 변경 이력과 혼동하지 않는다.

## 4. [P1] Asset OR의 혼합 입력과 artifact 일관성 계약이 미정

대상: §2.1~§2.3. 근거: 기존 `pipelines/orchestration/asset_contract.py`, 공통 DAG의 prepare/confirm_loaded_batch 경로.

두 Asset을 OR로 소비할 때 두 종류의 이벤트가 같은 DagRun에 포함될 수 있다. ‘서로 다른 kind를 조용히 섞지 않음’은 혼합 배치를 허용해 각각 처리한다는 뜻인지, 거부/분리한다는 뜻인지 불명확하다. 한쪽을 선택하고 나머지를 무시하는 구현은 허용하면 안 된다.

현재 resolver는 daily 경로와 24자리 raw_run_id를 요구하고 barrier는 batch 전체가 같은 artifact_version을 사용하도록 검증한다. Legacy는 기존 adapter에서 `legacy:<sha256>` source_run_id를 만들며, 별도 notebook이면 artifact의 의미도 달라질 수 있다. 이들을 단순 조건 분기로 연결할 수는 없다. 또한 §2.1 event 필드 목록에는 §2.2의 input_kind와 실제 생산자 lineage가 빠져 있다.

필수 수정: input_kind/bucket/manifest key/source identity/producer DAG·run/contract version을 포함한 discriminated READY 계약을 제시한다. 혼합 배치의 중복 제거와 완전 처리 기준을 명시하고, 공통 배포 artifact가 두 notebook을 모두 고정할지 input별 artifact를 barrier에서 검증할지 결정한다. 수동/Asset 모두 같은 검증 경로를 쓰고, daily+legacy+중복 이벤트가 포함된 배치의 성공/부분 실패 fixture를 완료 조건으로 둔다.

## 5. [P2] 시간 nullable 변경은 전체 계약 전환 순서와 이전 데이터 호환성이 필요함

대상: §2.4, §7. 근거: `pipelines/transforms/snowflake_exchange_loader.py`의 payload 검증과 DDL, 기존 observation_recency_order macro.

현재 loader는 source_observed_at이 없으면 거부하며 물리 movie/boxoffice 테이블 모두 NOT NULL이다. DDL만 nullable로 바꾸면 새 loader·Delta·Exchange 검증·dbt source/test 중 어느 하나와 불일치할 수 있다. 기존 API payload에는 새 known/basis 필드가 없으므로 ‘API 행을 변경하지 않음’과 새 필수 필드 계약을 동시에 충족할 호환 규칙도 필요하다.

필수 수정: 기존 API v1을 명시적 기본값(API_COLLECTED_AT/known=true)으로 읽는지, 새 contract 버전을 발급하는지 정한다. 변경 대상 계층·필드·검증·적용 순서를 표로 고정한다. Legacy가 없는 박스오피스까지 불필요하게 nullable로 완화하지 않는다. 알려진 시각은 known=true/non-null, 미상은 허용 source/basis/known=false/null이라는 불변식을 모두 테스트한다. NULLS LAST는 필요한 조건이지만 필드 상태·유효 연결 선택을 대신하지 않는다.

## 6. [P1] 구형 Gold가 레거시 적재 직후 게시되는 중간 상태를 차단해야 함

대상: §9. 근거: 현재 공통 DAG의 `loaded_batch >> dbt_gold >> confirmed_gold_identity >> publish_postgres`.

제안 순서에서는 3~5단계에 공통 DAG와 실제 적재를 먼저 연결하고, 필드별 current/bridge는 7~8단계에 구현한다. 현재 DAG는 적재 후 즉시 dbt와 PostgreSQL 분석 snapshot을 실행한다. 이 상태로 legacy를 넣으면 새 current 모델이 준비되기 전에 기존 행 전체 최신 선택/게시 경로가 작동한다. 사용자 공개 테이블은 아니어도 승인된 분석 snapshot을 의도하지 않은 중간 모델로 교체할 수 있다.

안전한 분할안:

1. **계약·순수 변환:** 위 ID/시각/상태/혼합 입력 계약 및 fixture부터 확정한다.
2. **격리 적재:** Legacy notebook/Exchange와 서비스 snapshot을 신규 staging 또는 게시 비활성 경로에 적재한다. 기존 성공 publication 선택 모델이 새 데이터를 자동 소비하지 않게 admission 경계를 둔다.
3. **후보 Gold:** 별도 schema/버전에서 identity/history/attribute/current를 만들고 기존 API 회귀, 서비스 보존값, 미연결 행을 대사한다.
4. **일괄 전환:** 검증된 dbt 배포와 호환 loader를 고정한 후 공통 DAG의 신규 입력을 활성화하고 분석 snapshot을 전환한다. 실제 서비스 공개 연결은 계속 별도 단계다.

이는 새 도구를 요구하는 것이 아니라 기존 배포/게시 경계로 중간 상태를 노출하지 않는 구현 순서다.

## 완료 기준 보완

- 5,985/4,320/1,665 검증은 정확한 legacy snapshot SHA와 정책 버전에 묶는다. 일반 모델에 모든 snapshot이 항상 그 건수여야 한다는 상수를 두지 않는다.
- movie_attribute_current는 movie_key × attribute_name 유일성, 타입별 값 표현, PRESENT(empty)와 null 구분, 선택 observation FK를 검증한다.
- fallback으로 서로 다른 시점의 필드를 조합한 후 policy_eligible이 어떤 입력 집합에 대한 판정인지 고정한다. 이전 observation의 정책 boolean을 새 조합 필드에 그대로 붙이면 모순될 수 있다. source별 정책 관측과 통합 current 정책을 분리하거나 current 값으로 재평가하고 버전을 남긴다.
- 118/80은 기존 검증 snapshot의 회귀 기준으로 사용한다. Legacy 편입 후 영화 수 증가를 회귀 실패로 보지 않고, 기존 날짜×영화 팩트의 값/키/계보 유지와 새 영화 합집합을 따로 검증한다.

위 계약과 격리 전환 순서를 보완하면 단계별 구현에 착수할 수 있다. 현 단계에서는 하나의 거대한 실제 적재 변경으로 진행하기보다 계약→격리 적재→후보 모델→전환의 네 단위로 나누는 것이 적절하다.
