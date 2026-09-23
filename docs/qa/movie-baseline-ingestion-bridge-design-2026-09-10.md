# 전체 영화 observation 편입과 식별자 bridge v1 설계

- 상태: 선검토 수정본
- 선행 승인: `movie-baseline-reconciliation-implementation-rereview-2026-09-10.md`
- 목적: Legacy 5,985건, 일일 API 관측, 서비스 보존 관측을 동일 영화 모델에서 추적한다.
- 비목표: 사용자 공개 mart 전환, KMDb VOD/등급 교정, 인물·장르 정상화, 리뷰 편입

## 1. 원칙

1. Legacy 5,985건은 필터링하지 않고 초기 원천 관측으로 보존한다.
2. 정책 제외는 행 삭제가 아니라 적격 여부와 사유로 표현한다.
3. Legacy/API/서비스 보존 관측은 서로 덮어쓰지 않고 함께 저장한다.
4. 서비스 DB는 service ID·공개·승인·운영 관계의 소유 원천이다. 서비스에만 남은 batch-owned 값은 낮은 신뢰도의 보존 관측으로 유지한다.
5. 시각 미상의 Legacy를 적재시각으로 최신화하지 않는다.
6. 확정 ID 연결, 매칭 평가, 후보를 서로 다른 grain으로 관리한다.
7. 기존 일일 파이프라인과 분석 snapshot은 후보 모델 검증 전까지 바꾸지 않는다.

## 2. 안전한 구현·전환 단위

한 번에 Main DAG에 연결하지 않고 네 단위로 진행한다.

1. **계약·순수 변환:** ID, stable key, decision, 시각, 값 상태와 현재값 fixture를 구현한다.
2. **격리 적재:** Legacy와 service-preserved snapshot을 candidate 전용 Databricks table, S3 prefix, Snowflake schema에 적재한다.
3. **후보 Gold:** candidate schema에서 registry, bridge, attribute current와 새 `dim_movie`를 만들고 기존 Gold와 대사한다.
4. **일괄 전환:** 호환 loader·DDL·dbt immutable deployment·DAG를 함께 고정해 production observation과 분석 snapshot을 전환한다.

격리 적재와 후보 Gold가 승인되기 전에는 현재 Main DAG의 `dbt_gold → publish_postgres` 체인에 Legacy를 연결하지 않는다.

## 3. Legacy candidate 파이프라인

신규 수동 DAG `pop_talk_movie_legacy_baseline_candidate`를 만든다.

```text
정확한 Legacy SUCCESS manifest
  → manifest·원본 bytes/hash/count 검증
  → immutable bundle
  → Databricks candidate Bronze/Silver
  → S3 candidate Exchange
  → Snowflake candidate observation
```

- 임의 latest를 사용하지 않고 정확한 manifest key를 받는다.
- 원본을 다시 수집하거나 변경하지 않는다.
- XCom에는 manifest key, source SHA-256, row count, bundle identity만 넣는다.
- 기존 Main DAG를 trigger하는 Asset은 후보 단계에서 발행하지 않는다.
- Legacy는 1회 기준선이므로 daily Asset과 OR 배치로 섞지 않는다.

Legacy bundle에는 manifest, 원본 snapshot, 변환 source, 사전 계산한 Silver JSONL과 checksum을 넣는다. Databricks는 같은 source로 다시 계산하여 JSONL을 byte 단위로 검증한다.

- Candidate Bronze grain: S3 Legacy 원본 객체 한 개
- Candidate Silver grain: source run × transform version × canonical movie key
- boxoffice JSONL: 명시적인 0행 파일
- candidate ledger grain: READY key × transform version × publication revision × processing attempt

후보 승인 후 baseline observation을 production staging으로 승격하고 새 dbt deployment를 전환한다. 기존 24자리 daily resolver와 batch barrier를 Legacy에 맞추려고 일반화하지 않는다.

## 4. 시각과 Exchange 호환 계약

Silver observation에 다음 필드를 추가한다.

| 필드 | 의미 |
|---|---|
| `source_observed_at` | 신뢰 가능한 원천 관측시각. Legacy는 null |
| `source_observed_at_known` | 원천 관측시각 존재 여부 |
| `source_time_basis` | `API_COLLECTED_AT`, `LEGACY_UNKNOWN`, `SERVICE_SNAPSHOT_AT` |
| `ingested_at`, `processed_at` | 저장·변환 시각. source 최신 순위로 직접 사용하지 않음 |

- API Exchange v1은 기존 payload를 읽을 때 known=true, basis=`API_COLLECTED_AT`을 합성한다.
- Legacy candidate는 Exchange v2로 게시하며 null/false/`LEGACY_UNKNOWN` 조합을 강제한다.
- Service snapshot은 추출시각/true/`SERVICE_SNAPSHOT_AT`이지만 source 신뢰도는 API·Legacy와 별도로 낮게 둔다.
- Candidate schema에서 nullable 계약을 먼저 검증한다.
- Production 전환 시 movie observation의 시간 컬럼만 nullable로 바꾸고 boxoffice NOT NULL은 유지한다.
- 적용 순서: v1/v2 reader → candidate DDL/staging → production movie DDL → 새 loader/dbt/DAG.

최신 순위에는 `source_observed_at DESC NULLS LAST`를 사용하지만, 이것만으로 현재값을 선택하지 않고 연결·값 상태·신뢰도를 먼저 적용한다.

## 5. Service-preserved snapshot

PostgreSQL의 서비스 ID 관계와 기존에 서비스에만 남은 KMDb ID·줄거리·포스터 등을 보호하기 위해 읽기 전용 snapshot을 만든다.

```text
PostgreSQL REPEATABLE READ READ ONLY
  → service ID·상태 + batch-owned 영화 속성
  → S3 raw/service_movie_snapshot/v1
  → Snowflake candidate staging
```

- 속성 행은 `SERVICE_PRESERVED`라는 낮은 신뢰도의 독립 관측이며 자동 최우선이 아니다.
- service ID, KOFIC/KMDb ID, batch-owned 속성, 공개·승인·논리삭제, source hash, 추출시각을 포함한다.
- 관리자 보정 본문, 회원 데이터, 임베딩 벡터는 포함하지 않는다.
- content SHA-256을 snapshot identity로 사용하고 `complete_snapshot=true`, DB/schema, 추출시각, row count를 manifest에 기록한다.
- snapshot 간 행 부재를 자동 삭제로 해석하지 않는다.
- 유효 KOFIC ID가 있지만 registry에 없는 service 영화는 결정론적 KOFIC key 발급 후보가 된다.
- missing/invalid/ambiguous KOFIC ID는 unresolved service relation으로 보존하고 placeholder movie를 만들지 않는다.

Candidate Gold 집합은 Legacy + API + 유효 KOFIC ID를 가진 SERVICE_PRESERVED 관측의 합집합이다. unresolved service relation은 Gold 영화나 성공 bridge 수에 포함하지 않는다.

## 6. 영화 key와 결정 원장

### 6.1 `movie_key_registry`

- 기존 `canonical_movie_key=kofic:<id>`의 MD5 `movie_key`를 호환 초기값으로 등록한다.
- 새 유효 KOFIC identity도 같은 결정론적 규칙으로 최초 key를 발급한다.
- 발급된 key는 다른 영화에 재사용하지 않는다.
- KOFIC ID 정정이 같은 영화로 승인되면 새 identity를 기존 movie key에 연결한다.
- 병합·분리는 `movie_key_replacement`에 old/new key, reason, immutable decision ID를 기록한다.

확정·취소·교체 결정에는 `decision_id`, `predecessor_decision_id`, `decision_sequence`, `decision_recorded_at`, `effective_at`, `effective_time_basis`, rule version, evidence hash가 있다.

- `decision_id = sha256(identity + movie_key + decision_type/status + rule_version + evidence_hash + predecessor_decision_id + effective_at/time_basis)`다.
- predecessor를 hash에 포함하므로 `A 연결 → 취소 → A 재연결`은 처음 A와 다른 event ID를 갖는다.
- 같은 event 재시도는 같은 ID로 idempotent하게 무시하고 최초 append의 recorded time과 sequence를 유지한다.
- recorded time은 최초 저장 시각이며 rebuild 때 갱신하지 않는다.
- effective time은 원천 효력시각으로 null일 수 있고 history 정렬 키로 단독 사용하지 않는다.
- `decision_sequence`는 identity별 append 시 한번 할당되는 단조 증가 순서다. 미상 effective time과 동일시각 결정도 sequence로 순서를 확정한다.
- history의 유효구간은 `valid_from_decision_sequence`와 `valid_to_decision_sequence`로 표현하고 identity별 중첩을 금지한다.

## 7. Source와 Service bridge

### 7.1 원천 entity와 연결

- `source_movie_identity`: source system × entity type × normalized source ID
- `bridge_movie_source_current`: source identity × movie key의 현재 확정 연결
- `bridge_movie_source_history`: source identity × movie key × valid from
- KOFIC identity의 활성 확정 movie key는 하나다.
- 같은 KMDb identity가 여러 movie에 연결되면 자동 확정하지 않고 품질 격리한다.

매칭은 두 모델로 분리한다.

- `movie_source_match_evaluation`: source run × canonical movie × rule version × evidence hash × candidate set hash. 후보 0건인 UNMATCHED도 한 행을 만든다. 같은 평가 identity에서 status/selected가 달라지면 새 평가가 아니라 immutable 내용 충돌이다.
- `movie_source_match_candidate`: evaluation ID × candidate source identity. 후보별 점수·순위·근거 hash를 저장한다.

Legacy의 `LEGACY_ACCEPTED`는 신규 MATCHED와 같은 확정 신뢰도로 승격하지 않는다. 다만 현재 보강값을 모두 잃지 않도록 다음 초기화 정책을 사용한다.

- 유효 KMDb ID가 있고 같은 KOFIC 영화 안에서 Legacy/Service가 충돌하지 않으면 `PROVISIONAL_LEGACY` 또는 `PROVISIONAL_SERVICE` 연결을 만든다.
- provisional은 confirmed bridge와 분리하며 candidate 관리자 모델에서만 사용할 수 있다.
- confirmed 연결이 생기면 provisional보다 항상 우선한다.
- 서로 다른 provisional KMDb ID가 충돌하면 연결하지 않고 `REVIEW_REQUIRED`로 격리한다.
- provisional 사용 행은 `kmdb_connection_confidence=PROVISIONAL`과 source를 반드시 노출하며 향후 사용자 공개 mart의 자동 승인 근거가 되지 않는다.
- 유효 ID가 없거나 충돌해 초기화하지 못한 영화 수를 품질 마트에서 집계한다.

### 7.2 서비스 연결

- `bridge_movie_service_current`: service system × service movie ID
- `bridge_movie_service_history`: service system × service movie ID × valid from
- 유효·유일 KOFIC이면 registry key와 연결하며 registry에 없으면 먼저 발급한다.
- KOFIC missing/invalid/ambiguous는 unresolved로 보존한다.
- service 상태 snapshot 이력과 ID bridge 변경 이력은 분리한다.

## 8. 속성 현재값과 provenance

`movie_attribute_observation` grain은 movie observation × attribute name이다. 상태는 `PRESENT`, `NOT_COLLECTED`, `MATCH_PENDING`, `EXPLICITLY_CLEARED`, `PARSE_ERROR`다. v1 원천에는 명시적 삭제 이벤트가 없으므로 `EXPLICITLY_CLEARED`를 생성하지 않는다.

`movie_attribute_current`는 movie key × attribute name 한 행이며 value, source system/ID/run, observed time, state, 연결 confidence, 선택 observation FK를 보존한다. `dim_movie`는 필요한 current 속성을 pivot한다.

선택 순서는 다음과 같다.

```text
활성 confirmed source 연결, 없으면 무충돌 provisional 연결
  → PRESENT 상태
  → 원천 신뢰도
  → 같은 원천의 최신 알려진 관측시각
  → immutable decision 순서
```

### 8.1 KOFIC 핵심 필드

- 최신 API PRESENT → 이전 API PRESENT → Legacy PRESENT → SERVICE_PRESERVED PRESENT
- 최신 API가 NOT_COLLECTED/PARSE_ERROR이면 바로 Legacy로 퇴행하지 않고 이전 API PRESENT를 먼저 유지한다.
- 빈 문자열은 명시적 삭제로 취급하지 않는다.

### 8.2 KMDb 보강 필드

- 먼저 현재 활성 확정 KMDb identity를 고른다.
- 같은 identity 안에서 속성별 최신 PRESENT → 이전 PRESENT를 선택한다.
- 최신 응답에서 포스터만 미수집이면 같은 identity의 이전 포스터를 유지한다.
- 다른 KMDb identity로 확정 교체되면 이전 identity의 모든 값을 후보에서 제외한다.
- 명시적 연결 취소는 이전 보강값을 current에서 제거한다.
- Legacy accepted와 API matched가 다른 entity면 REVIEW_REQUIRED다. 이전 active가 있으면 검토 중 유지하고, 없으면 KMDb current를 내지 않는다.
- 동일시각 상충 값도 REVIEW_REQUIRED이며 이전 active가 있으면 유지한다.

필수 fixture는 API PRESENT→최신 미수집, 같은 KMDb 부분 미수집, 다른 KMDb 교체, 명시 취소, Legacy/API 충돌, PARSE_ERROR, 무충돌 Legacy provisional 초기화, Service-only provisional, provisional 충돌 격리의 예상 value와 provenance를 고정한다.

## 9. 정책 모델

원천 observation의 `policy_eligible`은 해당 행에 대한 당시 정책 관측으로 보존한다. 여러 관측의 필드를 조합한 current 영화는 선택 완료 후 current 속성으로 정책을 다시 평가해 `current_policy_eligible`, reasons, policy version을 생성한다. 원천 정책 boolean을 혼합 current에 복사하지 않는다.

## 10. 테스트와 승격 조건

- 정확한 Legacy snapshot SHA에 대해 5,985/4,320/1,665 대사
- Legacy/API/Service 각 observation과 time-state 불변식
- API v1 호환 기본값과 Legacy v2 null 시간 계약
- registry key 재사용 금지와 replacement 계약
- 같은 decision replay의 sequence/recorded time 유지, A→취소→A의 서로 다른 event ID, effective time 미상·동시각 sequence fixture
- source/service current 유일성, history 기간 비중첩
- match evaluation 0후보와 candidate FK
- attribute current의 movie key × name 유일성, 타입/value state, 선택 observation FK
- 모든 fallback·KMDb 교체 fixture
- current 필드로 정책 재평가
- 기존 날짜×영화 80개 boxoffice fact의 key/value/계보 회귀
- Candidate Gold 집합 = Legacy + API + 유효 Service 보존 관측 합집합
- unresolved service 수와 이유 대사
- confirmed/provisional/미노출 KMDb current 수와 이유 대사

## 11. 구현 순서

1. 값 상태·stable key·decision·Exchange v2 순수 계약과 fixture
2. Legacy bundle/notebook과 candidate Databricks/S3/Snowflake 격리 적재
3. Service-preserved snapshot과 candidate staging
4. Candidate registry, source/service bridge와 decision history
5. Candidate attribute observation/current와 `dim_movie`
6. 기존 API fact·계보 회귀와 candidate 합집합 검증
7. v1/v2 loader, production DDL, dbt immutable deployment 동시 준비
8. 승인 후 일괄 전환과 PostgreSQL 분석 snapshot 검증
9. 단계별 독립 검토와 수정 반복

## 12. 완료 기준

- Legacy 5,985건이 candidate를 거쳐 production Bronze/Silver/Snowflake observation에 존재
- Legacy/API/Service 보존 관측이 별도 lineage로 공존
- Candidate 및 전환 후 Gold 영화 수가 합집합으로 설명되고 unresolved는 분리됨
- current source/service bridge와 history가 계약 테스트를 통과
- `dim_movie` 각 속성의 선택 원천을 SQL로 설명 가능
- 기존 박스오피스와 분석 snapshot이 회귀 없이 생성
- 실제 서비스 공개 테이블은 변경하지 않음
- 독립 검토 승인
