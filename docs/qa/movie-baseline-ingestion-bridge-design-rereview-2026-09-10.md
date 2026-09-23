# 전체 영화 observation·bridge 설계 재검토

최종 판정: APPROVED — 추가 수정본에서 남은 두 계약이 반영됐다. 계약·fixture, 격리 적재, 후보 current/history 구현을 단계별로 진행할 수 있다. production 전환은 후보 구현과 실제 검증 결과를 검토한 후 별도로 판단한다. 아래 최초 재검토 내용은 이력으로 남기며, 수정 요청의 최종 처리 결과는 마지막 절을 따른다.

## 반영된 사항

- 계약 → 격리 적재 → 후보 Gold → 전환의 admission 경계를 명시했다. 기존 daily resolver를 일반화하지 않고 Legacy candidate에서 운영 Asset을 발행하지 않는 방식은 적절하다.
- SERVICE_PRESERVED 속성 관측, unresolved service 관계, Legacy/API/유효 Service 합집합을 추가했다.
- 결정론적 초기 movie key와 replacement 기록, match evaluation/candidate 분리가 추가됐다.
- API 이전 PRESENT 우선, 같은 KMDb entity의 필드별 유지, 연결 교체/취소, current 정책 재평가가 반영됐다.
- API v1 기본값, Legacy v2 시간 계약, movie-only nullable 및 boxoffice NOT NULL 유지가 명시됐다.

## 1. [P1] 최초 KMDb 연결이 없을 때 보존 속성을 어떻게 current로 내보낼지 미정

대상: §5, §7.1, §8.

SERVICE_PRESERVED에 속성을 보존하는 것까지는 해결됐다. 그러나 전체 선택의 첫 조건은 ‘활성 source 연결’, KMDb 선택은 ‘현재 활성 확정 KMDb identity’다. Legacy accepted는 신규 MATCHED와 같은 신뢰도로 승격하지 않으며 SERVICE_PRESERVED가 활성 연결을 생성할 조건도 없다. 따라서 API MATCHED가 없는 기준선 영화는 포스터·줄거리 등을 저장해도 current 후보에서 전부 제외될 수 있다. ‘과거 active가 있으면 유지’ 규칙은 최초 registry/bridge 구성 시 active가 어떻게 생기는지 답하지 않는다.

다음 중 의도하는 정책을 명시해야 한다.

- Legacy/서비스의 단일·무충돌 ID를 제한된 신뢰도의 잠정 연결로 초기화하고, 보강값에 provisional 상태와 provenance를 표시한다. confirmed와 구분하며 공유 KMDb ID는 격리한다.
- 검수 전에는 current를 비우되 SERVICE_PRESERVED/Legacy 값을 보존 영역에만 둔다. 이 경우 보강값 미노출 수를 완료 조건에 넣고, ‘보존 관측 적재’와 ‘현재 영화 속성 보존’을 구분한다.

필수 fixture: API 없음+Legacy accepted만 존재, API 없음+Service에만 KMDb ID/보강값 존재, 두 보존 원천이 같은 ID, 서로 다른 ID, KMDb ID 없이 보강 텍스트만 존재, 공유 KMDb ID. 각 경우 초기 연결 상태와 최종 value/state를 정해야 한다. 관측 보존과 자동 확정 매칭을 동일시하지 않는 것이 핵심이다.

## 2. [P2] decision 원장은 추가됐지만 history 시간 키와 반복 결정의 정체성이 미정

대상: §6, §7, §8.

effective_at/decision_recorded_at을 구분한 방향은 맞다. 다만 history의 valid_from을 어느 시각에서 만들고, 시각 미상·동시각 결정을 어떻게 처리하는지 아직 연결 규칙이 없다. 같은 source가 A 연결 → 취소 → A 재연결되는 경우 content hash가 같은 최초 연결과 재연결을 같은 decision으로 축약할 수도 있다. 반대로 recorded_at을 hash에 넣으면 동일 입력 재실행이 새 결정으로 생길 수 있다.

보완 사항:

- decision_id hash에 포함할 source observation/evaluation 또는 명시적 결정 식별자와 rule version을 정한다. 동일 입력의 반복 처리는 같은 decision, 별개의 재연결 결정은 다른 decision이 되도록 한다.
- recorded_at은 최초 append 시 고정하고 dbt rebuild마다 재생성하지 않는다.
- valid_from/valid_to를 시스템 결정 이력으로 정의할지 원천 효력 이력으로 정의할지 선택한다. 미상 effective_at을 허위 시각으로 만들지 않는다.
- 동시각 결정에는 안정적인 sequence/선행 decision 관계 등을 두거나 REVIEW_REQUIRED로 격리한다. 임의 hash 정렬이 의미적 확정 결정을 대신하면 안 된다.
- fixture로 같은 결정 두 번 반영, A→취소→A, 시각 미상 Legacy, 같은 시각 상충 결정, 전체 history 재구성의 동일 결과를 확인한다.

## 구현 가능한 분할점

지금 진행 가능한 것은 명시된 v1/v2 reader·time-state 검증, 원문/관측 변환, immutable bundle, SERVICE_PRESERVED snapshot 및 candidate 격리 적재 준비다. 이는 위 두 미정 계약을 임의 결정하지 않고도 구현할 수 있다.

candidate attribute current와 bridge history는 위 정책을 fixture로 확정한 뒤 구현한다. 격리 적재 승인과 production 승격을 분리한 현재 계획은 유지한다. production 승격은 새 loader/DDL/dbt 배포의 준비만으로 자동 실행하지 않고, 실제 단계별 증빙을 보고 판단한다.

## 범위

수정 설계를 이전 선검토 보고서와 대조했다. 이번에 업무 코드 변경이나 외부 데이터 조회·적재는 수행하지 않았다. 이전 지적 중 Asset 혼합, 서비스 보존 관측의 부재, 구 Gold 중간 노출, 정책 복사 문제는 수정 방향을 수용하며 반복 차단하지 않는다. 최초 재검토의 남은 요청은 current 초기화와 결정 이력의 결정론적 생성에 한정했다.

## 추가 수정본 최종 확인

최신 설계 전체를 다시 읽고 다음 반영을 확인했다.

1. **초기 KMDb 보존 연결 — 해소.** 무충돌 Legacy/Service ID는 confirmed와 분리한 provisional 연결로 초기화한다. candidate 관리자 모델에서 confidence와 source를 표시하고, 충돌은 격리하며 사용자 공개 자동 승인 근거로 사용하지 않는다. Service-only/Legacy 초기화, provisional 충돌 fixture와 confirmed/provisional/미노출 품질 집계가 추가됐다.
2. **결정 이력 — 해소.** decision_id 입력에 predecessor/evidence/effective time 및 basis 등을 명시했다. 동일 event replay는 최초 sequence/recorded time을 유지하고, A→취소→A는 다른 결정으로 구분한다. history는 원천시각 대신 identity별 append sequence 구간으로 정의하며 미상/동시각 및 replay fixture가 추가됐다.

구현 검토에서는 다음 세부 불변식을 확인한다. 이는 설계 구현 과정에서 지켜야 할 조건이며 추가 설계 승인을 기다릴 사유는 아니다.

- replay 때 predecessor를 최신 head로 다시 계산하지 않는다. 원래 결정 입력을 유지하고 decision ID 중복 확인과 identity별 sequence 할당을 원자적으로 처리한다.
- 구간은 `[valid_from_decision_sequence, valid_to_decision_sequence)`처럼 경계 의미를 고정하고, service history에도 service identity 단위의 같은 규칙을 적용한다. §7의 축약된 valid from 표기는 §6의 sequence 계약을 따른다.
- §8.2의 ‘확정 identity 우선’은 §7/§8의 provisional fallback과 함께 구현한다. confirmed **결정이 존재하는 경우** 우선한다는 뜻이며 충돌 후보를 자동 확정한다는 뜻은 아니다. 취소된 연결이 과거 provisional로 다시 살아나지 않도록 취소 fixture에 포함한다.
- provisional 사용 범위는 명시된 candidate 관리자 모델로 유지한다. production 분석 snapshot 승격 시 포함 여부와 confidence 노출은 별도 전환 검토에서 확인한다.

현재 설계에서 더 이상 구현 시작을 막는 미결정 사항은 없다. 기존 네 단위의 admission 경계를 유지하며 순수 계약부터 구현을 진행할 수 있다. 이번 최종 검토는 문서 검토이며 업무 코드·데이터는 변경하지 않았다.
