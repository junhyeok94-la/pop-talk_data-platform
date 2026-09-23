# 영화 identity/current 계약 구현 기록

## 범위

전체 영화 편입 설계의 1단계인 순수 계약과 fixture만 구현했다. 클라우드 candidate 적재와 production 모델 변경은 아직 수행하지 않았다.

## 구현 파일

- `pipelines/modeling/movie_identity_contract.py`
- `pipelines/modeling/tests/test_movie_identity_contract.py`

## 고정한 계약

- 기존 dbt와 호환되는 결정론적 KOFIC movie key
- predecessor를 포함한 immutable decision ID
- identity별 단조 증가 decision sequence와 replay 시 최초 sequence/recorded time 유지
- `[valid_from_sequence, valid_to_sequence)` history
- 확정 KMDb 우선, 무충돌 Legacy/Service는 provisional 초기화
- provisional 충돌 시 REVIEW_REQUIRED와 이전 active 유지 규칙
- API 이전 PRESENT를 Legacy fallback보다 우선하는 속성 current
- 동일 KMDb identity 안의 필드별 이전 PRESENT 유지
- 취소/교체 identity의 과거 값 배제
- 동일 원천·동일 관측시각의 상충 값은 REVIEW_REQUIRED
- 후보가 0개여도 match evaluation 행 생성
- 평가 결과(`status`)와 유지 중인 연결 상태(`active_status`)를 분리
- 후보 집합 전체를 evaluation identity와 immutable payload에 포함
- API Exchange v1 시간 기본값과 Legacy/Service Exchange v2 시간 불변식

## 검증

기존 기준선 대사 11개와 identity 계약 25개, 총 36개 테스트가 통과했다.

```text
....................................
Ran 36 tests in 0.011s
OK
```

신규 fixture는 다음을 포함한다.

- decision replay
- connect → cancel → reconnect의 서로 다른 ID
- stale predecessor 거부
- sequence 반개구간
- Legacy/Service provisional 초기화와 충돌
- API PRESENT 이후 NOT_COLLECTED
- 같은 KMDb의 부분 미수집과 다른 KMDb 교체
- 취소한 provisional 값의 부활 차단
- 동일시각 상충 값과 이전 current 유지
- 후보 0건 UNMATCHED evaluation
- Exchange v1/v2 시간 계약
- 취소 연결의 과거 provisional 재생성 차단
- 이전 confirmed 연결의 낮은 신뢰도 후보 대체 차단
- timezone-aware 시각의 UTC 순위와 동일 instant 충돌
- 잘못된·naive timestamp 및 source/time-basis 불일치 거부
- candidate FK 주입, 중복 후보, 후보 밖 selected 거부
- movie×attribute 혼합 입력과 다른 그룹 previous current 거부
- REVIEW_REQUIRED로 이전 연결을 유지한 뒤 같은 충돌 관측이 반복돼도 자동 교체되지 않음
- 후보는 있지만 적격 후보가 없는 UNMATCHED evaluation과 후보 행 보존
- 같은 evaluation ID의 다른 outcome payload 충돌 거부
- confirmed A → B 충돌 → 빈 관측 → B 재관측에서도 A 활성 연결 유지
- 후보 ID·점수 등 후보 내용이 달라지면 evaluation identity도 달라짐
- UNMATCHED → 단일 provisional → 같은 ID confirmed 승격
- 활성 연결 없는 초기 provisional 충돌 → 이후 단일 후보 정상 연결

검토 반례를 반영해 관측시각을 문자열이 아닌 UTC instant로 비교한다. match evaluation ID의 실제 grain은 source run × canonical movie × rule version × evidence hash × candidate set hash다. status와 selected 값은 같은 평가 identity의 immutable 내용 충돌 검사 대상이다. `payload_hash`와 compatibility 검사로 이를 강제한다. candidate가 생성 FK를 덮을 수 없고 상태별 후보/선택 참조 무결성을 강제한다. UNMATCHED는 후보가 없거나 후보가 모두 탈락한 경우를 모두 허용하되 selected는 항상 null이다.

연결 초기화 결과에서 `status`는 이번 실행의 평가 결과이고 `active_status`는 실행 후에도 유효한 연결의 상태다. 따라서 기존 CONFIRMED 연결이 충돌로 `REVIEW_REQUIRED`가 되거나 다음 실행이 `UNMATCHED`여도 `active_status=CONFIRMED`와 기존 KMDb ID를 잃지 않는다.

이전 결과 객체가 있다는 사실만으로 연결을 보존하지 않는다. `active_status`가 CONFIRMED/PROVISIONAL이고 비어 있지 않은 KMDb ID가 함께 있을 때만 활성 연결로 간주한다. CANCELLED는 활성 연결과 분리된 tombstone으로 처리하여 명시적 재연결 결정 없이는 되살리지 않는다.

## 다음 단계

후속 방향은 PostgreSQL 기반 Phase 1로 변경되었습니다. 현재 계획은 docs/phase1-transition.md를 따릅니다.
