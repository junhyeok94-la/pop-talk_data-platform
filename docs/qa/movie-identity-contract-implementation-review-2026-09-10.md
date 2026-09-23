# 영화 identity/current 순수 계약 구현 검토

판정: CHANGES REQUESTED. 기본 fixture는 통과하지만 취소 상태와 시간 비교, 매칭 참조 무결성에 재현 가능한 문제가 있다. 이 모듈을 candidate current의 기준 구현으로 사용하기 전에 수정해야 한다.

검토 대상은 순수 모듈, 제출 테스트, 구현 기록이다. 실제 Airflow 컨테이너에서 기존 대사 포함 23개 테스트를 독립 실행하여 모두 통과했다. 아래 추가 반례도 같은 환경에서 실행했다. 업무 코드·외부 데이터는 변경하지 않았다.

## 1. [P1] 취소된 KMDb 연결이 초기화에서 다시 provisional로 생성됨

위치: initialize_kmdb_connection / cancel_connection.

```python
initialize_kmdb_connection(
    [{'source_kind': 'LEGACY', 'kmdb_id': 'K1', 'kmdb_matched': True}],
    previous_active=cancel_connection({
        'kmdb_id': 'K1', 'confidence': 'PROVISIONAL_LEGACY'
    }),
)
# {'status': 'PROVISIONAL', 'kmdb_id': 'K1', ...}
```

단일 provisional 후보 경로는 previous_active의 CANCELLED를 검사하지 않는다. 기존 취소 테스트는 별도로 만든 cancelled_source_identities 집합을 attribute selector에 전달하기 때문에 연결 초기화와의 결합 문제를 놓친다. 또한 이전 CONFIRMED 연결도 provisional 단일 후보 경로에서 낮은 신뢰도의 다른 연결로 대체될 수 있다.

수정: 초기화와 결정 적용을 분리하거나, 취소·이전 확정 연결 상태를 모든 초기화 경로에 적용한다. 단순 과거 관측 재입력은 재연결 권한이 아니며, 재연결은 새로운 명시적 decision으로만 허용한다. initial→cancel→과거 관측 replay→attribute 선택까지 연결한 fixture와 previous confirmed+provisional 후보 fixture가 필요하다.

## 2. [P1] 관측시각을 문자열로 정렬하여 더 오래된 값을 선택함

위치: AttributeObservation.__post_init__, resolve_attribute_current, normalize_exchange_time.

동일 영화/속성/source identity의 PRESENT 두 건으로 재현했다.

```text
older = 2026-09-10T09:00:00+09:00  # UTC 00:00
newer = 2026-09-10T01:00:00Z      # UTC 01:00
선택 결과: older
```

현재 정렬은 ISO 문자열 그대로 비교한다. 같은 시각을 Z와 +09:00으로 표현해도 동시각 충돌로 인식하지 못한다. normalize_exchange_time도 `source_observed_at='garbage'`를 API v1 정상 known=true로 수용한다.

수정: 허용한 timezone-aware timestamp를 실제 시각으로 파싱하고 UTC로 정규화해 순위와 동시각 충돌을 계산한다. naive/잘못된 타입·문자열은 계약에 따라 거부한다. 알려진 시각 여부는 단순 truthiness가 아니라 파싱 결과와 일치해야 한다. 시간대 역전, 동일 instant의 다른 표기, 잘못된 timestamp를 테스트한다.

v2 검증도 basis/known/time 조합만 확인한다. ingestion source와의 대응을 검증하거나 해당 source를 명시적 인자로 받아 API가 LEGACY_UNKNOWN을 사용하는 경우 등을 거부해야 승인된 source별 시간 불변식을 보장한다.

## 3. [P2] match evaluation/candidate grain과 FK를 검증하지 않음

위치: build_match_evaluation.

candidate_rows를 `{'evaluation_id': evaluation_id, **dict(candidate)}`로 만들기 때문에 candidate가 제공한 evaluation_id가 생성한 FK를 덮는다. 실제로 candidate에 evaluation_id='wrong'을 주면 반환 후보 FK가 평가 ID와 달랐다. 후보 K1만 있는데 selected_source_identity=K2, status=MATCHED로 호출해도 그대로 반환한다.

또한 승인된 evaluation grain은 source run × canonical movie × rule version인데 ID hash에는 status/selected/evidence도 들어간다. 같은 grain의 수정 결과가 두 evaluation ID로 생길 수 있다. 별도 유일성 검증이 없는 순수 계약에서는 어떤 기록이 canonical 평가인지 모호하다.

수정: 예약 FK는 입력에서 거부하거나 마지막에 서버 생성값으로 덮고, candidate identity 필수/유일성 및 선택 identity의 후보 소속을 검증한다. 상태별 0후보·선택값 불변식을 정의한다. evaluation ID를 선언 grain에서 생성하고 내용 충돌을 거부하거나, 실제 버전/evidence를 grain에 명시적으로 추가한다. 중복 후보·FK 주입·후보 밖 선택·동일 grain 상충 평가 fixture를 추가한다.

## 4. [P2] current 선택이 movie × attribute 단일 그룹을 보장하지 않음

위치: resolve_attribute_current.

API는 임의 Iterable을 받고 movie_key/attribute_name 일치를 확인하지 않는다. 서로 다른 영화나 서로 다른 속성의 관측을 섞으면 가장 높은 후보 하나를 반환한다. previous_current도 source identity만 검사하므로 동시각 충돌에서 다른 영화/속성의 이전 값을 유지할 수 있다. long-form current의 grain과 provenance 계약상 잘못된 결과다.

수정: 함수 입력을 단일 movie_key × attribute_name으로 명시하고 혼합 그룹을 거부한다. previous_current도 같은 그룹, 허용 상태, 유효 연결인지 검증한다. 혹은 그룹별 결과를 반환하도록 API를 바꾸되 하나의 결과로 묵시적으로 축약하지 않는다.

## 수용 사항과 구현 한계

- 동일 decision replay 시 최초 sequence/recorded time 반환, stale predecessor 거부, A→취소→A의 ID 구분 및 반개구간 history fixture는 통과했다.
- API 이전 PRESENT 우선, 명시적으로 전달한 active/cancelled identity 필터, 0후보 evaluation 생성 방향은 타당하다.
- append_decision은 순수 모형이므로 영속 저장의 동시성·원자성은 이번 테스트로 입증되지 않는다. 구현 기록도 이 범위를 유지해야 한다.
- deterministic_movie_key는 현재 빈 문자열만 거부한다. 승인된 KOFIC 형식 검증을 공유해 invalid ID의 key 발급을 차단하는 것이 좋다.
- decision_history는 sequence 연속성만 검사한다. 영속 원장 입력을 받을 때 predecessor 연결 및 decision_id/content 일치 검증을 어디서 책임질지도 정해야 한다.

현재 23개 테스트 통과는 유지하되 위 반례를 회귀 테스트로 추가하고 수정 후 재검토한다. 후보 적재 준비를 병행하더라도 이 current 결과를 검증 완료 모델로 사용하거나 production에 반영하지 않는다.
