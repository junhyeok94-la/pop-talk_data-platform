# 영화 identity/current 계약 재검토

최종 판정: APPROVED. 36개 테스트 수정본에서 마지막 초기화 반례까지 해소됐다. 순수 identity/current 계약 구현을 수용하고 다음 격리 candidate 적재 설계·구현으로 진행할 수 있다. 이전 수정 요청은 이력으로 보존하며 최종 처리 결과는 마지막 절을 따른다. 업무 코드는 변경하지 않았다.

## 독립 검증

실제 Airflow 컨테이너에서 기존 대사 포함 unittest 29개를 재실행하여 모두 통과했다. UTC instant 비교, source별 시간 basis, mixed movie/attribute 거부, candidate FK 주입·중복·후보 밖 선택 거부 코드를 확인했다. decision_history의 predecessor/content 검증 추가와 KOFIC key 형식 검사도 수용한다.

## 1. [P1] REVIEW_REQUIRED 결과를 재입력하면 이전 confirmed 보호가 사라짐

initialize_kmdb_connection은 previous_status가 정확히 CONFIRMED일 때만 교체를 막는다. _review_result가 상태를 REVIEW_REQUIRED로 바꾸면서 기존 연결의 confidence=CONFIRMED는 유지하므로, 다음 호출에서 보호 조건을 통과하지 않는다.

실제 재현:

```python
a = {'status': 'CONFIRMED', 'kmdb_id': 'A', 'confidence': 'CONFIRMED'}
b = [{'source_kind': 'API', 'connection_status': 'MATCHED', 'kmdb_id': 'B'}]
r = initialize_kmdb_connection(b, previous_active=a)
# REVIEW_REQUIRED, kmdb_id=A, confidence=CONFIRMED
initialize_kmdb_connection(b, previous_active=r)
# CONFIRMED, kmdb_id=B
```

같은 관측을 두 번 평가했을 뿐인데 명시적 교체 decision 없이 A→B가 일어난다. 수정 시 active 연결 상태와 새 평가의 review 상태를 분리하거나, retained previous의 원래 활성 confidence/identity를 모든 호출에서 보존해야 한다. A confirmed→B 관측→동일 B 반복 입력에도 A 유지, 이후 명시적 decision 적용 시에만 B 전환하는 연속 fixture가 필요하다.

## 2. [P2] UNMATCHED를 후보 0건으로 제한해 기존 후보 관측을 보존할 수 없음

build_match_evaluation은 UNMATCHED에서 candidates가 비어 있지 않으면 거부한다. 그러나 기존 `movie_bronze_silver.evaluate_kmdb_match`는 후보가 있어도 제목/연도 조건을 통과한 ranked 후보가 없으면 UNMATCHED와 candidate_count>0을 반환한다. 이는 검색은 됐지만 채택 가능한 후보가 없는 정상 상태다.

재현: candidates=[{'source_identity':'kmdb:unrelated'}], status='UNMATCHED', selected_source_identity=None으로 호출하면 `UNMATCHED evaluation cannot select or contain candidates` 오류가 난다.

수정: 수집·평가한 후보와 선택/적격 후보를 구분한다. UNMATCHED는 selected=None을 요구하되 거절된 후보와 근거는 보존해야 한다. 만약 이 함수의 candidates가 적격 후보만 의미한다면 원천 전체 후보 모델을 별도로 제공하고 명칭/건수 의미를 바꿔야 한다. 0후보와 비적격 후보 존재의 두 UNMATCHED fixture를 추가한다.

## 3. [P2] 동일 evaluation ID의 내용 충돌 검사는 기록에만 있고 구현에 없음

evidence hash를 grain에 포함하는 변경은 수용한다. 다만 같은 source run/movie/rule/evidence와 동일 후보 목록에 selected=K1 및 selected=K2를 각각 전달하면 둘 다 성공하며 같은 evaluation_id를 반환한다. status도 마찬가지다. 구현 기록은 이를 immutable 내용 충돌 검사 대상이라고 설명하지만 기존 결과 비교나 payload digest 검증 API가 없다.

수정: evaluation의 identity와 immutable payload hash를 분리하고 동일 ID의 다른 payload를 검증·거부하는 순수 함수를 제공하거나, evidence hash를 정의된 전체 평가 입력/출력 내용에서 계산·검증한다. status 허용값도 명시적으로 검사한다. 같은 grain의 동일 내용 재생은 허용하고 선택/상태/후보 내용이 바뀌면 충돌하는 fixture가 필요하다. 영속 적재기가 미래에 처리할 것이라면 이번 구현 기록에서 완료한 계약으로 주장하지 말고 검증 API와 책임을 명시해야 한다.

## 다음 단계

이전 날짜 정렬·취소 직접 입력·그룹 혼합 반례의 수정은 수용한다. 남은 문제는 반복 평가와 기존 매칭 의미를 결합했을 때의 계약이다. 위 연속/충돌 fixture를 추가한 뒤 재검토한다. 순수 검증만 수행했으며 외부 데이터 조회·적재·수정은 하지 않았다.

## 32개 테스트 수정본 추가 검토

실제 컨테이너에서 unittest 32개를 독립 실행해 모두 통과했다. 후보가 있는 UNMATCHED 보존은 해소됐다. 직접 반복 B 입력 보호와 평가 status/selected 변경 hash 검증도 추가됐다. 다만 다음 두 반례가 여전히 재현된다.

### [P1] 보류 후 미수집 입력이 기존 confirmed 연결을 지움

```python
a = {'status':'CONFIRMED', 'kmdb_id':'A', 'confidence':'CONFIRMED'}
b = [{'connection_status':'MATCHED', 'kmdb_id':'B', 'source_kind':'API'}]
r = initialize_kmdb_connection(b, previous_active=a)
x = initialize_kmdb_connection([], previous_active=r)
# UNMATCHED, kmdb_id=None
initialize_kmdb_connection(b, previous_active=x)
# CONFIRMED, kmdb_id=B
```

retained_previous 보호가 confirmed 후보 분기에만 추가됐다. 후보가 없으면 마지막 UNMATCHED 반환이 기존 A를 지우고, 이후 B를 새 연결처럼 받아들인다. provisional A만 들어오면 retained confirmed를 provisional로 낮추는 경로도 남아 있다. 개별 분기마다 조건을 보태기보다 **활성 연결과 새 평가 상태를 분리**하고 모든 후보 유무/신뢰도 경로에서 활성 연결을 명시적 decision 전까지 보존해야 한다. A→B보류→미수집→B 및 A→B보류→provisional A→B 연속 fixture를 추가한다.

### [P2] payload hash가 후보 내용에 대한 변경을 검출하지 못함

같은 source run/movie/rule/evidence, status=UNMATCHED, selected=None으로 후보를 각각 `[{'source_identity':'K1','score':0}]`, `[{'source_identity':'K2','score':99}]`로 생성했다. 두 evaluation의 payload_hash가 같고 assert_evaluation_compatible이 성공했다. hash에 후보 수만 들어가고 실제 candidate identity/점수/근거는 들어가지 않기 때문이다.

evaluation hash에 정규화한 전체 candidate payload 또는 candidate-set digest를 포함해야 한다. 후보 행은 source identity로 안정적으로 정렬하고 의미 있는 rank는 별도 필드로 보존한다. 같은 개수의 후보 교체/점수·근거 변경은 충돌하고 단순 전달 순서 차이는 계약에 따라 동일하게 처리하는 fixture가 필요하다. evidence_hash를 caller가 전달했다는 것만으로 후보 내용과의 일치를 검증한 것은 아니다.

이번 변경으로 이전 UNMATCHED 지적은 종료하고, 당시에는 기존 연결 보존 및 평가 내용 불변성 두 항목을 남겼다. 외부 데이터나 업무 코드는 변경하지 않았다.

## 34개 테스트 수정본 추가 검토

실제 컨테이너에서 34개 테스트가 모두 통과했다. status/active_status 분리로 보류→미수집→동일 후보 경로의 기존 confirmed 연결 보존을 확인했다. candidate set 전체를 정렬·hash하여 평가 identity와 payload에 포함하고 설계 grain도 갱신했으므로 후보 변경을 별도 평가로 보존하는 변경을 수용한다. 기존 두 지적은 종료한다.

### [P2] 활성 연결이 없는 UNMATCHED 결과가 최초 연결을 막음

실제 재현:

```python
x = initialize_kmdb_connection([])
# UNMATCHED, active_status=None, kmdb_id=None
y = initialize_kmdb_connection(
    [{'source_kind':'LEGACY', 'kmdb_id':'A', 'kmdb_matched':True}],
    previous_active=x,
)
# REVIEW_REQUIRED, active_status=None, kmdb_id=None, retained_previous=True
z = initialize_kmdb_connection(
    [{'source_kind':'API', 'kmdb_id':'A', 'connection_status':'MATCHED'}],
    previous_active=y,
)
# REVIEW_REQUIRED, active_status=None, kmdb_id=None, retained_previous=True
```

provisional 경로가 previous_active dict의 존재만으로 ‘기존 ID와 다름’을 검사해 None→A도 교체로 간주한다. _review_result 역시 실제 active가 없어도 retained_previous=True를 반환한다. 이후 확정 후보까지 없는 연결을 보호하느라 차단한다. 최초 미매칭 이후 정상 후보가 도착하는 일반 경로다.

수정: 이전 상태 객체의 존재와 유효한 활성 연결의 존재를 구분한다. retained 보호는 active_status가 CONFIRMED/PROVISIONAL이고 유효 ID가 있는 경우에만 적용한다. CANCELLED는 별도 tombstone 규칙을 유지한다. _review_result도 보존할 연결이 없으면 retained_previous=False여야 한다. UNMATCHED→provisional→confirmed, 초기 충돌/null→단일 정상 후보의 fixture를 추가하고 기존 취소/교체 보호 fixture를 유지한다.

당시 남은 요청은 이 초기화 경로 1건에 한정했다. 순수 실행만 수행했으며 업무 코드·외부 데이터는 변경하지 않았다.

## 36개 테스트 수정본 최종 확인

실제 Airflow 컨테이너에서 `python -B -m unittest discover -s pipelines/modeling/tests`를 독립 실행했다. 기준선 11개와 identity 25개, 총 36개가 모두 통과했다.

`_has_active_connection()`이 평가 결과 객체의 존재와 활성 연결의 존재를 구분하며, confirmed/provisional 교체 검사 및 `_review_result`가 이를 사용함을 확인했다. 따라서 active가 없는 UNMATCHED/초기 충돌 결과는 더 이상 최초 정상 연결을 막지 않는다. CANCELLED는 별도 tombstone 분기를 유지한다.

다음 신규 fixture와 기존 회귀를 함께 수용한다.

- UNMATCHED → 단일 provisional → 같은 ID confirmed.
- 초기 provisional 충돌/null active → 이후 단일 후보 정상 연결.
- confirmed A → B 보류 → 미수집 → B 반복에도 A 유지.
- 취소 연결의 과거 관측 재입력 차단.
- 후보 집합 내용 변경 시 별도 evaluation identity, 동일 identity의 다른 outcome 충돌.
- UTC 관측시각 비교, 그룹/FK 검증, decision replay/history 및 v1/v2 시간 계약.

이전 검토의 차단 항목은 모두 종료한다. 승인 범위는 정규화된 입력을 다루는 순수 계약과 fixture이며, 영속 원장의 동시 append·트랜잭션·실제 source 적재 통합까지 검증한 것은 아니다. 다음 단계에서는 이 함수를 호출하는 어댑터가 입력 정규화와 active_status 전달, compatibility 검사, 원자적 decision 저장을 실제로 적용하는지 확인한다.

격리 candidate 설계·구현은 진행 가능하다. 기존 Main DAG와 production Gold/서비스 게시 경계는 유지하고, 실제 승격은 후보 실행 결과 검토 후 판단한다. 이번 검토에서 업무 코드나 외부 데이터는 변경하지 않았다.
