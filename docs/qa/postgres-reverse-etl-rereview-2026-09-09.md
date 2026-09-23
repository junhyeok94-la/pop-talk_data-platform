# PostgreSQL reverse ETL v2 재검토

판정: 승인 보류. 기존 수정 일부 확인했으나 아래 계약 문제 유지.

## 확인한 개선

dataset advisory transaction lock 및 active row lock, stale 비교, 전체 입력 행 hash 계산, active와 attempt SUCCESS의 동일 transaction, 예외 후 active+SUCCESS 재조회가 추가됐다. 단위 테스트 3개 재실행 통과. 원격 게시/probe는 검토자가 재실행하지 않았다.

## P1 — 입력 ledger만으로 만든 publication ID가 Gold 모델 수정 게시를 막음

`publish_to_postgres.py` build_snapshot의 publication_id 계산 및 publish의 Existing publication conflict 분기.

publication_id는 여전히 원천 ledger identity 집합만 반영한다. 동일 입력에서 dbt 모델을 수정해 TITLE_KO 등 최종 출력이 변경되면 snapshot hash만 달라지고 publication ID는 같다. 메모리 입력으로 이 조건을 재현했다. v2는 조용히 옛 결과를 재사용하던 문제를 충돌 실패로 바꿨지만, 성공한 새 Gold 빌드 결과를 게시할 수 있는 식별자는 아직 없다.

권고: 성공한 Gold build의 고정 ID/모델 계약 버전/불변 export digest를 publication identity에 포함한다. 게시 함수가 mutable Gold 테이블을 임의 시점에 다시 읽어 build identity를 추정하지 않도록 한다. 동일 입력의 새 모델 출력은 새 publication이어야 한다.

## P1 — 원천별 max 집계는 Gold 게시 세대가 아님

build_snapshot의 max(observed)/max(revision)/max(completed)와 publish의 generation<=active 검사.

서로 다른 run의 revision은 독립적인 값이다. 예를 들어 기존 quality에 X rev10 완료03:00, Y rev1 완료00:00이 있고 Y만 rev2 완료02:00으로 바꾸면 정당한 새 입력 집합인데 세 값의 max는 모두 그대로다. 같은 source_observed_at의 모델 수정/과거분 정정에서도 발생한다. 검토자 메모리 재현에서 publication ID는 바뀌고 generation tuple은 같아서 현재 비교가 새 게시를 거부함을 확인했다. 반대로 하나의 최신 관측 최댓값만으로 모든 원천의 역행 여부를 보장할 수도 없다.

권고: dataset 단위 성공 Gold build/publication revision을 원자적으로 부여하거나 승인된 build snapshot 순서를 명시한다. 원천 시각과 원천별 revision은 provenance로 보존하되 전역 generation을 max로 합성하지 않는다. A→B→A뿐 아니라 일부 원천 갱신·동일 시각 정정·같은 입력 새 모델을 포함한 테스트가 필요하다.

## P2 — 저장 행의 실제 콘텐츠 대신 저장 hash 컬럼만 재검증

publish의 array_agg(row_sha256) SELECT 두 곳.

새 입력 전체 row hash는 올바르게 계산하지만 replay는 저장된 row_sha256을 그대로 비교한다. PostgreSQL의 title_ko/plot/금액 등이 바뀌고 hash 컬럼이 그대로면 통과한다. 이전 요구인 실제 저장 행 무결성 확인은 완전히 해결되지 않았다.

권고: PostgreSQL 저장 열을 읽어 입력과 동일한 canonical 표현으로 재해시하거나 각 필드 자체를 대사한다. JSON 문자열/JSONB, Decimal/integer, timestamp timezone 정규화도 양쪽에 일관되게 적용한다. 데이터 값만 바꾸고 row_sha256을 유지하는 테스트가 실패해야 한다.

## 후속

SUCCESS 원자성 개선은 확인했다. 커밋 결과 불확실 시 재조회에 실패하면 FAILED를 기록하지 않는 방향도 적절하다. 이 승인 보류는 위 게시 identity/세대/저장 대사 계약에 대한 것이다. 이전 단계 승인과 서비스 테이블 소유권 보존 판단은 유지한다.
