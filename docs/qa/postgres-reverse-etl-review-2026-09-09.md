# Gold → PostgreSQL reverse ETL 검토

판정: 승인 보류. P1 2건, P2 1건.

## P1 — 과거 게시본 재시도가 active pointer를 역행시킴

위치: `pipelines/dbt/scripts/publish_to_postgres.py:64-74`.

기존 publication이면 내용/건수만 확인한 뒤 동일하게 active pointer를 무조건 갱신한다. A 게시 → B 게시 → A snapshot 재시도 순서에서 A가 다시 활성화된다. source_as_of는 저장만 하고 비교하지 않으며 activation의 세대/잠금/조건부 갱신이 없다. 두 클라이언트에서 오래된 snapshot을 늦게 커밋해도 같은 문제가 발생한다.

권고: 활성화에 명시적 Gold build/publication 세대를 사용하고 dataset 단위 lock 또는 CAS로 이전보다 오래된 후보를 거부한다. 동일 publication replay는 실제 저장 무결성만 확인하고 이미 더 최신인 active를 바꾸지 않아야 한다. source_as_of=max 관측시각 하나만 비교하면 같은 시각의 revision/모델 수정은 구분되지 않으므로 단독 순서로 쓰지 않는다. A→B→A 및 늦은 구세대 커밋 테스트 필요.

## P1 — 실제 게시 콘텐츠와 Gold 빌드가 identity/hash에 고정되지 않음

위치: `publish_to_postgres.py:19-27,33-37,64-72`.

publication_id는 입력 ledger 집합만으로 만들고 snapshot hash는 upstream RECORD_SHA256 문자열 목록만 해시한다. RECORD_SHA256은 Silver 관측 payload의 해시이지 dbt가 가공해 최종 선택·projection한 게시 행의 해시가 아니다. 검토자 메모리 재현에서 TITLE_KO만 before→after로 바꾸고 upstream hash를 유지하면 snapshot_sha256이 동일했다. dbt 모델 변경으로 출력이 달라지거나 저장된 스냅샷 값이 변해도 동일 건수/hash 메타데이터만 보고 기존 publication을 재사용할 수 있다.

fetch_gold는 DIM/FCT/quality를 별도 SELECT로 읽으며 성공한 dbt build의 고정 식별자/모델 버전/입력 cutoff를 전달받지 않는다. 현재 단일 직렬 DAG에서는 정상 순서가 지켜지지만 별도 dbt 작업이나 재시도에서 서로 다른 build를 섞지 않도록 경계를 정의해야 한다.

권고: 게시 대상 전체 행을 날짜/시간/JSON 정규화 후 해시하고 저장된 행에서도 같은 방식으로 무결성을 대사한다. publication에는 성공한 Gold build와 출력 계약 버전 또는 불변 export digest를 포함한다. 동일 입력의 수정된 dbt 모델은 새 publication이어야 한다. 같은 upstream hash·다른 title/정책/금액 사례, 같은 건수 저장 행 변조, 동일 입력 새 모델 build 사례를 테스트한다.

## P2 — active 전환과 SUCCESS 시도 이력이 별도 커밋

위치: `publish_to_postgres.py:62-80`.

publication/snapshot/active transaction을 커밋한 뒤 publish_attempts SUCCESS를 별도 commit한다. 그 사이 실패하면 active는 이미 새 데이터인데 catch가 FAILED를 기록한다. 문서의 '실패 시 기존 active 유지'는 활성화 이전 실패에만 성립한다. 현재 probe는 fail_before_activation만 주입하므로 이 경우를 다루지 않는다.

권고: RUNNING은 별도 기록해도 되지만 SUCCESS는 active 전환 transaction 안에서 함께 갱신한다. 커밋 결과가 불확실한 연결 실패는 기존 publication/active/attempt를 재조회하여 판정한다. 이미 커밋된 성공을 무조건 FAILED로 덮어쓰지 않는다. 활성화 커밋 직후 오류/응답 유실 시나리오 추가 필요.

## 적절한 부분과 범위

- publication metadata/두 snapshot/active pointer를 한 PostgreSQL transaction으로 묶은 기본 방향은 적절하다.
- 서비스 dev.popcorn_movies, 관리자 승인/편집, 회원·리뷰 테이블은 쓰지 않는다. 내부 서비스 PK와 md5 기반 DW key도 혼동하지 않는다.
- 다만 이번 구현은 dw_serving 분석 게시다. 기존 서비스 카탈로그 I/U 트리거 또는 임베딩 큐 연결이 완료된 것은 아니다. UI/API가 새 current view를 사용할지, 기존 테이블에 변경분을 적용할지는 별도 후속 계약으로 남는다.
- 전체 snapshot 게시라서 내용이 거의 같아도 ledger 집합이 늘면 전체를 복제한다. 현재 107/70 규모에서는 차단 문제로 보지 않지만 장기 보관 정책/세대 정리와 불필요한 활성화 방지는 필요하다.
- reverse ETL 단위 테스트 3개 재실행 통과. 테스트는 build_snapshot 함수 중심이며 stale/replay 데이터 무결성/게시 후 실패를 다루지 않는다. 검토자는 실제 서비스 DB를 변경하거나 probe를 재실행하지 않았다.

위 3건 수정 후 재검토한다. Snowflake/dbt 이전 단계 승인 범위는 유지한다.
