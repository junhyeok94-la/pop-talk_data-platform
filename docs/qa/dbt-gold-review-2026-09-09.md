# dbt Gold 코드 검토

판정: 승인 보류. P1 1건 수정 필요.

## P1 — 최신값 선정 시 revision보다 완료 시각을 먼저 비교

위치: `pipelines/dbt/models/dw/dim_movie.sql:4-5`, `pipelines/dbt/models/dw/fct_boxoffice_daily.sql:4-5`.

두 모델의 ORDER BY는 source_observed_at → publication_completed_at → publication_revision이다. 앞서 Databricks에서 승인한 순서는 source_observed_at → publication_revision → completed_at이다. 같은 원본 A revision1을 늦게 적재하고 수정 B revision2를 먼저 적재하면 Gold가 A를 선택한다. Snowflake completed_at은 원천/변환 시각이 아니라 loader 완료 시각이므로 원격 처리 순서가 맞아도 적재 지연만으로 문제가 발생할 수 있다.

검토자는 실제 두 모델의 window 정렬식을 추출해 SQLite 메모리 DB에서 비교했다. 동일 source_run/source_observed_at, old rev1 완료03:00/new rev2 완료02:00 입력에서 두 모델 모두 old/rev1을 선택했다. 이는 원격 Snowflake 실행이 아니라 정렬 의미 재현이다.

권고: 두 DW 모델 모두 원천 관측 시각 → publication revision → 게시 완료 시각 → 결정적 tie-break 순으로 변경한다. 공통 macro로 순서를 공유하거나 두 모델에 같은 fixture 기반 회귀 검증을 둔다. 서로 다른 artifact의 같은 원본 재처리, 낮은 revision 늦은 적재, 더 오래된 원천에 높은 revision, 동일 artifact 높은 revision 재게시를 함께 검증한다.

## 적절한 부분

- SUCCESS ledger만 소비하고 run/artifact별 가장 높은 revision을 선정한다.
- immutable observation의 최초 revision 대신 선정된 ledger의 revision과 READY를 사용한다.
- 박스오피스 fact grain은 날짜+영화이며 mart의 LEFT JOIN은 dimension 누락 행을 보존한다. fact/mart의 unique 및 상호 key 대사도 있다.
- 품질 마트가 publication을 기준으로 LEFT JOIN/coalesce하여 빈 SUCCESS 실행을 표현한다.
- load_snowflake 뒤에 dbt build 의존성이 있다. DW table, staging/mart view로 구분되어 있다.

## 검증 한계와 후속 범위

제공된 실제 dbt PASS49는 현재 데이터의 건수·NOT NULL·유일성 등 검증 성공 기록이다. 검토자는 원격 Gold를 재생성하지 않았고 모델·YAML·singular tests·DAG를 읽고 위 순서 반례를 재현했다. 현재 테스트에는 이 최신 선택 의미를 검증하는 fixture가 없다.

여러 DW 테이블의 dbt build는 전체를 단일 게시 트랜잭션으로 묶지 않는다. 다음 reverse ETL은 성공한 build 결과에만 연결하고, 사용자가 읽는 게시 세대/고정 범위 계약을 별도로 정의해야 한다. 아직 서비스 게시가 없는 이번 단계의 추가 차단 finding으로 보지는 않는다.

위 P1 수정 및 순서 회귀 검증 후 재검토한다.
