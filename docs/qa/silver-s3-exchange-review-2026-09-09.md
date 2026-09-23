# Silver → S3 Exchange 검토

판정: 수정 후 재검토. 테스트 27개 재실행 통과. 아래 메모리 재현은 가짜 S3를 사용했으며 원격 데이터를 변경하지 않았다.

## P2 — 필수 Silver 파일의 checksum 검증을 생략할 수 있음

위치: `pipelines/transforms/exchange_publisher.py:77-82`.

검증은 index.documents에 나열된 항목만 수행한다. documents={}로 변경하고 movies JSONL의 제목을 같은 행 수로 수정한 ZIP을 전달했더니 EXCHANGE_READY까지 게시됐다. 필요한 두 Silver 파일이 checksum 목록에 있는지, bundle schema/version/identity가 예상 값인지 요구하지 않는다. 따라서 checksum 완전성 계약은 현재 부족하다.

권고: 필수 문서 집합과 schema/version을 먼저 검증한다. publisher에 staged run_id/artifact/READY와 ZIP 전체 digest를 전달하여 실제 승인된 bundle과 일치하는지 확인한다. 현재는 경로와 자기 선언 index에 의존한다. 최소한 두 JSONL의 누락 hash, 잘못된 hash, 잘못된 identity, index 변조가 첫 S3 쓰기 전에 거부되어야 한다.

## P2 — Exchange에 최신 관측 선택에 필요한 시각이 없음

위치: `pipelines/transforms/databricks_bridge.py`의 silver_movies.jsonl/silver_boxoffice.jsonl 생성부, notebook의 movie_rows/box_rows source_observed_at 추가부, `exchange_publisher.py`의 READY 구성부.

bundle은 순수 변환 결과를 내보내지만 source_observed_at은 notebook에서 Delta 행을 만들 때 나중에 추가한다. 따라서 두 Exchange JSONL에는 source_observed_at이 없고 READY에도 이를 복원할 시간 정보가 없다. source_run_id는 해시이므로 순서를 나타내지 않는다. 원본 S3 객체를 다시 추적하지 않는 한 소비자는 이전 단계에서 승인한 source_observed_at→revision 순서를 재현할 수 없다. 적재/게시 시각으로 대체하면 과거 run 재게시가 최신 데이터로 보일 수 있다.

권고: 각 영화/박스오피스 행에 원천 관측 시각을 보존하고 Databricks의 동일 값과 검증한다. Exchange가 실행별 관측인지 current snapshot인지 명시한다(현재는 실행별 관측). revision/artifact는 manifest에서 행에 결합할 수 있으므로 반복할 필요는 없다. Snowflake 구현 자체는 범위 밖이지만 소비 가능한 출력 계약은 이번 게시 단계에서 정의해야 한다.

## P2 — 게시 코드 변경이 Exchange identity에 반영되지 않음

위치: `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py:21-28`, `exchange_publisher.py` prefix/READY 생성부.

artifact에는 bridge/core/notebook만 들어가고 새 exchange_publisher.py는 빠져 있다. publisher가 파일명/manifest 필드를 수정해도 동일 artifact/revision/prefix에 쓰므로 기존 READY와 immutable conflict가 발생하거나 같은 artifact가 서로 다른 게시 계약을 뜻하게 된다. 수정 이후 publish 태스크만 재시도하면 이 경로가 특히 쉽게 발생한다.

권고: 처리 artifact에 publisher까지 포함하고 publish 시 staged artifact와 현재 artifact를 대사하거나, 별도의 고정 exchange_contract_version/source digest를 prefix와 READY에 넣는다. 변경된 publisher를 기존 실행 재시도에서 조용히 사용하지 않도록 fail closed 한다. 기존 파일은 보존하고 새 계약 경로로 게시한다.

## 적절한 부분과 검증 한계

- DAG의 transform→publish 기본 all_success 의존성 및 notebook JSONL 전체 바이트 재계산 비교는 정상 실행 경로의 승인 경계를 구성한다. 새 publisher가 임의 DAG 성공을 데이터 승인으로 추정하지 않도록 정확한 bundle identity를 추가로 연결하면 된다.
- S3 데이터 우선/READY 마지막 순서, 기존 객체 전체 바이트 비교, 조건부 PUT과 412 후 재조회는 부분 실패 재시도와 동시 최초 쓰기에 적절하다.
- 현재 두 exchange 테스트는 정상 재실행과 기존 내용 충돌만 검사한다. 중간 두 번째 파일 쓰기 실패→READY 없음→재시도 성공, 412 동일/상이 바이트, checksum 목록 누락은 회귀 테스트에 추가할 필요가 있다.
- 이번 검토에서 실제 병행 PUT나 원격 실패 주입은 수행하지 않았다. Snowflake 적재 및 기존 서비스 DB 반영은 승인 범위 밖이다.
