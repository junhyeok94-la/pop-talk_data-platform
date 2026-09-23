# 영화 원천 → Bronze/Silver 거시적 코드 검토

검토일: 2026-09-09. 검토 범위는 다른 작업 「프로젝트 할 일 정의하기」가 완료한 변환 코어 및 기존 downstream 샘플 코드와의 연결이다. 해당 작업의 완료 메시지와 QA 기록에 따르면 실행 DAG/Databricks 연결까지 완성된 상태는 아니다. 소스 코드는 수정하지 않았다. 기존 계약 테스트 8개 재실행 성공. 외부 서비스에 데이터를 쓰지 않는 메모리 재현으로 아래 3개 문제 확인.

## 판정

S3 원본 보존 → 순수 변환 코어 → 품질 판단 → 저장/게시의 분리는 적절하다. 그러나 현 상태를 운영 데이터 계약으로 확정하고 downstream을 연결하기에는 변경 감지, 매칭 확정, 입력 대사가 부족하다. 아래 P1/P2 수정 후 연결을 권장한다. 구현되지 않은 후속 단계는 별도 설계 과제로 분리했다.

## 재현한 결함

### P1: 업무 데이터 해시에 실행 식별자가 포함됨

`pipelines/transforms/movie_bronze_silver.py:268-272`: source_run_id와 source_object_key를 넣은 전체 row를 record_sha256으로 해시한다. 같은 상세/후보를 run day1/day2로 변환하면 해시가 다르다. 기존 Databricks 샘플 MERGE는 record_sha256 차이를 UPDATE 조건으로 사용한다. 따라서 이를 이어 쓰면 실제 내용이 그대로인 영화도 매일 변경으로 판정한다. 이후 서비스 DB 변경분 게시와 임베딩 재계산을 불필요하게 유발할 수 있다.

수정: 업무 필드 기반 content hash와 실행/객체 추적 메타데이터를 분리한다. 정책 변경을 감지할 hash와 임베딩 입력 hash도 필요에 맞게 분리한다. 동일 콘텐츠의 다른 실행 hash 동일, 업무 필드 수정 hash 변경 테스트를 추가한다. 마지막 관측 실행 갱신은 업무 콘텐츠 변경 여부와 별도로 관리한다.

### P1: 불완전하거나 모호한 후보를 확정 매칭

`movie_bronze_silver.py:147-173,267`: 동일 제목/연도의 DOCID A/B를 넣고 truncated=True로 변환해도 MATCHED A가 나온다. 사전순 ID는 재현 가능한 선택 기준일 뿐 올바른 동일 영화임을 보장하지 않는다. 잘린 나머지 후보에 더 나은 매칭이 있을 수도 있다. 결과가 포스터/줄거리 등 서비스 콘텐츠를 바꾸므로 자동 확정에는 근거가 부족하다. 품질 집계도 이 행을 REVIEW_REQUIRED에서 제외한다.

수정: 중복 DOCID를 제거하고, 동일 최상위 점수의 서로 다른 영화가 존재하거나 검색이 잘렸다면 검수/추가 조회 대상으로 둔다. 잠정 후보와 확정 매핑을 별도 표현한다. 감독 등 추가 근거 또는 기존 승인 매핑으로 확정하며, 기존 매핑을 낮은 신뢰도로 덮어쓰지 않는다. ambiguity/truncation 지표를 매칭 성공 여부와 독립적으로 집계한다.

### P2: manifest 전체 대사를 표방하지만 입력 영화 집합과 완료 조건을 대사하지 않음

`movie_bronze_silver.py:86-119,296-315,342`: candidate_scope_complete=False, SUCCESS.movie_count=999, movie_list.movies={OTHER}, movie_list.movie_count=999로 바꾸고 READY.movie_count=1/상세1건을 유지해도 transform_run이 성공했다. 현재 producer는 정상 입력을 생성하지만 이 consumer는 저장·전송·향후 producer 변경 경계에서 계약 불일치를 차단하지 못한다.

수정: 운영 sample=false와 candidate_scope_complete=true를 함께 요구하고, movie_list.movies/detail/outcomes ID 집합 및 READY/SUCCESS/각 stage 선언 건수를 대사한다. outcomes 중복은 dict 변환 전에 거부한다. boxoffice target_date도 scope의 날짜 및 원본 request.targetDt와 대사한다. 테스트 fixture에도 실제 producer의 필수 필드를 넣는다. 객체 checksum 검증은 유용하지만 업무 범위 대사를 대체하지 않는다.

## 다음 단계에서 확정해야 할 큰 흐름

1. 초기 snapshot 경로를 별도로 연결해야 한다. 현재 코어는 DAILY_READY와 4개 API stage만 받으므로 5,985건 movies_final.json snapshot SUCCESS를 소비하지 못한다. 초기 파일 어댑터와 일일 API 어댑터가 하나의 공통 Silver 계약을 출력해야 한다. 초기 원본에 없는 필드를 추측하거나 API를 전량 재수집할 필요는 없다.
2. 식별자와 필드 계약을 통일해야 한다. 새 코어는 kofic_movie_cd 문자열, countries, source_run_id를 출력한다. 기존 샘플 notebook/dbt/publisher는 DB의 숫자 source_movie_id, production_countries, run_id/ingestion_source 등을 요구한다. 외부 KOFIC 코드를 bigint로 변환하면 실제 영숫자 ID가 깨진다. KOFIC 외부 ID와 기존 서비스 내부 PK의 매핑을 유지하고, 양쪽 계약을 잇는 명시적 어댑터를 둔다.
3. 최신 상태와 실행별 관측 이력을 분리해야 한다. 기존 sample notebook은 hash가 바뀐 행만 UPDATE하면서 run_id별 건수가 전체 입력 건수와 같아야 한다고 검사한다. 동일 데이터가 다른 실행으로 다시 오면 갱신되지 않아 검증이 실패한다. 새 코어의 hash를 바로잡을 때 함께 해결해야 한다. 최신 영화 테이블과 run별 처리 ledger를 분리하고, 재시도/역순 실행에서 예전 수집이 최신 상태를 덮어쓰지 않도록 원천 관측 시각을 보존한다. dbt의 processed_at 기준 최신 선택만으로는 과거 데이터 재처리를 구분할 수 없다.
4. 서비스 DB 반영 책임을 명확히 해야 한다. 기존 publish_to_postgres.py는 dw_serving.movie_catalog에만 쓰는 샘플이다. 기존 서비스 테이블의 I/U 트리거 연결을 자동으로 충족하지 않는다. 서비스 메타데이터 변경 반영과 분석 mart 게시를 구분하고, 사용자 리뷰/관리자 승인 상태는 서비스 DB가 주도하도록 한다. 샘플의 approval_status/service_status 덮어쓰기 정책을 운영 게시에 그대로 가져오면 안 된다.
5. 박스오피스는 최근 7일을 매일 재수집하므로 관측 이력은 run별로 쌓되 서비스 집계는 target_date+KOFIC ID별 최신 관측 한 건을 사용해야 한다. 일별 스냅샷을 그대로 합산하면 중복 집계된다. 누적 관객수는 날짜 간 합산하지 않는다.
6. 초기 적재 완료와 일일 처리 완료 ledger, 변환 버전, 실패 후 재개 위치, 후속 게시 완료 표식이 필요하다. RAW SUCCESS는 raw만 완료했다는 의미이며 서비스 반영 성공이 아니다. 초기/일일 실행을 모두 이력으로 남기고 동일 manifest 재소비는 멱등 처리한다.

이 항목들은 아직 구현하지 않은 Databricks/Airflow 단계의 설계 입력이며, 현재 순수 코어가 이미 원격 저장·게시까지 구현해야 한다는 지적은 아니다.

## 우선순위

먼저 위 3개 재현 결함과 초기/일일 공통 Silver 계약을 수정·확정한다. 이어 최신 상태와 처리 ledger를 분리한 Databricks 적재를 구현하고, 마지막으로 Snowflake/dbt와 서비스 테이블 반영을 연결한다. 일일 raw 수집은 이 검토와 독립적으로 계속 보관할 수 있다.
