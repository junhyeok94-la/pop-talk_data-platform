# Databricks 일일 Bronze/Silver 파이프라인 구현 기록

구현일: 2026-09-09

## 실행 경로

`S3 DAILY_READY → Airflow 계약 검증 → Databricks Landing Volume → Databricks Job → Delta Bronze/Silver → S3 Exchange`

S3는 원본 기준 저장소다. Databricks Free Edition에서 외부 S3 직접 접근을
전제로 하지 않고, Airflow가 READY가 가리키는 성공한 실행의 문서만 읽어 순수
변환 계약을 먼저 통과시킨다. 이후 원문, manifest, 변환 소스를 결정적 ZIP 한
개로 묶어 `/Volumes/workspace/bronze/landing/movie_api_daily/<run_id>/<artifact_version>/bundle.zip`에
불변 적재한다. `artifact_version`은 순수 변환 모듈과 실행 노트북을 함께 해시한
SHA-256이다. 같은 원천을 새 코드로 재처리해도 별도 경로를 사용하며, 같은
버전 경로의 내용이 다르면 덮어쓰지 않고 실패한다.

## Delta 테이블

- `workspace.bronze.movie_raw_objects`: S3 raw 객체 원문 보존, `object_key` 불변
- `workspace.silver.movie_observations`: 실행별 영화 관측 이력
- `workspace.silver.movies_current`: 성공 ledger만 게시하는 영화별 최신 관측 view
- `workspace.silver.boxoffice_observations`: 실행/일자/영화별 박스오피스 이력
- `workspace.silver.boxoffice_current`: 성공 ledger만 게시하는 일자/영화별 최신 관측 view
- `workspace.silver.movie_transform_runs`: READY key와 변환 버전별 처리 ledger

관측 테이블의 식별자에는 `source_run_id + transform_version`이 포함된다. current
view는 성공한 ledger와 결합한 뒤 `source_observed_at`, 명시적
`publication_revision`, 처리 완료 시각 순으로
최신 행을 선택한다. 따라서 실패 중인 실행의 일부 관측은 노출되지 않고, 같은
원천을 새 코드로 성공적으로 재처리한 결과만 승격된다. publication revision은
READY별로 한 artifact에만 할당되며, 코드 수정 승격과 명시적 롤백 모두 이전보다
큰 값을 사용한다. 따라서 구버전의 늦은 성공이 신버전을 되돌리지 않는다. 같은 관측 시각과 업무
키에 서로 다른 hash가 들어오면 충돌로 실패한다. 박스오피스 입력 안에서도
`target_date + kofic_movie_cd` 중복을 거부한다.

## Airflow DAG

`pop_talk_movie_databricks_daily`는 수동 실행 전용이며 생성 시 pause 상태다.

1. `deploy_notebook`: `/Shared/pop-talk/movie_bronze_silver_daily_versions/<artifact_version>`에 불변 버전 배포
2. `stage_bundle`: S3 입력 대사 및 Landing Volume 불변 전송
3. `transform_bronze_silver`: Jobs API로 serverless notebook 실행

파라미터 `ready_manifest_key`로 처리할 `DAILY_READY.json`을 명시한다. 명시적
`processing_attempt`와 READY key, artifact version으로 Jobs API
`idempotency_token`을 만든다. 토큰에는 publication revision도 포함한다. 같은 시도에 대한 Airflow 재시도·동시 실행은 같은
Databricks run을 관찰하고, 실패 후 새 원격 실행이 필요할 때만 attempt를 올린다.
향후 Raw DAG와 자동 연결하기 전까지 스케줄을 두지 않았다.

`stage_bundle`은 `deploy_notebook`이 반환한 artifact version을 입력으로 받고,
staging 직전에 다시 읽은 전체 source version과 비교한다. 두 task 사이에 코드가
바뀌면 제출 전에 fail closed하며 새 DAG run으로 다시 시작해야 한다.

## 검증 결과

- Airflow 3.3.1 이미지의 변환/bridge 단위 테스트: 25개 통과
- Airflow DAG import error: 0
- 실제 S3 실행: `4834bb8200e5a76f2cf8b408`
- Landing 검증: raw 객체 331개, Silver 영화 107개, 박스오피스 70행
- Databricks Jobs run: `411486718912989`, 성공
- 첫 실행의 세 Airflow task 모두 성공
- 동일 READY 재실행 Jobs run: `1097642601085598`, 성공
- 재실행 시 동일 Landing ZIP 재사용 및 실행별 331/107/70건 대사 통과
- 검토 수정본 artifact: `1a4786849e688c8e7be1df72a796badad2e8805a257f8c187ed74f52dfc8c577`
- 검토 수정본 Jobs run: `390590853293455`, 성공
- Airflow 두 실행이 같은 submit token과 Jobs run `390590853293455`를 공유하여
  중복 원격 실행 방지 확인
- 검토 수정본에서도 raw 331개, Silver 영화 107개, 박스오피스 70행 대사 통과
- 2차 검토 수정본 artifact: `23bef131caa644e306389b9887fe98529b85920b8b635fee0d171c2767c9b97a`
- publication revision 2 / Jobs run `242880321667870` 실제 실행 성공
- deploy A 후 source B가 감지되면 staging이 제출 전 거부되는 회귀 테스트 통과
- A(revision 1)가 B(revision 2)보다 늦게 성공해도 B가 선택되는 회귀 테스트 통과

ledger는 `ready_manifest_key + transform_version + publication_revision + processing_attempt` 단위로
시도 결과를 남긴다. 성공 ledger가 이미 있으면 Bronze/영화 관측/박스오피스
관측의 해당 버전별 건수를 다시 대사한 뒤 `ALREADY_PROCESSED`로 종료한다.

## 검토 대상

- `pipelines/transforms/databricks_bridge.py`
- `pipelines/transforms/exchange_publisher.py`
- `pipelines/transforms/movie_bronze_silver.py`
- `pipelines/databricks/03_movie_bronze_silver_daily.py`
- `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py`
- `pipelines/transforms/tests/test_databricks_bridge.py`
- `pipelines/transforms/tests/test_movie_bronze_silver.py`
