# Cosmos dbt TaskGroup 통합 검증

검증일: 2026-09-09

결론: 샘플 데이터 기준으로 `S3 → Databricks Bronze/Silver → S3 exchange → Snowflake staging/dbt Gold → PostgreSQL serving` 경로가 끝까지 성공했다. Snowflake 적재 실패와 PostgreSQL 활성화 직전 실패도 각각 주입해, 부분 데이터 롤백과 마지막 정상 게시본 보존을 확인했다.

## 구현 기준과 참고 소스

외부 참고 프로젝트 `D:\01.DEV\dbt-airflow`를 직접 검토했다.

- `airflow\hbu\include\dbt_common_config.py`: dbt 프로젝트/프로필/실행 설정 분리 방식을 참고했다.
- `airflow\hbu\dw\bi\daily\bi_daily_cmm_pb_03.py`: `DbtTaskGroup`이 모델 의존성을 Airflow 작업 그래프로 펼치는 방식을 참고했다.
- 재사용한 방향: Cosmos `DbtTaskGroup`, 별도 dbt 가상환경, subprocess 실행, manifest 기반 파싱, dataset 발행 비활성화.
- 그대로 복사하지 않은 부분: 구 Airflow import, 공유 `target` 경로, tag만으로 선택하는 구조, 모델마다 test를 반복하는 `AFTER_EACH` 방식.

이 프로젝트에서는 배포 manifest를 고정하고, 7개 모델을 7개 Airflow 작업으로 펼친 뒤 전체 43개 테스트를 `project_test` 한 번으로 실행한다. 따라서 어떤 모델이 실패했는지는 Airflow 그래프에서 바로 찾을 수 있고, 품질 검사는 중복 실행하지 않는다.

## 정상 경로 검증

- DAG: `pop_talk_movie_databricks_daily`
- run id: `manual__cosmos_acceptance_20260909`
- 입력 ready manifest: `manifests/movie_api_daily/v1/collection_date=2026-09-09/run_id=4834bb8200e5a76f2cf8b408/DAILY_READY.json`
- publication revision: 7
- 결과: 16개 작업 모두 성공
- Databricks run id: `249476089354820`

Cosmos가 생성한 dbt 작업은 다음 8개다.

1. `stg_successful_exchange_publications_run`
2. `stg_movie_observations_run`
3. `stg_boxoffice_observations_run`
4. `dim_movie_run`
5. `fct_boxoffice_daily_run`
6. `mart_boxoffice_daily_run`
7. `mart_data_quality_run`
8. `project_test`

`project_test` 실행 결과는 `PASS=43 WARN=0 ERROR=0 SKIP=0 TOTAL=43`이다. 로그에서 시작된 43개 test 이름과 고정 manifest의 43개 test unique name을 별도로 대사했으며 누락과 예상 밖 항목은 모두 0개였다.

PostgreSQL v3 게시 결과:

| 항목 | 값 |
| --- | --- |
| publication id | `d676420d8e2b44490545ef0a5955eda751d6cff7d859ec4810f830a00c53a2ef` |
| generation | 16 |
| 영화 | 107건 |
| 박스오피스 | 70건 |
| 품질 결과 | 2건 |
| model version | `2276279bd968461b80301a7ad3999459b6bdebb5d6c8f5b27d7c02ef7e2ca8a5` |

## PostgreSQL 게시 실패 검증

- run id: `manual__cosmos_postgres_fail_20260909`
- 입력 옵션: `postgres_fail_before_activation=true`
- Snowflake와 dbt 전체 작업: 성공
- `publish_postgres`: 활성 publication 교체 직전에 실패, 1회 재시도 후 최종 실패
- `publish_attempts_v3`: `FAILED`, 오류 `Injected failure before activation`

실패 후에도 `active_publications_v3`의 `movie_gold`는 위 정상 publication을 가리켰고, 현재 serving view는 영화 107건과 박스오피스 70건을 유지했다. 다만 이 최초 실패 run은 정상 run과 publication ID가 같아 기존 publication 재검증 경로였다. 신규 snapshot INSERT rollback은 아래 2026-09-10 보완 시나리오에서 별도로 확인했다.

## Snowflake 적재 실패와 롤백 검증

동일 revision 7을 다시 실행한 `manual__cosmos_snowflake_fail_20260909`는 기존 SUCCESS 원장을 확인하고 적재를 재사용했다. 이 실행은 실패 주입 전에 반환됐으며, 이는 동일한 exchange 재처리 시 중복 적재하지 않는 멱등 동작이다.

실제 실패 전파는 새 revision으로 다시 검증했다.

- run id: `manual__cosmos_snowflake_rollback_20260909`
- publication revision: 8
- 입력 옵션: `snowflake_fail_after_movie_merge=true`
- `load_snowflake`: 영화 MERGE 직후 의도한 예외 발생, 1회 재시도 후 최종 `failed`
- 7개 dbt 모델 작업, `project_test`, `confirm_gold_build`, `publish_postgres`: 모두 `upstream_failed`
- DAG 최종 상태: `failed`

Snowflake에서 revision 8을 직접 조회한 결과:

| 확인 대상 | 남은 행 수 |
| --- | ---: |
| `STAGING.SILVER_EXCHANGE_LOADS` | 0 |
| `STAGING.MOVIE_OBSERVATIONS_RAW` | 0 |
| `STAGING.BOXOFFICE_OBSERVATIONS_RAW` | 0 |

이 revision 8은 revision 7과 source/artifact가 같았다. 따라서 위 결과는 신규 revision ledger rollback과 후속 작업 차단은 입증하지만, 영화 관측의 신규 INSERT rollback까지 입증하지는 않는다. 신규 영화 INSERT rollback은 아래 2026-09-10 보완 시나리오에서 별도 source로 확인했다. PostgreSQL 게시 작업은 실행되지 않았고, 정상 publication과 현재 view의 107/70건도 유지됐다.

검증 도중 Docker 엔진이 중단되어 실행 중이던 Databricks 작업이 orphan 처리됐다. 서비스 복구 후 Airflow scheduler가 해당 작업을 재채택했고, 저장된 Databricks external run id가 이미 성공했음을 확인해 재제출 없이 다음 단계로 진행했다. 이는 이번 실행에서 관찰한 복구 기록이며, 일반적인 장애 복구나 worker A/B 전환을 완전히 인증한 결과로 확대하지 않는다.

## 2026-09-10 신규 쓰기 rollback 보완 검증

최초 검토에서 기존 source/publication 재사용 경로만 검증됐다는 지적을 받았다. 기존 데이터를 삭제하거나 계약을 우회하지 않고, 원천 API에서 완전 범위 Raw를 두 번 새로 수집해 PostgreSQL과 Snowflake 실패에 각각 다른 source를 사용했다.

### PostgreSQL 신규 snapshot INSERT rollback

- Raw run: `manual__cosmos_new_full_a_20260910`
- source run id: `9205b8da4dd4eb4b488b5a01`
- 변환 run: `manual__cosmos_pg_new_write_full_retry_20260910`
- processing attempt: 2
- 신규 publication id: `596d7b0e68562fce1e5221d5bc31974381e70e0c32a66b04f127dc451bb40401`
- 선행 15개 작업: 성공
- `publish_postgres`: 활성화 직전 실패, 재시도 후 최종 `failed`

실패 후 신규 publication 기준 PostgreSQL 행 수:

| 대상 | 행 수 |
| --- | ---: |
| `dataset_publications_v3` | 0 |
| `movie_snapshot_v3` | 0 |
| `boxoffice_snapshot_v3` | 0 |
| `gold_build_registry_v3` | 1 |
| `publish_attempts_v3` FAILED | 1 |

registry 예약과 실패 attempt는 재시도/세대 순서 계약을 위해 의도적으로 남는다. publication과 snapshot 데이터는 같은 트랜잭션에서 rollback됐다. `active_publications_v3.movie_gold`는 기존 `d676...`를 유지했고 serving view도 영화 107건/박스오피스 70건을 유지했다.

첫 실행 `manual__cosmos_pg_new_write_full_20260910`은 Databricks 처리 중 Airflow API server 컨테이너가 외부에서 재생성되어 heartbeat가 3회 실패했고, Airflow가 원격 run을 취소했다. 데이터 결함과 구분해 기록했으며 새 processing attempt로 재실행했다.

### Snowflake 신규 영화 INSERT rollback

- Raw run: `manual__cosmos_new_full_b_20260910`
- source run id: `ecc4cf4cc8fa25ea6acd0459`
- 변환 run: `manual__cosmos_sf_new_insert_rollback_20260910`
- exchange 입력: 영화 104건, 박스오피스 10건
- artifact version: `fbd7e434b5a5984b7d0a1858153d907b778c5c64f879750bf13d69aeb5f6601e`
- `load_snowflake`: 영화 MERGE 직후 실패, 재시도 후 최종 `failed`
- 7개 dbt 모델, `project_test`, `confirm_gold_build`, `publish_postgres`: 모두 `upstream_failed`

이 source/artifact는 이전 Snowflake SUCCESS 원장이 없는 별도 identity다. 따라서 movie MERGE는 104개 관측 INSERT를 시도한 뒤 예외가 발생했다. 실패 후 같은 source/artifact를 직접 조회한 결과는 다음과 같다.

| 대상 | 잔여 행 수 |
| --- | ---: |
| `STAGING.SILVER_EXCHANGE_LOADS` | 0 |
| `STAGING.MOVIE_OBSERVATIONS_RAW` | 0 |
| `STAGING.BOXOFFICE_OBSERVATIONS_RAW` | 0 |

신규 movie INSERT와 ledger가 함께 0건이고 boxoffice 단계는 실행 전이므로, 영화 MERGE를 포함한 Snowflake 트랜잭션 rollback을 확인했다.

## 정적·컨테이너 검사

- Python 테스트: 65개 통과
  - collectors 17
  - transforms 33
  - orchestration 12
  - PostgreSQL publish 3
- DAG 구조/부정 회귀 검사: 6개 통과, import 오류 0
- Airflow 환경 `pip check`: 깨진 의존성 없음
- dbt 가상환경 `pip check`: 깨진 의존성 없음
- dbt pool: `pop_talk_dbt_pool`, slot 1
- 고정 배포 digest: `bf08df4b8ce3a5412d7931e5ed391e32d9772b0bb2cc624d70cbf2d5005ba2a8`
- manifest SHA-256: `e2f9191d56e68fe53ce08115bf8a969795b4e6336e103068c042e7aa2a4739de`

## 운영 상태

통합 검증 후 `pop_talk_movie_raw_daily`와 `pop_talk_movie_databricks_daily` DAG를 모두 다시 pause했다. 추가 API·클라우드 실행은 사용자가 명시적으로 unpause하거나 수동 trigger할 때만 발생한다.
