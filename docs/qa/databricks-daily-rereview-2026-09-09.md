# Databricks 일일 단계 2차 검토

판정: 승인 보류. 이전 지적의 주요 수정은 확인했지만 아래 버전 일관성 2건 수정 필요.

검증: 변환/bridge 단위 테스트 23개 재실행 통과. 원격 데이터 변경이나 실패 주입은 하지 않았으며 아래 시나리오는 실제 코드의 데이터 전달/정렬 조건을 따라 검토했다.

## 확인한 개선

- run_id/artifact_version별 ZIP과 버전별 노트북 경로, 버전별 observation 키 반영.
- current를 성공 ledger와 결합하는 view로 변경. 관측 검증 후 SUCCESS 기록하므로 이전의 실패 실행 부분 current 공개 경로 해소.
- READY+artifact+attempt 토큰 및 attempt별 ledger 반영. 같은 submit 요청 재시도에서 토큰을 재사용하도록 수정.
- 동일 raw/artifact 관측을 여러 attempt가 공유하는 구조 자체는 불변성·완전성 검증 후 성공 attempt로 공개한다는 범위에서 타당하다.

## P1 — 배포된 노트북과 ZIP의 artifact가 한 DAG 실행에서 달라질 수 있음

위치: `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py:47,83,109-114`.

deploy_notebook은 artifact A의 notebook을 배포하고 경로 A를 반환한다. stage_bundle은 직전 결과를 기준으로 하지 않고 공유 파일을 다시 읽어 artifact B를 계산한다. 두 태스크 사이에 다른 작업이 파일을 수정하거나 staging만 재시도할 때 이 경로가 성립한다. Submit은 deploy의 경로 A와 stage의 artifact B/ZIP B를 조합한다. 노트북 검증은 index B와 parameter B만 비교하므로 실행 중인 notebook A와 B의 일치를 확인하지 않는다. 결국 실제 실행 코드 A+B가 B로 ledger에 기록되고, 올바른 B 실행도 동일 토큰/ledger로 이미 완료된 것으로 취급될 수 있다.

권고: 실행 시작에 전체 artifact를 한 번 고정해 배포와 staging이 동일 불변 바이트를 소비하도록 한다. 최소한 staging에서 deploy_notebook.artifact_version과 재계산 결과를 비교해 변경 시 fail closed 해야 한다. 새 버전은 새 DAG 실행으로 명시적으로 시작한다. 노트북 artifact 검증에도 배포된 실행 코드와 bundled core의 연결을 포함한다.

회귀 검증: deploy A 후 source 변경 B를 주입한 staging이 제출 전에 거부되거나 A의 고정 바이트로 계속되어야 한다. 현재 테스트는 ZIP 경로 구분만 확인한다.

## P2 — 완료 시각만으로 코드 버전 승격을 결정해 구버전이 다시 최신이 될 수 있음

위치: `pipelines/databricks/03_movie_bronze_silver_daily.py:250,264`.

동일 source_observed_at에 대해 current는 completed_at DESC만 사용한다. A가 실패 → 수정 B가 성공 → A의 새 attempt가 늦게 성공하는 순서이면 A가 current로 선택된다. A에는 기존 SUCCESS가 없으므로 replay 조기 종료도 이를 차단하지 않는다. 이는 원천 시각 역행은 아니지만 수정된 변환 정책/매칭 결과의 역행이다.

권고: artifact 해시는 식별자이고 completed_at은 실행 이력으로 유지한다. 별도 승인된 artifact의 단조 증가 processing revision 또는 명시적 publication 선택을 두고, 동일 원천의 늦은 구버전 성공은 관측에만 보존한다. 롤백은 명시적인 승격 동작으로 처리한다. A 실패/B 성공/A 늦은 성공 시 B 유지 테스트가 필요하다.

## 검증 한계 및 다음 단계

실제 동일 토큰 재사용 성공 기록은 정상 경로의 근거다. 신규 view의 실패 중단/복구 및 서로 다른 artifact 완료 순서 테스트는 23개 테스트에 포함되지 않는다. 두 버전 계약을 수정하고 해당 시나리오를 검증한 뒤 재판정한다. 후속 Snowflake/서비스 DB 단계는 이번 검토 범위가 아니다.
