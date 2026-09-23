# Databricks 일일 파이프라인 검토

판정: 수정 후 재검토 필요. 현재 단계 승인 보류.

변환/bridge 테스트 22개 재실행 통과. 동일 READY 동일 코드 재실행 성공 기록은 확인했으나 원격 실패 주입/동시 실행 실험은 하지 않았다. 아래는 코드 경로 검토와 로컬 메모리 재현이다. 기존 데이터와 소스는 수정하지 않았다.

## P1 — 변환 버전별 재처리가 저장 계약과 충돌

- 위치: `pipelines/transforms/databricks_bridge.py:98,111,121`; `pipelines/databricks/03_movie_bronze_silver_daily.py:59,223-237`.
- READY run_id만으로 고정한 landing 경로에 변환 소스도 포함한다. 동일 READY에 소스 version1/version2를 전달한 메모리 재현에서 두 번째 staging은 Immutable conflict로 실패했다. 코드 수정 후 기존 원본 재처리가 막힌다.
- landing에 버전을 추가하는 것만으로 끝나지 않는다. ledger는 transform_version별이지만 observation 키에는 버전이 없어 변경된 정책/매칭 결과가 충돌한다. current는 동일 source_observed_at의 콘텐츠 변경을 거부하고, 정책/매칭만 달라지면 최신 시각 조건에 걸려 갱신하지 않는다.
- 노트북은 고정 WORKSPACE 경로를 overwrite하면서 transform_version에는 순수 코어 해시만 사용한다. 노트북 저장 로직을 수정해도 기존 성공 ledger 때문에 ALREADY_PROCESSED로 종료될 수 있다.
- 수정: raw run identity와 processing version을 분리한다. 불변 bundle/노트북 경로에 전체 처리 artifact 버전을 넣고 observation/ledger에 동일 버전을 사용한다. current는 원천 시각 우선 + 동일 원천에 대한 명시적 재처리 승격 정책을 둔다. 해시 사전순을 버전 최신순으로 간주하지 않는다. 새 버전으로 재처리해도 원본은 재수집하지 않아야 한다.

## P1 — 실패한 실행의 부분 current가 노출되고 SUCCESS가 검증보다 먼저 기록됨

- 위치: `pipelines/databricks/03_movie_bronze_silver_daily.py:232-264`.
- movie current MERGE 이후 boxoffice conflict/쓰기 실패가 나면 movie current만 새 실행으로 바뀌고 ledger는 FAILED가 된다. 두 current 테이블의 읽기 계약에 성공 실행만 공개하는 장치가 없다. 단순히 FAILED run을 필터하면 이미 덮어쓴 이전 movie current도 조회에서 사라져 복구되지 않는다.
- `upsert_ledger('SUCCESS')` 다음 `verify_persisted_run()` 순서이므로 검증 실패/프로세스 중단 전에 잠시 또는 지속적으로 잘못된 완료 표식이 공개될 수 있다. 재시도 검증도 observation 건수뿐이고 current 검증은 없다.
- 수정: 관측 이력을 먼저 검증·완료한 뒤 완료 실행만 대상으로 current를 계산하는 view 또는 publication pointer/세대별 snapshot을 사용한다. 최소한 SUCCESS는 검증 후 마지막에 기록하고 downstream 읽기 계약을 함께 구현한다. 여러 Delta 테이블을 순차 MERGE한 것을 하나의 원자적 트랜잭션으로 간주하면 안 된다.
- 검증: movie 처리 직후 실패를 주입했을 때 기존 공개 상태 유지, 재시도 후 movie/boxoffice와 ledger 일치, 완료 직전 중단 후 복구를 테스트한다.

## P1 — Jobs submit 응답 유실 시 중복 원격 실행 가능

- 위치: `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py:84-108`; 노트북 `reject_conflict`/`upsert_ledger`.
- operator에 idempotency_token을 지정하지 않았다. 설치된 provider 생성자에서도 기본값 None을 확인했다. submit은 성공했지만 응답이 유실되면 HTTP/태스크 재시도가 새 원격 run을 생성할 수 있다. Airflow max_active_runs=1은 이 원격 중복을 직렬화하지 않는다.
- 노트북은 존재 조회→충돌 조회→MERGE의 별도 단계이며 ledger claim/lease가 없다. 병행 실행에서 아직 SUCCESS가 없는 두 실행이 모두 처리할 수 있고 실패한 한 실행이 다른 실행의 SUCCESS를 FAILED로 덮어쓸 수 있다. 따라서 순차 2회 성공만으로 중복 실행 안전성을 입증하지 못한다.
- 수정: 논리적 submit 요청을 재시도할 때 같은 idempotency token을 재사용하고 원격 run ID를 회수/조회한다. 이미 종료된 실패 run을 새로 실행할 때만 새 attempt identity를 만든다. 별도 수동 실행까지 고려한다면 동일 READY+처리버전에 대한 single-writer/claim 또는 충돌 시 재조회하는 명시적 조정도 필요하다.
- 공식 계약: https://docs.databricks.com/api/jobs/v2/submit-run — 동일 토큰 요청은 새 run 대신 기존 run ID 반환, 최대 64자.

## 후속 연결 시 확인할 사항

- schedule=None과 명시 READY 입력은 현재 문서화된 수동 검증 범위와 일치하므로 결함으로 보지 않는다. raw DAG와 자동 연결·초기 snapshot remote 경로는 아직 별도 작업이다.
- current의 UpdateAll은 새 관측이 REVIEW_REQUIRED/UNMATCHED이면 기존 확인된 KMDb 포스터/줄거리도 null로 바꾼다. latest observation과 서비스용 last-known confirmed enrichment를 분리하거나 후속 게시에서 명시적으로 보존해야 한다. 현재 직접 서비스 게시가 없으므로 별도 연결 조건으로 기록한다.
- source_observed_at 비교는 정상 시각 입력의 과거 실행 역행을 막는 방향은 적절하다. 다만 movie의 시각은 KOFIC 상세 수집 시각만 사용하므로 KMDb 보강 결과의 관측 시각·정책 버전 승격을 포함한 상태 선택 계약도 정해야 한다.
- ZIP 내부 문서 검증과 raw payload checksum, 샘플 차단, 박스오피스 업무 키 중복 거부는 적절하다. 이후 bundle 자체 digest와 처리 artifact identity를 묶어 감사 가능하게 유지하는 것을 권장한다.

단위 테스트 22개는 core/bridge 위주이며 Delta current/ledger 실패 복구와 Jobs 응답 유실을 검증하지 않는다. 위 3건 수정 및 해당 실패 시나리오 검증 후 승인 여부를 재판정한다.
