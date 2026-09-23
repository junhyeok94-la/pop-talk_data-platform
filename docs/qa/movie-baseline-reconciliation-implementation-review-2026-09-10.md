# 영화 기준선 대사 v1 구현 독립 검토

판정: CHANGES REQUESTED. 기본 분류 보존과 제출 집계는 일관되지만, 파싱·Gold 계보·결과 해석의 아래 문제를 수정한 뒤 최종 승인해야 한다. 업무 코드는 변경하지 않았다.

## 1. [P2] 잘못된 날짜와 비문자열 ID를 정상 값으로 승격함

근거: `pipelines/modeling/movie_baseline_reconciliation.py`의 _date(90행), normalize_kofic_id(122행), _text.

실제 컨테이너에서 재현:

```python
_date('garbage2020xx01yy01')
# Parsed(state='VALUE', value='2020-01-01')
policy_for_legacy({'movie_nm': 'x', 'open_dt': 'garbage2020xx01yy01'})
# (True, ())
normalize_kofic_id(True)
# Parsed(state='VALUE', value='True')
normalize_kofic_id(123)
# Parsed(state='VALUE', value='123')
```

날짜에서 모든 비숫자를 제거하므로 허용하지 않은 문자열을 정상 개봉일/정책 적격으로 해석한다. ID도 str 변환으로 boolean·숫자를 문자열 ID와 결합할 수 있으며, 숫자로 이미 소실된 선행 0을 복구할 수 없다. 승인 설계는 문자열 ID와 invalid 격리를 요구한다.

수정: 날짜는 명시적으로 허용한 YYYYMMDD/ISO date와 date 타입만 파싱하고 기타 입력은 INVALID로 분리한다. ID는 null/빈 문자열 처리 외에는 문자열 타입을 요구한다. 일반 텍스트의 list/dict 등도 유효 문자열로 승격하지 않도록 타입 계약을 고정한다. 위 반례와 정상 compact/ISO date, 선행 0, 양쪽 invalid 테스트를 추가한다.

## 2. [P2] Gold ID와 계보가 같은 데이터 버전이라는 보장이 없음

근거: `scripts/reconcile_movie_baseline.py` _load_gold, 177–194행.

DIM_MOVIE에서 ID만 먼저 읽고 MART_DATA_QUALITY에서 source publication 목록을 따로 읽는다. Snowflake READ COMMITTED에서는 BEGIN으로 묶어도 다음 SELECT가 다른 커밋을 볼 수 있다. 따라서 두 조회 사이 Gold 갱신이 발생하면 영화 집합과 다른 버전의 계보를 함께 기록한다. 또한 품질 마트의 전체 입력 publication 목록은 개별 영화의 선택된 source 계보와 같지 않다. [Snowflake 트랜잭션 문서](https://docs.snowflake.com/en/sql-reference/transactions)

수정: DIM_MOVIE의 KOFIC ID와 source_run_id/artifact_version/publication_revision/exchange_ready_key를 **같은 SELECT**에서 추출해 그 결과에서 선택된 계보를 계산한다. 전체 build 입력 계보가 필요하면 별도 항목으로 구분하고 버전/시점 일치 검증을 추가한다. hard-coded POP_TALK_DW_DEV 조회와 connection의 current_database 메타데이터도 동일 대상인지 보장한다. 구현 기록의 ‘같은 트랜잭션에서 계보 추출’만으로 일관성이 보장되는 듯한 설명을 고친다. 실제 실행 시 혼합이 발생했다고 단정하는 것은 아니다.

## 3. [P2] ID 교집합과 필드별 집계만으로 수집 순서·동일 보강 집합을 확정함

근거: 결과 문서 §2, §3, §5, §7; FIELD_PARSERS와 결과 JSON.

5,309개 ID 및 제목·개봉일 일치는 공통 데이터 계통의 근거지만 ‘Legacy에서 이어짐’, 전용 10건이 ‘Legacy 이후 추가됨’이라는 시간적 출처를 입증하지 않는다. Gold-only 87건도 두 집합에 없다는 뜻이지 모두 스냅샷 이후 신규 영화라는 증거가 아니다. 수집 범위 차이로도 발생한다.

또한 서비스에만 KMDb ID가 있는 수는 194, 줄거리도 194지만 **포스터는 180**이다. 각 필드 집계만으로 이들이 같은 영화 집합이라고 확정할 수 없다. ‘KMDb ID·줄거리·포스터가 있는 194건’ 및 ‘레거시 적재 이후 보강’은 현재 증빙을 넘는다. 서비스 전용 source_system=KOFIC_KMDB 주장도 어댑터 SELECT에는 해당 컬럼이 없어 이 결과로 재현되지 않는다.

수정: 관측 사실과 출처/시점 추론을 구분한다. 194/180을 정확히 적고 동일 영화의 공동 보강 현상을 주장하려면 ID 교집합 집계를 추가한다. 별도 이력/수집 증거가 없으면 시간적 단정은 제거한다. 레거시 전체를 원천 관측으로 보존하자는 제안 자체는 합리적이나, 보강값과 서비스 전용 행의 보존 계약은 유지하고 채택 확정은 독립 검토 이후로 표시한다.

## 4. [P2] manifest의 읽기 대상 검증이 실제 객체 GET 뒤에 수행됨

근거: `scripts/reconcile_movie_baseline.py` _load_legacy, 80–90행.

manifest.key가 문자열인지만 확인한 뒤 해당 객체를 읽고, 그 다음에 bucket/status/raw prefix를 검사한다. 예를 들어 key가 허용된 legacy prefix 밖이어도 GET이 먼저 발생한다. 외부 쓰기는 없지만 승인 설계의 입력 경계를 요청 전에 강제하지 않는다.

수정: manifest의 schema/status/bucket/key prefix와 필드 타입을 먼저 검사하고, 허용된 객체만 GET한 후 bytes/hash/count를 검증한다. mock S3 테스트에서 잘못된 key/bucket/status일 때 원본 GET이 호출되지 않음을 확인한다. 오류 코드는 str(error).isupper() 판정 대신 알려진 SnapshotContractError 코드 allowlist를 사용한다. isupper는 비밀 제거 규칙이 아니며 대문자 예외 메시지를 그대로 통과시킬 수 있다.

## 검증 결과 및 수용 사항

- 실제 Airflow 컨테이너에서 제출된 순수 unittest 8개를 재실행하여 모두 통과했다. 위 추가 반례는 해당 테스트가 놓친 경우다.
- 기계 결과 JSON을 로컬에서 읽어 5,309+676=5,985, 5,309+10=5,319, Gold 28+3+87=118을 확인했다. 필드 비교 합계와 보고된 정책 불일치 2건도 일관된다. 이번에 원천 전체를 다시 추출해 동일 집합인지 재계산한 것은 아니다.
- 어느 한쪽 중복이면 상대 단일 행도 ambiguous로 격리하는 구조와 3개 원천의 행 수 보존은 타당하다.
- PostgreSQL 고정 SELECT와 단일 REPEATABLE READ READ ONLY 트랜잭션은 설계에 부합한다. 읽기 전용 업무 범위에서 발견된 DML은 없다.
- 현재 결과 JSON에는 원천 전체 행·전체 줄거리·전체 URL·DSN·토큰이 포함되지 않고 주로 집계 및 허용 식별자가 들어 있다. 단, adapter 출력/오류 부정 테스트는 제출 테스트에 없으므로 정상 결과 확인만으로 모든 실패 경로의 비밀 제거를 입증한 것은 아니다.
- 관계 집계는 전체 행/연결 영화/고아 행을 계산한다. 후속 보존 계획에서 service-only 및 정책 제외 그룹별 영향이 필요하면 해당 분류별 관계 집계를 추가한다.

## 비차단 보완

source_hash 존재 집계는 설계에 있지만 SELECT 이후 결과에 사용되지 않는다. `_split`은 배열 중복을 제거하므로 순서 보존 외에 중복 축약도 hash 계약에 명시하거나 비교 단계에서 보존해야 한다. 임시 legacy observation key는 설계에 있지만 현재 결과는 집계 전용이므로, 미해결 행별 검수 목록을 만들 때 snapshot hash와 행 번호를 사용한다.

검토 중 컨테이너 안에서 호스트의 `.local/...` 경로로 결과를 열려던 조회는 경로 차이로 실패했고 이후 호스트 파일을 직접 확인했다. 단위 테스트와 파싱 반례 실행에는 영향이 없다. 수정 후 관련 테스트와 읽기 전용 대사를 다시 수행하고 결과 해석을 갱신하면 재검토할 수 있다.
