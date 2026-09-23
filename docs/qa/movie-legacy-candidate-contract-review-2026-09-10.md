# Legacy candidate 적재 계약 구현 검토

## 최종 판정 — APPROVED (순수 적재 계약 구현 범위)

마지막 P2 수정 후 코드와 신규 fixture를 확인했다. `stage_legacy_candidate_bundle`은 최초 `_read_s3` 호출 전에 manifest 경로를 거부한다. 추가된 `test_stage_rejects_manifest_path_before_any_s3_get`은 잘못된 경로에 대해 S3 GET 0회와 Volume 쓰기 0회를 확인한다.

Airflow 컨테이너에서 `python -B -m unittest discover -s pipelines/transforms/tests`를 독립 재실행했다. **47개 테스트 모두 통과**했다(0.044초). 이전 검토에서 지적한 사항은 이번 구현 범위에서 해결됐으며, 미해결 차단 지적은 없다. 아래 CHANGES REQUESTED 절은 이전 검토 이력이다.

승인 대상은 순수 변환·bundle·Exchange·loader 계약이다. Databricks notebook 및 Airflow runtime/DAG, 실제 candidate 적재의 완료를 의미하지 않는다. 실제 source artifact 고정과 원격 재계산, 5,985건 및 정책 4,320/1,665건 대사, DB rollback·재실행 검증, production 전후 fingerprint는 후속 통합 검증 조건으로 유지한다. 이번 검토에서는 실제 외부 데이터를 변경하지 않았다.

## 재검토 결과 — 수정 반영 후

판정: **CHANGES REQUESTED — 기존 1번의 manifest 최초 GET 순서만 남음.** 아래 최초 검토 내용은 이력이며 현재 판정은 이 절을 따른다.

Airflow 컨테이너에서 `python -B -m unittest discover -s pipelines/transforms/tests`에 해당하는 전체 suite를 독립 실행했고 46개 모두 통과했다. 실제 클라우드 I/O 없이 제출된 S3/Files 모형으로 추가 반례를 확인했다.

- 기존 1번 부분 해결: manifest header와 정확한 source SHA 경로를 원본 GET 전에 검증하며 builder도 source 경로를 재검증한다. 그러나 `stage_legacy_candidate_bundle`은 여전히 manifest key 검증 전에 `_read_s3`를 호출한다.
- 기존 2번 해결: `validate_legacy_candidate_input`을 cursor 생성과 DDL 이전에 호출한다. 정상 fixture의 identity/lineage도 보완됐으며 잘못된 시간 계약은 SQL 실행 없이 거부된다.
- 기존 3번 핵심 보완 수용: 제어 가능한 ledger/count/hash mock과 SUCCESS replay 무 MERGE, 저장 count/hash 오염 거부 테스트가 추가됐다. 실제 DB rollback과 동시 writer 검증은 여전히 후속 격리 통합 실행 범위다.

### 남은 [P2] manifest 경로 검증을 최초 GET 앞으로 이동

위치: `pipelines/transforms/legacy_candidate_bridge.py:151`.

정상 manifest bytes를 `raw/outside-candidate/SUCCESS.json`이라는 잘못된 manifest key에 연결해 stage 함수를 호출했다. 최종적으로 `Legacy candidate manifest/source path identity mismatch`가 발생하고 Volume 쓰기는 0회지만, S3 GET 목록에는 해당 잘못된 key 1개가 남았다. 즉 잘못된 입력을 받으면 허용 범위 밖 객체를 먼저 읽는다. source GET 선검증만으로 최초 지적이 모두 해결되지는 않았다.

수정 범위는 작다. `MANIFEST_PATTERN.fullmatch(manifest_key)`와 실패 처리를 `_read_s3`보다 먼저 실행하고 이후 body 검증에서 match를 재사용한다. 잘못된 manifest key에 대해 GET 0회·Volume 쓰기 0회를 검증하는 stage 테스트를 추가한다. 기존 path 테스트는 builder를 직접 호출하므로 이 순서 오류를 잡지 못한다.

큰 데이터 흐름을 다시 설계할 사유는 발견하지 않았다. 이번 계약 검토는 notebook/runtime/DAG 구현 또는 실제 candidate 적재의 완료 승인을 뜻하지 않는다. source artifact 고정·원격 재계산, 실제 5,985건 대사, production 전후 fingerprint는 기존 후속 조건으로 유지한다.

---

## 최초 검토 이력

판정: CHANGES REQUESTED. 결정적 직렬화와 candidate SQL 범위, Exchange child 경로 검증 방향은 맞지만, bundle 입구와 loader의 쓰기 전 검증에 재현 가능한 누락이 있다. notebook/runtime/DAG와 실제 외부 쓰기는 아직 미구현이라는 범위를 유지한다.

## 독립 검증

실제 Airflow 컨테이너에서 transforms unittest 42개를 실행해 모두 통과했다. 추가 반례는 제출된 S3/Files/RecordingConnection 모형만 사용했다. 업무 코드나 실제 클라우드 데이터는 변경하지 않았다.

## 1. [P2] bundle staging이 원본 GET 전에 경로를 검증하지 않으며 정확한 source SHA 경로도 강제하지 않음

위치: `legacy_candidate_bridge.py`의 stage_legacy_candidate_bundle 및 build_legacy_candidate_archive.

stage 함수는 manifest key allowlist를 확인하기 전에 GET하고, manifest.key가 문자열인지만 확인한 뒤 원본을 GET한다. 이후 builder가 검증한다. 기존 대사 adapter에서 이미 수정했던 사전 검증 경계가 여기서 빠졌다.

제출 source_fixture의 manifest.key만 다음으로 바꾸고 동일 source bytes를 제공해 재현했다.

- `raw/production/private.json`: 최종적으로 실패하지만 mock GET 목록에 해당 원본 key가 포함됨.
- `raw/legacy_snapshot/v1/wrong/movies_final.json`: bundle 생성과 Files.put_immutable까지 성공함.

validate_legacy_snapshot은 raw prefix만 검사하기 때문에 source key의 sha256 경로가 source SHA와 다르더라도 builder가 수용한다. 뒤의 Exchange publisher는 정확한 source 경로를 요구하므로 앞 단계는 성공하고 후속 단계는 실패하는 계약 불일치도 생긴다.

수정: manifest key를 최초 GET 전에 검사하고, manifest header 및 `raw/legacy_snapshot/v1/sha256=<source_sha>/movies_final.json` 일치를 원본 GET 전에 검사한다. builder도 동일 검증을 재사용한다. 잘못된 manifest key는 GET 0회, 잘못된 body key/bucket/status는 manifest GET 1회 이후 중단, Volume 쓰기 0회 fixture가 필요하다.

## 2. [P2] Snowflake loader가 검증되지 않은 dataclass 입력을 그대로 쓰기 처리함

위치: `legacy_candidate_snowflake.py`의 load_legacy_candidate.

read_legacy_candidate_exchange의 검증은 적절하지만 LegacyCandidateInput은 공개 생성 가능한 dataclass이고 movies 내부 dict도 mutable이다. loader는 입력 불변식을 다시 확인하지 않는다. 제출 load_fixture 자체도 ready_key='ready', manifest_key='manifest' 및 row의 source lineage 누락 상태로 커밋 테스트를 통과한다.

제출 RecordingConnection으로 load_fixture의 movie를 다음처럼 바꿔 호출했으며 반환 1, commits=1이었다.

```python
source_observed_at = '2026-01-01T00:00:00Z'
source_observed_at_known = True
source_time_basis = 'API_COLLECTED_AT'
```

실제 CANDIDATE DDL에도 이를 막는 제약이 없으므로 reader를 우회하거나 파싱 이후 dict가 변경되면 Legacy 계약을 어긴 값을 저장할 수 있다. 수정: 외부 쓰기 전에 호출하는 공통 input validator를 reader/loader가 공유한다. identity/path/revision/source SHA, business key 유일성, 각 row lineage/time를 DDL 실행 전 검증한다. 정상 loader fixture도 실제 reader 반환과 같은 완전한 입력으로 바꾸고 잘못된 입력은 SQL 0회로 거부하는 테스트를 추가한다.

## 3. [P2] 성공 replay·conflict 대사가 테스트에서 실제 검증되지 않음

위치: `test_legacy_candidate_pipeline.py`의 RecordingCursor 및 LegacySnowflakeTransactionTests.

RecordingCursor는 ledger SELECT에 항상 None, hash mismatch SELECT에 항상 0, 저장된 count에는 생성자로 받은 상수를 반환한다. 성공 replay 분기, 기존 identity/hash 충돌, 저장된 key 누락을 거부하는지 검증할 수 없는 모형이다. 현재 rollback 테스트도 rollback 호출과 SUCCESS UPDATE 부재를 확인할 뿐 실제 데이터 복원을 입증하지 않는다.

SQL의 DDL-before-BEGIN과 observation/ledger DML 경계는 코드상 수용한다. 다만 구현 기록의 replay 검증을 입증하려면 최소한 제어 가능한 ledger/저장 상태 fixture로 다음을 추가해야 한다.

- 기존 SUCCESS replay가 실제 key/hash/count 대사를 수행하며 추가 merge 없이 끝남.
- ledger identity 또는 count 충돌, 관측 key 누락/다른 hash/추가 행이면 실패.
- observation 이후 실패에서 commit하지 않고 rollback함.

실제 rollback·동시 writer 효과는 이후 격리 통합 실행에서 확인한다. 이 단계에서 이미 입증했다고 표현하지 않는다.

## 수용 사항 및 후속 어댑터 검토 경계

- ZIP entry 시각/정렬과 JSONL 고정, Legacy null 시간 부여, boxoffice 빈 bytes, Exchange exact file set/child key 선검증은 확인했다.
- publisher의 data-first/READY-last와 동일 bytes 재사용 구조는 타당하다. Snowflake SQL의 업무 쓰기 대상은 CANDIDATE로 고정돼 있고 BEGIN 이후 DDL은 없다.
- artifact_version은 현재 caller가 전달하는 64hex이며 transform_source도 임의 bytes를 받는다. 현재 builder는 로컬에 import된 변환 함수를 실행하므로 ‘bundled source와 동일 코드 실행’이나 원격 재계산 일치는 아직 입증되지 않았다. notebook/runtime 구현에서 실제 source digest 산출·고정 배포·byte 재계산으로 닫아야 한다. 이 부분은 미구현 범위로 인정하며 이번 별도 결함으로 중복 요구하지 않는다.
- 정책 건수 5,985/4,320/1,665의 실제 대사와 production 전후 fingerprint는 실제 candidate 실행의 완료 조건으로 유지한다.

위 세 수정 후 계약 묶음을 재검토한다. 현재 단계에서는 실제 candidate 쓰기를 실행하지 않았다.
