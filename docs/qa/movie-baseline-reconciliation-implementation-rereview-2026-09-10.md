# 영화 기준선 대사 v1 구현 재검토

판정: APPROVED — 현재 POP_TALK_DW_DEV/dev 대상의 읽기 전용 대사 구현과 수정된 결과 해석을 수용한다. 이전 구현 검토의 네 수정 요청을 종료한다. 실제 observation 적재·bridge 적용·서비스 변경 승인은 별도다.

## 수정 확인

| 이전 지적 | 확인 결과 |
| --- | --- |
| 날짜/ID의 잘못된 정상화 | 날짜 문자열은 명시한 compact/ISO 형식만 허용하고 비문자열 ID 및 일반 텍스트는 INVALID 처리한다. 이전 garbage 날짜 반례와 정상 날짜 fixture를 포함한 테스트 통과 |
| Gold ID와 계보의 조회 버전 불일치 | DIM_MOVIE에서 ID와 선택된 source_run/artifact/revision/Exchange key를 동일 SELECT로 추출한다. 결과도 전체 build 입력 목록 대신 selected_movie_source_lineage로 구분 |
| 집계 이상의 출처·보강 시점 단정 | 결과 문서가 194개 KMDb/194개 줄거리/180개 포스터를 별도 집계로 표시하고 시간적 선후·동일 집합·Gold-only의 신규 여부를 단정하지 않음 |
| S3 GET 전 경계 미검증 | header 검증을 원본 GET 앞으로 이동. status/bucket/key 위반 시 두 번째 GET이 없음을 mock 테스트로 확인. SnapshotContractError는 고정 코드 allowlist 적용 |

service source_system을 실제 SELECT 및 분류별 집계에 추가했고, source_hash 존재 건수도 결과에 포함됐다. 배열 비교 hash는 순서와 중복을 보존한다. 정책 계산에 사용하는 별도 `_split`의 중복 제거와 비교 hash 동작을 구분한 것은 적절하다.

## 독립 검증

실제 Airflow 컨테이너에서 다음 명령을 실행했다.

```text
python -B -m unittest discover -s pipelines/modeling/tests -v
Ran 11 tests in 0.004s
OK
```

수정 소스, 순수/어댑터 테스트, 결과 문서, 구현 기록 및 호스트의 기계 결과 JSON을 대조했다.

- Legacy: 5,309 matched + 676 only = 5,985.
- Service: 5,309 matched + 10 only = 5,319.
- Gold: 양쪽 연결 28 + 서비스만 연결 3 + Gold-only 87 = 118.
- 서비스 전용 source_system 집계는 singleton_only:KOFIC_KMDB=10이다.
- 서비스 source_hash 존재 건수는 5,319다.
- 새 JSON에는 선택된 영화의 source 계보 4개가 기록된다. 이전 품질 마트 전체 입력 5개와 의미가 달라진 결과이며 건수 감소 자체가 오류는 아니다.
- 결과에는 원천 전체 행, 전체 줄거리/URL, 인증 토큰/DSN이 없었다. PostgreSQL 조회의 READ ONLY REPEATABLE READ 경계도 유지된다.

이번에는 원천 전체를 다시 추출하지 않았다. 실데이터 재실행 자체는 구현 작업의 보고이며, 본 검토는 제출 산출물 일관성·코드·독립 테스트를 확인했다. 업무 데이터나 업무 코드는 변경하지 않았다.

## 비차단 주의사항

- Gold SQL은 POP_TALK_DW_DEV로 고정되어 있다. CURRENT_DATABASE()==connection database 비교는 이 고정 조회 대상과의 일치를 검증하는 조건은 아니다. 현재 결과와 설정은 POP_TALK_DW_DEV로 일치하므로 본 승인은 해당 환경에 한정한다. 다른 환경 지원 시 조회 DB를 안전하게 매개변수화하거나 고정 대상과 설정을 직접 비교해야 한다.
- `test_failure_message_has_only_stage_and_allowlisted_code`는 안전한 오류 객체를 직접 만드는 테스트여서 SDK의 비밀 포함 예외부터 stderr까지의 전체 경로를 검증한 것은 아니다. 실제 allowlist/catch 코드 수정은 확인했으며, 구현 기록의 테스트 범위는 이 한계를 반영해 읽어야 한다.

## 다음 작업 경계

레거시 전체를 원천 관측으로 보존하고 정책 제외 사유를 유지한다는 제안은 이번 대사와 양립한다. 서비스 전용 행·보강값·공개 상태와 기존 관계를 보존한다는 조건도 유지한다. 다음 observation/bridge 설계에서는 이 조건을 실제 필드별 provenance와 참조 무결성으로 구현해야 한다. 이번 승인을 현재 서비스 값을 일괄 덮어쓰는 허가로 해석하지 않는다.
