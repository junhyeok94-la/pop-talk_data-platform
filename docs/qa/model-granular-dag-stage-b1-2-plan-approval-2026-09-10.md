# B1-2 설계 최종 승인

판정: **APPROVED** — 수정 계획 전체를 재검토했고, 구현 진행을 차단하던 설계 지적이 해결됐다.

## 마지막 두 지적 확인

1. publication 검증이 소비 generation plan의 exact binding provenance를 기준으로 변경됐다. REBUILD는 현재 attempt/fence 증거, REUSE는 지정된 원본 result의 검증된 receipt를 참조한다. 원본 실행 generation과 소비 generation을 구분하며 SOURCE_GATE는 exact snapshot/cutoff, DEPLOYMENT_GATE는 exact deployment/manifest에 연결한다. 고정 27+15+1 숫자 대신 registry의 exact required test ID 집합을 검사한다. 승인된 REUSE 흐름과의 충돌이 해소됐다.
2. DBT_LOG_FORMAT_FILE=json 및 DBT_LOG_LEVEL_FILE=debug 강제와 caller override 거부가 명시됐다. exclusive invocation 경로의 dbt.log에서 native ID를 관측하고 fresh run-results ID와 대사하며, 누락·복수 ID·불일치는 거부한다. 실제 설치본 fixture에서 두 ID를 확인하는 완료 기준도 추가됐다.

## 승인한 구현 방향

- orchestration/native invocation ID 분리와 manifest 기반 exact model/test 판정.
- strict/legacy entrypoint 분리, 실행 환경 및 인자 제한.
- subprocess 이전 원자적 예약과 중복 실행 방지, MODEL 성공 증거 이후 TEST 실행.
- Popen supervisor의 heartbeat timeout, signal 전달, kill/wait 및 만료 fence 재사용 금지.
- 영속 artifact 경로, 파일 봉인과 immutable DB 증거 분리, crash/응답 유실 복구.
- 모델 소유 테스트와 source/deployment gate의 책임 분리 및 publication 전제 유지.

## 검증 범위와 다음 단계

이번 승인은 **B1-2 구현 전 설계 승인**이다. 최신 계획을 이전 검토 근거와 대조했으며 구현 코드는 수정하지 않았다. 이번 재검토에서 테스트 또는 실제 dbt/Snowflake 실행을 수행한 것은 아니다.

계획에 따라 B1-2 구현을 진행할 수 있다. 구현 완료 시 실제 PostgreSQL 예약 경쟁, artifact crash/replay, supervisor 취소·만료, 실제 dbt v6 fixture/native ID, Cosmos command/env 호환성과 기존 회귀 검증 결과를 검토한다. B1-3 assembler/Asset 전달 및 B2 업무 Snowflake 실행의 완료를 이번 승인으로 대신하지 않는다.
