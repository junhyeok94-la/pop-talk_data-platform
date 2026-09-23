# B1-1 최종 승인 검토

판정: **APPROVED** — B1-1 범위의 기존 차단 지적이 해결됐다.

## 마지막 P1 해결 확인

create_release_delivery_registry는 모델 spec의 ID 중복 및 전체 집합을 검사하고, 각 direct_parent_unique_ids가 GraphSnapshot.dependencies의 부모 vector와 정확히 일치하는지 확인한다. 실제 direct child recipient도 GraphSnapshot.dependencies에서 계산한다. register_delivery_registry가 이 함수를 반드시 호출하므로, 기존의 동일 graph/model ID를 유지한 채 spec 부모만 삭제하는 반례는 등록 전에 거부된다.

추가된 정상 parent→child recipient 생성 및 변조 parent vector 거부 테스트를 읽고 독립 재실행했다. DB 등록 완료 후 전체 spec model 집합 대사와 runtime read의 registry 본문 exact 항목 대사도 유지된다. 이로써 직전 최종 검토 보고서의 남은 P1을 해소한 것으로 판단한다.

## 검증 근거

새 Airflow 컨테이너에서 RUN_DW_CONTROL_POSTGRES_TESTS=1로 전체 unittest discovery를 실행했다. **133 tests / 1.963s / OK**, 프로세스 종료 코드 0. 실제 PostgreSQL 통합 테스트 9개와 신규 순수 계약 테스트 2개를 포함한다.

직전 독립 검토에서 확인한 정상 cross-generation REUSE, runtime registry INSERT 금지, 001–004 migration의 DB ledger/local SHA-256 일치도 승인 근거에 포함한다. 이번 수정은 의존 관계 계산과 검증 및 테스트 보완이며 migration을 재실행하지 않았다.

검토 중 구현 코드는 수정하지 않았다. PostgreSQL 테스트의 fixture는 기존 rollback 및 exact-ID cleanup 경로를 사용하며 sequence 값은 소비될 수 있다.

## 승인 범위

B1-1의 PostgreSQL durable control store와 현재 계약 범위를 승인한다. B1-2의 실제 dbt model/test invocation receipt, B1-3의 Airflow Asset publisher/consumer 및 synthetic diamond 통합 검증으로 진행할 수 있다. 이 승인은 실제 Snowflake 물리 불변성, 업무 7-model 전체 실행 또는 운영 배포 완료 판정이 아니다.

B1-3에서는 기존 후속 검토 사항인 마지막 recipient ACK와 publisher 전송 기록의 순서 역전, durable pending work 복구를 실제 전달 흐름에서 확인한다.
