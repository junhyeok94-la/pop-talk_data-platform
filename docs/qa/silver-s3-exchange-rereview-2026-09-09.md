# Silver → S3 Exchange 재검토

판정: 현재 단일 Airflow DAG 경로의 Exchange 게시 범위 승인. 이전 P2 3건 종료. 추가 차단 finding 없음.

## 검토자 확인

- 전체 ZIP SHA-256을 staging XCom 값과 비교하고 run/artifact identity, bundle_version, 필수 Silver checksum, ZIP 중복·파일 집합 및 모든 문서 checksum을 첫 쓰기 전에 검증한다.
- 메모리 가짜 S3로 전체 digest/run/artifact 각각 불일치를 주입해 모두 첫 쓰기 전 거부 확인.
- 31개 테스트 재실행 성공. 필수 checksum 누락, 두 번째 파일 쓰기 실패 후 READY 부재 및 재시도 복구, 412 동일/상이 바이트 경로 포함.
- exchange_observations가 source_observed_at/ready_manifest_key/transform_version을 추가하며 bridge와 notebook Delta 생성이 동일 enriched rows를 사용한다. notebook은 JSONL 전체 바이트를 다시 계산해 비교한다.
- artifact 입력에 publisher 소스가 포함되며 게시 태스크가 staging artifact와 현재 artifact를 Files API 읽기·S3 게시 전에 비교한다.
- DAG import 정상. publish_exchange는 stage_bundle 및 transform_bronze_silver에 의존하며 ALL_SUCCESS이다. staging의 archive digest와 identity가 게시 함수에 전달된다.
- 데이터 먼저/EXCHANGE_READY 마지막, 불변 객체 재사용 및 조건부 최초 생성 흐름 유지.

## 범위

원격 실행 성공은 구현 작업의 기록이며, 이번 검토자는 원격 재게시 없이 코드·컨테이너 테스트·메모리 오류 주입으로 확인했다. Snowflake 적재, 서비스 DB 게시, 여러 독립 DAG의 병행 조정까지 승인한 것은 아니다. Exchange는 실행별 관측이며 후속 소비자는 원천 관측 시각과 publication revision으로 최신 상태를 판단하고 EXCHANGE_READY에 나열된 파일만 소비해야 한다.
