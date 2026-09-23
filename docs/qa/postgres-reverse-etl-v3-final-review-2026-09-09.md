# Reverse ETL v3 registry 재검토

판정: 현재 단일 직렬 Airflow DAG 및 registry 예약을 거친 게시 복구 범위 승인. 직전 P1 재현 경로 종료.

## 확인

- registry INSERT/identity 대사 후 별도 commit이 snapshot transaction보다 앞에 있다.
- 동일 publication은 registry의 model_version/snapshot_sha256을 대사하고 기존 generation을 재사용한다.
- dataset publication 생성 시 예약 generation을 명시적으로 넣는다. snapshot 적재/활성화 실패로 rollback되어도 registry 예약은 남는다.
- dataset lock 아래 active generation과 비교하므로 A 예약→snapshot 실패→B 활성화→A 재시도에서 A가 새 순번을 받지 않는다.
- probe가 위 경로를 추가했으며 B 유지와 A 거부를 확인한다. 실제 결과는 구현 작업의 보고이며 검토자는 원격 probe를 재실행하지 않았다.
- publication별 충돌 검증은 유지하고 snapshot_sha256의 전역 UNIQUE를 제거한다.
- reverse ETL 단위 테스트 3개 재실행 통과. 실제 JSONB payload 재해시, active/SUCCESS 동일 transaction 수정도 유지된다.

## 승인 범위와 운영 조건

registry generation은 원천 시간 순서가 아니라 승인된 게시 후보의 등록 순서다. 현재 단일 DAG가 Gold build 후 바로 후보를 등록하는 경로로 사용한다. 외부에서 미등록 과거 snapshot을 임의로 뒤늦게 제출하면 새 generation을 받으므로, 이 방식까지 자동 stale 판별이 보장되는 것은 아니다. 실행 경로를 확장할 때는 등록을 별도 build 승인 단계로 앞당기고 불변 snapshot/generation을 이후 게시에 전달해야 한다.

현재 구현은 dw_serving 스냅샷과 current view 게시 승인이다. dev.popcorn_movies의 I/U, 임베딩 트리거, 실제 사용자/관리자 API의 새 view 사용 완료까지 의미하지 않는다. 해당 서비스 연결은 별도 검토 대상으로 남는다.
