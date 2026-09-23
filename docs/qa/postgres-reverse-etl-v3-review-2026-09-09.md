# Reverse ETL v3 재검토

판정: 승인 보류. P1 1건 유지.

## 개선 확인

publication ID에 model_version을 추가했고 DAG가 dbt 전/게시 전 소스 hash를 비교한다. 실제 저장 JSONB payload를 읽어 전체 입력과 같은 canonical hash로 대사하므로 이전 저장 hash 컬럼만 비교하던 문제는 해결됐다. active/attempt SUCCESS 동일 transaction도 유지된다. 단위 테스트 3개 재실행 통과.

## P1 — generation이 빌드 순서가 아닌 최초 도착 순서

위치: `publish_to_postgres_v3.py:79-86`.

publication이 처음 저장될 때 sequence를 할당하므로 다음 경로가 허용된다: 오래된 Gold A를 추출했지만 PG 게시 전에 지연/실패 → 더 최신 Gold B 최초 게시(gen N) → 아직 dataset_publications에 없는 A 최초 게시(gen N+1). A는 stale 비교를 통과해 B를 밀어낸다. advisory lock은 이 순서를 직렬화할 뿐 최신성을 판단하지 않는다.

현재 probe는 A를 먼저 성공시켜 generation을 확보한 뒤 B→A 재실행을 검사한다. 위 'A 최초 게시 지연'이나 A가 활성화 전 rollback되어 publication 행이 없는 경우를 검증하지 않는다. 기존 publication replay 역행만 막힌 상태다.

권고: 승인된 Gold build/snapshot의 순서를 게시 쓰기보다 앞에서 고정하고, 실패해도 보존되는 build registry의 generation을 publication에 전달한다. 빌드 ID와 입력 cutoff/모델 버전/고정 출력 snapshot을 결합하고 이후 PG 게시 시 sequence를 새로 매기지 않는다. 별도 명시적 publication revision을 사용할 수도 있으나 뒤늦은 재시도가 임의로 더 높은 번호를 받지 않도록 해야 한다.

필수 검증: A generation 예약/Gold 출력 확보 → A PG 게시 전 실패 → B 게시 → A 최초 PG 삽입 시도. B active가 유지되어야 한다. 별도로 이미 저장된 A replay 차단 테스트도 유지한다.

## 범위

검토자는 원격 서비스 DB를 변경하지 않고 코드·probe·단위 테스트를 확인했다. 단일 DAG 직렬 실행을 사용하더라도 함수를 직접 호출하거나 실패한 과거 빌드의 게시를 복구할 때 잘못된 활성화를 막으려면 build generation 계약이 필요하다. 서비스 소유 데이터 미변경과 이전 단계 승인은 유지한다.
