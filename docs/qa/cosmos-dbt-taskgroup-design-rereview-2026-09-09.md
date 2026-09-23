# Cosmos 설계 재검토

판정: APPROVED — 구현 착수 가능한 설계. 이전 P1 두 건과 P2 한 건은 설계 수준에서 종결한다. 설치·DAG 코드·실제 연동에 대한 승인은 별도 구현 검토 대상이다.

## 지적 사항 종결

- 테스트 완전성: AFTER_ALL을 명시해 모든 모델 뒤 전체 dbt test를 실행한다. 독립 singular test 및 다중 부모 test를 포함한 기존 43개 unique_id를 manifest/dbt ls/실제 실행 결과와 대사하고, 실패·skip 시 게시 차단을 검증하는 계획이 추가됐다.
- 그래프/실행 버전 일치: source 복사본과 manifest/descriptor를 digest별 불변 배포에 묶고, 파싱 때 선택한 경로와 digest를 실행 인자로 고정한다. runtime에서 같은 배포를 대조하므로 live source 또는 current pointer 변경과 실행 중인 배포를 분리할 수 있다.
- 실행 자원과 부분 성공: 1-slot 전용 pool, subprocess 실행, threads=2, 태스크별 임시 파일 격리 및 30분 태스크/4시간 DAG 제한을 명시했다. 전체 품질 검사 전 PostgreSQL active snapshot 유지도 검증 범위에 포함됐다.

## 구현 검토에서 확인할 해석

1. 정상적인 current A→B 전환은 기존 A run이 불변 A를 계속 사용하면 성공해도 된다. 실패 대상은 고정된 A 내용의 변조, 경로/digest 불일치 또는 A 그래프와 B 실행의 혼합이다. 설계의 ‘A parse 뒤 B 배포는 모두 실패’ 표현은 이 구분으로 해석한다.
2. identify_gold_build와 publish_postgres의 버전 재검증도 고정된 deployment의 project를 사용해야 한다. 현재 runtime의 live pipelines/dbt hash를 그대로 두면 새 배포와 기존 run을 다시 결합하게 된다. source model_version과 deployment_id는 별도 식별자로 전달하고 기존 publication identity 의미는 보존한다.
3. serialized DAG와 worker의 실제 태스크 재구성에서도 고정 배포가 유지되는지 Airflow 3.3.1에서 검증한다. 경로가 digest별이라는 사실만으로 재구성 동작까지 입증된 것은 아니다. 사용 중인 배포 디렉터리는 삭제하지 않는다.
4. ALL_SUCCESS를 전체 모델/test/게시 경로에 적용하고, pool 생성과 모든 Cosmos 태스크의 pool 지정, 임시 복사본의 실제 target/log 경로 및 subprocess 종료를 구현 증빙으로 확인한다.

이번 재검토는 변경된 설계 문서와 앞선 공식 자료 검토에 근거한다. dependency 설치, DAG 수정, 외부 DB 실행은 수행하지 않았다.
