# B1-1 v4 최종 재검토

판정: **CHANGES_REQUESTED** — 기존 REUSE FK와 runtime registry INSERT 문제는 보완됐다. 다만 최초 등록 시 registry의 의존 관계를 권위 graph와 대사하지 않아 후속 작업 누락을 허용하는 문제가 남았다.

## 독립 확인 결과

- 새 Airflow 컨테이너에서 PostgreSQL 통합 검사를 활성화한 전체 suite: **131개 통과**. 정상 cross-generation REUSE 테스트도 포함된다.
- DB migration ledger와 로컬 001–004 SQL의 SHA-256: 모두 일치. 이번 검토에서 migration runner를 재실행한 것은 아니다.
- 실제 runtime 계정의 release_delivery_specs 및 release_delivery_registries INSERT 권한: 모두 false.
- v4의 generated rebuilt_result_manifest_id와 composite FK는 REBUILD에만 same-slot 제약을 적용한다. 정상 REUSE 통과는 실제 테스트로 확인했고, 잘못된 REBUILD 참조 차단은 SQL 제약을 검토했다. 별도 잘못된 참조 SQL 재현까지 실행했다고 보지는 않는다.
- 업무 구현은 변경하지 않았다. 아래 추가 반례는 메모리에서 생성했고 잘못된 registry를 DB에 등록하지 않았다.

## 1. [P1] 전체 모델 집합만 확인하고 의존 관계는 확인하지 않음

근거: `pipelines/orchestration/postgres_control_store.py:121–125`, `:333–340`.

register_delivery_registry는 deployment ID와 model ID 집합의 일치만 확인한다. 이후 create_release_delivery_registry는 graph.dependency_map 대신 model_registry.models의 direct_parent_unique_ids로 후속 recipient를 계산한다. 두 의존 관계 표현이 같은지 검증하지 않는다. registry_digest도 이 입력에서 새로 계산하므로 graph와의 불일치를 발견하지 못한다.

현재 배포 manifest로 만든 정상 registry에서 graph와 모든 모델 ID를 그대로 두고 각 ModelDagSpec의 direct_parent_unique_ids만 빈 tuple로 바꿨다.

```python
bad = replace(
    registry,
    models=tuple(replace(spec, direct_parent_unique_ids=()) for spec in registry.models),
)
delivery = create_release_delivery_registry(bad)
```

독립 실행 결과, 등록 함수의 deployment/model 집합 조건은 통과했고 생성된 전체 recipient 수는 **0**이었다. DB 최초 등록까지 실행한 재현은 아니지만, 등록 코드의 후속 검사는 자체 생성 digest·spec 형식·기존 등록과의 일치만 확인하여 이 graph 불일치를 거부하지 않는다. 기존 정상 registry가 이미 있으면 불변성 검사로 막히지만 새 deployment 최초 등록은 보호하지 못한다.

배포 writer에서 모델 spec을 잘못 조립하거나 의존 관계가 누락된 객체를 넘기면, 유효한 graph_digest를 가진 registry에 후속 모델 전달이 빠진다. 이후 result commit은 이 registry를 권위로 사용하므로 필요한 cohort 준비 작업이 생성되지 않는다. runtime INSERT 제한은 적절한 개선이지만 배포 입력의 의미 검증을 대신하지 않는다.

수정 방향: 검증된 graph.dependency_map에서 direct child를 직접 계산하거나, 모든 ModelDagSpec의 부모 집합을 graph와 정확히 대사한 후 계산한다. 모델 spec 중복도 거부하고 graph 자체의 digest/배포 연결은 기존 권위 검증 경로로 확인한다. terminal publication 매핑은 별도 승인 계약에서 공급하는 경계를 유지한다.

필수 회귀: 모델 ID는 모두 유지한 상태에서 부모 전체 삭제, 일부 edge 삭제/추가를 각각 거부하거나 권위 graph에서 정상 recipient로 복원해야 한다. 기존 subset/empty model 검사와 별개로 정상 diamond graph의 exact direct recipient 집합도 검증한다.

## 범위와 결론

v4의 조건부 FK, 배포 writer와 runtime 권한 분리, registry 본문/digest 및 spec FK 보완은 수용한다. 위 의존 관계 대사 보완 후 B1-1 승인 여부를 재확인한다. 실제 dbt invocation 증명과 Airflow Asset 전달 전체 검증은 예정된 B1-2/B1-3 범위이며 이번 테스트 통과로 완료됐다고 간주하지 않는다.
