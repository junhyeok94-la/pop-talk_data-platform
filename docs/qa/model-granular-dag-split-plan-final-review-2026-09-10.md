# 모델 단위 DAG 분할 계획 최종 검토

판정: **APPROVED — 계획 범위**.

최신 `model-granular-dag-split-plan-2026-09-10.md` 전체를 다시 읽고 직전 두 P1 반례와 대조했다. 두 항목 모두 계획 수준에서 해결됐다. 기존 검토서의 CHANGES_REQUESTED는 이력이며 현재 미해결 차단 지적은 없다. 구현·배포·실제 데이터 검증을 완료했다는 의미는 아니다.

## 1. 미래 digest 요구와 추가 전역 장벽 해결

GenerationPlan은 source snapshot과 논리 build slot만 확정하고 신규 결과 digest를 요구하지 않는다. 모델별 result는 계산 후 추가하며, 소비 모델의 cohort는 직접 부모 result/binding이 준비된 뒤 봉인한다. 최초 plan을 수정할 필요가 없다.

구체적인 실행 순서는 `GenerationPlan → 해당 모델의 부모 cohort 확정 → 모델 실행/test → 해당 모델의 ResultManifest → 자식 cohort 확정`이다. 문서의 Plan→Result→Cohort 표기는 부모 결과가 자식 cohort로 이어지는 관계로 해석하며, 자기 결과를 자기 실행의 선행조건으로 삼지 않는다. REBUILD slot은 아직 없는 결과의 논리 참조이고, plan 생성 시 실제 result binding을 가질 수 있는 것은 REUSE slot이다.

PostgreSQL도 전역 header를 먼저 요구하지 않고 영화/박스오피스 component를 독립 적재한다. required component가 모인 뒤 publication header/junction을 조립하고 activation CAS를 수행하므로 영화 candidate 적재까지 박스오피스 digest를 기다리는 문제가 해결됐다.

## 2. carry-forward의 영향 분석과 readiness 해결

수정 계획은 source 변경의 후손을 manifest dependency graph로 추적하고, deployment/모델 계약/exact parent vector가 같을 때만 재사용한다. movie title/policy 변경 시 fact 재사용과 mart/영향받은 serving component 재계산을 명시했다.

새 generation의 논리 parent slot을 이전 immutable result에 연결하는 binding을 cohort readiness 증거로 인정한다. 따라서 재사용한 부모가 새 result event를 만들지 않아도 진행할 수 있다. 같은 generation이라는 조건은 소비 build의 plan 일치를 뜻하며, 재사용 부모의 물리 generation까지 같다는 조건이 아니다.

## 유지되는 구현·통합 검증 조건

- 최초 plan의 불변성, parent cohort→실행→result 순서, 재사용 binding과 후손 재계산을 순수 계약 테스트로 검증한다.
- g1/g2 교차 완료에도 SQL/test/export가 exact parent result만 읽는지 확인한다. append-only partition의 부분 실패 및 동시 writer가 성공 결과를 중복·변경시키지 않아야 한다.
- partition_key 실제 발행, pending ledger 등록과 event 발행 사이 실패, 누락 알림의 scan/replay 복구를 실제 Airflow에서 검증한다.
- Cosmos 단일 모델과 소유 test ID만 실행되는지, 고정 graph deployment와 generation deployment가 일치하는지 확인한다.
- 영화 branch 조기 실행, 박스오피스 지연 중 독립 component 봉인, stale generation activation 거부를 검증한다.
- production 전환은 candidate 대사와 drain/fence/watermark/backlog/bootstrap/rollback rehearsal 이후 수행한다.

이번 검토에서는 문서만 추가했다. 업무 코드 변경, DAG 실행, 클라우드 쓰기는 하지 않았다. 승인된 계획의 단계 A부터 계약과 구조 테스트 구현으로 진행할 수 있다.
