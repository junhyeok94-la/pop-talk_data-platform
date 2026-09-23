# DAG 개발 표준 재검토

판정: APPROVED. 이전 P2 두 건 종료. 추가 차단 사항 없음.

- §4에서 신규 작성 기본 규칙과 기존 Params/secret 공급/schedule/시각/pause 계약 보존의 우선순위를 분리했다. §13도 해당 호환성 규칙을 명시적으로 참조한다.
- §6.1에 전이적 helper 의존성, artifact digest 변경, 불변 경로/revision 보호, 진행 중 run 고정 또는 변경 차단, 새 run 전환 절차가 추가됐다.
- §5 XCom dict/직렬화/multiple_outputs 보존, §6 단일 DAG 동시성 범위와 DB transaction/commit 불확실성, §9 shell/Jinja 입력 경계, §11 구조 baseline이 보완됐다.
- 설명 기준과 변경 규모별 검증 gate는 현재 프로젝트의 단계적 리팩터링에 적용 가능하다.

이번 승인은 표준 문서에 대한 것이다. 실제 DAG 리팩터링의 동작 보존 여부는 변경 전 baseline과 변경 후 import/구조/XCom/artifact 검증으로 별도 판정한다. 이번 검토에서는 DAG 코드나 운영 데이터에 변경을 가하지 않았다.
