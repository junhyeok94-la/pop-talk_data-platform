# Airflow Workbench 테마 연동 검증

환경: 로컬 Airflow 3.3.1, 기본 사용자 외관 메뉴, 같은 출처의 플러그인 iframe.

## 반영 범위

- Dashboard Studio와 Model Lab이 Airflow의 Light / Dark / Follow System을 사용한다.
- 배경·본문·버튼·입력·상태·표·대화상자는 Airflow Chakra 공통 토큰을 사용한다.
- ECharts 축·범례·툴팁·게이지·줌 컨트롤은 테마 변경 이벤트로 갱신한다.
- SQL 편집기는 CodeMirror Compartment를 재설정한다. 편집기를 재생성하지 않는다.
- 직접 지정한 시리즈 색상과 패널 설정은 수정하지 않는다.
- 플러그인 직접 URL 및 부모 접근이 불가능한 임베딩은 시스템 색상 설정을 따른다.

## 검증 결과

- `node --test scripts/test_workbench_theme.cjs`: 5/5 통과.
  - OS보다 Airflow의 명시적 Light/Dark 선택이 우선한다.
  - 부모의 최종 테마 변경과 시스템 이벤트를 모의해 양방향 전환을 확인했다.
  - 독립 실행/부모 접근 실패 시 OS 설정 추종, 사용자 테마 토큰 변경/제거를 확인했다.
  - 차트 인스턴스·사용자 시리즈 색상·범례 선택·확대 범위를 보존했다.
- `npm run build`: 성공. 라이브러리는 정적 번들로 제공한다.
- `smoke_workbench_studio.py`: 인증, 네 개 개인 보드 실제 조회, 테마 파일 포함 정적
  리소스 제공, 기존 DAG/Pool 설정 등 9개 통합 확인 항목 통과.
- 실제 Airflow 사용자 → 외관 메뉴에서 Light/Dark/System 선택을 확인했다.
  Model Lab 검색 질문과 SQL 초안이 전환 후 유지되었으며, SQL 구문 색상도 변경되었다.
- Dashboard Studio 차트와 Model Lab 환경 편집 대화상자를 시각 검토했다.
- 검증 탭 브라우저 오류 로그: 0건.

OS 자체의 전역 테마는 변경하지 않았다. 실시간 OS 전환은 위 모의 이벤트 회귀 테스트로
확인했으며, 실제 브라우저에서는 System 선택 후 현재 OS의 Light 모드 적용을 확인했다.

검증용 SQL/패널 초안은 저장하거나 실행하지 않았다. 기존 보드 버전 v9 유지,
학습/GPU 작업 미실행. 종료 시 원래의 **시스템 설정 따르기**를 복원했다.

## 화면

- [대시보드 Light](images/workbench-dashboard-light-2026-09-10.png)
- [대시보드 Dark](images/workbench-dashboard-dark-2026-09-10.png)
- [Model Lab Light](images/workbench-model-lab-light-2026-09-10.png)
- [Model Lab Dark](images/workbench-model-lab-dark-2026-09-10.png)
- [SQL 편집기 Light](images/workbench-editor-light-2026-09-10.png)
- [SQL 편집기 Dark](images/workbench-editor-dark-2026-09-10.png)
- [환경 편집 Dark](images/workbench-dialog-dark-2026-09-10.png)

부모 문서의 테마 속성과 Chakra 토큰을 사용하는 어댑터이므로, Airflow 업그레이드 시
내장 경로 두 곳에서 위 전환 검증을 다시 수행한다.

## Light 구획 대비 후속 개선

기존 `/dags` 카드의 실제 스타일을 비교했다. Light 모드 카드 외곽선은
`border-emphasized` (`oklch(0.85 0.016 253)`), 제목 배경은 `bg-muted`,
모서리는 8px이며 그림자를 사용하지 않았다.

- 플러그인 카드와 주요 카테고리 경계에 같은 강조선 토큰을 적용했다.
- 대시보드 및 Model Lab 카드 제목 영역에 같은 회색 배경을 적용했다.
- 대시보드 카드 모서리를 8px로 맞추고 그림자를 제거했다.
- Model Lab 카테고리 탭을 Dags 상단 탭처럼 밑줄로 현재 항목을 표시하도록 조정했다.
- 폼 내부 구분선과 패널 편집기의 쿼리·시각화 영역 경계도 강조했다.
  차트 내부 눈금선 색상과 사용자 데이터 설정은 변경하지 않았다.
- CSS 형식 검사 통과, 실제 Light 대시보드·생성 모델 비교·파인튜닝 화면 및
  Dark 전환을 시각 확인했다.

[Dags 기준 화면](images/airflow-dags-light-reference-2026-09-10.png) ·
[개선한 대시보드](images/workbench-dashboard-light-sections-2026-09-10.png) ·
[개선한 Model Lab](images/workbench-model-lab-light-sections-2026-09-10.png)
