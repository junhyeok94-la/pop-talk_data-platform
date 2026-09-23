# Phase 1 범위 정리 검증 — 2026-09-23

최신 변경: `pipeline_runs`를 제거하고 기존 영화 5,985건의 실제 초기 적재를 완료했습니다.
아래 빈 데이터 검증은 초기 구현 시점의 기록이며, 현재 건수·실행 결과는 [최초 적재 결과](phase1-pipeline.md#최초-데이터-적재-결과--2026-09-23)를 참고합니다.

아래 첫 절은 클라우드 코드 정리 직후 기록입니다. 이후 분석 구현·최종 검증은 하단의 **Phase 1 분석 파이프라인 추가 검증**을 참고합니다.

## 변경 범위

클라우드 변환·웨어하우스 DAG 4개, 노트북·bridge·exchange loader, 전용 dbt 프로젝트·실행 래퍼,
모델별 실행·배포 관리 및 그 전용 PostgreSQL 제어 스키마 생성 코드를 제거했습니다.
관련 테스트·예전 설계 및 검증 문서도 정리했습니다.

Raw 수집, 저장소와 무관한 Python 정제·영화 식별, PostgreSQL 서비스 게시 함수와 테스트는 유지합니다.
플랫폼 환경 검사 DAG는 플랫폼 DB에 읽기 전용으로 접속합니다.
이 시점에는 PostgreSQL 분석 적재와 dbt DW/Mart를 아직 구현하지 않았습니다. 이후 구현을 완료했습니다.

## 검사 결과

- 기본 이미지의 클라우드 provider·SDK와 전용 dbt 런타임을 제거한 Airflow 이미지 빌드 성공.
- 패키지 부재 검사 및 `pip check` 통과.
- 기본 Compose와 모델 워커 선택 구성 검사 통과.
- 파이프라인 검사: 69개 실행, 66개 통과, 서비스 DB 통합 검사 3개 skip.
- 새 이미지에서 DAG 9개 import 및 구조 계약 검사 통과.
- 플랫폼 환경 점검 함수 실행: `database=pop_talk_platform, user=airflow_user` 확인.
- 실행 이력이 0건인 삭제 대상 DAG 4개의 메타데이터와 전용 실행 Pool 정리 완료.
- 최종 이미지 재기동 후 metadata DB·scheduler·triggerer·DAG processor 모두 healthy.
  메타데이터의 DAG 9개, import 오류 0개 확인.

실제 원본 API·S3 작업, 서비스 DB 게시, GPU 학습은 실행하지 않았습니다.
기존 DB 볼륨과 데이터는 유지합니다. 사용 중이던 클라우드 리소스나 계정은 변경하지 않습니다.

## 후속 정리: 모델 실험 DAG 제거

기본 등록 파일 2개에서 생성되던 GPU 진단·임베딩 학습·LLM 학습·Ollama 평가 DAG 4개를 제거했습니다.
실행 이력이 없음을 확인한 뒤 Airflow 목록의 메타데이터도 정리했습니다.
이 정리 시점의 DAG는 원본 수집 2개·환경/S3 점검 2개·Workbench 재시도 검증 1개로 총 5개였으며,
변경된 목록의 DAG import·구조 계약 검사가 통과했습니다.

## Phase 1 분석 파이프라인 추가 검증 — 2026-09-23

- 구형 `dw_control` 17개 테이블·3개 시퀀스: 백업 후 행 수를 재검사하고 RESTRICT 삭제 완료.
- 실제 플랫폼: ops 3개·stg 3개·dw 5개 테이블, mart 3개 뷰. 원천 업무 적재 0건.
- Airflow 시스템 71개, Workbench 13개 및 migration 이력 5건 보존.
- 파이프라인 unittest 80개: 일반 검사 70개 통과, 격리 DB 전제인 10개는 일반 실행에서 skip.
  이 10개는 별도로 모두 실행해 통과: 신규 Phase 1 7개, 실제 애플리케이션 DDL 기반 서비스 게시 3개.
- dbt: 실제 플랫폼의 빈 STG에서 모델 8개·품질 검사 19개, 총 27개 통과.
- DAG: 최종 7개 import·구조·직렬화 계약 통과. 문자열 URI 형식 Asset 이벤트와 초기 SUCCESS Params 소비 확인.
- 재기동 후 실제 API: Workbench 대시보드·대시보드 설정 조회·스냅샷 HTTP 200, 스냅샷 오류 없음.
  Airflow DAG import 오류 0건. 신규 DAG 2개 일시정지 확인.
- 실행 계정 `platform_pipeline`으로 DB 접근 확인, STG UPDATE 권한 없음 확인.
- 검사에 사용한 일회용 DB는 삭제했습니다. 운영 서비스 DB와 원천 API는 실행하지 않았습니다.

실행·복구·남은 운영 연결은 [Phase 1 실행 안내](phase1-pipeline.md)를 참고합니다.
