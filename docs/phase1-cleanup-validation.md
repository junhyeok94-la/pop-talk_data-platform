# Phase 1 범위 정리 검증 — 2026-09-23

## 변경 범위

클라우드 변환·웨어하우스 DAG 4개, 노트북·bridge·exchange loader, 전용 dbt 프로젝트·실행 래퍼,
모델별 실행·배포 관리 및 그 전용 PostgreSQL 제어 스키마 생성 코드를 제거했습니다.
관련 테스트·예전 설계 및 검증 문서도 정리했습니다.

Raw 수집, 저장소와 무관한 Python 정제·영화 식별, PostgreSQL 서비스 게시 함수와 테스트는 유지합니다.
플랫폼 환경 검사 DAG는 플랫폼 DB에 읽기 전용으로 접속합니다.
PostgreSQL 분석 적재와 dbt DW/Mart는 아직 구현하지 않았습니다.

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
