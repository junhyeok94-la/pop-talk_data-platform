# Airflow Workbench 0.1.2-dev 배포 마무리

## 결과

기존 UI 개선을 유지하고 설치·배포 도구와 사용자 가이드를 보완했다. GitHub 저장소, 이미지 레지스트리, 작성 코드의 공개 라이선스는 사용자가 아직 정하지 않았으므로 로컬 패키지와 이미지까지 준비했다.

- `verify_installation.py --existing`: 프로젝트가 있거나 DAG를 활성화한 기존 설치를 변경 없이 검사한다. 등록된 평가 DAG·Pool 설정과 Connection 목록도 확인한다. `--repeat-init`과 함께 사용할 수 없다.
- `verify_source.py`: Docker·Airflow 없이 150개 파일의 SHA-256과 누락·변경을 검사한다. 설치 후 추가된 `.env`·데이터 파일은 검사 대상이 아니다.
- `export_airflow_workbench.py --archive`: 명시한 배포 템플릿만 내보내며 소스 디렉터리, 재현 가능한 ZIP, 체크섬을 한 번에 만든다. 기존 결과를 덮어쓰지 않는다.
- `WALKTHROUGH.md`: 권한, 연결·DAG 관계, 기준선·후보 평가, 검토와 적용 구성, 별도 파인튜닝 경로를 설명한다.

## 검증

- 새 공통 이미지에서 공통 74개 + 배포 도구 9개 + 워커 8개, 총 91개 검사 통과.
- 개발 저장소 export 검사 3개 통과. 점이 포함된 버전명 보존, 동일 ZIP 재생성, 기존 ZIP 보존, 미지정 템플릿 파일 제외 확인.
- 실행·대기 DAG Run과 진행 중 워커 작업 모두 0개를 확인한 뒤 localhost:18080의 `workbench-package-qa` 설치를 업그레이드했다.
- API 서버·scheduler·DAG processor·triggerer·제어 워커 모두 0.1.2-dev 사용. API 서버 healthy.
- `--existing --worker`를 실행하고 전후 DB 레코드, 관리자 비밀번호 hash, DAG 활성 상태가 동일함을 확인했다. 별도 워커 무인증 401 및 인증된 상태 API 왕복 성공.
- 최종 ZIP의 CRC, 파일별 manifest와 SHA-256 체크섬을 확인했다. 이미지 빌드 소스와 비교하여 런타임 파일은 동일하고 최종 검증 문서만 변경되었음을 확인했다.
- 이번 변경은 플러그인 화면이나 평가 실행 코드를 바꾸지 않으므로 기존 개발 서버 localhost:8080 재시작은 수행하지 않았다.

## 산출물

- 소스: `.local/releases/airflow-workbench-0.1.2-dev`
- ZIP: `.local/releases/airflow-workbench-0.1.2-dev.zip` (704,093 bytes)
- SHA-256: `9cc6d8a378296c2cae76b821348e4065f46f73aee6313b7e54abab624e85250f`
- 공통 이미지: `airflow-workbench:0.1.2-dev-airflow3.3.1`, image ID `sha256:d70e6a9794b4dc9d8b6ed148a016b710161af4c4497b94d85a252710a458b4ad`
- 제어 워커: `airflow-workbench-worker:0.1.2-dev-control`, image ID `sha256:b4f21052d659abef7b2b151094797b9db72d03212b8e6719a1bca9be1aadac44`

이전 배포 파일과 설치 계정·비밀번호를 보존했다. 공개 게시와 실제 GPU training/batch 이미지 빌드·학습 실행은 이번 검증에 포함하지 않았다. 설치·평가 수동 흐름은 가이드로 제공하며 사용자의 실제 평가를 자동 재실행하지 않았다.
