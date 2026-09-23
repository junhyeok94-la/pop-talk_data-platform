# Model Lab 연결·DAG 운영 UX 및 0.1.1-dev 배포 검증

## 변경 결과

- 환경·연결 카드에 추론 연결 ID, Airflow Connection ID, 평가 DAG 링크와 파일명, 접속 정보 출처를 표시한다.
- 등록 폼에서 조회 권한이 있는 HTTP Connection을 선택한다. 환경변수 Connection도 검색 대상이다. 외부 Secrets Backend에서 공급한 ID는 직접 입력할 수 있다.
- 실제 Airflow Secrets Backend 우선순위로 출처를 판별하며 주소·비밀번호·토큰·backend 예외 내용을 목록 API에 반환하지 않는다.
- DAG의 활성·일시중지와 평가 설정 일치 여부를 구분한다. 설정 일치는 추론 서버 응답이나 모델 품질 검증을 뜻하지 않는다.
- 내 대시보드와 Model Lab 평가·학습 환경은 동일한 DAG 상태 변경 API 및 호출자 Airflow DAG PUT 권한을 사용한다. Model Lab 태그 자체로 활성화를 차단하지 않는다.
- 실제 평가·학습 요청의 준비 상태 점검은 유지한다. 기존 식별자, 사용자 역할, DB schema 변경은 없다.

## 확인 환경과 결과

- 기존 개발 설치: localhost:8080. 플러그인 bind mount와 API 서버 재시작으로 반영; 인증된 연결 목록·평가 상태 API HTTP 200 확인.
- 독립 래핑 설치: localhost:18080, `workbench-package-qa`. 실행·대기 DAG Run 0개 확인 후 공통 API 서버·scheduler·DAG processor·triggerer를 0.1.1-dev 이미지로 교체했다. DB·DAG·artifact·log 볼륨과 관리자 계정을 보존했다.
- 실제 브라우저에서 환경변수 Connection 목록, 기존 연결 선택, 새로운 추론 연결 ID에 따른 DAG/파일 미리보기와 카드 렌더링을 확인했다. 검사용 입력은 저장하지 않았다.
- Model Lab에서 `model_lab_eval_ollama`를 일시중지하고 내 대시보드에서 활성화했다. 기본 DAG 상세의 활성 체크와 Model Lab 카드에서도 결과를 확인했다. 기존의 활성 상태로 복원했다. 평가·학습 실행을 제출하지 않았다.
- 공통 회귀 검사 74개와 워커 검사 8개, 총 82개 통과. 목록 권한, 비밀값 비노출, 외부 backend 우선순위, native DAG 권한 재검사, 상태 충돌, 잘못된 Pool의 평가 제출 차단을 포함한다.
- 호스트 화면 이동 Node 검사 2개 통과. 변경한 JavaScript 5개 파일 문법 검사 통과.
- 첫 전체 검사 호출에서 worker 소스 경로를 빠뜨려 `batch_entrypoint` import 오류가 발생했다. 배포 README에 있는 `/worker` mount와 PYTHONPATH를 적용한 재실행에서 82개가 모두 통과했다.

## 빌드 산출물

| 이미지 | 로컬 image ID |
| --- | --- |
| `airflow-workbench:0.1.1-dev-airflow3.3.1` | `sha256:e15bdcd3f0f474b72d8e2ecd2e1e9e908398db79458a29f2eb3d087b8cb4cca5` |
| `airflow-workbench-worker:0.1.1-dev-control` | `sha256:c2bb026eb65134cd53587486f4c039f2d03b99c42b4862e980c1b2da7164e891` |

독립 소스 패키지는 `.local/releases/airflow-workbench-0.1.1-dev`와 같은 이름의 zip으로 제공한다. 소스 allowlist와 SHA-256 manifest를 사용한다. 이전 0.1.0-dev 패키지는 보존한다. 개발 업무 DAG, `.env`, 실제 계정과 데이터는 내보내지 않는다.

146개 소스 파일의 manifest와 zip 내부 CRC를 확인했다. 이미지 빌드 소스와 최종 패키지의 런타임 파일은 동일하며 최종 검증 문서만 갱신했다. zip 크기는 695,373 bytes, SHA-256은 `430ac8a3ebe16030a5bcaf9773bc39a12cac90eff5b1883d7e19edf71a052c1e`이다.

이번 변경은 별도 GPU training/batch 이미지 전체 빌드, 실제 GPU 학습, 외부 모델 품질 평가, 공개 저장소·레지스트리 게시를 포함하지 않는다. 브라우저 검사는 위 연결·상태 변경 흐름을 대상으로 하며 전체 앱의 종단 검사를 뜻하지 않는다.
