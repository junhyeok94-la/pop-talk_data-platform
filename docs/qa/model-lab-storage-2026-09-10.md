# Model Lab 프로젝트 저장소 검증 · 2026-09-10

## 변경 범위

- 프로젝트별 저장소 메뉴와 이름·실행 환경·데이터/기본 모델/결과 루트·기본 모델 입력값 설정.
- 기존 Airflow PostgreSQL의 `workbench.lab_records` 사용. 별도 SQLite/DB 계정/자격 증명 생성 없음.
- manager 설정 저장, developer 접근 확인·학습 실행, viewer 조회. 변경 시 optimistic version 검사.
- 학습 화면에서 같은 실행 환경의 프로필 선택, 최종 파일 경로 미리보기, draft/preset 연동.
- 레시피·DAG 입력·Run에 프로필과 경로 snapshot 저장. 이미 제출된 요청의 재조회·재전송은 저장된 snapshot 사용.
- 외부 HTTP 워커가 허용한 마운트 루트만 선택. 입력 디렉터리 읽기·결과 경로 임시 쓰기·여유 공간 확인.
- HTTP 학습 코드가 선택한 입력·모델 루트를 사용하고 별도 결과 루트에 실행별 모델·지표·receipt 기록.
- Kubernetes 프로필은 데이터·결과 객체 저장소 prefix와 HF 모델 ID/revision을 사용. 원격 접근은 Pod 실행 시 확인한다고 명시.

## 자동 검증

관련 Python suite **63개 통과**, 기존 테마 JavaScript suite **5개 통과**. 추가 저장소 테스트는 11개다.

```powershell
docker compose cp orchestration/model_worker/. airflow-apiserver:/tmp/storage-worker
docker compose exec -T -e PYTHONPATH=/tmp/storage-worker:/opt/airflow/plugins:/opt/pop-talk-dw/scripts airflow-apiserver python -m unittest test_model_lab_storage test_model_lab_workflow test_airflow_workbench test_workbench_portable test_workbench_batch test_executor_state
node --test scripts/test_workbench_theme.cjs
```

격리 PostgreSQL에서 프로젝트 권한·충돌·다른 Connection 선택·실행 후 프로필 수정·같은 요청 재사용을 검사했다. 임시 파일 시스템에서 허용 경로·symlink 탈출 거부·원본/결과 분리·데이터 사본 checksum·실제 선택한 모델 경로와 결과 디스크 검사를 확인했다. 실제 trainer 함수의 파일 입출력은 CPU용 모의 학습 라이브러리를 사용해 `model`, `metrics.jsonl`, `receipt.json`이 선택한 결과 디렉터리에 작성되는지 검사했다. 이는 GPU 학습 실행이나 품질 검증이 아니다.

## 실제 로컬 확인

Airflow 계정으로 `챗봇 모델 개발` 프로젝트에 화면을 통해 `local-training` 프로필을 저장했다. 실행 환경은 `local-gpu`이며 워커 API에서 실제 기본 경로를 불러왔다.

| 목적 | 워커 내부 경로 | 결과 |
|---|---|---|
| 학습·검증 데이터 | `/datasets/training` | 디렉터리 읽기 가능 |
| 원본 모델 | `/models` | 디렉터리 읽기 가능 |
| 학습 결과 | `/state/artifacts` | 기존 상위 폴더 쓰기 가능; 실행 시 하위 폴더 생성 |

이 프로필은 경로를 등록한 상태다. HF 원본 가중치·고정 revision·학습 데이터·학습 이미지와 RAM의 준비 완료를 의미하지 않는다. 기존 Ollama 모델을 HF 학습 가중치로 등록하지 않았다.

Airflow 기본 외관 메뉴에서 Light/Dark를 바꿔 저장소 화면의 배경·표·구분선이 함께 바뀌는 것을 확인했다.
학습 화면에서 저장한 프로필을 선택하여 최종 경로가 표시되는 것을 확인했고, 평가 화면으로 이동 후 다시 학습 화면에 들어왔을 때 선택이 복원되었다. 검증 후 Airflow 외관은 시스템 설정 따르기로 되돌렸다.

- [Light 화면](images/model-lab-storage-light-2026-09-10.png)
- [Dark 화면](images/model-lab-storage-dark-2026-09-10.png)

## 한계

실제 GPU 파인튜닝, 원격 S3/GCS 접근, Kubernetes 실행을 이번 변경에서 수행하지 않았다. 객체 저장소에서 임의 모델 디렉터리를 내려받는 기능도 제공하지 않는다. 대용량 파일 업로드·자동 마운트·버킷/IAM 생성은 이 메뉴의 기능이 아니다. 학습 후 모델 변환·배포·재색인·롤백을 연결하는 기존 잔여 개발 범위는 유지된다.

[설정과 사용 방법](../../orchestration/airflow/plugins/MODEL_LAB.md)
