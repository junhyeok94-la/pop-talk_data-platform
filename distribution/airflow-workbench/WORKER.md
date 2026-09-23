# 별도 학습 워커

공통 Airflow 이미지를 먼저 설치합니다. 워커는 Airflow가 등록한 외부 작업을 HTTP로 받아 실행하며, 상태를 Workbench API에 기록합니다. 워커 컨테이너에 Airflow metadata DB 비밀번호를 전달하지 않습니다. 양쪽 Connection과 워커가 공유하는 토큰은 `scripts/configure.py`가 생성합니다.

## 제어 연결부터 확인

```console
docker compose exec airflow-apiserver python /opt/workbench/scripts/initialize.py --bootstrap-only --config /opt/workbench/config/worker.json
docker compose -f compose.yaml -f compose.worker.yaml build model-worker
docker compose -f compose.yaml -f compose.worker.yaml up -d model-worker
```

`model_lab_worker_diagnostic`, `model_lab_worker_llm_train`, `model_lab_worker_embedding_train` DAG가 생성됩니다. 환경·연결 탭에서 실행기 연결을 확인하세요. 실제 DAG 이름은 초기화 출력으로도 확인할 수 있습니다. `control` 이미지는 학습 라이브러리가 없어서 학습 준비 상태가 미충족으로 나오는 것이 정상입니다. GPU 없는 호스트에서는 자원 진단도 GPU 부재를 보고합니다.

HTTP 워커는 호스트에 포트를 공개하지 않고 Compose 내부 네트워크를 사용합니다. 원격 워커로 분리할 경우 Connection host와 워커의 상태 API URL을 서로 접근 가능한 주소로 바꾸고 HTTPS를 구성하세요.

## GPU 학습 이미지로 전환

NVIDIA GPU가 컨테이너에 노출되는 호스트에서 실행합니다. 메모리 요구량은 모델·학습 방식에 따라 달라집니다. 워커의 사전 점검 결과를 확인하세요.

```console
docker compose -f compose.yaml -f compose.worker.yaml -f compose.gpu.yaml build model-worker
docker compose -f compose.yaml -f compose.worker.yaml -f compose.gpu.yaml up -d model-worker
```

| 호스트 경로/볼륨 | 컨테이너 경로 | 용도 |
| --- | --- | --- |
| `data/training` | `/datasets/training` | JSONL 학습·검증 입력, 읽기 전용 |
| `models` | `/models` | 원본 모델 가중치, 읽기 전용 |
| `worker-state` 명명 볼륨 | `/state` | 작업 상태 파일·학습 결과, 쓰기 가능 |

현재 HTTP 학습은 로컬에 준비한 Hugging Face 원본 가중치와 고정 revision을 사용합니다. Ollama에 내려받은 모델만으로 학습 입력이 준비되지는 않습니다. 기본 설정은 offline이므로 가중치를 자동 다운로드하지 않습니다. Model Lab 저장소 탭에서 마운트 경로·데이터 hash·모델 revision을 점검한 뒤 학습 후보를 만드세요. `model_directory()`가 검증하는 디렉터리 형식은 `models/<모델이름의 /를 --로 바꾼 값>/<revision>`입니다.

학습이 끝나면 결과를 추론 엔진이 읽을 수 있는 형태로 준비하고 같은 평가셋으로 재평가합니다. 그 평가를 모델 버전에 연결해 검토·적용 구성을 저장합니다. 적용 구성 JSON을 내려받아도 챗봇 서비스에 자동 배포되지는 않습니다.

## 선택: Kubernetes batch

```console
docker build -f Dockerfile.worker --target batch -t airflow-workbench-worker:0.1.2-dev-batch .
```

이 이미지는 HTTP 서비스 대신 batch entrypoint를 실행합니다. 실행 환경의 Kubernetes Connection, 이미지 고정 tag/digest, service account, 객체 저장소 권한을 별도로 설정해야 합니다. 첫 배포의 기본 실행 경로는 HTTP 워커이며 Kubernetes 클러스터 설치는 이 Compose에 포함하지 않습니다.
