# Model Lab 모델 개발 작업 안내

Model Lab은 Airflow 안에서 프로젝트별 평가·실험·외부 학습·모델 검토를 연결하는 작업 공간입니다. 메타데이터는 Airflow PostgreSQL을 재사용합니다. 별도 메타 DB Connection이나 워커 DB 계정은 필요하지 않습니다.

## 지금 시작하기

1. **Model Lab → 프로젝트 → 챗봇 모델 개발**을 선택합니다.
2. **데이터·평가셋**에서 `질문 해석 Golden QA · 검토 초안` 또는 `고정 근거 답변 QA · 가상 자료`를 확인합니다. 16개·6개 사례의 `reference`는 사람 검토용이며 모델 입력으로 전달하지 않습니다. 모두 검토 전 초안입니다.
3. **실험**에서 `질문 해석 모델 기준선` 또는 `근거 기반 답변 모델 비교`를 선택합니다. 대응하는 평가셋, 로컬 Ollama, `qwen3:8b`를 선택합니다. 질문 해석에서는 JSON 출력 모드와 충분한 생성 토큰을 설정하세요. 제공 기본 프롬프트는 모델 단독 개발 평가용이며 실제 LangGraph 실행과 동일하다고 간주하지 않습니다.
4. **평가 DAG 실행**을 누릅니다. 실행 메뉴에서 진행 상황·DAG·사례별 응답을 봅니다. 브라우저를 닫아도 Airflow 실행은 계속됩니다.
5. 각 사례를 열어 정답 조건과 응답을 대조하고 통과/부분 통과/실패와 사유를 기록합니다. 만족스러운 첫 실행을 **기준선으로 지정**합니다.
6. **설정으로 다음 후보 평가**에서 모델·프롬프트·파라미터를 바꿉니다. 같은 평가셋으로 완료하면 **기준선과 비교**할 수 있습니다.
7. 학습이 필요하면 **저장소**에서 데이터·기본 모델·결과 경로를 등록하고 접근을 확인합니다. 같은 실험의 **학습 후보 만들기**에서 실행 환경과 저장소 프로필을 선택합니다. 고정 모델 revision·train/validation·파라미터를 검증한 뒤 외부 학습 DAG를 요청합니다.
8. 학습한 모델은 목표 추론 엔진에서 로드 가능한 형식으로 준비합니다. 해당 추론 연결에서 후보를 평가하고 완료된 평가를 **모델 버전 등록**으로 연결합니다. 같은 실험의 학습 실행도 참조할 수 있습니다.
9. **모델·적용 구성**에서 평가 근거를 검토·승인하고 적용 대상을 지정해 구성 JSON을 내보냅니다. 이전 구성도 다시 내려받을 수 있습니다.

`Model Lab 동작 검증` 프로젝트의 작은 질문/문서와 결과는 기능 검증용입니다. 챗봇의 실제 품질 벤치마크가 아닙니다.

## 권한과 프로젝트 공개 범위

권한은 세 단계에서 함께 검사합니다.

| 단계 | 검사하는 권한 |
|---|---|
| 메뉴·API 진입 | `menu access on Plugins`, `can read on Plugins`. 현재 Workbench 메뉴들은 이 공통 권한을 사용하며 별도 Model Lab 메뉴 리소스는 없습니다. |
| 프로젝트 작업 | 소유자 또는 프로젝트 구성원의 `viewer` / `developer` / `manager`. 소유자는 manager로 취급합니다. |
| 실제 DAG 실행 | Airflow의 대상 DAG `can_edit`와 DAG Runs `can_create`. 프로젝트 역할만으로 실행 권한이 생기지는 않습니다. |

| Airflow 역할 조합 | 현재 Model Lab에서의 효과 |
|---|---|
| Viewer + WorkbenchAccess | 자신이 만들었거나 공유받은 프로젝트 접근. 소유 프로젝트의 실험·평가셋 작성 가능. 기본적으로 DAG 실행 불가. |
| User + WorkbenchAccess | 자신 또는 공유 프로젝트에서 프로젝트 역할이 허용하는 평가 실행 가능. Airflow에서는 다른 DAG의 수정·삭제 권한도 포함. |
| Op | Plugins 접근 포함. 현재 플러그인이 Configurations 조회 권한을 Workbench 관리자 조건으로 사용하므로 모든 프로젝트와 전역 연결·환경 관리 가능. |
| Admin | Op의 권한과 Airflow 사용자·역할 관리 포함. |

Airflow Viewer와 프로젝트의 `viewer`는 서로 다른 역할입니다. 현재 플러그인 접근 사용자는 프로젝트를 만들 수 있고, 만든 사람은 해당 프로젝트의 소유자가 됩니다. Airflow Viewer라고 해서 자신이 만든 프로젝트에서도 읽기만 가능해지는 것은 아닙니다. 프로젝트의 `viewer` 구성원은 그 프로젝트의 기록을 조회합니다. `developer`는 평가셋·실험·실행·모델 등록 작업을 하고, `manager`는 구성원·저장소·검토/적용 구성 관리도 수행합니다.

관리자는 모든 프로젝트를 봅니다. 일반 사용자는 자신이 소유하거나 `members`에 등록된 프로젝트만 봅니다. 프로젝트 개수 제한 때문에 하나만 보이는 것이 아닙니다. 예를 들어 현재 사용자 ID `2`는 `tester`의 소유자이며, `chatbot-development`와 `model-lab-verification`의 구성원은 아니므로 `tester`만 표시됩니다. User나 ModelLabRunner 역할을 추가해도 다른 프로젝트에 자동 가입되지는 않습니다.
면 해당 프로
다른 프로젝트를 공유하려젝트의 **환경·연결 → 프로젝트 접근 권한**에서 구성원을 설정합니다. 이 JSON의 키는 로그인 이름이 아닌 **Airflow 사용자 ID**입니다. 예를 들어 기존 구성원을 유지한 채 `"2": "developer"`를 추가하면 사용자 ID 2가 그 프로젝트의 실험을 개발할 수 있습니다. 실제 DAG 실행에는 위 Airflow 실행 권한도 필요합니다.

### 프로젝트 만들기의 ID

프로젝트 ID는 같은 Model Lab 설치 내에서 프로젝트를 구별하는 고유 키입니다. 예를 들어 이름이 `로컬 LLM모델 테스트 프로젝트 1`, ID가 `tester`이면 API 경로는 `/workbench/api/lab/projects/tester/...`이고 평가셋·실험·실행 기록이 이 ID에 연결됩니다. 사용자 ID·모델 ID·DAG ID가 아닙니다.

ID는 영문 대소문자·숫자·`-`·`_`로 1~80자를 사용합니다. 표시 이름은 한글로 정할 수 있습니다. 이름과 달리 ID를 바꾸는 프로젝트 이름 변경 기능은 없으며, 다른 ID로 생성하면 별도 프로젝트가 됩니다. 다른 계정의 프로젝트를 포함해 기존 ID와 중복해서 새 프로젝트를 만들 수 없습니다.

### 현재 로컬 평가 테스트 계정

2026-09-10 사용자 요청에 따라 `pop-talk-viewer` 계정에 `ModelLabRunner`를 추가했습니다. 현재 조합은 `Viewer + WorkbenchAccess + ModelLabRunner`입니다. ModelLabRunner의 실행 권한은 아래 두 개이며 Airflow가 기본 `can read on Website`도 포함시킵니다.

```text
can edit on DAG:model_lab_eval_default_ollama
can create on DAG Runs
```

대상 DAG의 edit 권한은 native Airflow의 해당 DAG 활성화·일시중지 권한도 포함합니다. 전역 DAG edit, DAG/실행 삭제, 다른 진단·학습 DAG의 실행 권한은 추가하지 않았습니다. 실제 인증 관리자 검사에서 로컬 평가 DAG 실행 허용, `pop_talk_model_lab_diagnostic` 실행 거부, 프로젝트 조회 범위 유지가 확인됐습니다. 새 추론 연결이나 학습 DAG를 허용하려면 해당 DAG를 역할에 명시적으로 추가합니다. 역할은 PostgreSQL에 저장되므로 컨테이너 재시작 후에도 유지됩니다. 화면에서 변경을 읽으려면 새로고침합니다.

기본 역할의 세부 권한은 [Airflow FAB 공식 권한 안내](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/access-control.html)를 참고합니다. 위 표의 프로젝트 공개 범위와 관리자 판정은 현재 Workbench 구현 기준입니다.

## 화면 구성

평가 실행에는 **프로젝트 developer/manager(또는 소유자)** 권한과 **Airflow의 대상 DAG 실행 권한**이 모두 필요합니다. 프로젝트를 만든 Airflow Viewer도 프로젝트 데이터·실험은 저장할 수 있지만 DAG를 실행할 수는 없습니다. `WorkbenchAccess`는 플러그인 진입 권한만 제공합니다.

FAB 인증 관리자에서는 실행 계정에 `User + WorkbenchAccess`를 부여하거나, 제한된 별도 역할에 `can edit on DAG:<평가 DAG ID>`와 `can create on DAG Runs`를 부여합니다. 기존 Viewer의 DAG·실행·Task Instance·Pool 조회 권한은 유지합니다. 예를 들어 로컬 Ollama 연결의 DAG ID가 `model_lab_eval_default_ollama`이면 해당 DAG에만 edit을 허용할 수 있습니다. `User`는 다른 DAG의 수정·삭제 권한도 포함하므로 평가만 허용하려면 별도 역할을 사용합니다. 기본 Viewer 역할은 수정하지 않습니다.

실험 화면은 선택한 추론 연결의 DAG 실행 권한을 확인해 버튼 옆에 차단 이유를 보여주며, 서버도 제출 전에 다시 검사합니다. 새 제출이 Airflow에서 401/403으로 거부되면 **제출 거부**로 기록합니다. 통신 장애로 생성 여부가 불명확한 요청은 **제출 확인 필요**를 유지합니다. 권한을 변경한 뒤 다시 로그인하고 같은 입력·요청 ID로 재시도하면 저장된 입력을 사용합니다.

| 메뉴 | 하는 일 |
|---|---|
| 개요 | 프로젝트 준비 상태와 다음 행동, 최근 실행 |
| 데이터·평가셋 | JSON 가져오기·예제·질문 한 개로 만들기, 내용 checksum이 있는 새 버전 등록 |
| 실험 | 개선 목적·기본 프롬프트, 기준선·후보 평가, 학습 레시피 연결 |
| 실행 | 생성·임베딩·에이전트 평가와 학습 이력, 진행·사례·사람 검토·비교·DAG 링크 |
| 모델·적용 구성 | 평가한 모델 버전, 품질 검토, 적용 구성과 이전 구성 내보내기 |
| 저장소 | 프로젝트·실행 환경별 데이터/모델/결과 루트, 기본 모델 입력값, 워커 접근 확인 |
| 환경·연결 | 추론 제공자와 Connection, 평가 DAG·Pool, 기존 HTTP/Kubernetes 학습 환경, 프로젝트 권한 |

평가·학습 초안은 입력 후 잠시 뒤 PostgreSQL에 계정별로 저장됩니다. 프로젝트별 학습 프리셋은 프로젝트 구성원과 공유합니다. 이전 공용 실험은 환경·연결의 이전 기록 조회로 접근하며 신규 프로젝트에 소유권을 임의 배정하지 않습니다.

## 프로젝트 저장소 설정

1. **저장소 → 저장소 프로필 등록**에서 이름과 실행 환경을 선택합니다. 프로젝트 manager가 설정을 저장하고 developer는 접근 확인과 실행에 사용할 수 있습니다.
2. HTTP 실행 환경은 **실행기 경로 설정 불러오기**로 실제 워커의 기본 경로와 허용 루트를 확인합니다. 해당 루트 또는 하위 폴더를 지정합니다. 화면의 `/models`는 워커 내부 경로입니다. Windows의 `D:`/`E:` 디렉터리를 쓰려면 운영자가 그 디렉터리를 워커에 먼저 마운트합니다.
3. 데이터 루트에는 분리된 train/validation JSONL을 준비합니다. 원본 모델은 `{모델 루트}/{모델 ID에서 /를 --로 치환}/{40자리 revision}` 구조로 배치합니다. LoRA adapter나 Ollama 양자화 파일을 HF 원본 가중치로 간주하지 않습니다.
4. 결과 루트는 원본 데이터·모델과 분리합니다. 학습 시 `{결과 루트}/{실행 ID}/model`에 모델을 저장하고 같은 실행 폴더에 `metrics.jsonl`, `receipt.json`을 기록합니다. HTTP 제어 로그·입력 사본은 기존 워커 상태 볼륨에 유지됩니다.
5. 선택 사항으로 기본 모델 ID와 revision을 저장합니다. 생성·임베딩 모델용 프로필을 각각 만들 수 있습니다. 모델 ID만 지정하면 선택 시 이전 revision을 지우고 새 revision을 입력하게 합니다.
6. **프로필 저장 → 접근 확인**을 진행합니다. HTTP는 실제 워커에서 입력 디렉터리 읽기, 결과 경로의 쓰기 가능 여부(임시 파일 생성·삭제), 여유 공간을 검사합니다. 경로가 존재하지 않는 결과 폴더는 가장 가까운 기존 상위 폴더에서 검사하며 실제 폴더는 학습 실행 때 생성합니다. 개별 가중치와 데이터 내용 검사는 학습 레시피의 **설정 검증** 단계에서 수행합니다.
7. **실험 → 학습 후보 만들기**에서 저장소 프로필을 선택하면 입력·출력의 최종 경로를 확인할 수 있습니다. 프로필 버전과 경로를 레시피, DAG 입력, 실행 기록에 고정하므로 이후 프로필 수정이 이미 제출한 작업의 경로를 바꾸지 않습니다. 저장된 초안·프리셋보다 프로필이 새 버전이면 다시 선택하고 검증해야 합니다.

Kubernetes 프로필은 데이터·결과의 객체 저장소 URI를 설정합니다. 기본 모델은 모델 ID와 고정 revision으로 Hugging Face에서 학습 Pod에 내려받는 현재 참조 실행기 계약을 따릅니다. 임의 모델 버킷에서 가중치를 가져오는 기능은 아직 없습니다. URI에는 비밀번호나 서명 토큰을 넣지 않습니다. 실제 저장소 권한은 Pod의 IAM/Service Account에서 부여하고 사용하는 URI 드라이버를 학습 이미지에 설치해야 합니다. 현재 batch 이미지는 S3·GCS 드라이버를 포함합니다. 이 프로필의 접근 확인은 **실행 시 확인 필요**로 표시합니다. URI 형식 검사만으로 원격 읽기·쓰기 성공을 표시하지 않습니다.

저장소 프로필 자체는 기존 `workbench.lab_records`에 저장하며 데이터·모델 바이너리는 PostgreSQL에 넣지 않습니다. 추론 서버가 제공하는 모델 ID와 학습용 원본 모델 저장 위치는 별개입니다. 평가 상세 artifact 공유 경로는 Airflow 배포 설정을 유지합니다.

HTTP 참조 실행기의 관리 설정 예시:

```json
{
  "data_root": ["/datasets/training", "/mnt/team-data"],
  "model_root": ["/models", "/mnt/model-cache"],
  "artifact_root": ["/state/artifacts", "/mnt/model-results"]
}
```

위 JSON을 워커 환경변수 `MODEL_WORKER_STORAGE_ROOTS`로 지정합니다. 실제 볼륨 마운트·파일 권한도 함께 준비해야 합니다. 미지정 시 `MODEL_WORKER_DATA_DIR`, `MODEL_WORKER_WEIGHTS_DIR`, `MODEL_WORKER_ARTIFACT_DIR`(기본값 `{MODEL_WORKER_STATE_DIR}/artifacts`)을 각각 허용 루트로 사용합니다. 웹 화면에서 이 허용 범위를 넓히거나 OS 경로를 마운트하지 않습니다. 별도 DB 계정·자격 증명을 만들지 않고 기존 워커 Connection을 사용합니다.

기존 HTTP Job API 구현을 연결하는 경우 `/storage`·`/storage/check`와 레시피의 `storage` 계약을 지원해야 합니다. 업데이트 전 실행기는 프로필 접근 확인이 실패하므로 업데이트 후 사용하세요. `storage`가 없는 기존 레시피는 기존 기본 경로 방식을 유지합니다.

## 연결 등록

자격 증명은 Airflow Connections 또는 기존 secrets backend에서 관리합니다. 플러그인에는 Connection ID만 저장합니다. Connection host에는 자격 증명 없는 HTTP(S) base URL, password에는 필요한 API key/bearer 값을 넣습니다.

| 제공자 | host 예 | 기능·주의점 |
|---|---|---|
| Ollama | `http://inference.internal:11434` | 생성·임베딩, 모델 목록과 digest. 현재 로컬 설정은 `default-ollama`로 재사용 |
| OpenAI 호환 | `https://inference.example/v1` | chat/completions·embeddings·models 계약. 호환 서버가 지원하는 모델/옵션만 사용 |
| Gemini | `https://generativelanguage.googleapis.com/v1beta` | generateContent 기반 생성. 모델 이름은 연결에 직접 등록. API key는 password에 저장 |
| 평가 API | `https://evaluation.internal/api` | 프로젝트의 에이전트/검색 평가 어댑터. 아래 계약 필요 |

평가 API는 `POST /evaluate`를 구현합니다. 입력은 `{contract_version:1, model, input, system, parameters, read_only:true}`, 응답은 `{output:"문자열 응답", trace:{...}, tokens:null}`입니다. `read_only`를 실제로 보장하는 격리된 평가 서버를 연결해야 합니다. 이 플래그만으로 기존 에이전트의 쓰기 도구가 자동 차단되는 것은 아닙니다.

현재 제공자별 공통 파라미터는 temperature와 최대 생성 토큰입니다. JSON 출력 모드를 선택할 수 있습니다. 고급 옵션은 제공자별 허용 목록과 범위를 검사합니다. Ollama는 기존 top-p/top-k/context/repeat penalty/seed 설정을 지원합니다. 제공자가 특정 모델에서 옵션을 거부하면 해당 실행에서 오류를 확인할 수 있습니다. Gemini/OpenAI 호환 계약은 모의 응답으로 검증했고 실제 원격 계정에서의 호출은 별도 확인이 필요합니다.

연결 저장 후 **DAG 준비 → DAG processor 반영 확인·활성화**를 진행합니다. 갱신 주기는 설치의 bundle 설정을 따르며 현재 로컬 기본값은 300초입니다. 읽기 전용/Git 배포 환경은 `/workbench/api/lab/endpoints/{id}/bundle`의 Python bundle을 배포합니다. 기존 Pool의 용량을 자동 덮어쓰지 않습니다.

## 실행과 저장

| 대상 | 실행·저장 위치 |
|---|---|
| 프로젝트·실험·평가셋 버전·Run·모델·검토·초안 | 기존 PostgreSQL `workbench.lab_records` |
| 실제 실행 상태 | 인증된 Airflow DAG Run API. 화면 조회 시 저장 기록과 대조 |
| 생성·임베딩 계산 | 선택한 추론 서버. Airflow에는 모델 가중치를 적재하지 않음 |
| 평가 대기 | `RemoteEvaluationOperator`가 defer하고 trigger가 제한된 HTTP 요청을 수행 |
| 사례별 응답·지표 | 공유 artifact 디렉터리. XCom에는 작은 결과 참조만 전달 |
| 학습 | 기존 HTTP Job API 실행기 또는 Kubernetes 학습 이미지 |
| 학습 데이터·가중치·학습 산출물 | 실행기 파일/객체 저장소 |

API 서버·triggerer·task에 같은 `AIRFLOW_WORKBENCH_ARTIFACT_DIR`를 설정하고 공유 볼륨을 마운트합니다. 기본값은 `/opt/airflow/workbench/lab-artifacts`입니다. 현재 compose의 기존 workbench 볼륨을 사용합니다. 여러 호스트에서는 원자적 파일 교체와 POSIX 파일 잠금을 지원하는 공유 파일시스템이 필요합니다. 이 평가 결과 저장 경로의 객체 저장소 직접 backend는 아직 제공하지 않습니다.

설치/업그레이드 시 플러그인 경로를 PYTHONPATH에 두고 `python -m airflow_workbench.metadata`를 실행합니다. 이번 schema migration은 기존 Airflow 테이블을 바꾸지 않고 `workbench.lab_records`를 추가합니다. API와 triggerer에 새 Python 코드가 반영되도록 재시작합니다.

같은 요청 ID는 저장된 입력과 DAG 실행을 재사용하고 입력이 달라지면 409로 거부합니다. 응답을 잃어도 동일 요청으로 복구합니다. 결과 파일은 저장된 입력의 signature와 대조합니다. DAG를 수동 성공 처리한 것만으로 모델 등록 조건을 충족하지 않습니다.

평가 DAG는 `model-lab`, `mlops`, `workload:evaluation`, `contract:lab-v1`, endpoint와 설정 fingerprint 태그를 사용합니다. 모델·평가마다 DAG 파일을 만들지 않습니다. 기다리는 작업도 자원 Pool을 점유하도록 검사합니다. [Airflow deferral](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)

공유 GPU에서는 학습과 평가가 같은 Pool을 사용하고 추론 연결에 외부 실행기의 Connection ID를 예약 연결로 지정합니다. 제공 워커의 예약은 요청마다 갱신합니다. 이 협약에 참여하지 않는 챗봇 호출까지 제어하지는 않습니다. 독립 장비는 별도 Pool과 연결로 분리합니다.

평가 취소는 새 사례/임베딩 batch 제출을 멈춥니다. 이미 진행 중인 HTTP 추론은 응답 종료까지 기다립니다. 추론 요청의 종료를 확인하지 못하면 예약을 즉시 해제하지 않습니다. HTTP 학습 취소는 외부 job에 전달하며 Kubernetes 학습의 실제 Pod 중단은 연결된 DAG에서 관리합니다.

## 평가 데이터와 점수

- 현재 내장 일괄 평가는 JSON 256KB 요청 한도, 사례 최대 100개, 코퍼스 문서 최대 500개입니다. 대규모 코퍼스/실제 검색기 평가는 프로젝트 평가 API와 별도 계산 환경으로 확장합니다.
- 데이터 저장은 새 레코드와 SHA256을 만듭니다. 기준선 비교는 동일 평가셋 checksum을 요구합니다. 참조 데이터가 달라진 비교를 같은 조건으로 표시하지 않습니다.
- 생성 자동 채점은 검토 완료 사례의 문자열 포함 조건과 기대 JSON의 부분 구조입니다. 배열은 순서를 포함한 정확한 값으로 비교합니다. 도메인 의미 판정과 답변의 종합 품질은 사람 검토 또는 프로젝트 평가 어댑터를 사용합니다.
- `reference`는 검토용 정보이며 추론 입력에 넣지 않습니다. `input`에 제공한 문맥만 실제 모델 입력입니다. 에이전트 API에는 실제 도구의 쓰기를 차단한 평가 모드를 마련하세요.
- 임베딩은 고정 코퍼스를 실제 임베딩하고 cosine으로 순위를 계산합니다. 관련도 0~3으로 Recall/NDCG/MRR을 계산합니다. 미라벨·정답 없음·Top-K에 미판정 문서가 있는 사례는 무조건 오답으로 집계하지 않습니다.
- 자동 채점, 사람의 사례별 검토, 모델 버전의 승인/반려는 별도 기록입니다. 실행 실패 수와 미채점 사례를 분모와 함께 확인합니다. 작은 개발 평가셋의 점수를 일반화 성능으로 해석하지 않습니다.

## 모델 등록과 적용의 경계

모델 버전은 완료된 실제 추론 평가를 참조합니다. 학습 실행 연결과 산출물 메모를 추가할 수 있습니다. 학습 → adapter 병합/변환 → 추론 엔진 등록을 자동 수행하는 패키징 작업은 아직 제공하지 않습니다. 학습과 추론 모델의 연결은 사용자가 확인하는 참조이며 변환 receipt로 자동 검증된 계보라고 표시하지 않습니다.

HF adapter, QLoRA 산출물, 임베딩 모델이 같은 방식으로 Ollama에 들어가는 것은 아닙니다. 목표 엔진·원본 모델·형식의 호환성을 확인하고 실제 서빙 형식으로 평가하세요. [Ollama 모델 가져오기](https://docs.ollama.com/import)

적용 구성 JSON은 검토 승인된 모델·endpoint 참조·실제 기록된 모델 digest·프롬프트·파라미터·평가 근거를 묶습니다. 임베딩 모델에는 인덱스 버전·코퍼스 버전·전처리·차원·거리함수가 추가로 필요합니다. 모델이 달라지면 해당 모델로 만든 문서 벡터와 질문 임베딩을 함께 전환해야 합니다.

구성 생성 상태는 `configuration_ready`입니다. Git/CI 또는 서비스의 배포 절차에 전달할 수 있지만, 모델 자동 배포·서비스 적용 확인·인덱스 생성·자동 롤백까지 구현된 것은 아닙니다. 이전 구성 내보내기는 지원합니다. 현재 Airflow 버전/FAB 밖의 인증과 실제 Kubernetes/GPU 학습은 해당 환경에서 추가 검증해야 합니다.

## 검증

- [프로젝트 저장소·워커 파일 경로 검증](../../../docs/qa/model-lab-storage-2026-09-10.md)
- [회귀·실제 실행 기록](../../../docs/qa/model-lab-workflow-2026-09-10.md)
- [설계 제안과 단계별 완료 기준](../../../docs/model-lab-workflow-review-2026-09-10.md)
- 현재 로컬 control 워커는 학습 패키지·HF 원본 가중치·학습 데이터와 RAM 준비가 필요합니다. 이번 실제 검증은 Qwen 생성·BGE 임베딩 평가이며 파인튜닝 완료를 의미하지 않습니다.
