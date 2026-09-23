# Pop Talk 로컬 모델 저장·실행 정책

작성일: 2026-09-09 · 상태: 운영 기준

## 1. 결정

Pop Talk의 로컬 AI 모델은 **D 드라이브의 프로젝트 로컬 경로에서 관리·실행**한다. 모델과 챗봇 구성이 안정화된 시점에 E 드라이브로 검증된 백업을 만든다.

- D 드라이브: 현재 사용 중인 Ollama 모델의 기준 저장소이자 실행 경로
- E 드라이브: 안정화·해시 검증 후 만드는 장기 백업, Hugging Face 학습 가중치, LoRA adapter, 평가 결과 보관소
- PostgreSQL: 임베딩 벡터와 서빙 데이터 저장소. 모델 파일 보관소가 아님

외장 E 드라이브에서 Qwen3 8B를 직접 실행했을 때 5.2GB 모델의 첫 로딩이 Ollama의 5분 제한을 초과했다. 같은 파일을 D 드라이브 캐시에서 읽으면 모델 등록과 GPU 적재가 가능했으므로, 저장과 실행 경로를 분리한다.

## 2. 표준 디렉터리

```text
E:\PopTalkAI\
├─ models\
│  ├─ ollama-runtime\       # 검증된 Ollama GGUF 원본 보관소
│  └─ huggingface\          # 향후 QLoRA용 원본 가중치
├─ adapters\                # LoRA/QLoRA adapter
├─ cache\                   # 다운로드·변환 임시 파일
├─ evaluations\             # 모델 평가 결과
└─ README.md                 # 이 문서의 E 드라이브 사본

D:\01.DEV\pop_talk_dw_project\.local\models\ollama\
└─ Ollama 실행 캐시
```

E의 `models\ollama`과 `models\ollama-runtime`은 2026-09-09 설치·복사 과정에서 손상 및 partial 파일이 확인된 임시 경로이므로 운영 원본으로 사용하지 않는다. D 모델이 안정화된 뒤 E 백업을 새로 생성한다.

## 3. 현재 모델

| 역할 | 모델 | 형식·크기 | E 원본 | D 실행 캐시 | 상태 |
|---|---|---:|---|---|---|
| 생성·QueryPlan 후보 | `qwen3:8b` | Ollama Q4_K_M, 약 5.2GB | 안정화 후 백업 | 있음 | SHA-256 일치, 한국어 생성과 GPU 실행 확인 |
| 임베딩 | `bge-m3` | Ollama, 약 1.2GB | 안정화 후 백업 | 있음 | 강제 재다운로드 후 SHA-256 일치, 1024차원 출력 확인 |

Ollama 모델은 추론용이다. QLoRA 학습에 사용할 `Qwen/Qwen3-8B` Hugging Face 원본 가중치와 BGE-M3 학습용 원본은 아직 받지 않았다. 학습을 시작할 때 `E:\PopTalkAI\models\huggingface`에 revision을 고정해 내려받는다.

## 4. 실행 원칙

평상시 Ollama는 한 서버만 11434 포트에서 실행한다. 11435와 11436은 설치 검증에만 사용한 임시 포트이며 운영 설정에 남기지 않는다. 여러 Ollama 서버가 동시에 모델을 GPU에 올리면 RTX 3060 12GB의 VRAM을 거의 모두 사용해 로딩 실패나 비정상 출력이 발생할 수 있다.

새 PowerShell에서 다음과 같이 D 캐시를 지정해 실행한다.

```powershell
$env:OLLAMA_MODELS = 'D:\01.DEV\pop_talk_dw_project\.local\models\ollama'
ollama serve
```

챗봇 컨테이너는 호스트의 단일 Ollama 서버를 참조한다.

```dotenv
LOCAL_EMBEDDING_BASE_URL=http://host.docker.internal:11434
LOCAL_EMBEDDING_MODEL=bge-m3
```

기존 CLOVA 임베딩과 BGE-M3 임베딩은 벡터 공간이 다르므로 섞지 않는다. `LOCAL_EMBEDDING_ENABLED`는 BGE-M3로 문서 벡터를 다시 생성하고 모델 버전을 기록한 뒤 활성화한다.

## 5. D 기준 저장소에서 E 백업으로 동기화

현재 기준은 `D 관리·실행 → 안정화 후 E 백업` 한 방향이다. Ollama `pull`, 모델 교체, adapter 변환, 품질 평가는 D에서 먼저 완료한다. 모델 blob의 SHA-256과 manifest 참조, 챗·임베딩 스모크 테스트가 모두 통과한 시점에만 E의 새 staging 폴더로 복사한다. E에서 다시 해시를 검증한 후 백업으로 승격하며, `.partial`이나 중단된 복사는 폐기한다.

모델마다 다음 정보를 `manifest` 또는 평가 기록에 남긴다.

- 모델 이름과 공급자
- 원본 revision 또는 Ollama blob ID
- 양자화 방식
- 파일 크기와 SHA-256
- 다운로드·변환 날짜
- adapter 및 학습 데이터 버전
- Golden QA 평가 결과

E 드라이브가 연결되지 않아도 D 캐시와 PostgreSQL 스냅샷으로 로컬 시연이 가능해야 한다. D 캐시가 없으면 서비스는 Gemini 폴백 또는 명확한 모델 미사용 오류를 반환한다.

## 6. 용량과 성능 관리

- 현재 Ollama 모델 두 개의 순수 용량은 약 6.4GB다.
- E 드라이브는 약 255GB 중 설치 후 약 247GB가 남아 있었다.
- D 드라이브는 설치 시 약 303GB의 여유 공간이 확인됐다.
- QLoRA 원본 가중치·adapter·체크포인트는 E에 보관하되, 반복해서 읽는 학습 캐시와 현재 실행 모델은 D에 둔다.
- 평가가 끝난 이전 D 캐시는 E 원본의 해시와 복구 가능 여부를 확인한 뒤 정리한다.
- PostgreSQL 데이터, 모델 원본, 평가 결과의 유일한 사본을 한 번에 이동하거나 삭제하지 않는다.

## 7. 재부팅 후 확인 절차

1. 작업 관리자에서 남은 `ollama.exe`와 `llama-server.exe`가 없는지 확인한다.
2. D 캐시를 `OLLAMA_MODELS`로 지정해 Ollama 서버 하나만 실행한다.
3. `ollama list`에서 `qwen3:8b`, `bge-m3`를 확인한다.
4. BGE-M3가 1024차원 임베딩을 반환하는지 확인한다.
5. Qwen3가 한국어 한 문장을 정상 생성하는지 확인한다.
6. `ollama ps`에서 필요한 모델만 100% GPU로 적재되는지 확인한다.
7. 검증 후 챗봇의 로컬 임베딩 연결 테스트를 진행한다.

프로젝트에서 직접 시험할 때는 PowerShell을 열고 다음 명령을 순서대로 실행한다.

```powershell
cd D:\01.DEV\pop_talk_dw_project
.\orchestration\airflow\scripts\start-local-ollama.ps1
.\orchestration\airflow\tests\smoke\test-local-models.ps1
```

첫 번째 스크립트는 D 캐시를 사용해 11434 서버를 시작하고 모델 목록을 표시한다. 이미 정상 Ollama 서버가 실행 중이면 새 서버를 중복 실행하지 않는다. 두 번째 스크립트는 BGE-M3의 1024차원 임베딩을 확인하고 해당 모델을 GPU에서 내린 뒤 Qwen3의 한국어 한 문장 생성을 시험한다.

Ollama 프로그램 화면에서는 `qwen3:8b`를 선택해 일반 채팅을 확인할 수 있다. `bge-m3`는 채팅 모델이 아니므로 프로그램에서 대화를 시도하지 않고 검증 스크립트나 `/api/embed`를 사용한다.
