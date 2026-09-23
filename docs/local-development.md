> 저장소 분리(2026-09-23) 전의 설명입니다. 현재 설치·DB 분리·연결 방법은 저장소 루트 README.md를 우선합니다.

# 로컬 개발 환경

2026-09-11 디렉토리 정리를 반영했다. 아래 날짜별 결과는 과거 검증 기록이다.

## 2026-09-09 통합 검증

사용자 로그인·검색·리뷰 작성/수정/삭제·마이페이지와 mock 챗봇 응답을 브라우저에서 확인했다. API 통합 검증은 36건 중 33건 통과했으며 관리자 직접 API 인증 누락 3건이 실패했다. 상세 결함과 검증 범위는 [통합 검토 보고서](qa/integration-review-2026-09-09.md)를 참고한다.

호스트 8000 포트가 다른 로컬 프로그램과 충돌해 챗봇 외부 포트를 58000으로 변경했다. 컨테이너 내부 주소는 여전히 `chatbot:8000`이다.

## 2026-09-08 검증 결과

- PostgreSQL 17 + pgvector 실행, 기존 dev 스키마 덤프 복원 성공.
- 영화 5,319건, 서비스 영화 뷰 5,319건, 리뷰 124,941건, 영화 카테고리 14건 확인. 초기 JSON 5,985건과는 다른 스냅샷이다.
- 사용자·관리자 루트 HTTP 200, WAS·챗봇 health의 database connected 확인.
- 사용자 /be/catalog/movies 프록시, 관리자 /api/health 프록시 HTTP 200 확인.
- 사용자 /api/chat을 통한 실제 카탈로그 영화 정보 요청: movie_info 응답·세션 생성 확인(mock 모드).
- 화면의 시각적 QA·로그인·관리자 쓰기·전체 기능 테스트는 아직 수행하지 않았다.
- mock 모드에서 따옴표 없는 일부 문장은 영화 제목을 잘못 추출한다. 실제 Gemini 질의 해석 성능 검증은 별도다.

기존 소스는 수정하지 않고 `scripts/prepare-local.ps1`로 `apps/`에 개발용 복사본을 만든다. 기존 `.env`·키·의존성·빌드 결과는 복사하지 않는다. `apps`의 개발 소스는 Git 관리 대상이며 비밀 설정과 의존성·빌드 캐시는 제외한다.

## 준비와 실행

PowerShell 7, Docker Desktop Linux 컨테이너 엔진이 필요하다.

```powershell
./scripts/prepare-local.ps1
docker --config .docker-local compose config --quiet
docker --config .docker-local compose up -d db
./scripts/restore-local.ps1
docker --config .docker-local compose up -d was chatbot fe admin
```

| 대상 | 주소 |
|---|---|
| 사용자 화면 | http://localhost:3000 |
| 관리자 화면 | http://localhost:3100 |
| WAS 문서 | http://localhost:3200/docs |
| 챗봇 API 문서 | http://localhost:58000/docs |
| PostgreSQL | 127.0.0.1:55432 / pop_talk_local / dev |

DB 도구용 전체 접속정보는 `.local/config/db-connection.txt`에서 확인한다. 비밀번호는 무작위 생성하며 문서·Git에 넣지 않는다. 컨테이너 내부에서는 `db:5432`를 사용한다. 외부 접속 포트는 localhost에만 바인딩한다.

챗봇의 실제 POST 경로는 `/api/chat`이며, 사용자 FE도 같은 경로로 프록시한다.

현재 챗봇은 외부 API 키 없이 **mock 모드**로 실행한다. 실제 생성은 Gemini API, 임베딩은 로컬 Ollama 호환 API를 사용하도록 경계를 교체했지만 둘 다 기본 비활성화 상태다. 문서 임베딩 배치는 아직 자동 실행하지 않는다.

관리자 서버는 일부 조회·관리에서 DB에 직접 연결하는 기존 구조가 있어 별도 DB URL을 제공한다. 화면이 보이더라도 snapshot/mock fallback인지 실제 DB인지 확인해야 한다.

## DB 복원 주의사항

기존 `dump-popcorndb-202608172118.sql`은 PGDMP 사용자 정의 형식이다. psql로 실행할 SQL 파일이 아니며 pg_restore가 필요하다. 신규 전용 DB에만 복원하고 원본·기존 DB에 덮어쓰지 않는다. 먼저 archive 목록·스키마를 확인하고 owner/ACL 및 확장 호환성을 점검한다. 기존 WAS 마이그레이션에는 users/reviews 최초 DDL이 없으므로 001부터 무조건 재실행하지 않는다. 덤프 적용 상태와 현재 코드 요구사항을 대조한 뒤 필요한 변경만 적용한다.

확인한 덤프는 PostgreSQL 16.14에서 pg_dump 17로 생성됐다. 로컬은 PostgreSQL 17을 사용하며 `cdb_admin.vector` 타입 호환을 위해 해당 스키마에 vector 확장을 설치한다. 복원 스크립트는 dev 스키마가 있으면 중단하고, no-owner/no-acl 및 단일 트랜잭션으로 복원한다. 기존 벡터는 CLOVA 모델의 과거 결과이며 새 모델과 혼합하지 않는다. 기존 회원·리뷰도 로컬 스냅샷에 포함될 수 있으므로 공개 배포용 데이터로 취급하지 않는다.

## 상태 확인·종료

```powershell
docker --config .docker-local compose ps
docker --config .docker-local compose logs --tail 50 was chatbot
docker --config .docker-local compose stop
```

`stop`은 DB 볼륨을 보존한다. `down -v`는 데이터 삭제이므로 일반 종료에 사용하지 않는다. 초기 구성은 개발용 단일 DB 계정을 사용하며 역할별 권한 분리는 후속 작업이다. 의존성은 기존 manifest의 범위로 설치하므로 첫 실행 검증 후 버전 고정이 필요하다.
