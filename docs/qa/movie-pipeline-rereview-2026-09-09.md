# 수정 반영 재검토

## 최종 재확인 — 추가 P1/P2 종료

후속 수정본을 검토자가 재확인했다. 아래 본문의 P1/P2는 수정 전 이력이며 현재 종료한다.

- 실제 로컬 초기 원본 5,985건을 컨테이너 메모리에서 재변환: 성공, 고유 canonical key 5,985개, KOFIC ID/제목 누락 0, 생성된 service_movie_id 0.
- content hash가 명시적 CONTENT_HASH_FIELDS만 사용하며 출처·서비스 PK·승인 관측·실행 추적을 제외함을 확인.
- 초기/일일 동일 업무 콘텐츠 hash 일치 테스트를 포함한 17개 테스트 재실행 성공.
- 이번 수정 범위에서 추가 차단 결함은 발견하지 못했다. 변환 코어는 다음 Databricks 연결 단계로 진행 가능하다. 전체 운영 파이프라인 승인 또는 원격 적재 검증 완료를 의미하지는 않는다.
- 최신 상태/처리 ledger 분리, 역순 실행 방지, 박스오피스 중복 제거, 기존 서비스 I/U와 승인 상태 보존은 후속 저장·게시 단계에서 검증한다.

기존 지적 3건은 해당 재현 조건에서 수정됨: 실행별 trace를 hash에서 제외, truncated/동점 후보 REVIEW_REQUIRED, ID 집합/건수/박스오피스 날짜 대사. 수정 테스트 16개 통과. 아래 2건은 추가 수정 필요. 소스 코드와 외부 데이터는 수정하지 않았다.

## P1 실제 초기 snapshot을 새 어댑터가 읽지 못함

`movie_bronze_silver.py:370`은 record.id를 필수 서비스 PK로 요구하고 이후 kofic_movie_cd/title_ko/release_date 등 DB export 필드를 읽는다. 그러나 S3에 보관한 실제 movies_final.json 5,985건은 movie_cd/movie_nm/open_dt/prdt_year/nation_alt/genre_alt/director_nm 등의 기존 배치 출력이다. id 필드가 없다.

실제 원본 파일을 읽어 올바른 checksum/bytes/count manifest를 구성하고 메모리 변환한 결과 `TransformContractError: Legacy movie has invalid service ID`로 실패했다. 테스트는 실제 파일 형식이 아닌 DB export 모양의 fixture를 사용해 이 불일치를 발견하지 못했다.

수정: 실제 파일 계약에 맞춰 movie_cd→kofic_movie_cd, movie_nm→title_ko, open_dt→release_date 등 명시적 변환과 다중값 파싱을 구현한다. service_movie_id는 원본에 없으므로 None으로 두고 기존 서비스 DB와 KOFIC 코드로 별도 매핑해야 한다. 초기 원본에서 서비스 PK를 만들어내지 않는다. 실제 5,985건 전체 변환 및 필수 필드/다중값 보존을 확인해야 한다.

## P2 content hash에 출처와 서비스 관측 메타데이터가 남아 있음

`movie_bronze_silver.py:289-295`은 제외 목록 외 모든 필드를 업무 내용으로 해시한다. ingestion_source, service_movie_id 및 snapshot에만 있는 source_service_status/source_approval_status가 포함된다. 실제 메모리 재현에서 다른 필드를 고정하고 ingestion_source만 바꿔도 content_sha256이 달라졌다.

초기/일일이 공통 Silver에 합류하면 동일 콘텐츠도 서로 다른 내용으로 판정될 수 있다. source 승인 상태 관측값 변화도 콘텐츠 변경으로 섞인다. 업무 필드 명시적 허용 목록으로 hash를 만들고 출처/내부 PK 매핑/서비스 상태 관측은 분리해야 한다. 초기·일일 어댑터가 동일 업무 콘텐츠에 대해 동일 hash를 생성하는 교차 어댑터 테스트가 필요하다.

## 판정

기존 수정 방향은 적절하지만 초기+일일 공통 Silver 계약 완료 판정은 보류한다. 실제 snapshot 어댑터 호환성과 교차 출처 hash를 수정한 뒤 Databricks 연결을 진행하는 것이 좋다. 기존 리뷰의 최신 상태/관측 ledger 분리, 과거 실행 역전 방지, 기존 서비스 I/U 연결은 여전히 후속 저장·게시 단계의 검토 항목이다.
