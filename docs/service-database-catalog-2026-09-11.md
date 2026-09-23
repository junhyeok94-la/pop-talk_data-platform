# 서비스 DB 실제 카탈로그 — 2026-09-11

읽기 전용 조사에서 확인한 55개 테이블·뷰의 상세 정의. 건수는 조사 시점 기준이며 스냅샷 테이블은 과거 게시본을 포함한다.

목적·사용처·정리 판단은 [전수 점검 보고서](service-database-audit-2026-09-11.md)를 참고한다.

## dev.admin_audit_logs

종류: 테이블. 건수: 0. 저장공간: 24,576 bytes.

관리자 영화·카테고리·리뷰 관리 행위의 감사 로그.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | NO | nextval('dev.admin_audit_logs_id_seq'::regclass) |
| actor_id | uuid / uuid | YES |  |
| action | character varying / varchar | NO |  |
| target_type | character varying / varchar | NO |  |
| target_id | character varying / varchar | NO |  |
| payload | jsonb / jsonb | NO | '{}'::jsonb |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |

### 제약

```sql
FOREIGN KEY (actor_id) REFERENCES dev.users(id) ON DELETE SET NULL;
PRIMARY KEY (id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX admin_audit_logs_pkey ON dev.admin_audit_logs USING btree (id);
CREATE INDEX idx_admin_audit_logs_target ON dev.admin_audit_logs USING btree (target_type, target_id, created_at DESC);
```

## dev.batch_runs

종류: 테이블. 건수: 9. 저장공간: 49,152 bytes.

APScheduler와 초기 데이터 적재 작업의 실행 이력 및 처리 건수

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | NO | nextval('dev.batch_runs_id_seq'::regclass) |
| job_name | character varying / varchar | NO |  |
| scheduled_for | timestamp with time zone / timestamptz | NO |  |
| status | USER-DEFINED / job_status | NO | 'PENDING'::dev.job_status |
| source_file | character varying / varchar | YES |  |
| source_hash | character / bpchar | YES |  |
| processed_count | integer / int4 | NO | 0 |
| inserted_count | integer / int4 | NO | 0 |
| updated_count | integer / int4 | NO | 0 |
| failed_count | integer / int4 | NO | 0 |
| result | jsonb / jsonb | NO | '{}'::jsonb |
| last_error | text / text | YES |  |
| started_at | timestamp with time zone / timestamptz | YES |  |
| finished_at | timestamp with time zone / timestamptz | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |

### 제약

```sql
CHECK ((failed_count >= 0));
CHECK ((inserted_count >= 0));
UNIQUE (job_name, scheduled_for);
PRIMARY KEY (id);
CHECK ((processed_count >= 0));
CHECK ((updated_count >= 0));
```

### 인덱스

```sql
CREATE UNIQUE INDEX batch_runs_job_name_scheduled_for_key ON dev.batch_runs USING btree (job_name, scheduled_for);
CREATE UNIQUE INDEX batch_runs_pkey ON dev.batch_runs USING btree (id);
```

## dev.chat_messages

종류: 테이블. 건수: 666. 저장공간: 688,128 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | NO | nextval('dev.chat_messages_id_seq'::regclass) |
| session_id | uuid / uuid | NO |  |
| role | USER-DEFINED / chat_role | NO |  |
| content | text / text | NO |  |
| intent | character varying / varchar | YES |  |
| sources | jsonb / jsonb | NO | '[]'::jsonb |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |

### 제약

```sql
CHECK ((length(btrim(content)) > 0));
PRIMARY KEY (id);
FOREIGN KEY (session_id) REFERENCES dev.chat_sessions(id) ON DELETE CASCADE;
CHECK ((jsonb_typeof(sources) = 'array'::text));
```

### 인덱스

```sql
CREATE UNIQUE INDEX chat_messages_pkey ON dev.chat_messages USING btree (id);
CREATE INDEX idx_chat_messages_session_created ON dev.chat_messages USING btree (session_id, created_at DESC, id DESC);
CREATE INDEX ix_chat_messages_session_created ON dev.chat_messages USING btree (session_id, created_at);
```

## dev.chat_sessions

종류: 테이블. 건수: 328. 저장공간: 98,304 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | uuid / uuid | NO | gen_random_uuid() |
| title | character varying / varchar | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| updated_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| user_id | uuid / uuid | YES |  |

### 제약

```sql
PRIMARY KEY (id);
FOREIGN KEY (user_id) REFERENCES dev.users(id) ON DELETE SET NULL;
```

### 인덱스

```sql
CREATE UNIQUE INDEX chat_sessions_pkey ON dev.chat_sessions USING btree (id);
CREATE INDEX idx_chat_sessions_user_updated ON dev.chat_sessions USING btree (user_id, updated_at DESC) WHERE (user_id IS NOT NULL);
```

## dev.display_categories

종류: 테이블. 건수: 16. 저장공간: 65,536 bytes.

사용자 화면에 보이는 추천 알약 문구. 영화가 붙는 대상

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | NO | nextval('dev.display_categories_id_seq'::regclass) |
| name | character varying / varchar | NO |  |
| description | text / text | YES |  |
| sort_order | integer / int4 | NO | 0 |
| is_active | boolean / bool | NO | true |
| created_by | character varying / varchar | YES |  |
| updated_by | character varying / varchar | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| updated_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| short_label | character varying / varchar | YES |  |
| category_codes | ARRAY / _text | NO | '{}'::text[] |

### 제약

```sql
CHECK ((NOT (category_codes && ARRAY[''::text])));
CHECK ((cardinality(category_codes) > 0));
CHECK (((short_label IS NULL) OR (length(btrim((short_label)::text)) > 0)));
CHECK ((length(btrim((name)::text)) > 0));
PRIMARY KEY (id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX display_categories_pkey ON dev.display_categories USING btree (id);
CREATE INDEX idx_display_categories_active_sort ON dev.display_categories USING btree (is_active, sort_order, id);
CREATE INDEX idx_display_categories_codes ON dev.display_categories USING gin (category_codes);
```

### 트리거

```sql
CREATE TRIGGER trg_display_categories_updated_at BEFORE UPDATE ON dev.display_categories FOR EACH ROW EXECUTE FUNCTION dev.set_updated_at();
```

## dev.display_categories_service

종류: 뷰. 건수: 뷰 — 별도 집계. 저장공간: 0 bytes.

화면 문구에 카테고리 이름과 영화 수를 붙인 조회용 뷰. movie_count는 묶은 카테고리들의 영화 합집합입니다(수동 연결 + match_keywords 자동 분류). unknown_codes가 비어 있지 않으면 실재하지 않는 카테고리를 가리키고 있다는 뜻입니다.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | YES |  |
| name | character varying / varchar | YES |  |
| short_label | character varying / varchar | YES |  |
| category_codes | ARRAY / _text | YES |  |
| category_names | ARRAY / _varchar | YES |  |
| unknown_codes | ARRAY / _text | YES |  |
| description | text / text | YES |  |
| sort_order | integer / int4 | YES |  |
| is_active | boolean / bool | YES |  |
| created_by | character varying / varchar | YES |  |
| updated_by | character varying / varchar | YES |  |
| created_at | timestamp with time zone / timestamptz | YES |  |
| updated_at | timestamp with time zone / timestamptz | YES |  |
| movie_count | integer / int4 | YES |  |

### 뷰 정의

```sql
 SELECT id,
    name,
    short_label,
    category_codes,
    COALESCE(( SELECT array_agg(c.name ORDER BY (array_position(d.category_codes, c.code::text))) AS array_agg
           FROM dev.movie_categories c
          WHERE c.code::text = ANY (d.category_codes)), '{}'::character varying[]) AS category_names,
    COALESCE(( SELECT array_agg(x.x) AS array_agg
           FROM unnest(d.category_codes) x(x)
          WHERE NOT (EXISTS ( SELECT 1
                   FROM dev.movie_categories c
                  WHERE c.code::text = x.x))), '{}'::text[]) AS unknown_codes,
    description,
    sort_order,
    is_active,
    created_by,
    updated_by,
    created_at,
    updated_at,
    ( SELECT count(DISTINCT m.id)::integer AS count
           FROM dev.popcorn_movies m
          WHERE (EXISTS ( SELECT 1
                   FROM dev.movie_categories c
                  WHERE (c.code::text = ANY (d.category_codes)) AND ((EXISTS ( SELECT 1
                           FROM dev.movie_category_links l
                          WHERE l.movie_id = m.id AND l.category_id = c.id)) OR c.is_active AND c.match_keywords && m.source_keywords)))) AS movie_count
   FROM dev.display_categories d;
```

## dev.movie_categories

종류: 테이블. 건수: 14. 저장공간: 114,688 bytes.

관리자용 영화 분류 축 (장르·분위기·테마·등급). 아직 영화와 잇지 않는다

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | NO | nextval('dev.movie_categories_id_seq'::regclass) |
| code | character varying / varchar | NO |  |
| name | character varying / varchar | NO |  |
| type | character varying / varchar | NO |  |
| description | text / text | YES |  |
| sort_order | integer / int4 | NO | 0 |
| is_active | boolean / bool | NO | true |
| created_by | character varying / varchar | YES |  |
| updated_by | character varying / varchar | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| updated_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| aliases | ARRAY / _text | NO | '{}'::text[] |
| match_keywords | ARRAY / _text | NO | '{}'::text[] |

### 제약

```sql
CHECK ((NOT (aliases @> ARRAY[''::text])));
CHECK ((NOT (match_keywords @> ARRAY[''::text])));
CHECK ((length(btrim((name)::text)) > 0));
PRIMARY KEY (id);
CHECK (((type)::text = ANY (ARRAY[('GENRE'::character varying)::text, ('MOOD'::character varying)::text, ('THEME'::character varying)::text, ('RATING'::character varying)::text, ('SITUATION'::character varying)::text])));
UNIQUE (code);
```

### 인덱스

```sql
CREATE INDEX idx_movie_categories_active_sort ON dev.movie_categories USING btree (is_active, sort_order, code);
CREATE INDEX idx_movie_categories_aliases ON dev.movie_categories USING gin (aliases);
CREATE INDEX idx_movie_categories_match_keywords ON dev.movie_categories USING gin (match_keywords);
CREATE INDEX idx_movie_categories_type ON dev.movie_categories USING btree (type);
CREATE UNIQUE INDEX movie_categories_pkey ON dev.movie_categories USING btree (id);
CREATE UNIQUE INDEX uq_movie_categories_code ON dev.movie_categories USING btree (code);
```

### 트리거

```sql
CREATE TRIGGER trg_movie_categories_updated_at BEFORE UPDATE ON dev.movie_categories FOR EACH ROW EXECUTE FUNCTION dev.set_updated_at();
```

## dev.movie_category_links

종류: 테이블. 건수: 0. 저장공간: 16,384 bytes.

영화와 Pop Talk 자체 운영 카테고리의 다대다 연결 정보.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| movie_id | bigint / int8 | NO |  |
| category_id | bigint / int8 | NO |  |
| assigned_by | uuid / uuid | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |

### 제약

```sql
FOREIGN KEY (assigned_by) REFERENCES dev.users(id) ON DELETE SET NULL;
FOREIGN KEY (category_id) REFERENCES dev.movie_categories(id) ON DELETE RESTRICT;
FOREIGN KEY (movie_id) REFERENCES dev.popcorn_movies(id) ON DELETE CASCADE;
PRIMARY KEY (movie_id, category_id);
```

### 인덱스

```sql
CREATE INDEX idx_movie_category_links_category ON dev.movie_category_links USING btree (category_id, movie_id);
CREATE UNIQUE INDEX movie_category_links_pkey ON dev.movie_category_links USING btree (movie_id, category_id);
```

### 트리거

```sql
CREATE TRIGGER trg_queue_category_link_movie_embedding AFTER INSERT OR DELETE OR UPDATE ON dev.movie_category_links FOR EACH ROW EXECUTE FUNCTION dev.queue_curated_movie_embedding();
```

## dev.movie_editorial

종류: 테이블. 건수: 0. 저장공간: 16,384 bytes.

배치 수집 원본과 분리해 운영자가 보정하는 영화 정보. 이후 수집 동기화에도 보정값을 보존한다.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| movie_id | bigint / int8 | NO |  |
| plot_override | text / text | YES |  |
| is_removed | boolean / bool | NO | false |
| removal_reason | text / text | YES |  |
| updated_by | uuid / uuid | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| updated_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |

### 제약

```sql
FOREIGN KEY (movie_id) REFERENCES dev.popcorn_movies(id) ON DELETE CASCADE;
PRIMARY KEY (movie_id);
CHECK (((plot_override IS NULL) OR (length(btrim(plot_override)) > 0)));
CHECK (((NOT is_removed) OR ((removal_reason IS NOT NULL) AND (length(btrim(removal_reason)) > 0))));
FOREIGN KEY (updated_by) REFERENCES dev.users(id) ON DELETE SET NULL;
```

### 인덱스

```sql
CREATE UNIQUE INDEX movie_editorial_pkey ON dev.movie_editorial USING btree (movie_id);
```

### 트리거

```sql
CREATE TRIGGER trg_queue_editorial_movie_embedding AFTER INSERT OR DELETE OR UPDATE ON dev.movie_editorial FOR EACH ROW EXECUTE FUNCTION dev.queue_curated_movie_embedding();
```

## dev.movie_embedding_jobs

종류: 테이블. 건수: 5338. 저장공간: 1,024,000 bytes.

신규·변경·승인 영화의 임베딩 작업 큐

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | NO | nextval('dev.movie_embedding_jobs_id_seq'::regclass) |
| movie_id | bigint / int8 | NO |  |
| embedding_model | character varying / varchar | NO | 'bge-m3'::character varying |
| operation | USER-DEFINED / embedding_job_operation | NO | 'UPSERT'::dev.embedding_job_operation |
| status | USER-DEFINED / job_status | NO | 'PENDING'::dev.job_status |
| attempts | smallint / int2 | NO | 0 |
| max_attempts | smallint / int2 | NO | 3 |
| last_error | text / text | YES |  |
| available_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| started_at | timestamp with time zone / timestamptz | YES |  |
| finished_at | timestamp with time zone / timestamptz | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| lease_token | uuid / uuid | YES |  |
| lease_expires_at | timestamp with time zone / timestamptz | YES |  |

### 제약

```sql
CHECK ((attempts >= 0));
CHECK ((max_attempts > 0));
FOREIGN KEY (movie_id) REFERENCES dev.popcorn_movies(id) ON DELETE CASCADE;
PRIMARY KEY (id);
```

### 인덱스

```sql
CREATE INDEX idx_movie_embedding_jobs_dispatch ON dev.movie_embedding_jobs USING btree (status, available_at, id);
CREATE INDEX idx_movie_embedding_jobs_lease_recovery ON dev.movie_embedding_jobs USING btree (lease_expires_at, id) WHERE (status = 'PROCESSING'::dev.job_status);
CREATE UNIQUE INDEX movie_embedding_jobs_pkey ON dev.movie_embedding_jobs USING btree (id);
CREATE UNIQUE INDEX uq_movie_embedding_jobs_active ON dev.movie_embedding_jobs USING btree (movie_id, embedding_model, operation) WHERE (status = ANY (ARRAY['PENDING'::dev.job_status, 'PROCESSING'::dev.job_status]));
```

## dev.popcorn_movie_embeddings

종류: 테이블. 건수: 5310. 저장공간: 35,348,480 bytes.

영화 메타정보와 줄거리의 RAG 검색용 1024차원 임베딩

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | NO | nextval('dev.popcorn_movie_embeddings_id_seq'::regclass) |
| movie_id | bigint / int8 | NO |  |
| document_type | character varying / varchar | NO | 'PROFILE'::character varying |
| chunk_no | integer / int4 | NO | 0 |
| embedding_model | character varying / varchar | NO | 'bge-m3'::character varying |
| status | USER-DEFINED / embedding_status | NO | 'PENDING'::dev.embedding_status |
| embedding_text | text / text | NO |  |
| content_hash | character / bpchar | NO |  |
| embedding | USER-DEFINED / vector | YES |  |
| attempts | smallint / int2 | NO | 0 |
| last_error | text / text | YES |  |
| embedded_at | timestamp with time zone / timestamptz | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| updated_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |

### 제약

```sql
CHECK ((attempts >= 0));
CHECK (((status <> 'READY'::dev.embedding_status) OR ((embedding IS NOT NULL) AND (embedded_at IS NOT NULL))));
CHECK ((chunk_no >= 0));
UNIQUE (movie_id, document_type, chunk_no, embedding_model);
FOREIGN KEY (movie_id) REFERENCES dev.popcorn_movies(id) ON DELETE CASCADE;
PRIMARY KEY (id);
```

### 인덱스

```sql
CREATE INDEX idx_movie_embeddings_movie_status ON dev.popcorn_movie_embeddings USING btree (movie_id, status);
CREATE UNIQUE INDEX popcorn_movie_embeddings_movie_id_document_type_chunk_no_em_key ON dev.popcorn_movie_embeddings USING btree (movie_id, document_type, chunk_no, embedding_model);
CREATE UNIQUE INDEX popcorn_movie_embeddings_pkey ON dev.popcorn_movie_embeddings USING btree (id);
```

### 트리거

```sql
CREATE TRIGGER trg_movie_embeddings_updated_at BEFORE UPDATE ON dev.popcorn_movie_embeddings FOR EACH ROW EXECUTE FUNCTION dev.set_updated_at();
```

## dev.popcorn_movie_media

종류: 테이블. 건수: 52082. 저장공간: 15,974,400 bytes.

movies_final.json의 전체 포스터와 스틸컷 URL 목록

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | NO | nextval('dev.popcorn_movie_media_id_seq'::regclass) |
| movie_id | bigint / int8 | NO |  |
| media_type | USER-DEFINED / movie_media_type | NO |  |
| url | text / text | NO |  |
| display_order | integer / int4 | NO | 0 |
| is_primary | boolean / bool | NO | false |
| source_system | character varying / varchar | NO | 'KMDB'::character varying |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |

### 제약

```sql
CHECK ((display_order >= 0));
FOREIGN KEY (movie_id) REFERENCES dev.popcorn_movies(id) ON DELETE CASCADE;
UNIQUE (movie_id, media_type, url);
PRIMARY KEY (id);
CHECK ((length(btrim(url)) > 0));
```

### 인덱스

```sql
CREATE INDEX idx_popcorn_movie_media_movie_type ON dev.popcorn_movie_media USING btree (movie_id, media_type, display_order);
CREATE UNIQUE INDEX popcorn_movie_media_movie_id_media_type_url_key ON dev.popcorn_movie_media USING btree (movie_id, media_type, url);
CREATE UNIQUE INDEX popcorn_movie_media_pkey ON dev.popcorn_movie_media USING btree (id);
CREATE UNIQUE INDEX uq_popcorn_movie_primary_poster ON dev.popcorn_movie_media USING btree (movie_id) WHERE ((media_type = 'POSTER'::dev.movie_media_type) AND (is_primary = true));
```

## dev.popcorn_movies

종류: 테이블. 건수: 5319. 저장공간: 10,936,320 bytes.

KOFIC 영화코드를 기준으로 KOFIC와 KMDB 정제 데이터를 통합한 서비스 영화 마스터

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | NO | nextval('dev.popcorn_movies_id_seq'::regclass) |
| kofic_movie_cd | character varying / varchar | NO |  |
| kmdb_id | character varying / varchar | YES |  |
| kmdb_matched | boolean / bool | NO | false |
| title_ko | text / text | NO |  |
| title_en | text / text | YES |  |
| title_original | text / text | YES |  |
| release_date | date / date | NO |  |
| production_year | smallint / int2 | YES |  |
| runtime_minutes | smallint / int2 | YES |  |
| movie_type | character varying / varchar | YES |  |
| production_status | character varying / varchar | YES |  |
| production_countries | ARRAY / _text | NO | '{}'::text[] |
| representative_country | character varying / varchar | YES |  |
| genres | ARRAY / _text | NO | '{}'::text[] |
| representative_genre | character varying / varchar | YES |  |
| directors | ARRAY / _text | NO | '{}'::text[] |
| director_names_en | ARRAY / _text | NO | '{}'::text[] |
| actors | ARRAY / _text | NO | '{}'::text[] |
| actor_roles | ARRAY / _text | NO | '{}'::text[] |
| production_companies | ARRAY / _text | NO | '{}'::text[] |
| viewing_grade | character varying / varchar | YES |  |
| poster_url | text / text | YES |  |
| plot | text / text | YES |  |
| source_keywords | ARRAY / _text | NO | '{}'::text[] |
| service_status | USER-DEFINED / movie_service_status | NO | 'DRAFT'::dev.movie_service_status |
| approval_status | USER-DEFINED / movie_approval_status | NO | 'PENDING'::dev.movie_approval_status |
| approved_by | character varying / varchar | YES |  |
| approved_at | timestamp with time zone / timestamptz | YES |  |
| rejection_reason | text / text | YES |  |
| source_system | character varying / varchar | NO | 'KOFIC_KMDB'::character varying |
| source_hash | character / bpchar | NO |  |
| source_synced_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| updated_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |

### 제약

```sql
CHECK (((approval_status <> 'APPROVED'::dev.movie_approval_status) OR (approved_at IS NOT NULL)));
CHECK (((kmdb_matched = false) OR ((kmdb_id IS NOT NULL) AND (btrim((kmdb_id)::text) <> ''::text))));
CHECK (((approval_status <> 'REJECTED'::dev.movie_approval_status) OR ((rejection_reason IS NOT NULL) AND (btrim(rejection_reason) <> ''::text))));
PRIMARY KEY (id);
CHECK (((production_year IS NULL) OR ((production_year >= 1888) AND (production_year <= 2200))));
CHECK (((runtime_minutes IS NULL) OR (runtime_minutes > 0)));
CHECK ((length(btrim(title_ko)) > 0));
UNIQUE (kofic_movie_cd);
```

### 인덱스

```sql
CREATE INDEX idx_popcorn_movies_actors ON dev.popcorn_movies USING gin (actors);
CREATE INDEX idx_popcorn_movies_countries ON dev.popcorn_movies USING gin (production_countries);
CREATE INDEX idx_popcorn_movies_directors ON dev.popcorn_movies USING gin (directors);
CREATE INDEX idx_popcorn_movies_genres ON dev.popcorn_movies USING gin (genres);
CREATE INDEX idx_popcorn_movies_kmdb_id ON dev.popcorn_movies USING btree (kmdb_id) WHERE (kmdb_id IS NOT NULL);
CREATE INDEX idx_popcorn_movies_release_date ON dev.popcorn_movies USING btree (release_date DESC);
CREATE INDEX idx_popcorn_movies_service_release ON dev.popcorn_movies USING btree (service_status, approval_status, release_date DESC);
CREATE INDEX idx_popcorn_movies_source_keywords ON dev.popcorn_movies USING gin (source_keywords);
CREATE INDEX idx_popcorn_movies_title_ko ON dev.popcorn_movies USING btree (title_ko);
CREATE UNIQUE INDEX popcorn_movies_pkey ON dev.popcorn_movies USING btree (id);
CREATE UNIQUE INDEX uq_popcorn_movies_kofic ON dev.popcorn_movies USING btree (kofic_movie_cd);
```

### 트리거

```sql
CREATE TRIGGER trg_popcorn_movies_updated_at BEFORE UPDATE ON dev.popcorn_movies FOR EACH ROW EXECUTE FUNCTION dev.set_updated_at();
CREATE TRIGGER trg_protect_removed_movie BEFORE UPDATE ON dev.popcorn_movies FOR EACH ROW EXECUTE FUNCTION dev.protect_removed_movie();
CREATE TRIGGER trg_queue_movie_embedding AFTER INSERT OR UPDATE OF title_ko, title_en, title_original, genres, production_countries, production_year, directors, actors, viewing_grade, release_date, runtime_minutes, source_keywords, plot, service_status, approval_status ON dev.popcorn_movies FOR EACH ROW EXECUTE FUNCTION dev.queue_movie_embedding();
```

## dev.popcorn_movies_service

종류: 뷰. 건수: 뷰 — 별도 집계. 저장공간: 0 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | YES |  |
| kofic_movie_cd | character varying / varchar | YES |  |
| kmdb_id | character varying / varchar | YES |  |
| kmdb_matched | boolean / bool | YES |  |
| title_ko | text / text | YES |  |
| title_en | text / text | YES |  |
| title_original | text / text | YES |  |
| release_date | date / date | YES |  |
| production_year | smallint / int2 | YES |  |
| runtime_minutes | smallint / int2 | YES |  |
| movie_type | character varying / varchar | YES |  |
| production_status | character varying / varchar | YES |  |
| production_countries | ARRAY / _text | YES |  |
| representative_country | character varying / varchar | YES |  |
| genres | ARRAY / _text | YES |  |
| representative_genre | character varying / varchar | YES |  |
| directors | ARRAY / _text | YES |  |
| director_names_en | ARRAY / _text | YES |  |
| actors | ARRAY / _text | YES |  |
| actor_roles | ARRAY / _text | YES |  |
| production_companies | ARRAY / _text | YES |  |
| viewing_grade | character varying / varchar | YES |  |
| poster_url | text / text | YES |  |
| plot | text / text | YES |  |
| source_keywords | ARRAY / _text | YES |  |
| service_status | USER-DEFINED / movie_service_status | YES |  |
| approval_status | USER-DEFINED / movie_approval_status | YES |  |
| approved_by | character varying / varchar | YES |  |
| approved_at | timestamp with time zone / timestamptz | YES |  |
| rejection_reason | text / text | YES |  |
| source_system | character varying / varchar | YES |  |
| source_hash | character / bpchar | YES |  |
| source_synced_at | timestamp with time zone / timestamptz | YES |  |
| created_at | timestamp with time zone / timestamptz | YES |  |
| updated_at | timestamp with time zone / timestamptz | YES |  |
| is_embedded | boolean / bool | YES |  |
| media | jsonb / jsonb | YES |  |

### 뷰 정의

```sql
 SELECT id,
    kofic_movie_cd,
    kmdb_id,
    kmdb_matched,
    title_ko,
    title_en,
    title_original,
    release_date,
    production_year,
    runtime_minutes,
    movie_type,
    production_status,
    production_countries,
    representative_country,
    genres,
    representative_genre,
    directors,
    director_names_en,
    actors,
    actor_roles,
    production_companies,
    viewing_grade,
    poster_url,
    plot,
    source_keywords,
    service_status,
    approval_status,
    approved_by,
    approved_at,
    rejection_reason,
    source_system,
    source_hash,
    source_synced_at,
    created_at,
    updated_at,
    (EXISTS ( SELECT 1
           FROM dev.popcorn_movie_embeddings e
          WHERE e.movie_id = m.id AND e.embedding_model::text = 'bge-m3'::text AND e.document_type::text = 'PROFILE'::text AND e.status = 'READY'::dev.embedding_status AND e.embedding IS NOT NULL)) AS is_embedded,
    COALESCE(( SELECT jsonb_agg(jsonb_build_object('type', media.media_type, 'url', media.url, 'order', media.display_order, 'primary', media.is_primary) ORDER BY media.media_type, media.display_order) AS jsonb_agg
           FROM dev.popcorn_movie_media media
          WHERE media.movie_id = m.id), '[]'::jsonb) AS media
   FROM dev.popcorn_movies m;
```

## dev.reviews

종류: 테이블. 건수: 124944. 저장공간: 61,063,168 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | uuid / uuid | NO | gen_random_uuid() |
| user_id | uuid / uuid | YES |  |
| movie_id | bigint / int8 | NO |  |
| rating | numeric / numeric | NO |  |
| content | text / text | YES |  |
| contains_spoiler | boolean / bool | NO | false |
| status | character varying / varchar | NO | 'ACTIVE'::character varying |
| created_at | timestamp with time zone / timestamptz | NO | now() |
| updated_at | timestamp with time zone / timestamptz | NO | now() |
| deleted_at | timestamp with time zone / timestamptz | YES |  |
| source_system | character varying / varchar | YES |  |
| source_user_key | character varying / varchar | YES |  |
| source_review_key | character / bpchar | YES |  |

### 제약

```sql
CHECK (((rating * (2)::numeric) = trunc((rating * (2)::numeric))));
FOREIGN KEY (movie_id) REFERENCES dev.popcorn_movies(id) ON DELETE CASCADE;
PRIMARY KEY (id);
CHECK (((rating >= 0.5) AND (rating <= 5.0)));
CHECK ((((source_system IS NULL) AND (source_user_key IS NULL) AND (source_review_key IS NULL)) OR ((source_system IS NOT NULL) AND (length(btrim((source_system)::text)) > 0) AND (source_user_key IS NOT NULL) AND (length(btrim((source_user_key)::text)) > 0) AND (source_review_key IS NOT NULL))));
FOREIGN KEY (user_id) REFERENCES dev.users(id) ON DELETE CASCADE;
```

### 인덱스

```sql
CREATE INDEX idx_reviews_movie_created ON dev.reviews USING btree (movie_id, created_at DESC) WHERE (deleted_at IS NULL);
CREATE UNIQUE INDEX reviews_pkey ON dev.reviews USING btree (id);
CREATE UNIQUE INDEX uq_reviews_active_user_movie ON dev.reviews USING btree (user_id, movie_id) WHERE (deleted_at IS NULL);
CREATE UNIQUE INDEX uq_reviews_source_review_key ON dev.reviews USING btree (source_review_key) WHERE (source_review_key IS NOT NULL);
```

## dev.user_refresh_tokens

종류: 테이블. 건수: 110. 저장공간: 122,880 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | bigint / int8 | NO | nextval('dev.user_refresh_tokens_id_seq'::regclass) |
| user_id | uuid / uuid | NO |  |
| token_hash | bytea / bytea | NO |  |
| issued_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |
| expires_at | timestamp with time zone / timestamptz | NO |  |
| revoked_at | timestamp with time zone / timestamptz | YES |  |
| replaced_by | bigint / int8 | YES |  |
| user_agent | character varying / varchar | YES |  |
| ip | inet / inet | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | CURRENT_TIMESTAMP |

### 제약

```sql
CHECK ((expires_at > issued_at));
FOREIGN KEY (replaced_by) REFERENCES dev.user_refresh_tokens(id) ON DELETE SET NULL;
FOREIGN KEY (user_id) REFERENCES dev.users(id) ON DELETE CASCADE;
UNIQUE (token_hash);
PRIMARY KEY (id);
```

### 인덱스

```sql
CREATE INDEX idx_user_refresh_tokens_expires ON dev.user_refresh_tokens USING btree (expires_at);
CREATE INDEX idx_user_refresh_tokens_user ON dev.user_refresh_tokens USING btree (user_id) WHERE (revoked_at IS NULL);
CREATE UNIQUE INDEX uq_user_refresh_tokens_hash ON dev.user_refresh_tokens USING btree (token_hash);
CREATE UNIQUE INDEX user_refresh_tokens_pkey ON dev.user_refresh_tokens USING btree (id);
```

## dev.users

종류: 테이블. 건수: 319. 저장공간: 204,800 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| id | uuid / uuid | NO | gen_random_uuid() |
| email | character varying / varchar | NO |  |
| nickname | character varying / varchar | NO |  |
| profile_image_url | text / text | YES |  |
| status | character varying / varchar | NO | 'ACTIVE'::character varying |
| onboarding_status | character varying / varchar | NO | 'NOT_STARTED'::character varying |
| last_login_at | timestamp with time zone / timestamptz | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | now() |
| updated_at | timestamp with time zone / timestamptz | NO | now() |
| deleted_at | timestamp with time zone / timestamptz | YES |  |
| role | character varying / varchar | YES |  |
| hashed_password | character varying / varchar | YES |  |
| failed_login_count | integer / int4 | NO | 0 |
| locked_until | timestamp with time zone / timestamptz | YES |  |
| onboarding_movie_category_ids | ARRAY / _int8 | NO | '{}'::bigint[] |

### 제약

```sql
CHECK ((failed_login_count >= 0));
CHECK ((cardinality(onboarding_movie_category_ids) <= 20));
CHECK ((array_position(onboarding_movie_category_ids, NULL::bigint) IS NULL));
CHECK (((onboarding_status)::text = ANY (ARRAY[('NOT_STARTED'::character varying)::text, ('IN_PROGRESS'::character varying)::text, ('COMPLETED'::character varying)::text, ('SKIPPED'::character varying)::text])));
PRIMARY KEY (id);
CHECK (((status)::text = ANY (ARRAY[('ACTIVE'::character varying)::text, ('SUSPENDED'::character varying)::text, ('WITHDRAWN'::character varying)::text])));
```

### 인덱스

```sql
CREATE UNIQUE INDEX uq_users_active_email ON dev.users USING btree (lower((email)::text)) WHERE (deleted_at IS NULL);
CREATE UNIQUE INDEX users_pkey ON dev.users USING btree (id);
```

## dw_control.asset_deliveries

종류: 테이블. 건수: 21. 저장공간: 49,152 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| event_id | text / text | NO |  |
| consumer_id | text / text | NO |  |
| state | text / text | NO |  |
| lease_owner | text / text | YES |  |
| lease_expires_at | timestamp with time zone / timestamptz | YES |  |
| acknowledged_at | timestamp with time zone / timestamptz | YES |  |
| completed_at | timestamp with time zone / timestamptz | YES |  |

### 제약

```sql
FOREIGN KEY (event_id) REFERENCES dw_control.asset_outbox(event_id) ON DELETE RESTRICT;
PRIMARY KEY (event_id, consumer_id);
CHECK ((state = ANY (ARRAY['PENDING'::text, 'SENDING'::text, 'ACKED'::text, 'COMPLETED'::text])));
```

### 인덱스

```sql
CREATE UNIQUE INDEX asset_deliveries_pkey ON dw_control.asset_deliveries USING btree (event_id, consumer_id);
```

## dw_control.asset_inbox

종류: 테이블. 건수: 21. 저장공간: 131,072 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| consumer_id | text / text | NO |  |
| event_id | text / text | NO |  |
| asset_uri | text / text | NO |  |
| partition_key | text / text | NO |  |
| aggregate_id | text / text | NO |  |
| event_body | jsonb / jsonb | NO |  |
| work_kind | text / text | NO |  |
| work_identity | text / text | NO |  |
| state | text / text | NO |  |
| claim_owner | text / text | YES |  |
| claim_expires_at | timestamp with time zone / timestamptz | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |
| completed_at | timestamp with time zone / timestamptz | YES |  |
| work_claim_token | bigint / int8 | YES |  |

### 제약

```sql
CHECK (((work_claim_token IS NULL) OR (work_claim_token > 0)));
UNIQUE (consumer_id, work_kind, work_identity);
FOREIGN KEY (event_id, consumer_id) REFERENCES dw_control.asset_deliveries(event_id, consumer_id) ON DELETE RESTRICT;
PRIMARY KEY (consumer_id, event_id);
CHECK ((state = ANY (ARRAY['PENDING'::text, 'CLAIMED'::text, 'COMPLETED'::text, 'FAILED'::text])));
```

### 인덱스

```sql
CREATE UNIQUE INDEX asset_inbox_consumer_id_work_kind_work_identity_key ON dw_control.asset_inbox USING btree (consumer_id, work_kind, work_identity);
CREATE UNIQUE INDEX asset_inbox_pkey ON dw_control.asset_inbox USING btree (consumer_id, event_id);
CREATE INDEX inbox_recovery_idx ON dw_control.asset_inbox USING btree (state, claim_expires_at);
```

## dw_control.asset_outbox

종류: 테이블. 건수: 14. 저장공간: 131,072 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| event_id | text / text | NO |  |
| aggregate_kind | text / text | NO |  |
| aggregate_id | text / text | NO |  |
| asset_uri | text / text | NO |  |
| partition_key | text / text | NO |  |
| event_body | jsonb / jsonb | NO |  |
| state | text / text | NO |  |
| lease_owner | text / text | YES |  |
| lease_expires_at | timestamp with time zone / timestamptz | YES |  |
| send_attempts | integer / int4 | NO | 0 |
| last_sent_at | timestamp with time zone / timestamptz | YES |  |
| completed_at | timestamp with time zone / timestamptz | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |
| publisher_claim_token | bigint / int8 | YES |  |
| last_sent_claim_token | bigint / int8 | YES |  |

### 제약

```sql
UNIQUE (aggregate_kind, aggregate_id, asset_uri, partition_key);
CHECK ((aggregate_kind = ANY (ARRAY['COHORT_READY'::text, 'MODEL_READY'::text])));
CHECK (((publisher_claim_token IS NULL) OR (publisher_claim_token > 0)));
PRIMARY KEY (event_id);
CHECK ((send_attempts >= 0));
CHECK ((state = ANY (ARRAY['PENDING'::text, 'SENDING'::text, 'COMPLETE'::text])));
```

### 인덱스

```sql
CREATE UNIQUE INDEX asset_outbox_aggregate_kind_aggregate_id_asset_uri_partitio_key ON dw_control.asset_outbox USING btree (aggregate_kind, aggregate_id, asset_uri, partition_key);
CREATE UNIQUE INDEX asset_outbox_pkey ON dw_control.asset_outbox USING btree (event_id);
CREATE INDEX outbox_recovery_idx ON dw_control.asset_outbox USING btree (state, lease_expires_at);
```

## dw_control.dbt_invocation_artifacts

종류: 테이블. 건수: 14. 저장공간: 188,416 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| orchestration_invocation_id | text / text | NO |  |
| dbt_native_invocation_id | uuid / uuid | YES |  |
| build_id | text / text | NO |  |
| attempt_no | integer / int4 | NO |  |
| fence_token | bigint / int8 | NO |  |
| cohort_manifest_id | text / text | NO |  |
| deployment_id | character / bpchar | NO |  |
| model_unique_id | text / text | NO |  |
| invocation_kind | text / text | NO |  |
| expected_unique_ids | jsonb / jsonb | NO |  |
| executed_unique_ids | jsonb / jsonb | NO |  |
| run_results_sha256 | character / bpchar | YES |  |
| argv_sha256 | character / bpchar | NO |  |
| environment_sha256 | character / bpchar | NO |  |
| artifact_path | text / text | NO |  |
| status | text / text | NO |  |
| body | jsonb / jsonb | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |

### 제약

```sql
FOREIGN KEY (build_id, attempt_no, fence_token) REFERENCES dw_control.model_build_attempts(build_id, attempt_no, fence_token) ON DELETE RESTRICT;
UNIQUE (build_id, attempt_no, fence_token, invocation_kind);
CHECK ((((invocation_kind = 'EMPTY_TEST_SET'::text) AND (status = 'EMPTY_TEST_SET'::text) AND (dbt_native_invocation_id IS NULL) AND (run_results_sha256 IS NULL) AND (expected_unique_ids = '[]'::jsonb) AND (executed_unique_ids = '[]'::jsonb)) OR ((invocation_kind = ANY (ARRAY['MODEL'::text, 'OWNED_TESTS'::text])) AND (status = 'SUCCEEDED'::text) AND (dbt_native_invocation_id IS NOT NULL) AND (run_results_sha256 IS NOT NULL))));
CHECK ((jsonb_typeof(executed_unique_ids) = 'array'::text));
CHECK ((jsonb_typeof(expected_unique_ids) = 'array'::text));
FOREIGN KEY (orchestration_invocation_id) REFERENCES dw_control.dbt_invocation_reservations(orchestration_invocation_id) ON DELETE RESTRICT;
PRIMARY KEY (orchestration_invocation_id);
CHECK ((status = ANY (ARRAY['SUCCEEDED'::text, 'EMPTY_TEST_SET'::text])));
```

### 인덱스

```sql
CREATE UNIQUE INDEX dbt_invocation_artifacts_build_id_attempt_no_fence_token_in_key ON dw_control.dbt_invocation_artifacts USING btree (build_id, attempt_no, fence_token, invocation_kind);
CREATE UNIQUE INDEX dbt_invocation_artifacts_pkey ON dw_control.dbt_invocation_artifacts USING btree (orchestration_invocation_id);
```

## dw_control.dbt_invocation_registries

종류: 테이블. 건수: 1. 저장공간: 196,608 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| deployment_id | character / bpchar | NO |  |
| graph_digest | character / bpchar | NO |  |
| invocation_registry_digest | character / bpchar | NO |  |
| release_registry_digest | character / bpchar | NO |  |
| body | jsonb / jsonb | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |
| manifest_sha256 | character / bpchar | NO |  |
| model_version | character / bpchar | NO |  |

### 제약

```sql
UNIQUE (deployment_id, invocation_registry_digest);
FOREIGN KEY (deployment_id, release_registry_digest) REFERENCES dw_control.release_delivery_registries(deployment_id, registry_digest) ON DELETE RESTRICT;
PRIMARY KEY (deployment_id);
UNIQUE (deployment_id, manifest_sha256, model_version);
```

### 인덱스

```sql
CREATE UNIQUE INDEX dbt_invocation_registries_deployment_id_invocation_registry_key ON dw_control.dbt_invocation_registries USING btree (deployment_id, invocation_registry_digest);
CREATE UNIQUE INDEX dbt_invocation_registries_pkey ON dw_control.dbt_invocation_registries USING btree (deployment_id);
CREATE UNIQUE INDEX dbt_invocation_registry_deployment_identity_unique ON dw_control.dbt_invocation_registries USING btree (deployment_id, manifest_sha256, model_version);
```

## dw_control.dbt_invocation_reservations

종류: 테이블. 건수: 14. 저장공간: 172,032 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| orchestration_invocation_id | text / text | NO |  |
| build_id | text / text | NO |  |
| attempt_no | integer / int4 | NO |  |
| fence_token | bigint / int8 | NO |  |
| cohort_manifest_id | text / text | NO |  |
| plan_id | text / text | NO |  |
| deployment_id | character / bpchar | NO |  |
| model_unique_id | text / text | NO |  |
| invocation_kind | text / text | NO |  |
| contract_sha256 | character / bpchar | NO |  |
| contract_body | jsonb / jsonb | NO |  |
| claim_owner | text / text | NO |  |
| state | text / text | NO |  |
| dbt_native_invocation_id | uuid / uuid | YES |  |
| artifact_path | text / text | YES |  |
| heartbeat_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |
| started_at | timestamp with time zone / timestamptz | YES |  |
| completed_at | timestamp with time zone / timestamptz | YES |  |
| failure_reason | text / text | YES |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |
| manifest_sha256 | character / bpchar | NO |  |
| model_version | character / bpchar | NO |  |

### 제약

```sql
FOREIGN KEY (deployment_id, manifest_sha256, model_version) REFERENCES dw_control.dbt_invocation_registries(deployment_id, manifest_sha256, model_version) ON DELETE RESTRICT;
FOREIGN KEY (build_id, attempt_no, fence_token) REFERENCES dw_control.model_build_attempts(build_id, attempt_no, fence_token) ON DELETE RESTRICT;
UNIQUE (build_id, attempt_no, fence_token, invocation_kind);
CHECK ((((invocation_kind = 'EMPTY_TEST_SET'::text) AND (dbt_native_invocation_id IS NULL)) OR (invocation_kind <> 'EMPTY_TEST_SET'::text)));
FOREIGN KEY (cohort_manifest_id, plan_id, model_unique_id, build_id) REFERENCES dw_control.model_cohorts(cohort_manifest_id, plan_id, model_unique_id, build_id) ON DELETE RESTRICT;
FOREIGN KEY (deployment_id, model_unique_id) REFERENCES dw_control.dbt_model_invocation_specs(deployment_id, model_unique_id) ON DELETE RESTRICT;
CHECK ((invocation_kind = ANY (ARRAY['MODEL'::text, 'OWNED_TESTS'::text, 'EMPTY_TEST_SET'::text])));
PRIMARY KEY (orchestration_invocation_id);
CHECK ((state = ANY (ARRAY['RESERVED'::text, 'RUNNING'::text, 'FILE_SEALED'::text, 'COMPLETED'::text, 'FAILED'::text, 'ABANDONED'::text])));
```

### 인덱스

```sql
CREATE INDEX dbt_invocation_recovery_idx ON dw_control.dbt_invocation_reservations USING btree (state, heartbeat_at);
CREATE UNIQUE INDEX dbt_invocation_reservations_build_id_attempt_no_fence_token_key ON dw_control.dbt_invocation_reservations USING btree (build_id, attempt_no, fence_token, invocation_kind);
CREATE UNIQUE INDEX dbt_invocation_reservations_pkey ON dw_control.dbt_invocation_reservations USING btree (orchestration_invocation_id);
```

## dw_control.dbt_model_invocation_specs

종류: 테이블. 건수: 7. 저장공간: 49,152 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| deployment_id | character / bpchar | NO |  |
| model_unique_id | text / text | NO |  |
| model_selector | text / text | NO |  |
| owned_test_unique_ids | jsonb / jsonb | NO |  |
| owned_test_selectors | jsonb / jsonb | NO |  |
| body_sha256 | character / bpchar | NO |  |
| invocation_registry_digest | character / bpchar | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |

### 제약

```sql
FOREIGN KEY (deployment_id, invocation_registry_digest) REFERENCES dw_control.dbt_invocation_registries(deployment_id, invocation_registry_digest) ON DELETE RESTRICT;
CHECK ((jsonb_typeof(owned_test_selectors) = 'array'::text));
CHECK ((jsonb_typeof(owned_test_unique_ids) = 'array'::text));
PRIMARY KEY (deployment_id, model_unique_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX dbt_model_invocation_specs_pkey ON dw_control.dbt_model_invocation_specs USING btree (deployment_id, model_unique_id);
```

## dw_control.generation_gate_receipts

종류: 테이블. 건수: 4. 저장공간: 90,112 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| gate_receipt_id | text / text | NO |  |
| gate_kind | text / text | NO |  |
| generation_id | text / text | NO |  |
| plan_id | text / text | NO |  |
| deployment_id | character / bpchar | NO |  |
| owner_unique_id | text / text | NO |  |
| source_snapshot_id | text / text | YES |  |
| evidence_sha256 | character / bpchar | NO |  |
| body | jsonb / jsonb | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |

### 제약

```sql
CHECK ((((gate_kind = 'SOURCE_GATE'::text) AND (source_snapshot_id IS NOT NULL)) OR ((gate_kind = 'DEPLOYMENT_GATE'::text) AND (source_snapshot_id IS NULL))));
CHECK ((gate_kind = ANY (ARRAY['SOURCE_GATE'::text, 'DEPLOYMENT_GATE'::text])));
PRIMARY KEY (gate_receipt_id);
FOREIGN KEY (plan_id) REFERENCES dw_control.generation_plans(plan_id) ON DELETE RESTRICT;
UNIQUE (plan_id, gate_kind, owner_unique_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX generation_gate_receipts_pkey ON dw_control.generation_gate_receipts USING btree (gate_receipt_id);
CREATE UNIQUE INDEX generation_gate_receipts_plan_id_gate_kind_owner_unique_id_key ON dw_control.generation_gate_receipts USING btree (plan_id, gate_kind, owner_unique_id);
```

## dw_control.generation_plans

종류: 테이블. 건수: 1. 저장공간: 172,032 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| plan_id | text / text | NO |  |
| generation_id | text / text | NO |  |
| generation_sequence | bigint / int8 | NO |  |
| deployment_id | character / bpchar | NO |  |
| graph_digest | character / bpchar | NO |  |
| body | jsonb / jsonb | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |

### 제약

```sql
UNIQUE (generation_id);
CHECK ((generation_sequence > 0));
UNIQUE (generation_sequence);
PRIMARY KEY (plan_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX generation_plans_generation_id_key ON dw_control.generation_plans USING btree (generation_id);
CREATE UNIQUE INDEX generation_plans_generation_sequence_key ON dw_control.generation_plans USING btree (generation_sequence);
CREATE UNIQUE INDEX generation_plans_pkey ON dw_control.generation_plans USING btree (plan_id);
```

## dw_control.model_build_attempts

종류: 테이블. 건수: 7. 저장공간: 98,304 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| build_id | text / text | NO |  |
| attempt_no | integer / int4 | NO |  |
| fence_token | bigint / int8 | NO |  |
| claim_owner | text / text | NO |  |
| state | text / text | NO |  |
| lease_expires_at | timestamp with time zone / timestamptz | NO |  |
| heartbeat_at | timestamp with time zone / timestamptz | NO |  |
| relation_id | text / text | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |
| completed_at | timestamp with time zone / timestamptz | YES |  |

### 제약

```sql
CHECK ((attempt_no > 0));
UNIQUE (build_id, attempt_no, fence_token);
FOREIGN KEY (build_id) REFERENCES dw_control.model_build_slots(build_id) ON DELETE RESTRICT;
CHECK ((fence_token > 0));
UNIQUE (fence_token);
PRIMARY KEY (build_id, attempt_no);
UNIQUE (relation_id);
CHECK ((state = ANY (ARRAY['CLAIMED'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'EXPIRED'::text])));
```

### 인덱스

```sql
CREATE UNIQUE INDEX model_build_attempts_build_id_attempt_no_fence_token_key ON dw_control.model_build_attempts USING btree (build_id, attempt_no, fence_token);
CREATE UNIQUE INDEX model_build_attempts_fence_token_key ON dw_control.model_build_attempts USING btree (fence_token);
CREATE UNIQUE INDEX model_build_attempts_pkey ON dw_control.model_build_attempts USING btree (build_id, attempt_no);
CREATE UNIQUE INDEX model_build_attempts_relation_id_key ON dw_control.model_build_attempts USING btree (relation_id);
```

## dw_control.model_build_slots

종류: 테이블. 건수: 7. 저장공간: 98,304 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| plan_id | text / text | NO |  |
| model_unique_id | text / text | NO |  |
| build_id | text / text | NO |  |
| mode | text / text | NO |  |
| state | text / text | NO |  |
| reuse_result_manifest_id | text / text | YES |  |
| current_attempt_no | integer / int4 | YES |  |
| current_fence_token | bigint / int8 | YES |  |
| claim_owner | text / text | YES |  |
| claim_expires_at | timestamp with time zone / timestamptz | YES |  |
| heartbeat_at | timestamp with time zone / timestamptz | YES |  |
| result_manifest_id | text / text | YES |  |
| rebuilt_result_manifest_id | text / text | YES |  |

### 제약

```sql
UNIQUE (build_id);
CHECK ((((mode = 'REBUILD'::text) AND (reuse_result_manifest_id IS NULL)) OR ((mode = 'REUSE'::text) AND (reuse_result_manifest_id IS NOT NULL))));
CHECK ((((state = 'PLANNED'::text) AND (mode = 'REBUILD'::text) AND (result_manifest_id IS NULL)) OR ((state = 'CLAIMED'::text) AND (mode = 'REBUILD'::text) AND (result_manifest_id IS NULL) AND (current_attempt_no IS NOT NULL) AND (current_fence_token IS NOT NULL) AND (claim_owner IS NOT NULL) AND (claim_expires_at IS NOT NULL)) OR ((state = 'FAILED'::text) AND (mode = 'REBUILD'::text) AND (result_manifest_id IS NULL)) OR ((state = 'SUCCEEDED'::text) AND (mode = 'REBUILD'::text) AND (result_manifest_id IS NOT NULL)) OR ((state = 'REUSED'::text) AND (mode = 'REUSE'::text) AND (result_manifest_id IS NOT NULL))));
CHECK (((current_attempt_no IS NULL) OR (current_attempt_no > 0)));
CHECK (((current_fence_token IS NULL) OR (current_fence_token > 0)));
CHECK ((mode = ANY (ARRAY['REBUILD'::text, 'REUSE'::text])));
PRIMARY KEY (plan_id, model_unique_id);
FOREIGN KEY (plan_id) REFERENCES dw_control.generation_plans(plan_id) ON DELETE RESTRICT;
UNIQUE (plan_id, model_unique_id, build_id);
FOREIGN KEY (rebuilt_result_manifest_id, plan_id, model_unique_id, build_id) REFERENCES dw_control.model_results(result_manifest_id, plan_id, model_unique_id, build_id) ON DELETE RESTRICT;
FOREIGN KEY (reuse_result_manifest_id) REFERENCES dw_control.model_results(result_manifest_id) ON DELETE RESTRICT;
CHECK (((state <> 'REUSED'::text) OR (result_manifest_id = reuse_result_manifest_id)));
CHECK ((state = ANY (ARRAY['PLANNED'::text, 'CLAIMED'::text, 'SUCCEEDED'::text, 'REUSED'::text, 'FAILED'::text])));
```

### 인덱스

```sql
CREATE UNIQUE INDEX model_build_slots_build_id_key ON dw_control.model_build_slots USING btree (build_id);
CREATE UNIQUE INDEX model_build_slots_pkey ON dw_control.model_build_slots USING btree (plan_id, model_unique_id);
CREATE UNIQUE INDEX model_build_slots_plan_id_model_unique_id_build_id_key ON dw_control.model_build_slots USING btree (plan_id, model_unique_id, build_id);
CREATE INDEX model_slots_recovery_idx ON dw_control.model_build_slots USING btree (state, claim_expires_at);
```

## dw_control.model_cohorts

종류: 테이블. 건수: 7. 저장공간: 106,496 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| cohort_manifest_id | text / text | NO |  |
| plan_id | text / text | NO |  |
| model_unique_id | text / text | NO |  |
| build_id | text / text | NO |  |
| body | jsonb / jsonb | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |

### 제약

```sql
UNIQUE (build_id);
UNIQUE (cohort_manifest_id, plan_id, model_unique_id, build_id);
PRIMARY KEY (cohort_manifest_id);
FOREIGN KEY (plan_id, model_unique_id, build_id) REFERENCES dw_control.model_build_slots(plan_id, model_unique_id, build_id) ON DELETE RESTRICT;
```

### 인덱스

```sql
CREATE UNIQUE INDEX model_cohorts_build_id_key ON dw_control.model_cohorts USING btree (build_id);
CREATE UNIQUE INDEX model_cohorts_cohort_manifest_id_plan_id_model_unique_id_bu_key ON dw_control.model_cohorts USING btree (cohort_manifest_id, plan_id, model_unique_id, build_id);
CREATE UNIQUE INDEX model_cohorts_pkey ON dw_control.model_cohorts USING btree (cohort_manifest_id);
```

## dw_control.model_execution_receipts

종류: 테이블. 건수: 7. 저장공간: 204,800 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| build_id | text / text | NO |  |
| attempt_no | integer / int4 | NO |  |
| fence_token | bigint / int8 | NO |  |
| cohort_manifest_id | text / text | NO |  |
| deployment_id | character / bpchar | NO |  |
| parent_vector_sha256 | character / bpchar | NO |  |
| claim_owner | text / text | NO |  |
| relation_id | text / text | NO |  |
| row_count | bigint / int8 | NO |  |
| content_sha256 | character / bpchar | NO |  |
| model_invocation_id | text / text | NO |  |
| model_unique_id | text / text | NO |  |
| model_run_results_sha256 | character / bpchar | NO |  |
| test_invocation_id | text / text | YES |  |
| test_run_results_sha256 | character / bpchar | YES |  |
| executed_test_ids | jsonb / jsonb | NO |  |
| tests_sha256 | character / bpchar | NO |  |
| relation_validation_query_id | text / text | NO |  |
| status | text / text | NO |  |
| body | jsonb / jsonb | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |

### 제약

```sql
FOREIGN KEY (build_id, attempt_no, fence_token) REFERENCES dw_control.model_build_attempts(build_id, attempt_no, fence_token) ON DELETE RESTRICT;
CHECK ((((status = 'EMPTY_TEST_SET'::text) AND (test_invocation_id IS NULL) AND (test_run_results_sha256 IS NULL) AND (executed_test_ids = '[]'::jsonb)) OR ((status = ANY (ARRAY['SUCCEEDED'::text, 'FAILED'::text])) AND (test_invocation_id IS NOT NULL) AND (test_run_results_sha256 IS NOT NULL))));
FOREIGN KEY (cohort_manifest_id) REFERENCES dw_control.model_cohorts(cohort_manifest_id) ON DELETE RESTRICT;
PRIMARY KEY (build_id, attempt_no);
CHECK ((row_count >= 0));
CHECK ((status = ANY (ARRAY['SUCCEEDED'::text, 'FAILED'::text, 'EMPTY_TEST_SET'::text])));
```

### 인덱스

```sql
CREATE UNIQUE INDEX model_execution_receipts_pkey ON dw_control.model_execution_receipts USING btree (build_id, attempt_no);
```

## dw_control.model_results

종류: 테이블. 건수: 7. 저장공간: 237,568 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| result_manifest_id | text / text | NO |  |
| plan_id | text / text | NO |  |
| cohort_manifest_id | text / text | NO |  |
| model_unique_id | text / text | NO |  |
| build_id | text / text | NO |  |
| attempt_no | integer / int4 | NO |  |
| fence_token | bigint / int8 | NO |  |
| relation_id | text / text | NO |  |
| row_count | bigint / int8 | NO |  |
| content_sha256 | character / bpchar | NO |  |
| body | jsonb / jsonb | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |

### 제약

```sql
FOREIGN KEY (build_id, attempt_no, fence_token) REFERENCES dw_control.model_build_attempts(build_id, attempt_no, fence_token) ON DELETE RESTRICT;
FOREIGN KEY (build_id, attempt_no) REFERENCES dw_control.model_execution_receipts(build_id, attempt_no) ON DELETE RESTRICT;
UNIQUE (build_id);
UNIQUE (cohort_manifest_id);
FOREIGN KEY (cohort_manifest_id, plan_id, model_unique_id, build_id) REFERENCES dw_control.model_cohorts(cohort_manifest_id, plan_id, model_unique_id, build_id) ON DELETE RESTRICT;
PRIMARY KEY (result_manifest_id);
FOREIGN KEY (plan_id, model_unique_id, build_id) REFERENCES dw_control.model_build_slots(plan_id, model_unique_id, build_id) ON DELETE RESTRICT;
UNIQUE (relation_id);
CHECK ((row_count >= 0));
UNIQUE (result_manifest_id, plan_id, model_unique_id, build_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX model_results_build_id_key ON dw_control.model_results USING btree (build_id);
CREATE UNIQUE INDEX model_results_cohort_manifest_id_key ON dw_control.model_results USING btree (cohort_manifest_id);
CREATE UNIQUE INDEX model_results_pkey ON dw_control.model_results USING btree (result_manifest_id);
CREATE UNIQUE INDEX model_results_relation_id_key ON dw_control.model_results USING btree (relation_id);
CREATE UNIQUE INDEX model_results_slot_identity_unique ON dw_control.model_results USING btree (result_manifest_id, plan_id, model_unique_id, build_id);
```

## dw_control.release_delivery_registries

종류: 테이블. 건수: 1. 저장공간: 81,920 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| deployment_id | character / bpchar | NO |  |
| graph_digest | character / bpchar | NO |  |
| registry_digest | character / bpchar | NO |  |
| body | jsonb / jsonb | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |

### 제약

```sql
UNIQUE (deployment_id, registry_digest);
PRIMARY KEY (deployment_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX release_delivery_registries_deployment_id_registry_digest_key ON dw_control.release_delivery_registries USING btree (deployment_id, registry_digest);
CREATE UNIQUE INDEX release_delivery_registries_pkey ON dw_control.release_delivery_registries USING btree (deployment_id);
```

## dw_control.release_delivery_specs

종류: 테이블. 건수: 7. 저장공간: 81,920 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| deployment_id | character / bpchar | NO |  |
| model_unique_id | text / text | NO |  |
| asset_uri | text / text | NO |  |
| recipient_work | jsonb / jsonb | NO |  |
| body_sha256 | character / bpchar | NO |  |
| created_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |
| registry_digest | character / bpchar | NO |  |

### 제약

```sql
UNIQUE (deployment_id, asset_uri);
PRIMARY KEY (deployment_id, model_unique_id);
FOREIGN KEY (deployment_id, registry_digest) REFERENCES dw_control.release_delivery_registries(deployment_id, registry_digest) ON DELETE RESTRICT;
```

### 인덱스

```sql
CREATE UNIQUE INDEX release_delivery_specs_deployment_id_asset_uri_key ON dw_control.release_delivery_specs USING btree (deployment_id, asset_uri);
CREATE UNIQUE INDEX release_delivery_specs_pkey ON dw_control.release_delivery_specs USING btree (deployment_id, model_unique_id);
```

## dw_control.schema_migrations

종류: 테이블. 건수: 7. 저장공간: 32,768 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| version | integer / int4 | NO |  |
| name | text / text | NO |  |
| sha256 | character / bpchar | NO |  |
| applied_at | timestamp with time zone / timestamptz | NO | clock_timestamp() |

### 제약

```sql
PRIMARY KEY (version);
```

### 인덱스

```sql
CREATE UNIQUE INDEX schema_migrations_pkey ON dw_control.schema_migrations USING btree (version);
```

## dw_serving.active_publications

종류: 테이블. 건수: 1. 저장공간: 32,768 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| dataset_name | text / text | NO |  |
| publication_id | text / text | NO |  |
| activated_at | timestamp with time zone / timestamptz | NO |  |

### 제약

```sql
PRIMARY KEY (dataset_name);
FOREIGN KEY (publication_id) REFERENCES dw_serving.dataset_publications(publication_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX active_publications_pkey ON dw_serving.active_publications USING btree (dataset_name);
```

## dw_serving.active_publications_v2

종류: 테이블. 건수: 2. 저장공간: 32,768 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| dataset_name | text / text | NO |  |
| publication_id | text / text | NO |  |
| activated_at | timestamp with time zone / timestamptz | NO |  |

### 제약

```sql
PRIMARY KEY (dataset_name);
FOREIGN KEY (publication_id) REFERENCES dw_serving.dataset_publications_v2(publication_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX active_publications_v2_pkey ON dw_serving.active_publications_v2 USING btree (dataset_name);
```

## dw_serving.active_publications_v3

종류: 테이블. 건수: 3. 저장공간: 32,768 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| dataset_name | text / text | NO |  |
| publication_id | text / text | NO |  |
| activated_at | timestamp with time zone / timestamptz | NO |  |
| source_generation | bigint / int8 | YES |  |

### 제약

```sql
PRIMARY KEY (dataset_name);
FOREIGN KEY (publication_id) REFERENCES dw_serving.dataset_publications_v3(publication_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX active_publications_v3_pkey ON dw_serving.active_publications_v3 USING btree (dataset_name);
```

## dw_serving.boxoffice_daily_current

종류: 뷰. 건수: 뷰 — 별도 집계. 저장공간: 0 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | YES |  |
| boxoffice_key | text / text | YES |  |
| payload | jsonb / jsonb | YES |  |

### 뷰 정의

```sql
 SELECT s.publication_id,
    s.boxoffice_key,
    s.payload
   FROM dw_serving.boxoffice_snapshot_v3 s
     JOIN dw_serving.active_publications_v3 a ON a.dataset_name = 'movie_gold'::text AND a.publication_id = s.publication_id;
```

## dw_serving.boxoffice_daily_snapshot

종류: 테이블. 건수: 70. 저장공간: 106,496 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | NO |  |
| boxoffice_key | text / text | NO |  |
| movie_key | text / text | NO |  |
| target_date | date / date | NO |  |
| kofic_movie_cd | text / text | NO |  |
| rank | integer / int4 | NO |  |
| sales_amount | bigint / int8 | NO |  |
| audience_count | bigint / int8 | NO |  |
| audience_accumulated | bigint / int8 | NO |  |
| source_run_id | text / text | NO |  |
| artifact_version | text / text | NO |  |
| publication_revision | integer / int4 | NO |  |
| source_observed_at | timestamp with time zone / timestamptz | NO |  |
| record_sha256 | text / text | NO |  |

### 제약

```sql
PRIMARY KEY (publication_id, boxoffice_key);
FOREIGN KEY (publication_id) REFERENCES dw_serving.dataset_publications(publication_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX boxoffice_daily_snapshot_pkey ON dw_serving.boxoffice_daily_snapshot USING btree (publication_id, boxoffice_key);
```

## dw_serving.boxoffice_daily_snapshot_v2

종류: 테이블. 건수: 210. 저장공간: 221,184 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | NO |  |
| boxoffice_key | text / text | NO |  |
| movie_key | text / text | NO |  |
| target_date | date / date | NO |  |
| kofic_movie_cd | text / text | NO |  |
| rank | integer / int4 | NO |  |
| sales_amount | bigint / int8 | NO |  |
| audience_count | bigint / int8 | NO |  |
| audience_accumulated | bigint / int8 | NO |  |
| source_run_id | text / text | NO |  |
| artifact_version | text / text | NO |  |
| publication_revision | integer / int4 | NO |  |
| source_observed_at | timestamp with time zone / timestamptz | NO |  |
| record_sha256 | text / text | NO |  |
| row_sha256 | text / text | NO |  |

### 제약

```sql
PRIMARY KEY (publication_id, boxoffice_key);
FOREIGN KEY (publication_id) REFERENCES dw_serving.dataset_publications_v2(publication_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX boxoffice_daily_snapshot_v2_pkey ON dw_serving.boxoffice_daily_snapshot_v2 USING btree (publication_id, boxoffice_key);
```

## dw_serving.boxoffice_snapshot_v3

종류: 테이블. 건수: 740. 저장공간: 1,490,944 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | NO |  |
| boxoffice_key | text / text | NO |  |
| payload | jsonb / jsonb | NO |  |
| row_sha256 | text / text | NO |  |

### 제약

```sql
PRIMARY KEY (publication_id, boxoffice_key);
FOREIGN KEY (publication_id) REFERENCES dw_serving.dataset_publications_v3(publication_id);
```

### 인덱스

```sql
CREATE INDEX boxoffice_snapshot_movie_date_v3 ON dw_serving.boxoffice_snapshot_v3 USING btree (publication_id, ((payload ->> 'KOFIC_MOVIE_CD'::text)), ((payload ->> 'TARGET_DATE'::text)) DESC);
CREATE UNIQUE INDEX boxoffice_snapshot_v3_pkey ON dw_serving.boxoffice_snapshot_v3 USING btree (publication_id, boxoffice_key);
```

## dw_serving.dataset_publications

종류: 테이블. 건수: 1. 저장공간: 49,152 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | NO |  |
| snapshot_sha256 | text / text | NO |  |
| movie_count | integer / int4 | NO |  |
| boxoffice_count | integer / int4 | NO |  |
| quality_count | integer / int4 | NO |  |
| source_as_of | timestamp with time zone / timestamptz | NO |  |
| published_at | timestamp with time zone / timestamptz | NO |  |

### 제약

```sql
PRIMARY KEY (publication_id);
UNIQUE (snapshot_sha256);
```

### 인덱스

```sql
CREATE UNIQUE INDEX dataset_publications_pkey ON dw_serving.dataset_publications USING btree (publication_id);
CREATE UNIQUE INDEX dataset_publications_snapshot_sha256_key ON dw_serving.dataset_publications USING btree (snapshot_sha256);
```

## dw_serving.dataset_publications_v2

종류: 테이블. 건수: 3. 저장공간: 49,152 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | NO |  |
| snapshot_sha256 | text / text | NO |  |
| movie_count | integer / int4 | NO |  |
| boxoffice_count | integer / int4 | NO |  |
| quality_count | integer / int4 | NO |  |
| source_as_of | timestamp with time zone / timestamptz | NO |  |
| max_revision | integer / int4 | NO |  |
| generation_completed_at | timestamp with time zone / timestamptz | NO |  |
| published_at | timestamp with time zone / timestamptz | NO |  |

### 제약

```sql
PRIMARY KEY (publication_id);
UNIQUE (snapshot_sha256);
```

### 인덱스

```sql
CREATE UNIQUE INDEX dataset_publications_v2_pkey ON dw_serving.dataset_publications_v2 USING btree (publication_id);
CREATE UNIQUE INDEX dataset_publications_v2_snapshot_sha256_key ON dw_serving.dataset_publications_v2 USING btree (snapshot_sha256);
```

## dw_serving.dataset_publications_v3

종류: 테이블. 건수: 10. 저장공간: 49,152 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | NO |  |
| generation | bigint / int8 | NO | nextval('dw_serving.publication_generation_v3'::regclass) |
| model_version | text / text | NO |  |
| snapshot_sha256 | text / text | NO |  |
| movie_count | integer / int4 | NO |  |
| boxoffice_count | integer / int4 | NO |  |
| quality_count | integer / int4 | NO |  |
| published_at | timestamp with time zone / timestamptz | NO |  |

### 제약

```sql
UNIQUE (generation);
PRIMARY KEY (publication_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX dataset_publications_v3_generation_key ON dw_serving.dataset_publications_v3 USING btree (generation);
CREATE UNIQUE INDEX dataset_publications_v3_pkey ON dw_serving.dataset_publications_v3 USING btree (publication_id);
```

## dw_serving.gold_build_registry_v3

종류: 테이블. 건수: 12. 저장공간: 49,152 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | NO |  |
| generation | bigint / int8 | NO | nextval('dw_serving.publication_generation_v3'::regclass) |
| model_version | text / text | NO |  |
| snapshot_sha256 | text / text | NO |  |
| registered_at | timestamp with time zone / timestamptz | NO |  |

### 제약

```sql
UNIQUE (generation);
PRIMARY KEY (publication_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX gold_build_registry_v3_generation_key ON dw_serving.gold_build_registry_v3 USING btree (generation);
CREATE UNIQUE INDEX gold_build_registry_v3_pkey ON dw_serving.gold_build_registry_v3 USING btree (publication_id);
```

## dw_serving.movie_catalog

종류: 테이블. 건수: 100. 저장공간: 352,256 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| movie_key | text / text | NO |  |
| ingestion_source | text / text | NO |  |
| source_movie_id | bigint / int8 | NO |  |
| kofic_movie_cd | text / text | YES |  |
| kmdb_id | text / text | YES |  |
| title_ko | text / text | NO |  |
| title_en | text / text | YES |  |
| title_original | text / text | YES |  |
| release_date | date / date | YES |  |
| production_year | integer / int4 | YES |  |
| runtime_minutes | integer / int4 | YES |  |
| movie_type | text / text | YES |  |
| production_status | text / text | YES |  |
| production_countries | jsonb / jsonb | NO | '[]'::jsonb |
| representative_country | text / text | YES |  |
| genres | jsonb / jsonb | NO | '[]'::jsonb |
| representative_genre | text / text | YES |  |
| directors | jsonb / jsonb | NO | '[]'::jsonb |
| actors | jsonb / jsonb | NO | '[]'::jsonb |
| viewing_grade | text / text | YES |  |
| poster_url | text / text | YES |  |
| plot | text / text | YES |  |
| service_status | text / text | YES |  |
| approval_status | text / text | YES |  |
| source_system | text / text | YES |  |
| source_synced_at | timestamp without time zone / timestamp | YES |  |
| source_as_of | timestamp without time zone / timestamp | YES |  |
| published_run_id | text / text | NO |  |
| published_at | timestamp with time zone / timestamptz | NO |  |
| record_sha256 | text / text | NO |  |

### 제약

```sql
UNIQUE (ingestion_source, source_movie_id);
PRIMARY KEY (movie_key);
```

### 인덱스

```sql
CREATE INDEX idx_movie_catalog_release_date ON dw_serving.movie_catalog USING btree (release_date);
CREATE INDEX idx_movie_catalog_title_ko ON dw_serving.movie_catalog USING btree (title_ko);
CREATE UNIQUE INDEX movie_catalog_ingestion_source_source_movie_id_key ON dw_serving.movie_catalog USING btree (ingestion_source, source_movie_id);
CREATE UNIQUE INDEX movie_catalog_pkey ON dw_serving.movie_catalog USING btree (movie_key);
```

## dw_serving.movie_catalog_current

종류: 뷰. 건수: 뷰 — 별도 집계. 저장공간: 0 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | YES |  |
| movie_key | text / text | YES |  |
| payload | jsonb / jsonb | YES |  |

### 뷰 정의

```sql
 SELECT s.publication_id,
    s.movie_key,
    s.payload
   FROM dw_serving.movie_snapshot_v3 s
     JOIN dw_serving.active_publications_v3 a ON a.dataset_name = 'movie_gold'::text AND a.publication_id = s.publication_id;
```

## dw_serving.movie_catalog_snapshot

종류: 테이블. 건수: 107. 저장공간: 212,992 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | NO |  |
| movie_key | text / text | NO |  |
| canonical_movie_key | text / text | NO |  |
| kofic_movie_cd | text / text | YES |  |
| kmdb_id | text / text | YES |  |
| title_ko | text / text | NO |  |
| title_en | text / text | YES |  |
| title_original | text / text | YES |  |
| release_date | date / date | YES |  |
| production_year | integer / int4 | YES |  |
| runtime_minutes | integer / int4 | YES |  |
| movie_type | text / text | YES |  |
| production_status | text / text | YES |  |
| countries | jsonb / jsonb | NO |  |
| genres | jsonb / jsonb | NO |  |
| directors | jsonb / jsonb | NO |  |
| actors | jsonb / jsonb | NO |  |
| viewing_grade | text / text | YES |  |
| poster_url | text / text | YES |  |
| plot | text / text | YES |  |
| policy_eligible | boolean / bool | NO |  |
| policy_exclusion_reasons | jsonb / jsonb | NO |  |
| policy_version | text / text | YES |  |
| ingestion_source | text / text | YES |  |
| source_run_id | text / text | NO |  |
| artifact_version | text / text | NO |  |
| publication_revision | integer / int4 | NO |  |
| source_observed_at | timestamp with time zone / timestamptz | NO |  |
| record_sha256 | text / text | NO |  |

### 제약

```sql
PRIMARY KEY (publication_id, movie_key);
FOREIGN KEY (publication_id) REFERENCES dw_serving.dataset_publications(publication_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX movie_catalog_snapshot_pkey ON dw_serving.movie_catalog_snapshot USING btree (publication_id, movie_key);
```

## dw_serving.movie_catalog_snapshot_v2

종류: 테이블. 건수: 321. 저장공간: 622,592 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | NO |  |
| movie_key | text / text | NO |  |
| canonical_movie_key | text / text | NO |  |
| kofic_movie_cd | text / text | YES |  |
| kmdb_id | text / text | YES |  |
| title_ko | text / text | NO |  |
| title_en | text / text | YES |  |
| title_original | text / text | YES |  |
| release_date | date / date | YES |  |
| production_year | integer / int4 | YES |  |
| runtime_minutes | integer / int4 | YES |  |
| movie_type | text / text | YES |  |
| production_status | text / text | YES |  |
| countries | jsonb / jsonb | NO |  |
| genres | jsonb / jsonb | NO |  |
| directors | jsonb / jsonb | NO |  |
| actors | jsonb / jsonb | NO |  |
| viewing_grade | text / text | YES |  |
| poster_url | text / text | YES |  |
| plot | text / text | YES |  |
| policy_eligible | boolean / bool | NO |  |
| policy_exclusion_reasons | jsonb / jsonb | NO |  |
| policy_version | text / text | YES |  |
| ingestion_source | text / text | YES |  |
| source_run_id | text / text | NO |  |
| artifact_version | text / text | NO |  |
| publication_revision | integer / int4 | NO |  |
| source_observed_at | timestamp with time zone / timestamptz | NO |  |
| record_sha256 | text / text | NO |  |
| row_sha256 | text / text | NO |  |

### 제약

```sql
PRIMARY KEY (publication_id, movie_key);
FOREIGN KEY (publication_id) REFERENCES dw_serving.dataset_publications_v2(publication_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX movie_catalog_snapshot_v2_pkey ON dw_serving.movie_catalog_snapshot_v2 USING btree (publication_id, movie_key);
```

## dw_serving.movie_catalog_staging

종류: 테이블. 건수: 100. 저장공간: 319,488 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| run_id | text / text | NO |  |
| movie_key | text / text | NO |  |
| ingestion_source | text / text | NO |  |
| source_movie_id | bigint / int8 | NO |  |
| kofic_movie_cd | text / text | YES |  |
| kmdb_id | text / text | YES |  |
| title_ko | text / text | NO |  |
| title_en | text / text | YES |  |
| title_original | text / text | YES |  |
| release_date | date / date | YES |  |
| production_year | integer / int4 | YES |  |
| runtime_minutes | integer / int4 | YES |  |
| movie_type | text / text | YES |  |
| production_status | text / text | YES |  |
| production_countries | jsonb / jsonb | NO | '[]'::jsonb |
| representative_country | text / text | YES |  |
| genres | jsonb / jsonb | NO | '[]'::jsonb |
| representative_genre | text / text | YES |  |
| directors | jsonb / jsonb | NO | '[]'::jsonb |
| actors | jsonb / jsonb | NO | '[]'::jsonb |
| viewing_grade | text / text | YES |  |
| poster_url | text / text | YES |  |
| plot | text / text | YES |  |
| service_status | text / text | YES |  |
| approval_status | text / text | YES |  |
| source_system | text / text | YES |  |
| source_synced_at | timestamp without time zone / timestamp | YES |  |
| record_sha256 | text / text | NO |  |
| staged_at | timestamp with time zone / timestamptz | NO | now() |

### 제약

```sql
PRIMARY KEY (run_id, movie_key);
```

### 인덱스

```sql
CREATE UNIQUE INDEX movie_catalog_staging_pkey ON dw_serving.movie_catalog_staging USING btree (run_id, movie_key);
```

## dw_serving.movie_snapshot_v3

종류: 테이블. 건수: 7071. 저장공간: 23,863,296 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| publication_id | text / text | NO |  |
| movie_key | text / text | NO |  |
| payload | jsonb / jsonb | NO |  |
| row_sha256 | text / text | NO |  |

### 제약

```sql
PRIMARY KEY (publication_id, movie_key);
FOREIGN KEY (publication_id) REFERENCES dw_serving.dataset_publications_v3(publication_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX movie_snapshot_v3_pkey ON dw_serving.movie_snapshot_v3 USING btree (publication_id, movie_key);
```

## dw_serving.publish_attempts

종류: 테이블. 건수: 3. 저장공간: 32,768 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| attempt_id | text / text | NO |  |
| publication_id | text / text | NO |  |
| status | text / text | NO |  |
| error_message | text / text | YES |  |
| started_at | timestamp with time zone / timestamptz | NO |  |
| completed_at | timestamp with time zone / timestamptz | YES |  |

### 제약

```sql
PRIMARY KEY (attempt_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX publish_attempts_pkey ON dw_serving.publish_attempts USING btree (attempt_id);
```

## dw_serving.publish_attempts_v2

종류: 테이블. 건수: 5. 저장공간: 32,768 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| attempt_id | text / text | NO |  |
| publication_id | text / text | NO |  |
| status | text / text | NO |  |
| error_message | text / text | YES |  |
| started_at | timestamp with time zone / timestamptz | NO |  |
| completed_at | timestamp with time zone / timestamptz | YES |  |

### 제약

```sql
PRIMARY KEY (attempt_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX publish_attempts_v2_pkey ON dw_serving.publish_attempts_v2 USING btree (attempt_id);
```

## dw_serving.publish_attempts_v3

종류: 테이블. 건수: 19. 저장공간: 32,768 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| attempt_id | text / text | NO |  |
| publication_id | text / text | NO |  |
| status | text / text | NO |  |
| error_message | text / text | YES |  |
| started_at | timestamp with time zone / timestamptz | NO |  |
| completed_at | timestamp with time zone / timestamptz | YES |  |

### 제약

```sql
PRIMARY KEY (attempt_id);
```

### 인덱스

```sql
CREATE UNIQUE INDEX publish_attempts_v3_pkey ON dw_serving.publish_attempts_v3 USING btree (attempt_id);
```

## dw_serving.publish_runs

종류: 테이블. 건수: 1. 저장공간: 32,768 bytes.

DB 주석 없음.

| 컬럼 | 타입 | NULL 허용 | 기본값 |
|---|---|---|---|
| run_id | text / text | NO |  |
| status | text / text | NO |  |
| expected_count | integer / int4 | NO |  |
| staged_count | integer / int4 | YES |  |
| published_count | integer / int4 | YES |  |
| started_at | timestamp with time zone / timestamptz | NO |  |
| published_at | timestamp with time zone / timestamptz | YES |  |
| source_system | text / text | NO |  |
| note | text / text | YES |  |

### 제약

```sql
PRIMARY KEY (run_id);
CHECK ((status = ANY (ARRAY['RUNNING'::text, 'SUCCESS'::text, 'FAILED'::text])));
```

### 인덱스

```sql
CREATE UNIQUE INDEX publish_runs_pkey ON dw_serving.publish_runs USING btree (run_id);
```

## dev 사용자 함수

### duplicated_category_aliases

```sql
CREATE OR REPLACE FUNCTION dev.duplicated_category_aliases()
 RETURNS TABLE(alias text, codes text[])
 LANGUAGE sql
 STABLE
 SET search_path TO 'dev'
AS $function$
    SELECT a AS alias, array_agg(c.code ORDER BY c.code) AS codes
      FROM movie_categories c, unnest(c.aliases) a
     GROUP BY a
    HAVING count(*) > 1
$function$

```

### protect_removed_movie

```sql
CREATE OR REPLACE FUNCTION dev.protect_removed_movie()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF EXISTS (
        SELECT 1 FROM movie_editorial
         WHERE movie_id = NEW.id AND is_removed = TRUE
    ) THEN
        NEW.service_status := 'HIDDEN';
        NEW.approval_status := 'REJECTED';
        NEW.rejection_reason := COALESCE(
            (SELECT removal_reason FROM movie_editorial WHERE movie_id = NEW.id),
            NEW.rejection_reason,
            'Removed by administrator'
        );
    END IF;
    RETURN NEW;
END;
$function$

```

### queue_curated_movie_embedding

```sql
CREATE OR REPLACE FUNCTION dev.queue_curated_movie_embedding()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'dev', 'public', 'cdb_admin', '$user'
AS $function$
DECLARE
    target_movie_id BIGINT;
    is_embeddable BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        target_movie_id := OLD.movie_id;
    ELSE
        target_movie_id := NEW.movie_id;
    END IF;

    SELECT m.service_status = 'PUBLISHED'
           AND m.approval_status = 'APPROVED'
           AND COALESCE(e.is_removed, FALSE) = FALSE
      INTO is_embeddable
      FROM popcorn_movies m
      LEFT JOIN movie_editorial e ON e.movie_id = m.id
     WHERE m.id = target_movie_id;

    IF COALESCE(is_embeddable, FALSE) THEN
        UPDATE popcorn_movie_embeddings
           SET status = 'STALE', updated_at = CURRENT_TIMESTAMP
         WHERE movie_id = target_movie_id
           AND status = 'READY';

        INSERT INTO movie_embedding_jobs (movie_id, operation)
        VALUES (target_movie_id, 'UPSERT')
        ON CONFLICT DO NOTHING;
    ELSE
        UPDATE popcorn_movie_embeddings
           SET status = 'STALE', updated_at = CURRENT_TIMESTAMP
         WHERE movie_id = target_movie_id
           AND status = 'READY';
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$function$

```

### queue_movie_embedding

```sql
CREATE OR REPLACE FUNCTION dev.queue_movie_embedding()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF NEW.service_status <> 'PUBLISHED'
       OR NEW.approval_status <> 'APPROVED'
    THEN
        UPDATE popcorn_movie_embeddings
           SET status = 'STALE', updated_at = CURRENT_TIMESTAMP
         WHERE movie_id = NEW.id
           AND status = 'READY';
        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT'
       OR OLD.title_ko IS DISTINCT FROM NEW.title_ko
       OR OLD.title_en IS DISTINCT FROM NEW.title_en
       OR OLD.title_original IS DISTINCT FROM NEW.title_original
       OR OLD.genres IS DISTINCT FROM NEW.genres
       OR OLD.production_countries IS DISTINCT FROM NEW.production_countries
       OR OLD.production_year IS DISTINCT FROM NEW.production_year
       OR OLD.directors IS DISTINCT FROM NEW.directors
       OR OLD.actors IS DISTINCT FROM NEW.actors
       OR OLD.viewing_grade IS DISTINCT FROM NEW.viewing_grade
       OR OLD.release_date IS DISTINCT FROM NEW.release_date
       OR OLD.runtime_minutes IS DISTINCT FROM NEW.runtime_minutes
       OR OLD.source_keywords IS DISTINCT FROM NEW.source_keywords
       OR OLD.plot IS DISTINCT FROM NEW.plot
       OR OLD.service_status IS DISTINCT FROM NEW.service_status
       OR OLD.approval_status IS DISTINCT FROM NEW.approval_status
    THEN
        UPDATE popcorn_movie_embeddings
           SET status = 'STALE', updated_at = CURRENT_TIMESTAMP
         WHERE movie_id = NEW.id
           AND status = 'READY';

        INSERT INTO movie_embedding_jobs (movie_id, operation)
        VALUES (NEW.id, 'UPSERT')
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN NEW;
END;
$function$

```

### set_updated_at

```sql
CREATE OR REPLACE FUNCTION dev.set_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$function$

```
