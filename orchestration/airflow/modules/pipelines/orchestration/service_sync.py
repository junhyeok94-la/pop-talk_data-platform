"""검사 완료한 DW 결과를 서비스 원장·흥행·배치 이력에 한 트랜잭션으로 반영한다."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime

from psycopg.types.json import Jsonb

JOB = "dw-service-sync"
# 배치가 소유하는 필드만 명시한다. id·운영 상태·editorial·media·keywords는 보존한다.
FIELDS = {
    "kmdb_id": "KMDB_ID", "kmdb_matched": "KMDB_MATCHED",
    "title_ko": "TITLE_KO", "title_en": "TITLE_EN", "title_original": "TITLE_ORIGINAL",
    "release_date": "RELEASE_DATE", "production_year": "PRODUCTION_YEAR", "runtime_minutes": "RUNTIME_MINUTES",
    "movie_type": "MOVIE_TYPE", "production_status": "PRODUCTION_STATUS",
    "production_countries": "COUNTRIES", "genres": "GENRES", "directors": "DIRECTORS",
    "director_names_en": "DIRECTOR_NAMES_EN", "actors": "ACTORS", "actor_roles": "ACTOR_ROLES",
    "production_companies": "PRODUCTION_COMPANIES", "viewing_grade": "VIEWING_GRADE",
    "poster_url": "POSTER_URL", "plot": "PLOT",
}
ARRAYS = {"production_countries", "genres", "directors", "director_names_en", "actors", "actor_roles", "production_companies"}


def movie_values(row):
    """입력 배열의 JSON 문자열을 PostgreSQL 배열로 바꾸고 신규 필수값을 검증한다."""
    if row.get("POLICY_ELIGIBLE") is not True:
        return None
    values = {"kofic_movie_cd": row.get("KOFIC_MOVIE_CD")}
    for field, source in FIELDS.items():
        value = row.get(source)
        if field in ARRAYS:
            value = json.loads(value) if isinstance(value, str) else value
            value = value or []
            if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
                raise ValueError(f"잘못된 영화 배열: {field}")
        elif isinstance(value, str):
            value = value.strip() or None
        values[field] = value
    values["kmdb_matched"] = row.get("KMDB_MATCHED") is True and bool(values["kmdb_id"])
    if not values["kmdb_matched"]:
        values["kmdb_id"] = None
    if not values["kofic_movie_cd"] or not values["title_ko"] or not values["release_date"]:
        return None
    for field, low, high in (("production_year", 1888, 2200), ("runtime_minutes", 1, 32767)):
        if values[field] is not None and not low <= int(values[field]) <= high:
            values[field] = None
    return values


def sync_service(connection, movies, facts, *, cutoff, run_id, model_version):
    """재시도는 같은 입력을 재사용하며, 성공한 최신 cutoff보다 오래된 실행은 거부한다."""
    cutoff = datetime.fromisoformat(cutoff) if isinstance(cutoff, str) else cutoff
    if cutoff.tzinfo is None:
        raise ValueError("시간대가 있는 입력 cutoff가 필요합니다")
    prepared = [value for row in movies if (value := movie_values(row)) is not None]
    if len({row["kofic_movie_cd"] for row in prepared}) != len(prepared):
        raise ValueError("DW 영화 코드 중복")
    names = ["kofic_movie_cd", *FIELDS, "source_hash"]
    # 동일 입력의 비교 증빙. JSON 정규화로 날짜·Decimal을 안정적으로 기록한다.
    digest = hashlib.sha256(json.dumps([
        sorted(movies, key=lambda row: str(row.get("KOFIC_MOVIE_CD"))),
        sorted(facts, key=lambda row: (str(row.get("KOFIC_MOVIE_CD")),str(row.get("TARGET_DATE")))),
        model_version], default=str, sort_keys=True).encode()).hexdigest()
    try:
        with connection.transaction():
            connection.execute("SET LOCAL search_path TO dev, public, cdb_admin")
            connection.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (JOB,))
            receipt = connection.execute("SELECT source_hash,result FROM batch_runs WHERE job_name=%s AND scheduled_for=%s AND status='SUCCEEDED'", (JOB,cutoff)).fetchone()
            if receipt:
                if receipt[0].strip() != digest:
                    raise RuntimeError("동일 cutoff의 DW 내용이 변경됐습니다")
                return receipt[1]
            previous = connection.execute("SELECT scheduled_for,source_hash,result FROM batch_runs WHERE job_name=%s AND status='SUCCEEDED' ORDER BY scheduled_for DESC LIMIT 1", (JOB,)).fetchone()
            if previous and previous[0] > cutoff:
                raise RuntimeError("최신 서비스 적재보다 오래된 DW 실행입니다")
            if previous and previous[0] == cutoff:
                if previous[1].strip() != digest:
                    raise RuntimeError("동일 cutoff의 DW 내용이 변경됐습니다")
                return previous[2]
            connection.execute("CREATE TEMP TABLE incoming_service_movies ON COMMIT DROP AS SELECT " + ",".join(names) + " FROM popcorn_movies WITH NO DATA")
            with connection.cursor() as cursor:
                with cursor.copy("COPY incoming_service_movies (" + ",".join(names) + ") FROM STDIN") as copy:
                    for row in prepared:
                        source_hash = hashlib.sha256(json.dumps(row, default=str, sort_keys=True).encode()).hexdigest()
                        copy.write_row([row[name] for name in names[:-1]] + [source_hash])
            # SQL의 현재 행 기준으로 병합해 동시에 수정한 운영 정보와 빈 원천 필드를 보존한다.
            effective = {field: (f"CASE WHEN cardinality(EXCLUDED.{field})>0 THEN EXCLUDED.{field} ELSE m.{field} END"
                if field in ARRAYS else f"COALESCE(EXCLUDED.{field},m.{field})") for field in FIELDS}
            effective["kmdb_matched"] = "CASE WHEN EXCLUDED.kmdb_id IS NOT NULL THEN EXCLUDED.kmdb_matched ELSE m.kmdb_matched END"
            assignments = ",".join(f"{field}={value}" for field, value in effective.items())
            changed = " OR ".join(f"m.{field} IS DISTINCT FROM ({value})" for field, value in effective.items())
            result = connection.execute(f"""INSERT INTO popcorn_movies AS m ({','.join(names)})
                SELECT {','.join(names)} FROM incoming_service_movies
                ON CONFLICT (kofic_movie_cd) DO UPDATE SET {assignments},
                source_hash=EXCLUDED.source_hash,source_synced_at=CURRENT_TIMESTAMP
                WHERE {changed} RETURNING (xmax=0) AS inserted""").fetchall()
            inserted = sum(row[0] for row in result)
            connection.execute("""CREATE TEMP TABLE incoming_service_boxoffice ON COMMIT DROP AS
                SELECT kofic_movie_cd,target_date,rank,audience_count,audience_accumulated,sales_amount,source_observed_at,source_run_id
                FROM movie_boxoffice_daily WITH NO DATA""")
            with connection.cursor() as cursor:
                with cursor.copy("COPY incoming_service_boxoffice FROM STDIN") as copy:
                    for row in facts:
                        copy.write_row([row.get(key) for key in ("KOFIC_MOVIE_CD", "TARGET_DATE", "RANK", "AUDIENCE_COUNT",
                            "AUDIENCE_ACCUMULATED", "SALES_AMOUNT", "SOURCE_OBSERVED_AT", "SOURCE_RUN_ID")])
            boxoffice = connection.execute("""INSERT INTO movie_boxoffice_daily AS b
                (kofic_movie_cd,target_date,movie_id,rank,audience_count,audience_accumulated,sales_amount,source_observed_at,source_run_id)
                SELECT i.kofic_movie_cd,i.target_date,m.id,i.rank,i.audience_count,i.audience_accumulated,i.sales_amount,i.source_observed_at,i.source_run_id
                FROM incoming_service_boxoffice i LEFT JOIN popcorn_movies m ON m.kofic_movie_cd=i.kofic_movie_cd
                ON CONFLICT (kofic_movie_cd,target_date) DO UPDATE SET movie_id=EXCLUDED.movie_id,
                rank=EXCLUDED.rank,audience_count=EXCLUDED.audience_count,audience_accumulated=EXCLUDED.audience_accumulated,
                sales_amount=EXCLUDED.sales_amount,source_observed_at=EXCLUDED.source_observed_at,
                source_run_id=EXCLUDED.source_run_id,updated_at=CURRENT_TIMESTAMP
                WHERE (b.movie_id,b.rank,b.audience_count,b.audience_accumulated,b.sales_amount,b.source_observed_at,b.source_run_id)
                IS DISTINCT FROM (EXCLUDED.movie_id,EXCLUDED.rank,EXCLUDED.audience_count,EXCLUDED.audience_accumulated,
                    EXCLUDED.sales_amount,EXCLUDED.source_observed_at,EXCLUDED.source_run_id)""").rowcount
            counts = {"movie_count":len(prepared), "inserted":inserted,"updated":len(result)-inserted,
                      "skipped":len(movies)-len(prepared),"boxoffice_count":len(facts),"boxoffice_changed":boxoffice,
                      "model_version":model_version}
            connection.execute("""INSERT INTO batch_runs(job_name,scheduled_for,status,source_file,source_hash,
                processed_count,inserted_count,updated_count,result,started_at,finished_at)
                VALUES(%s,%s,'SUCCEEDED',%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                ON CONFLICT(job_name,scheduled_for) DO UPDATE SET status='SUCCEEDED',source_file=EXCLUDED.source_file,
                source_hash=EXCLUDED.source_hash,processed_count=EXCLUDED.processed_count,inserted_count=EXCLUDED.inserted_count,
                updated_count=EXCLUDED.updated_count,result=EXCLUDED.result,failed_count=0,last_error=NULL,finished_at=CURRENT_TIMESTAMP""",
                (JOB,cutoff,run_id,digest,len(movies),inserted,len(result)-inserted,Jsonb(counts)))
            return counts
    except Exception as error:
        # 데이터 트랜잭션은 이미 롤백됐다. 운영 화면에는 실패 기록만 별도로 남긴다.
        with connection.transaction():
            connection.execute("SET LOCAL search_path TO dev, public, cdb_admin")
            connection.execute("""INSERT INTO batch_runs(job_name,scheduled_for,status,source_file,source_hash,failed_count,last_error,started_at,finished_at)
                VALUES(%s,%s,'FAILED',%s,%s,1,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                ON CONFLICT(job_name,scheduled_for) DO UPDATE SET status='FAILED',failed_count=1,
                last_error=EXCLUDED.last_error,finished_at=CURRENT_TIMESTAMP WHERE batch_runs.status<>'SUCCEEDED'""",
                (JOB,cutoff,run_id,digest,str(error)[:1000]))
        raise
