"""Validate movie raw manifests and produce deterministic Bronze/Silver rows.

Storage-independent Python validation and normalization of preserved API inputs.
Database loading is a separate Phase 1 implementation.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import UTC, date, datetime
from typing import Any, Mapping

CONTRACT_VERSION = 1
POLICY_VERSION = "movie-service-eligibility-v1"
REQUIRED_STAGES = ("movie_list", "details", "kmdb_candidates", "boxoffice")
EXCLUDED_GENRE_KEYWORDS = ("성인물", "에로", "다큐멘터리")
EXCLUDED_COMPANIES = frozenset({
    "(주)영진크리에이티브", "(주)가온콘텐츠", "(주)라온컴퍼니플러스",
    "(주)도키엔터테인먼트", "주식회사 케이앤아이", "(주)영화사가을",
    "(주)영화사 가을", "엔트리커뮤니케이션즈", "스마일컨텐츠", "에스팀",
    "(주)아이피큐", "영드래곤 미디어", "(주)컨텐츠 빌리지",
    "(주)디에이치미디어", "주식회사 피에스앤제이",
})
EXCLUDED_COMPANY_GROUPS = (frozenset({"라임필름", "케이엘 픽쳐스"}),)
EXCLUDED_DIRECTORS = frozenset({
    "김종석", "천성준", "사쿠라비토", "버드맨 텟페이", "디렉터 O",
    "토미죠 타로", "나기라 겐조", "모소조쿠", "츠지 코지", "사노 B사쿠",
    "나카노 야요이", "오오사키히로하루", "아카바네 키쿠지로", "히이라기 엔부",
    "키무라 히로유키", "하라다 칸토나", "코이케.Jp", "이즈미 류지",
    "나가에 타카미", "아나콘다 아나키", "사다오카 사다오", "비바 곤조",
    "아카바네 류지", "쿠로아카 긴조",
})
CONTENT_HASH_FIELDS = (
    "canonical_movie_key", "kofic_movie_cd", "kmdb_id", "kmdb_matched",
    "title_ko", "title_en", "title_original", "release_date", "production_year",
    "runtime_minutes", "movie_type", "production_status", "countries", "genres",
    "directors", "director_names_en", "actors", "actor_roles",
    "production_companies", "viewing_grade", "poster_url", "posters", "stills",
    "plot", "keywords", "kmdb_rating", "vod_url",
)


class TransformContractError(ValueError):
    """Raised when immutable raw input violates the downstream contract."""


def packed(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(value if isinstance(value, bytes) else packed(value)).hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise TransformContractError(message)


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _clean_kmdb(value: Any) -> str:
    return _clean(re.sub(r"!H[ES]", "", str(value or ""), flags=re.IGNORECASE))


def _title_key(value: Any) -> str:
    normalized = unicodedata.normalize("NFC", _clean_kmdb(value))
    return re.sub(r"[^\w가-힣]", "", normalized).lower()


def _year(value: Any) -> int | None:
    match = re.search(r"\d{4}", str(value or ""))
    return int(match.group()) if match else None


def _date(value: Any) -> str | None:
    text = re.sub(r"\D", "", str(value or ""))
    if len(text) != 8:
        return None
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:])).isoformat()
    except ValueError:
        return None


def validate_control_documents(
    ready: Mapping[str, Any],
    success: Mapping[str, Any],
    stage_manifests: Mapping[str, Mapping[str, Any]],
    *,
    allow_sample: bool = False,
) -> None:
    """Validate DAILY_READY -> SUCCESS -> stage manifest ownership and scope."""
    _require(ready.get("schema_version") == CONTRACT_VERSION, "Unsupported READY schema")
    _require(ready.get("stage") == "DAILY_READY" and ready.get("status") == "SUCCESS",
             "DAILY_READY is not complete")
    _require(allow_sample or ready.get("sample") is False,
             "Sample READY cannot be consumed by the operational transform")
    _require(allow_sample or ready.get("candidate_scope_complete") is True,
             "Incomplete candidate scope cannot be consumed operationally")
    _require(success.get("schema_version") == CONTRACT_VERSION and
             success.get("status") == "SUCCESS", "Raw SUCCESS is not complete")
    _require(success.get("run_id") == ready.get("run_id"), "READY/SUCCESS run mismatch")
    _require(success.get("scope") == ready.get("scope"), "READY/SUCCESS scope mismatch")
    declared = success.get("stage_manifests") or {}
    _require(set(declared) == set(REQUIRED_STAGES), "Raw SUCCESS stage set is incomplete")
    _require(set(stage_manifests) == set(REQUIRED_STAGES), "Loaded stage set is incomplete")
    for stage in REQUIRED_STAGES:
        manifest = stage_manifests[stage]
        _require(manifest.get("stage") == stage and manifest.get("status") == "SUCCESS",
                 f"Stage {stage} is not complete")
        _require(manifest.get("run_id") == ready.get("run_id"),
                 f"Stage {stage} belongs to another run")
        _require(manifest.get("scope") == ready.get("scope"),
                 f"Stage {stage} scope mismatch")
    movie_count = ready.get("movie_count")
    _require(isinstance(movie_count, int) and movie_count >= 0, "READY movie_count is invalid")
    _require(success.get("movie_count") == movie_count, "READY/SUCCESS movie count mismatch")
    for stage in ("movie_list", "details", "kmdb_candidates"):
        _require(stage_manifests[stage].get("movie_count") == movie_count,
                 f"Stage {stage} movie count mismatch")
    listing = stage_manifests["movie_list"]
    _require(allow_sample or listing.get("candidate_scope_complete") is True,
             "Movie list candidate scope is incomplete")
    listed_ids = set((listing.get("movies") or {}).keys())
    detail_ids = [str(ref.get("movie_cd") or "") for ref in
                  stage_manifests["details"].get("objects") or []]
    outcome_rows = stage_manifests["kmdb_candidates"].get("outcomes") or []
    outcome_ids = [str(item.get("movie_cd") or "") for item in outcome_rows]
    _require(len(detail_ids) == len(set(detail_ids)), "Duplicate detail movie ID")
    _require(len(outcome_ids) == len(set(outcome_ids)), "Duplicate KMDb outcome movie ID")
    _require(listed_ids == set(detail_ids) == set(outcome_ids),
             "Movie list/detail/KMDb ID sets differ")
    _require(len(listed_ids) == movie_count, "Declared movie count differs from ID set")
    scope_dates = set(ready.get("scope", {}).get("boxoffice_dates") or [])
    box_refs = stage_manifests["boxoffice"].get("objects") or []
    ref_dates = [str(ref.get("target_date") or "") for ref in box_refs]
    _require(len(ref_dates) == len(set(ref_dates)), "Duplicate boxoffice target date")
    _require(set(ref_dates) == scope_dates, "Boxoffice dates differ from collection scope")
    _require(stage_manifests["boxoffice"].get("day_count") == len(scope_dates),
             "Boxoffice day count mismatch")


def bronze_row(key: str, ref: Mapping[str, Any], envelope: Mapping[str, Any],
               run_id: str) -> dict[str, Any]:
    """Verify one raw envelope and retain its payload without projection."""
    _require(envelope.get("schema_version") == CONTRACT_VERSION, "Unsupported raw schema")
    _require(envelope.get("run_id") == run_id, "Raw envelope run mismatch")
    payload = envelope.get("payload")
    payload_sha = digest(payload)
    _require(payload_sha == envelope.get("payload_sha256"), "Raw envelope checksum mismatch")
    _require(payload_sha == ref.get("sha256"), "Manifest reference checksum mismatch")
    _require(envelope.get("source") in {"kofic", "kmdb"}, "Unknown raw source")
    return {
        "run_id": run_id,
        "object_key": key,
        "source": envelope["source"],
        "resource": envelope.get("resource", ""),
        "request_json": json.dumps(envelope.get("request", {}), ensure_ascii=False,
                                   sort_keys=True, separators=(",", ":")),
        "collected_at": envelope.get("collected_at"),
        "payload_sha256": payload_sha,
        "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                   separators=(",", ":")),
    }


def _kmdb_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for block in payload.get("Data") or []:
        rows.extend(row for row in (block.get("Result") or []) if isinstance(row, dict))
    return rows


def evaluate_kmdb_match(detail: Mapping[str, Any], candidates: list[dict[str, Any]], *,
                        truncated: bool = False) -> dict[str, Any]:
    """Return a confirmed or provisional match without guessing through ambiguity."""
    ko, en = _title_key(detail.get("movieNm")), _title_key(detail.get("movieNmEn"))
    target_year = _year(detail.get("prdtYear"))
    ranked: list[tuple[int, int, str, dict[str, Any]]] = []
    deduplicated: dict[str, dict[str, Any]] = {}
    for row in candidates:
        identity = _clean(row.get("DOCID") or f"{row.get('movieId', '')}|{row.get('movieSeq', '')}")
        _require(bool(identity), "KMDb candidate has no stable identity")
        deduplicated.setdefault(identity, row)
    for identity, row in deduplicated.items():
        ko_match = bool(ko and _title_key(row.get("title")) == ko)
        en_match = bool(en and _title_key(row.get("titleEng")) == en)
        if not ko_match and not en_match:
            continue
        candidate_year = _year(row.get("prodYear"))
        difference = abs(target_year - candidate_year) if target_year and candidate_year else 999
        if ko_match and difference <= 1:
            priority = 1
        elif en_match and difference <= 1:
            priority = 2
        elif ko_match and difference <= 2:
            priority = 3
        elif en_match and difference <= 2:
            priority = 4
        elif ko_match and difference <= 5:
            priority = 5
        else:
            continue
        ranked.append((priority, difference, identity, row))
    if not ranked:
        return {"status": "REVIEW_REQUIRED" if truncated else "UNMATCHED",
                "confirmed": None, "provisional": None,
                "ambiguous": False, "candidate_count": len(deduplicated)}
    ranked.sort(key=lambda item: item[:3])
    best = ranked[0]
    peers = [item for item in ranked if item[:2] == best[:2]]
    ambiguous = len(peers) > 1
    if truncated or ambiguous:
        return {"status": "REVIEW_REQUIRED", "confirmed": None,
                "provisional": best[3], "ambiguous": ambiguous,
                "candidate_count": len(deduplicated)}
    return {"status": "MATCHED", "confirmed": best[3], "provisional": None,
            "ambiguous": False, "candidate_count": len(deduplicated)}


def choose_kmdb_match(detail: Mapping[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Compatibility helper returning only an unambiguous, complete match."""
    return evaluate_kmdb_match(detail, candidates)["confirmed"]


def _names(items: Any, field: str) -> list[str]:
    if not isinstance(items, list):
        return []
    return list(dict.fromkeys(_clean(item.get(field)) for item in items
                              if isinstance(item, dict) and _clean(item.get(field))))


def _split(value: Any, separator: str) -> list[str]:
    if isinstance(value, list):
        values = value
    else:
        values = str(value or "").split(separator)
    return list(dict.fromkeys(_clean(item) for item in values if _clean(item)))


def _kmdb_enrichment(match: Mapping[str, Any] | None) -> dict[str, Any]:
    if not match:
        return {"kmdb_id": None, "kmdb_matched": False, "poster_url": None,
                "posters": [], "stills": [], "plot": None, "keywords": [],
                "kmdb_rating": None, "vod_url": None}
    posters = [_clean(value) for value in str(match.get("posters") or "").split("|") if _clean(value)]
    stills = [_clean(value) for value in str(match.get("stlls") or "").split("|") if _clean(value)]
    plots = match.get("plots") or {}
    plot = ""
    if isinstance(plots, dict):
        choices = plots.get("plot") or []
        preferred = [p for p in choices if isinstance(p, dict) and p.get("plotLang") in ("한국어", "", None)]
        chosen = (preferred or [p for p in choices if isinstance(p, dict)])[:1]
        plot = _clean_kmdb(chosen[0].get("plotText")) if chosen else ""
    elif isinstance(plots, str):
        plot = _clean_kmdb(plots)
    ratings = match.get("ratings") or {}
    rating_rows = ratings.get("rating") or [] if isinstance(ratings, dict) else []
    rating = _clean(rating_rows[0].get("ratingMain") or rating_rows[0].get("ratingGrade")) \
        if rating_rows and isinstance(rating_rows[0], dict) else ""
    return {
        "kmdb_id": _clean(match.get("DOCID")) or None,
        "kmdb_matched": True,
        "poster_url": posters[0] if posters else None,
        "posters": posters,
        "stills": stills,
        "plot": plot or None,
        "keywords": [_clean_kmdb(v) for v in str(match.get("keywords") or "").split(",") if _clean_kmdb(v)],
        "kmdb_rating": rating or None,
        "vod_url": _clean(match.get("vodUrl")) or None,
    }


def _policy(detail: Mapping[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    released = _date(detail.get("openDt"))
    if not released:
        reasons.append("release_date_missing")
    elif released < "2020-01-01":
        reasons.append("release_date_before_2020")
    genres = _names(detail.get("genres"), "genreNm")
    genre_text = " ".join(genres)
    if any(keyword in genre_text for keyword in EXCLUDED_GENRE_KEYWORDS):
        reasons.append("excluded_genre")
    companies = set(_names(detail.get("companys"), "companyNm"))
    if companies & EXCLUDED_COMPANIES or any(group <= companies for group in EXCLUDED_COMPANY_GROUPS):
        reasons.append("excluded_company")
    if set(_names(detail.get("directors"), "peopleNm")) & EXCLUDED_DIRECTORS:
        reasons.append("excluded_director")
    if not _clean(detail.get("movieNm")):
        reasons.append("title_missing")
    return not reasons, reasons


def _attach_hashes(row: dict[str, Any]) -> dict[str, Any]:
    policy_fields = {"policy_eligible", "policy_exclusion_reasons", "policy_version"}
    matching_fields = {"kmdb_mapping_status", "provisional_kmdb_id",
                       "kmdb_candidate_count", "kmdb_candidates_truncated",
                       "kmdb_candidates_ambiguous"}
    content = {key: row.get(key) for key in CONTENT_HASH_FIELDS}
    row["content_sha256"] = digest(content)
    row["policy_sha256"] = digest({"canonical_movie_key": row["canonical_movie_key"],
                                    **{key: row[key] for key in sorted(policy_fields)}})
    row["embedding_input_sha256"] = digest({
        key: row.get(key) for key in ("title_ko", "title_en", "title_original", "plot",
                                      "genres", "directors", "actors", "keywords")
    })
    row["matching_sha256"] = digest({"canonical_movie_key": row["canonical_movie_key"],
                                      **{key: row.get(key) for key in sorted(matching_fields)}})
    return row


def silver_movie(detail: Mapping[str, Any], candidates: list[dict[str, Any]], *,
                 run_id: str, source_object_key: str, kmdb_truncated: bool) -> dict[str, Any]:
    movie_cd = _clean(detail.get("movieCd"))
    _require(bool(re.fullmatch(r"[A-Za-z0-9]{1,32}", movie_cd)), "Invalid KOFIC movie ID")
    decision = evaluate_kmdb_match(detail, candidates, truncated=kmdb_truncated)
    match = decision["confirmed"]
    eligible, reasons = _policy(detail)
    actors = detail.get("actors") or []
    row = {
        "kofic_movie_cd": movie_cd,
        "title_ko": _clean(detail.get("movieNm")),
        "title_en": _clean(detail.get("movieNmEn")) or None,
        "title_original": _clean(detail.get("movieNmOg")) or None,
        "release_date": _date(detail.get("openDt")),
        "production_year": _year(detail.get("prdtYear")),
        "runtime_minutes": int(detail["showTm"]) if str(detail.get("showTm") or "").isdigit() else None,
        "movie_type": _clean(detail.get("typeNm")) or None,
        "production_status": _clean(detail.get("prdtStatNm")) or None,
        "countries": _names(detail.get("nations"), "nationNm"),
        "genres": _names(detail.get("genres"), "genreNm"),
        "directors": _names(detail.get("directors"), "peopleNm"),
        "director_names_en": _names(detail.get("directors"), "peopleNmEn"),
        "actors": _names(actors, "peopleNm"),
        "actor_roles": _names(actors, "cast"),
        "production_companies": _names(detail.get("companys"), "companyNm"),
        "viewing_grade": (_names(detail.get("audits"), "watchGradeNm") or [None])[0],
        "policy_eligible": eligible,
        "policy_exclusion_reasons": reasons,
        "policy_version": POLICY_VERSION,
        "canonical_movie_key": f"kofic:{movie_cd}",
        "service_movie_id": None,
        "ingestion_source": "movie_api_daily",
        "kmdb_candidate_count": decision["candidate_count"],
        "kmdb_candidates_truncated": bool(kmdb_truncated),
        "kmdb_candidates_ambiguous": decision["ambiguous"],
        "kmdb_mapping_status": decision["status"],
        "provisional_kmdb_id": _clean((decision["provisional"] or {}).get("DOCID")) or None,
        "source_run_id": run_id,
        "source_object_key": source_object_key,
        **_kmdb_enrichment(match),
    }
    return _attach_hashes(row)


def transform_legacy_snapshot(manifest: Mapping[str, Any], source_bytes: bytes) -> dict[str, Any]:
    """Adapt the immutable 5,985-row legacy JSON snapshot to the common Silver schema."""
    _require(manifest.get("schema_version") == CONTRACT_VERSION and
             manifest.get("status") == "SUCCESS", "Legacy snapshot is not complete")
    _require(len(source_bytes) == manifest.get("bytes"), "Legacy snapshot byte count mismatch")
    _require(digest(source_bytes) == manifest.get("sha256"), "Legacy snapshot checksum mismatch")
    try:
        records = json.loads(source_bytes)
    except (UnicodeError, ValueError):
        raise TransformContractError("Legacy snapshot is not valid UTF-8 JSON") from None
    _require(isinstance(records, list), "Legacy snapshot must be a JSON array")
    _require(len(records) == manifest.get("record_count"), "Legacy snapshot record count mismatch")
    source_run_id = f"legacy:{manifest['sha256']}"
    rows: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        _require(isinstance(record, dict), "Legacy movie must be an object")
        code = _clean(record.get("movie_cd"))
        _require(not code or bool(re.fullmatch(r"[A-Za-z0-9]{1,32}", code)),
                 "Legacy movie has invalid KOFIC ID")
        release_date = _date(record.get("open_dt"))
        countries = _split(record.get("nation_alt"), ",")
        genres = _split(record.get("genre_alt"), ",")
        companies = _split(record.get("company_nm"), "|")
        directors = _split(record.get("director_nm"), "|")
        actors = _split(record.get("actor_nm"), "|")
        actor_roles = _split(record.get("actor_cast"), "|")
        keywords = _split(record.get("keywords"), "|")
        reasons: list[str] = []
        if not release_date:
            reasons.append("release_date_missing")
        elif release_date < "2020-01-01":
            reasons.append("release_date_before_2020")
        if any(word in " ".join(map(str, genres)) for word in EXCLUDED_GENRE_KEYWORDS):
            reasons.append("excluded_genre")
        company_set = set(map(str, companies))
        if company_set & EXCLUDED_COMPANIES or any(group <= company_set for group in EXCLUDED_COMPANY_GROUPS):
            reasons.append("excluded_company")
        if set(map(str, directors)) & EXCLUDED_DIRECTORS:
            reasons.append("excluded_director")
        if not _clean(record.get("movie_nm")):
            reasons.append("title_missing")
        canonical = f"kofic:{code}" if code else f"legacy-row:{manifest['sha256']}:{index}"
        runtime = str(record.get("show_tm") or "")
        row = {
            "canonical_movie_key": canonical,
            "service_movie_id": None,
            "kofic_movie_cd": code or None,
            "kmdb_id": _clean(record.get("kmdb_id")) or None,
            "kmdb_matched": bool(record.get("kmdb_matched")),
            "kmdb_mapping_status": "LEGACY_ACCEPTED" if record.get("kmdb_matched") else "UNMATCHED",
            "provisional_kmdb_id": None,
            "kmdb_candidate_count": None,
            "kmdb_candidates_truncated": False,
            "kmdb_candidates_ambiguous": False,
            "title_ko": _clean(record.get("movie_nm")),
            "title_en": _clean(record.get("movie_nm_en")) or None,
            "title_original": _clean(record.get("movie_nm_og")) or None,
            "release_date": release_date,
            "production_year": _year(record.get("prdt_year")),
            "runtime_minutes": int(runtime) if runtime.isdigit() else None,
            "movie_type": _clean(record.get("type_nm")) or None,
            "production_status": _clean(record.get("prdt_stat_nm")) or None,
            "countries": countries,
            "genres": genres,
            "directors": directors,
            "director_names_en": _split(record.get("director_nm_en"), "|"),
            "actors": actors,
            "actor_roles": actor_roles,
            "production_companies": companies,
            "viewing_grade": _clean(record.get("watch_grade")) or None,
            "poster_url": _clean(record.get("poster_url")) or None,
            "posters": _split(record.get("posters_all"), "|"),
            "stills": _split(record.get("stlls_all"), "|"),
            "plot": _clean(record.get("plot")) or None,
            "keywords": keywords,
            "kmdb_rating": None,
            "vod_url": None,
            "policy_eligible": not reasons,
            "policy_exclusion_reasons": reasons,
            "policy_version": POLICY_VERSION,
            "ingestion_source": "legacy_snapshot",
            "source_run_id": source_run_id,
            "source_object_key": manifest.get("key"),
            # Serving-owned values are observations only; downstream must not overwrite them.
            "source_service_status": None,
            "source_approval_status": None,
        }
        rows.append(_attach_hashes(row))
    _require(len({row["canonical_movie_key"] for row in rows}) == len(rows),
             "Duplicate canonical movie key in legacy snapshot")
    return {"silver_movies": rows,
            "quality": {"source_run_id": source_run_id, "movie_count": len(rows),
                        "eligible_count": sum(row["policy_eligible"] for row in rows),
                        "excluded_count": sum(not row["policy_eligible"] for row in rows)}}


def transform_run(
    ready: Mapping[str, Any],
    success: Mapping[str, Any],
    stage_manifests: Mapping[str, Mapping[str, Any]],
    raw_envelopes: Mapping[str, Mapping[str, Any]],
    *,
    allow_sample: bool = False,
) -> dict[str, Any]:
    """Transform one fully loaded raw run into Bronze, Silver, and quality rows."""
    validate_control_documents(ready, success, stage_manifests, allow_sample=allow_sample)
    run_id = str(ready["run_id"])
    refs: dict[str, Mapping[str, Any]] = {}
    for manifest in stage_manifests.values():
        for ref in manifest.get("objects") or []:
            key = ref.get("key")
            _require(bool(key), "Manifest object reference has no key")
            if key in refs:
                _require(refs[key].get("sha256") == ref.get("sha256"), "Conflicting duplicate object reference")
            refs[key] = ref
    _require(set(refs) == set(raw_envelopes), "Loaded raw object set differs from manifests")
    bronze = [bronze_row(key, refs[key], raw_envelopes[key], run_id) for key in sorted(refs)]

    detail_manifest = stage_manifests["details"]
    details: dict[str, tuple[Mapping[str, Any], str]] = {}
    for ref in detail_manifest.get("objects") or []:
        key, code = ref["key"], str(ref.get("movie_cd") or "")
        detail = raw_envelopes[key]["payload"]["movieInfoResult"]["movieInfo"]
        _require(str(detail.get("movieCd")) == code, "Detail manifest/movie ID mismatch")
        _require(code not in details, "Duplicate KOFIC detail")
        details[code] = (detail, key)

    candidates: dict[str, list[dict[str, Any]]] = {code: [] for code in details}
    for ref in stage_manifests["kmdb_candidates"].get("objects") or []:
        code = str(ref.get("movie_cd") or "")
        _require(code in candidates, "KMDb candidates reference an unknown movie")
        candidates[code].extend(_kmdb_rows(raw_envelopes[ref["key"]]["payload"]))
    outcomes = {str(item["movie_cd"]): item for item in
                stage_manifests["kmdb_candidates"].get("outcomes") or []}
    _require(set(outcomes) == set(details), "KMDb outcomes do not cover every detail")

    movies = [silver_movie(detail, candidates[code], run_id=run_id,
                           source_object_key=key,
                           kmdb_truncated=bool(outcomes[code].get("truncated")))
              for code, (detail, key) in sorted(details.items())]
    boxoffice: list[dict[str, Any]] = []
    boxoffice_keys: set[tuple[str | None, str]] = set()
    for ref in stage_manifests["boxoffice"].get("objects") or []:
        envelope = raw_envelopes[ref["key"]]
        payload = envelope["payload"]
        _require(str(envelope.get("request", {}).get("targetDt") or "") ==
                 str(ref.get("target_date") or ""),
                 "Boxoffice reference/request date mismatch")
        target_date = _date(ref.get("target_date"))
        for item in payload["boxOfficeResult"]["dailyBoxOfficeList"]:
            code = _clean(item.get("movieCd"))
            _require(bool(code), "Boxoffice row has no KOFIC movie ID")
            business_key = (target_date, code)
            _require(business_key not in boxoffice_keys,
                     "Duplicate boxoffice movie for target date")
            boxoffice_keys.add(business_key)
            row = {"source_run_id": run_id, "target_date": target_date,
                   "kofic_movie_cd": code, "rank": int(item["rank"]),
                   "sales_amount": int(item.get("salesAmt") or 0),
                   "audience_count": int(item.get("audiCnt") or 0),
                   "audience_accumulated": int(item.get("audiAcc") or 0),
                   "source_object_key": ref["key"]}
            row["observation_sha256"] = digest({
                key: row[key] for key in ("target_date", "kofic_movie_cd", "rank",
                                          "sales_amount", "audience_count",
                                          "audience_accumulated")
            })
            boxoffice.append(row)
    quality = {
        "source_run_id": run_id,
        "movie_count": len(movies),
        "eligible_count": sum(row["policy_eligible"] for row in movies),
        "excluded_count": sum(not row["policy_eligible"] for row in movies),
        "kmdb_matched_count": sum(row["kmdb_matched"] for row in movies),
        "kmdb_review_required_count": sum(row["kmdb_mapping_status"] == "REVIEW_REQUIRED" for row in movies),
        "kmdb_truncated_count": sum(row["kmdb_candidates_truncated"] for row in movies),
        "kmdb_ambiguous_count": sum(row["kmdb_candidates_ambiguous"] for row in movies),
        "kmdb_provisional_count": sum(bool(row["provisional_kmdb_id"]) for row in movies),
        "boxoffice_row_count": len(boxoffice),
        "transformed_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    _require(len(movies) == int(ready["movie_count"]), "Silver movie count differs from READY")
    return {"bronze": bronze, "silver_movies": movies,
            "silver_boxoffice": boxoffice, "quality": quality}
