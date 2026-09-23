"""레거시·서비스·Gold 영화 집합을 KOFIC ID 기준으로 안전하게 대사한다.

외부 I/O와 분리된 순수 모듈이다. 잘못된 ID와 중복을 먼저 격리하고,
양쪽에 정확히 한 행씩 존재하는 영화만 속성 비교 대상으로 삼는다.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Iterable, Mapping, Sequence

from pipelines.transforms.movie_bronze_silver import (
    EXCLUDED_COMPANIES,
    EXCLUDED_COMPANY_GROUPS,
    EXCLUDED_DIRECTORS,
    EXCLUDED_GENRE_KEYWORDS,
    POLICY_VERSION,
)

RULE_VERSION = "movie-baseline-reconciliation-v1"
KOFIC_PATTERN = re.compile(r"[A-Za-z0-9]{1,32}")


@dataclass(frozen=True)
class Parsed:
    """비교 값과 입력 상태를 함께 보존한다."""

    state: str
    value: Any = None


class SnapshotContractError(ValueError):
    """레거시 manifest와 원본 바이트가 계약을 위반한다."""


SNAPSHOT_ERROR_CODES = frozenset({
    "INVALID_SHAPE", "INVALID_STATUS", "BUCKET_MISMATCH", "RAW_KEY_NOT_ALLOWED",
    "BYTE_COUNT_MISMATCH", "SHA256_MISMATCH", "INVALID_JSON", "NOT_ARRAY",
    "RECORD_COUNT_MISMATCH", "ROW_NOT_OBJECT",
})


def validate_legacy_manifest_header(
    manifest: Mapping[str, Any], *, expected_bucket: str,
) -> None:
    """원본 객체를 요청하기 전에 manifest의 읽기 경계를 검증한다."""
    required = {"schema_version", "status", "bucket", "key", "bytes", "sha256", "record_count"}
    if not required <= manifest.keys():
        raise SnapshotContractError("INVALID_SHAPE")
    if manifest["schema_version"] != 1 or manifest["status"] != "SUCCESS":
        raise SnapshotContractError("INVALID_STATUS")
    if manifest["bucket"] != expected_bucket:
        raise SnapshotContractError("BUCKET_MISMATCH")
    if not isinstance(manifest["key"], str) or not manifest["key"].startswith("raw/legacy_snapshot/v1/"):
        raise SnapshotContractError("RAW_KEY_NOT_ALLOWED")
    if not isinstance(manifest["bytes"], int) or manifest["bytes"] < 0:
        raise SnapshotContractError("INVALID_SHAPE")
    if not isinstance(manifest["record_count"], int) or manifest["record_count"] < 0:
        raise SnapshotContractError("INVALID_SHAPE")
    if not isinstance(manifest["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", manifest["sha256"]):
        raise SnapshotContractError("INVALID_SHAPE")


def validate_legacy_snapshot(
    manifest: Mapping[str, Any], source_bytes: bytes, *, expected_bucket: str,
) -> list[dict[str, Any]]:
    """적재 변환 전에 manifest와 원본 무결성만 독립 검증한다."""
    validate_legacy_manifest_header(manifest, expected_bucket=expected_bucket)
    if len(source_bytes) != manifest["bytes"]:
        raise SnapshotContractError("BYTE_COUNT_MISMATCH")
    if hashlib.sha256(source_bytes).hexdigest() != manifest["sha256"]:
        raise SnapshotContractError("SHA256_MISMATCH")
    try:
        rows = json.loads(source_bytes)
    except (UnicodeError, ValueError):
        raise SnapshotContractError("INVALID_JSON") from None
    if not isinstance(rows, list):
        raise SnapshotContractError("NOT_ARRAY")
    if len(rows) != manifest["record_count"]:
        raise SnapshotContractError("RECORD_COUNT_MISMATCH")
    if not all(isinstance(row, dict) for row in rows):
        raise SnapshotContractError("ROW_NOT_OBJECT")
    return rows


def _text(value: Any) -> Parsed:
    if value is None:
        return Parsed("MISSING")
    if not isinstance(value, str):
        return Parsed("INVALID")
    if not value.strip():
        return Parsed("MISSING")
    normalized = unicodedata.normalize("NFC", re.sub(r"\s+", " ", value).strip())
    return Parsed("VALUE", normalized)


def _integer(value: Any, *, minimum: int | None = None, maximum: int | None = None) -> Parsed:
    if value is None or (isinstance(value, str) and not value.strip()):
        return Parsed("MISSING")
    text = str(value).strip()
    if not re.fullmatch(r"[+-]?\d+", text):
        return Parsed("INVALID")
    number = int(text)
    if minimum is not None and number < minimum or maximum is not None and number > maximum:
        return Parsed("INVALID")
    return Parsed("VALUE", number)


def _date(value: Any) -> Parsed:
    if value is None or (isinstance(value, str) and not value.strip()):
        return Parsed("MISSING")
    if isinstance(value, datetime):
        return Parsed("VALUE", value.date().isoformat())
    if isinstance(value, date):
        return Parsed("VALUE", value.isoformat())
    if not isinstance(value, str):
        return Parsed("INVALID")
    text = value.strip()
    if re.fullmatch(r"\d{8}", text):
        digits = text
    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        digits = text.replace("-", "")
    else:
        return Parsed("INVALID")
    try:
        parsed = date(int(digits[:4]), int(digits[4:6]), int(digits[6:]))
    except ValueError:
        return Parsed("INVALID")
    return Parsed("VALUE", parsed.isoformat())


def _boolean(value: Any) -> Parsed:
    if value is None or (isinstance(value, str) and not value.strip()):
        return Parsed("MISSING")
    if isinstance(value, bool):
        return Parsed("VALUE", value)
    if isinstance(value, int) and value in (0, 1):
        return Parsed("VALUE", bool(value))
    lowered = str(value).strip().lower()
    if lowered in {"true", "t", "1"}:
        return Parsed("VALUE", True)
    if lowered in {"false", "f", "0"}:
        return Parsed("VALUE", False)
    return Parsed("INVALID")


def normalize_kofic_id(value: Any) -> Parsed:
    """선행 0과 대소문자를 보존한 비교용 KOFIC ID를 반환한다."""
    parsed = _text(value)
    if parsed.state != "VALUE":
        return parsed
    if not KOFIC_PATTERN.fullmatch(parsed.value):
        return Parsed("INVALID")
    return parsed


def _split(value: Any, separator: str) -> list[str]:
    values = value if isinstance(value, (list, tuple)) else str(value or "").split(separator)
    result: list[str] = []
    for item in values:
        parsed = _text(item)
        if parsed.state == "VALUE" and parsed.value not in result:
            result.append(parsed.value)
    return result


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _large_value(value: Any, *, separator: str | None = None) -> Parsed:
    if separator is not None:
        raw_items = value if isinstance(value, (list, tuple)) else str(value or "").split(separator)
        items: list[str] = []
        for item in raw_items:
            parsed = _text(item)
            if parsed.state == "INVALID":
                return parsed
            if parsed.state == "VALUE":
                items.append(parsed.value)
        return Parsed("MISSING") if not items else Parsed("VALUE", _canonical_hash(items))
    parsed = _text(value)
    return parsed if parsed.state != "VALUE" else Parsed("VALUE", _canonical_hash(parsed.value))


def compare_values(left: Parsed, right: Parsed) -> str:
    """두 파싱 결과를 상호 배타적인 비교 상태로 축약한다."""
    if left.state == "INVALID" and right.state == "INVALID":
        return "both_invalid"
    if left.state == "INVALID":
        return "legacy_invalid"
    if right.state == "INVALID":
        return "service_invalid"
    if left.state == "MISSING" and right.state == "MISSING":
        return "both_missing"
    if left.state == "MISSING":
        return "service_only_value"
    if right.state == "MISSING":
        return "legacy_only_value"
    return "equal" if left.value == right.value else "conflict"


FIELD_PARSERS: dict[str, tuple[str, str, Callable[[Any], Parsed], Callable[[Any], Parsed]]] = {
    "title_ko": ("movie_nm", "title_ko", _text, _text),
    "title_en": ("movie_nm_en", "title_en", _text, _text),
    "title_original": ("movie_nm_og", "title_original", _text, _text),
    "release_date": ("open_dt", "release_date", _date, _date),
    "production_year": (
        "prdt_year", "production_year",
        lambda value: _integer(value, minimum=1888, maximum=2200),
        lambda value: _integer(value, minimum=1888, maximum=2200),
    ),
    "kmdb_id": ("kmdb_id", "kmdb_id", _text, _text),
    "kmdb_matched": ("kmdb_matched", "kmdb_matched", _boolean, _boolean),
    "runtime_minutes": (
        "show_tm", "runtime_minutes",
        lambda value: _integer(value, minimum=1),
        lambda value: _integer(value, minimum=1),
    ),
    "genres_hash": (
        "genre_alt", "genres",
        lambda value: _large_value(value, separator=","),
        lambda value: _large_value(value, separator=","),
    ),
    "directors_hash": (
        "director_nm", "directors",
        lambda value: _large_value(value, separator="|"),
        lambda value: _large_value(value, separator="|"),
    ),
    "production_companies_hash": (
        "company_nm", "production_companies",
        lambda value: _large_value(value, separator="|"),
        lambda value: _large_value(value, separator="|"),
    ),
    "countries_hash": (
        "nation_alt", "production_countries",
        lambda value: _large_value(value, separator=","),
        lambda value: _large_value(value, separator=","),
    ),
    "plot_hash": ("plot", "plot", _large_value, _large_value),
    "poster_hash": ("poster_url", "poster_url", _large_value, _large_value),
}


def policy_for_legacy(row: Mapping[str, Any]) -> tuple[bool, tuple[str, ...]]:
    """현행 legacy adapter와 같은 정책을 진단용으로 계산한다."""
    reasons: list[str] = []
    released = _date(row.get("open_dt"))
    if released.state != "VALUE":
        reasons.append("release_date_missing" if released.state == "MISSING" else "release_date_invalid")
    elif released.value < "2020-01-01":
        reasons.append("release_date_before_2020")
    genres = _split(row.get("genre_alt"), ",")
    if any(keyword in " ".join(genres) for keyword in EXCLUDED_GENRE_KEYWORDS):
        reasons.append("excluded_genre")
    companies = set(_split(row.get("company_nm"), "|"))
    if companies & EXCLUDED_COMPANIES or any(group <= companies for group in EXCLUDED_COMPANY_GROUPS):
        reasons.append("excluded_company")
    if set(_split(row.get("director_nm"), "|")) & EXCLUDED_DIRECTORS:
        reasons.append("excluded_director")
    if _text(row.get("movie_nm")).state != "VALUE":
        reasons.append("title_missing")
    return not reasons, tuple(reasons)


def policy_for_service(row: Mapping[str, Any]) -> tuple[bool, tuple[str, ...]]:
    """서비스 영화에도 같은 정책 버전을 적용해 범위 차이를 비교한다."""
    reasons: list[str] = []
    released = _date(row.get("release_date"))
    if released.state != "VALUE":
        reasons.append("release_date_missing" if released.state == "MISSING" else "release_date_invalid")
    elif released.value < "2020-01-01":
        reasons.append("release_date_before_2020")
    genres = _split(row.get("genres"), ",")
    if any(keyword in " ".join(genres) for keyword in EXCLUDED_GENRE_KEYWORDS):
        reasons.append("excluded_genre")
    companies = set(_split(row.get("production_companies"), "|"))
    if companies & EXCLUDED_COMPANIES or any(group <= companies for group in EXCLUDED_COMPANY_GROUPS):
        reasons.append("excluded_company")
    if set(_split(row.get("directors"), "|")) & EXCLUDED_DIRECTORS:
        reasons.append("excluded_director")
    if _text(row.get("title_ko")).state != "VALUE":
        reasons.append("title_missing")
    return not reasons, tuple(reasons)


def _year_bucket(value: Any) -> str:
    parsed = _date(value)
    if parsed.state == "VALUE":
        return parsed.value[:4]
    return "MISSING" if parsed.state == "MISSING" else "INVALID"


def _index(rows: Sequence[Mapping[str, Any]], field: str) -> tuple[list[Parsed], dict[str, list[int]]]:
    parsed_ids: list[Parsed] = []
    groups: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        parsed = normalize_kofic_id(row.get(field))
        parsed_ids.append(parsed)
        if parsed.state == "VALUE":
            groups[parsed.value].append(index)
    return parsed_ids, dict(groups)


def _classify_side(
    parsed_ids: Sequence[Parsed], own: Mapping[str, list[int]], other: Mapping[str, list[int]],
) -> tuple[Counter[str], list[str]]:
    counts: Counter[str] = Counter()
    classes: list[str] = []
    for parsed in parsed_ids:
        if parsed.state == "MISSING":
            category = "missing_kofic"
        elif parsed.state == "INVALID":
            category = "invalid_kofic"
        elif len(own[parsed.value]) > 1 or len(other.get(parsed.value, [])) > 1:
            category = "duplicate_ambiguous"
        elif len(other.get(parsed.value, [])) == 1:
            category = "singleton_matched"
        else:
            category = "singleton_only"
        counts[category] += 1
        classes.append(category)
    return counts, classes


def _casefold_collisions(*groups: Mapping[str, list[int]]) -> int:
    values: dict[str, set[str]] = defaultdict(set)
    for group in groups:
        for value in group:
            values[value.casefold()].add(value)
    return sum(1 for originals in values.values() if len(originals) > 1)


def reconcile_movies(
    legacy_rows: Sequence[Mapping[str, Any]],
    service_rows: Sequence[Mapping[str, Any]],
    gold_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """세 영화 집합을 대사하고 안전한 집계 결과만 반환한다."""
    legacy_ids, legacy_groups = _index(legacy_rows, "movie_cd")
    service_ids, service_groups = _index(service_rows, "kofic_movie_cd")
    gold_ids, gold_groups = _index(gold_rows, "kofic_movie_cd")
    legacy_counts, legacy_classes = _classify_side(legacy_ids, legacy_groups, service_groups)
    service_counts, service_classes = _classify_side(service_ids, service_groups, legacy_groups)

    field_counts = {name: Counter() for name in FIELD_PARSERS}
    matched_ids = sorted(
        value for value in legacy_groups.keys() & service_groups.keys()
        if len(legacy_groups[value]) == len(service_groups[value]) == 1
    )
    policy_agreement: Counter[str] = Counter()
    policy_mismatches: list[dict[str, Any]] = []
    for value in matched_ids:
        legacy = legacy_rows[legacy_groups[value][0]]
        service = service_rows[service_groups[value][0]]
        for name, (legacy_field, service_field, left_parser, right_parser) in FIELD_PARSERS.items():
            field_counts[name][compare_values(left_parser(legacy.get(legacy_field)),
                                              right_parser(service.get(service_field)))] += 1
        legacy_eligible, legacy_reasons = policy_for_legacy(legacy)
        service_eligible, service_reasons = policy_for_service(service)
        agreement = f"legacy_{'eligible' if legacy_eligible else 'excluded'}:" \
                    f"service_{'eligible' if service_eligible else 'excluded'}"
        policy_agreement[agreement] += 1
        if legacy_eligible != service_eligible:
            policy_mismatches.append({
                "kofic_movie_cd": value,
                "legacy_reasons": list(legacy_reasons),
                "service_reasons": list(service_reasons),
            })

    policy_counts: Counter[str] = Counter()
    policy_reason_counts: Counter[str] = Counter()
    legacy_year_counts: Counter[str] = Counter()
    for row, category in zip(legacy_rows, legacy_classes):
        eligible, reasons = policy_for_legacy(row)
        policy_counts[f"{category}:{'eligible' if eligible else 'excluded'}"] += 1
        policy_reason_counts.update(reasons)
        legacy_year_counts[f"{category}:{_year_bucket(row.get('open_dt'))}"] += 1

    service_state_counts = Counter(
        f"{category}:{row.get('service_status')}:{row.get('approval_status')}:{bool(row.get('is_removed'))}"
        for row, category in zip(service_rows, service_classes)
    )
    service_source_counts = Counter(
        f"{category}:{row.get('source_system')}"
        for row, category in zip(service_rows, service_classes)
    )
    service_state_by_policy: Counter[str] = Counter()
    service_policy_counts: Counter[str] = Counter()
    service_policy_reason_counts: Counter[str] = Counter()
    service_year_counts: Counter[str] = Counter()
    for row, category in zip(service_rows, service_classes):
        eligible, reasons = policy_for_service(row)
        service_policy_counts[f"{category}:{'eligible' if eligible else 'excluded'}"] += 1
        service_policy_reason_counts.update(reasons)
        service_year_counts[f"{category}:{_year_bucket(row.get('release_date'))}"] += 1
        service_state_by_policy[
            f"{'eligible' if eligible else 'excluded'}:{row.get('service_status')}:"
            f"{row.get('approval_status')}:{bool(row.get('is_removed'))}"
        ] += 1

    gold_counts: Counter[str] = Counter()
    for parsed in gold_ids:
        if parsed.state == "MISSING":
            category = "missing_kofic"
        elif parsed.state == "INVALID":
            category = "invalid_kofic"
        elif len(gold_groups[parsed.value]) > 1 or len(legacy_groups.get(parsed.value, [])) > 1 \
                or len(service_groups.get(parsed.value, [])) > 1:
            category = "duplicate_ambiguous"
        else:
            in_legacy = len(legacy_groups.get(parsed.value, [])) == 1
            in_service = len(service_groups.get(parsed.value, [])) == 1
            if in_legacy and in_service:
                category = "linked_both"
            elif in_legacy:
                category = "linked_legacy_only"
            elif in_service:
                category = "linked_service_only"
            else:
                category = "gold_only"
        gold_counts[category] += 1

    result = {
        "rule_version": RULE_VERSION,
        "policy_version": POLICY_VERSION,
        "legacy": {
            "total_rows": len(legacy_rows),
            "classification": dict(sorted(legacy_counts.items())),
            "duplicate_id_groups": sum(len(indexes) > 1 for indexes in legacy_groups.values()),
        },
        "service": {
            "total_rows": len(service_rows),
            "classification": dict(sorted(service_counts.items())),
            "duplicate_id_groups": sum(len(indexes) > 1 for indexes in service_groups.values()),
            "source_hash_present_count": sum(bool(row.get("source_hash")) for row in service_rows),
        },
        "gold": {
            "total_rows": len(gold_rows),
            "classification": dict(sorted(gold_counts.items())),
            "duplicate_id_groups": sum(len(indexes) > 1 for indexes in gold_groups.values()),
        },
        "id_group_counts": {
            "legacy_valid_ids": len(legacy_groups),
            "service_valid_ids": len(service_groups),
            "gold_valid_ids": len(gold_groups),
            "legacy_service_singleton_matches": len(matched_ids),
            "casefold_collision_groups": _casefold_collisions(
                legacy_groups, service_groups, gold_groups
            ),
        },
        "matched_field_comparison": {
            name: dict(sorted(counts.items())) for name, counts in field_counts.items()
        },
        "matched_policy_comparison": {
            "classification": dict(sorted(policy_agreement.items())),
            "mismatch_count": len(policy_mismatches),
            "mismatches": sorted(policy_mismatches, key=lambda row: row["kofic_movie_cd"])[:20],
        },
        "legacy_policy": {
            "classification": dict(sorted(policy_counts.items())),
            "exclusion_reasons": dict(sorted(policy_reason_counts.items())),
        },
        "service_policy": {
            "classification": dict(sorted(service_policy_counts.items())),
            "exclusion_reasons": dict(sorted(service_policy_reason_counts.items())),
        },
        "release_year_by_classification": {
            "legacy": dict(sorted(legacy_year_counts.items())),
            "service": dict(sorted(service_year_counts.items())),
        },
        "service_state_by_classification": dict(sorted(service_state_counts.items())),
        "service_state_by_policy": dict(sorted(service_state_by_policy.items())),
        "service_source_by_classification": dict(sorted(service_source_counts.items())),
        "diagnostic_ids": {
            "legacy_singleton_only_sample": sorted(
                value for value, indexes in legacy_groups.items()
                if len(indexes) == 1 and value not in service_groups
            )[:20],
            "service_singleton_only": sorted(
                value for value, indexes in service_groups.items()
                if len(indexes) == 1 and value not in legacy_groups
            )[:20],
            "gold_only": sorted(
                value for value, indexes in gold_groups.items()
                if len(indexes) == 1 and value not in legacy_groups and value not in service_groups
            )[:20],
        },
    }
    _assert_conservation(result)
    return result


def _assert_conservation(result: Mapping[str, Any]) -> None:
    for source in ("legacy", "service", "gold"):
        total = result[source]["total_rows"]
        classified = sum(result[source]["classification"].values())
        if total != classified:
            raise AssertionError(f"{source} row conservation failed: {classified} != {total}")


def relation_impact_counts(
    service_rows: Sequence[Mapping[str, Any]], relation_rows: Iterable[Mapping[str, Any]],
) -> dict[str, int]:
    """서비스 영화 FK 관계의 연결 영화 수와 고아 행 수를 계산한다."""
    movie_ids = {row.get("id") for row in service_rows}
    related_ids: set[Any] = set()
    row_count = orphan_count = 0
    for row in relation_rows:
        row_count += 1
        movie_id = row.get("movie_id")
        if movie_id in movie_ids:
            related_ids.add(movie_id)
        else:
            orphan_count += 1
    return {
        "row_count": row_count,
        "linked_movie_count": len(related_ids),
        "orphan_row_count": orphan_count,
    }
