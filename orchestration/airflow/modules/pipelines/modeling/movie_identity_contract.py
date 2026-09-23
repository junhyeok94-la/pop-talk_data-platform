"""영화 stable key, 연결 결정, 속성 현재값의 순수 계약.

이 모듈은 DB나 Airflow를 모른다. 후보 적재와 Gold 구현 전에 같은 입력이
항상 같은 결정 ID와 현재값을 만드는지 fixture로 검증하기 위한 기준 코드다.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping, Sequence

IDENTITY_RULE_VERSION = "movie-identity-v1"
ATTRIBUTE_RULE_VERSION = "movie-attribute-current-v1"
ALLOWED_VALUE_STATES = frozenset({
    "PRESENT", "NOT_COLLECTED", "MATCH_PENDING", "EXPLICITLY_CLEARED", "PARSE_ERROR",
})


def _packed(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_packed(value)).hexdigest()


def deterministic_movie_key(kofic_movie_cd: str) -> str:
    """기존 dbt movie_key와 호환되는 최초 KOFIC stable key를 발급한다."""
    if not isinstance(kofic_movie_cd, str) or not re.fullmatch(
        r"[A-Za-z0-9]{1,32}", kofic_movie_cd.strip()
    ):
        raise ValueError("Valid KOFIC movie ID is required")
    canonical = f"kofic:{kofic_movie_cd.strip()}"
    return hashlib.md5(canonical.encode("utf-8"), usedforsecurity=False).hexdigest()


@dataclass(frozen=True)
class DecisionDraft:
    identity_key: str
    movie_key: str | None
    decision_type: str
    status: str
    evidence_hash: str
    effective_at: str | None
    effective_time_basis: str
    rule_version: str = IDENTITY_RULE_VERSION
    predecessor_decision_id: str | None = None

    def decision_id(self) -> str:
        return _digest({
            "identity_key": self.identity_key,
            "movie_key": self.movie_key,
            "decision_type": self.decision_type,
            "status": self.status,
            "evidence_hash": self.evidence_hash,
            "effective_at": self.effective_at,
            "effective_time_basis": self.effective_time_basis,
            "rule_version": self.rule_version,
            "predecessor_decision_id": self.predecessor_decision_id,
        })


@dataclass(frozen=True)
class DecisionEvent:
    decision_id: str
    identity_key: str
    movie_key: str | None
    decision_type: str
    status: str
    evidence_hash: str
    effective_at: str | None
    effective_time_basis: str
    rule_version: str
    predecessor_decision_id: str | None
    decision_sequence: int
    decision_recorded_at: str


def append_decision(
    existing: Sequence[DecisionEvent], draft: DecisionDraft, *, recorded_at: str,
) -> tuple[DecisionEvent, bool]:
    """한 identity의 append 원장을 흉내 내며 replay는 최초 event를 반환한다.

    실제 DB 구현은 같은 검사를 lock/transaction 안에서 수행해야 한다.
    caller가 predecessor를 현재 head에서 계산해 바꾸지 못하도록 직접 검증한다.
    """
    identity_events = sorted(
        (event for event in existing if event.identity_key == draft.identity_key),
        key=lambda event: event.decision_sequence,
    )
    expected_predecessor = identity_events[-1].decision_id if identity_events else None
    candidate_id = draft.decision_id()
    replay = next((event for event in identity_events if event.decision_id == candidate_id), None)
    if replay:
        return replay, False
    if draft.predecessor_decision_id != expected_predecessor:
        raise ValueError("Decision predecessor is not the current identity head")
    event = DecisionEvent(
        decision_id=candidate_id,
        identity_key=draft.identity_key,
        movie_key=draft.movie_key,
        decision_type=draft.decision_type,
        status=draft.status,
        evidence_hash=draft.evidence_hash,
        effective_at=draft.effective_at,
        effective_time_basis=draft.effective_time_basis,
        rule_version=draft.rule_version,
        predecessor_decision_id=draft.predecessor_decision_id,
        decision_sequence=(identity_events[-1].decision_sequence + 1) if identity_events else 1,
        decision_recorded_at=recorded_at,
    )
    return event, True


def decision_history(events: Sequence[DecisionEvent]) -> list[dict[str, Any]]:
    """identity별 sequence 반개구간 [from, to) history를 투영한다."""
    grouped: dict[str, list[DecisionEvent]] = {}
    for event in events:
        grouped.setdefault(event.identity_key, []).append(event)
    rows: list[dict[str, Any]] = []
    for identity_key, identity_events in sorted(grouped.items()):
        ordered = sorted(identity_events, key=lambda event: event.decision_sequence)
        if [event.decision_sequence for event in ordered] != list(range(1, len(ordered) + 1)):
            raise ValueError("Decision sequence must be contiguous per identity")
        for index, event in enumerate(ordered):
            expected_predecessor = ordered[index - 1].decision_id if index else None
            if event.predecessor_decision_id != expected_predecessor:
                raise ValueError("Decision predecessor chain is invalid")
            rebuilt = DecisionDraft(
                identity_key=event.identity_key, movie_key=event.movie_key,
                decision_type=event.decision_type, status=event.status,
                evidence_hash=event.evidence_hash, effective_at=event.effective_at,
                effective_time_basis=event.effective_time_basis,
                rule_version=event.rule_version,
                predecessor_decision_id=event.predecessor_decision_id,
            )
            if rebuilt.decision_id() != event.decision_id:
                raise ValueError("Decision ID does not match its immutable content")
            next_sequence = ordered[index + 1].decision_sequence if index + 1 < len(ordered) else None
            rows.append({
                **event.__dict__,
                "valid_from_decision_sequence": event.decision_sequence,
                "valid_to_decision_sequence": next_sequence,
                "is_current": next_sequence is None,
            })
    return rows


def initialize_kmdb_connection(
    observations: Sequence[Mapping[str, Any]],
    *,
    previous_active: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """확정 연결을 우선하고 Legacy/Service 보강은 provisional로만 초기화한다."""
    confirmed = sorted({
        str(row["kmdb_id"]).strip() for row in observations
        if row.get("connection_status") == "MATCHED" and row.get("kmdb_id")
    })
    provisional_by_source: dict[str, set[str]] = {"LEGACY": set(), "SERVICE_PRESERVED": set()}
    for row in observations:
        source = row.get("source_kind")
        if source in provisional_by_source and row.get("kmdb_id") and row.get("kmdb_matched"):
            provisional_by_source[source].add(str(row["kmdb_id"]).strip())
    provisional = sorted(set().union(*provisional_by_source.values()))

    previous_status = (
        previous_active.get("active_status", previous_active.get("status"))
        if previous_active else None
    )
    has_active_connection = _has_active_connection(previous_active)
    if previous_status == "CANCELLED":
        return {**dict(previous_active), "retained_previous": True,
                "active_status": "CANCELLED", "status": "CANCELLED",
                "reason": "EXPLICIT_RECONNECT_DECISION_REQUIRED"}
    if len(confirmed) == 1:
        if has_active_connection and previous_active.get("kmdb_id") != confirmed[0]:
            return _review_result(previous_active, "CONFIRMED_ID_REPLACEMENT_REQUIRES_DECISION")
        return {"status": "CONFIRMED", "active_status": "CONFIRMED",
                "kmdb_id": confirmed[0], "confidence": "CONFIRMED"}
    if len(confirmed) > 1:
        return _review_result(previous_active, "CONFIRMED_ID_CONFLICT")
    if previous_status == "CONFIRMED":
        evaluation_status = "UNMATCHED" if not provisional else "PROVISIONAL_IGNORED"
        return {**dict(previous_active), "status": evaluation_status,
                "active_status": "CONFIRMED", "retained_previous": True,
                "reason": "LOWER_CONFIDENCE_OBSERVATION_IGNORED"}
    if len(provisional) == 1:
        if has_active_connection and previous_active.get("kmdb_id") != provisional[0]:
            return _review_result(previous_active, "PROVISIONAL_ID_REPLACEMENT_REQUIRES_DECISION")
        sources = [source for source, ids in provisional_by_source.items() if provisional[0] in ids]
        confidence = "PROVISIONAL_LEGACY" if "LEGACY" in sources else "PROVISIONAL_SERVICE"
        return {"status": "PROVISIONAL", "active_status": "PROVISIONAL",
                "kmdb_id": provisional[0], "confidence": confidence}
    if len(provisional) > 1:
        return _review_result(previous_active, "PROVISIONAL_ID_CONFLICT")
    if previous_active and previous_status == "PROVISIONAL":
        return {**dict(previous_active), "status": "UNMATCHED",
                "active_status": "PROVISIONAL", "retained_previous": True,
                "reason": "NO_NEW_CONNECTION_EVIDENCE"}
    return {"status": "UNMATCHED", "active_status": None,
            "kmdb_id": None, "confidence": None}


def _review_result(previous_active: Mapping[str, Any] | None, reason: str) -> dict[str, Any]:
    active_status = (
        previous_active.get("active_status", previous_active.get("status"))
        if previous_active else None
    )
    if _has_active_connection(previous_active):
        return {
            "status": "REVIEW_REQUIRED",
            "active_status": active_status,
            "kmdb_id": previous_active.get("kmdb_id"),
            "confidence": previous_active.get("confidence"),
            "reason": reason,
            "retained_previous": True,
        }
    return {
        "status": "REVIEW_REQUIRED", "active_status": None,
        "kmdb_id": None, "confidence": None,
        "reason": reason, "retained_previous": False,
    }


def _has_active_connection(previous_active: Mapping[str, Any] | None) -> bool:
    """평가 결과 객체가 아니라 실제로 보존할 수 있는 활성 연결인지 판정한다."""
    if not previous_active:
        return False
    active_status = previous_active.get(
        "active_status", previous_active.get("status")
    )
    kmdb_id = previous_active.get("kmdb_id")
    return active_status in {"CONFIRMED", "PROVISIONAL"} and bool(
        str(kmdb_id).strip() if kmdb_id is not None else ""
    )


@dataclass(frozen=True)
class AttributeObservation:
    observation_id: str
    movie_key: str
    attribute_name: str
    value: Any
    value_state: str
    source_kind: str
    source_identity: str
    source_run_id: str
    source_observed_at: str | None
    source_observed_at_known: bool
    decision_sequence: int

    def __post_init__(self) -> None:
        if self.value_state not in ALLOWED_VALUE_STATES:
            raise ValueError("Unsupported attribute value state")
        if self.source_observed_at_known != bool(self.source_observed_at):
            raise ValueError("Known observation time must agree with its value")
        if self.source_observed_at_known:
            _utc_instant(self.source_observed_at)


def _utc_instant(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Observation time must be a timezone-aware ISO string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("Observation time must be a timezone-aware ISO string") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Observation time must include a timezone")
    return parsed.astimezone(UTC)


SOURCE_TRUST = {"API": 3, "LEGACY": 2, "SERVICE_PRESERVED": 1}


def select_attribute_current(
    observations: Iterable[AttributeObservation],
    *,
    active_source_identity: str | None = None,
    cancelled_source_identities: frozenset[str] = frozenset(),
) -> AttributeObservation | None:
    """PRESENT 후보 중 같은 원천의 과거 정상값을 먼저 보존해 current를 선택한다."""
    return resolve_attribute_current(
        observations,
        active_source_identity=active_source_identity,
        cancelled_source_identities=cancelled_source_identities,
    )["selected"]


def resolve_attribute_current(
    observations: Iterable[AttributeObservation],
    *,
    active_source_identity: str | None = None,
    cancelled_source_identities: frozenset[str] = frozenset(),
    previous_current: AttributeObservation | None = None,
) -> dict[str, Any]:
    """동일 관측시각 상충을 자동 선택하지 않고 상태와 선택행을 함께 반환한다."""
    observation_rows = list(observations)
    groups = {(row.movie_key, row.attribute_name) for row in observation_rows}
    if len(groups) > 1:
        raise ValueError("Attribute current input must contain one movie and attribute")
    if previous_current and groups and (
        previous_current.movie_key, previous_current.attribute_name
    ) != next(iter(groups)):
        raise ValueError("Previous current belongs to another movie or attribute")
    if previous_current and previous_current.value_state != "PRESENT":
        raise ValueError("Previous current must be a PRESENT observation")
    candidates = []
    for observation in observation_rows:
        if observation.value_state != "PRESENT":
            continue
        if observation.source_identity in cancelled_source_identities:
            continue
        if active_source_identity is not None and observation.source_identity != active_source_identity:
            continue
        trust = SOURCE_TRUST.get(observation.source_kind, 0)
        observed = _utc_instant(observation.source_observed_at) \
            if observation.source_observed_at_known else datetime.min.replace(tzinfo=UTC)
        candidates.append((trust, observation.source_observed_at_known, observed,
                           observation.decision_sequence, observation.observation_id, observation))
    if not candidates:
        return {"status": "NO_VALUE", "selected": None, "reason": "NO_PRESENT_CANDIDATE"}
    candidates.sort(reverse=True, key=lambda item: item[:5])
    top = candidates[0]
    same_observation_rank = [
        item for item in candidates
        if item[:3] == top[:3] and item[-1].source_identity == top[-1].source_identity
    ]
    if len({_digest(item[-1].value) for item in same_observation_rank}) > 1:
        retained = previous_current
        if retained and (
            retained.source_identity in cancelled_source_identities
            or active_source_identity is not None
            and retained.source_identity != active_source_identity
        ):
            retained = None
        return {
            "status": "REVIEW_REQUIRED", "selected": retained,
            "reason": "SAME_TIME_VALUE_CONFLICT",
        }
    return {"status": "SELECTED", "selected": top[-1], "reason": None}


def build_match_evaluation(
    *,
    source_run_id: str,
    canonical_movie_key: str,
    rule_version: str,
    candidates: Sequence[Mapping[str, Any]],
    status: str,
    selected_source_identity: str | None,
    evidence_hash: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """후보가 0개여도 evaluation 한 행을 만들고 후보 행을 별도로 반환한다."""
    candidate_identities: list[str] = []
    for candidate in candidates:
        if "evaluation_id" in candidate:
            raise ValueError("Candidate cannot provide the generated evaluation ID")
        identity = candidate.get("source_identity")
        if not isinstance(identity, str) or not identity:
            raise ValueError("Candidate source identity is required")
        candidate_identities.append(identity)
    if len(candidate_identities) != len(set(candidate_identities)):
        raise ValueError("Candidate source identity must be unique per evaluation")
    if status == "UNMATCHED" and selected_source_identity is not None:
        raise ValueError("UNMATCHED evaluation cannot select a candidate")
    if status == "MATCHED" and selected_source_identity not in candidate_identities:
        raise ValueError("MATCHED selection must reference one candidate")
    if selected_source_identity is not None and selected_source_identity not in candidate_identities:
        raise ValueError("Selected source identity must reference one candidate")
    normalized_candidates = sorted(
        (dict(candidate) for candidate in candidates),
        key=lambda candidate: candidate["source_identity"],
    )
    candidate_set_hash = _digest(normalized_candidates)
    evaluation_id = _digest({
        "source_run_id": source_run_id,
        "canonical_movie_key": canonical_movie_key,
        "rule_version": rule_version,
        "evidence_hash": evidence_hash,
        "candidate_set_hash": candidate_set_hash,
    })
    evaluation = {
        "evaluation_id": evaluation_id,
        "source_run_id": source_run_id,
        "canonical_movie_key": canonical_movie_key,
        "rule_version": rule_version,
        "status": status,
        "selected_source_identity": selected_source_identity,
        "candidate_count": len(candidates),
        "evidence_hash": evidence_hash,
        "candidate_set_hash": candidate_set_hash,
    }
    evaluation["payload_hash"] = _digest(evaluation)
    candidate_rows = [
        {"evaluation_id": evaluation_id, **candidate} for candidate in normalized_candidates
    ]
    return evaluation, candidate_rows


def assert_evaluation_compatible(
    existing: Mapping[str, Any], incoming: Mapping[str, Any],
) -> None:
    """같은 evaluation grain/ID의 outcome payload가 바뀌는 것을 거부한다."""
    if existing.get("evaluation_id") != incoming.get("evaluation_id"):
        return
    if existing.get("payload_hash") != incoming.get("payload_hash"):
        raise ValueError("Immutable match evaluation payload conflict")


def cancel_connection(previous: Mapping[str, Any]) -> dict[str, Any]:
    """취소된 연결이 과거 provisional 값으로 부활하지 않도록 명시한다."""
    return {
        "status": "CANCELLED",
        "active_status": "CANCELLED",
        "kmdb_id": previous.get("kmdb_id"),
        "confidence": previous.get("confidence"),
    }


def normalize_exchange_time(
    row: Mapping[str, Any], *, exchange_contract_version: int, source_kind: str,
) -> dict[str, Any]:
    """기존 API v1과 Legacy v2의 시간 상태를 공통 형태로 변환한다."""
    normalized = dict(row)
    if exchange_contract_version == 1:
        if source_kind != "API":
            raise ValueError("Exchange v1 is only valid for API observations")
        if not row.get("source_observed_at"):
            raise ValueError("API v1 observation time is required")
        normalized["source_observed_at"] = _utc_instant(row["source_observed_at"]).isoformat()
        normalized["source_observed_at_known"] = True
        normalized["source_time_basis"] = "API_COLLECTED_AT"
        return normalized
    if exchange_contract_version != 2:
        raise ValueError("Unsupported exchange contract version")
    known = row.get("source_observed_at_known")
    basis = row.get("source_time_basis")
    observed_at = row.get("source_observed_at")
    expected = {
        "API": (True, "API_COLLECTED_AT"),
        "SERVICE_PRESERVED": (True, "SERVICE_SNAPSHOT_AT"),
        "LEGACY": (False, "LEGACY_UNKNOWN"),
    }.get(source_kind)
    if expected is None:
        raise ValueError("Unsupported observation source kind")
    valid = known is expected[0] and basis == expected[1] and (
        (known is True and bool(observed_at)) or (known is False and observed_at is None)
    )
    if not valid:
        raise ValueError("Exchange v2 observation time invariant failed")
    if known:
        normalized["source_observed_at"] = _utc_instant(observed_at).isoformat()
    return normalized
