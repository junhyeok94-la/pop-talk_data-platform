import unittest

from pipelines.modeling.movie_identity_contract import (
    AttributeObservation,
    assert_evaluation_compatible,
    DecisionDraft,
    append_decision,
    cancel_connection,
    decision_history,
    deterministic_movie_key,
    build_match_evaluation,
    initialize_kmdb_connection,
    normalize_exchange_time,
    resolve_attribute_current,
    select_attribute_current,
)


class MovieIdentityContractTest(unittest.TestCase):
    def test_movie_key_is_compatible_and_deterministic(self):
        self.assertEqual(deterministic_movie_key("20260001"), deterministic_movie_key(" 20260001 "))
        self.assertEqual(len(deterministic_movie_key("20260001")), 32)

    def test_decision_replay_keeps_first_sequence_and_recorded_time(self):
        draft = DecisionDraft("kmdb:A", "movie:1", "CONNECT", "CONFIRMED", "e" * 64,
                              None, "UNKNOWN")
        first, created = append_decision([], draft, recorded_at="2026-09-10T00:00:00Z")
        replay, replay_created = append_decision([first], draft, recorded_at="2026-09-11T00:00:00Z")
        self.assertTrue(created)
        self.assertFalse(replay_created)
        self.assertEqual(replay, first)

    def test_connect_cancel_reconnect_has_distinct_ids_and_half_open_history(self):
        first_draft = DecisionDraft("kmdb:A", "movie:1", "CONNECT", "CONFIRMED", "a" * 64,
                                    None, "UNKNOWN")
        first, _ = append_decision([], first_draft, recorded_at="t1")
        cancel_draft = DecisionDraft("kmdb:A", "movie:1", "CANCEL", "CANCELLED", "b" * 64,
                                     None, "UNKNOWN", predecessor_decision_id=first.decision_id)
        cancelled, _ = append_decision([first], cancel_draft, recorded_at="t2")
        reconnect_draft = DecisionDraft("kmdb:A", "movie:1", "CONNECT", "CONFIRMED", "a" * 64,
                                        None, "UNKNOWN", predecessor_decision_id=cancelled.decision_id)
        reconnected, _ = append_decision([first, cancelled], reconnect_draft, recorded_at="t3")
        self.assertEqual(len({first.decision_id, cancelled.decision_id, reconnected.decision_id}), 3)
        history = decision_history([reconnected, first, cancelled])
        self.assertEqual([row["valid_to_decision_sequence"] for row in history], [2, 3, None])

    def test_stale_predecessor_is_rejected(self):
        first, _ = append_decision([], DecisionDraft(
            "kmdb:A", "movie:1", "CONNECT", "CONFIRMED", "a" * 64, None, "UNKNOWN"
        ), recorded_at="t1")
        with self.assertRaisesRegex(ValueError, "predecessor"):
            append_decision([first], DecisionDraft(
                "kmdb:A", "movie:2", "REPLACE", "CONFIRMED", "b" * 64, None, "UNKNOWN"
            ), recorded_at="t2")

    def test_legacy_and_service_initialize_provisional_only(self):
        legacy = initialize_kmdb_connection([
            {"source_kind": "LEGACY", "kmdb_id": "K1", "kmdb_matched": True}
        ])
        service = initialize_kmdb_connection([
            {"source_kind": "SERVICE_PRESERVED", "kmdb_id": "K2", "kmdb_matched": True}
        ])
        self.assertEqual((legacy["status"], legacy["confidence"]),
                         ("PROVISIONAL", "PROVISIONAL_LEGACY"))
        self.assertEqual(service["confidence"], "PROVISIONAL_SERVICE")

    def test_provisional_conflict_keeps_previous_or_returns_null(self):
        observations = [
            {"source_kind": "LEGACY", "kmdb_id": "K1", "kmdb_matched": True},
            {"source_kind": "SERVICE_PRESERVED", "kmdb_id": "K2", "kmdb_matched": True},
        ]
        empty = initialize_kmdb_connection(observations)
        retained = initialize_kmdb_connection(
            observations,
            previous_active={"status": "PROVISIONAL", "kmdb_id": "K1",
                             "confidence": "PROVISIONAL_LEGACY"},
        )
        self.assertIsNone(empty["kmdb_id"])
        self.assertEqual(retained["kmdb_id"], "K1")

    def test_cancelled_connection_is_not_revived_by_old_observation(self):
        cancelled = cancel_connection({"kmdb_id": "K1", "confidence": "PROVISIONAL_LEGACY"})
        result = initialize_kmdb_connection([
            {"source_kind": "LEGACY", "kmdb_id": "K1", "kmdb_matched": True}
        ], previous_active=cancelled)
        self.assertEqual(result["status"], "CANCELLED")
        self.assertEqual(result["reason"], "EXPLICIT_RECONNECT_DECISION_REQUIRED")

    def test_previous_confirmed_is_not_replaced_by_provisional(self):
        result = initialize_kmdb_connection([
            {"source_kind": "LEGACY", "kmdb_id": "K2", "kmdb_matched": True}
        ], previous_active={"status": "CONFIRMED", "kmdb_id": "K1", "confidence": "CONFIRMED"})
        self.assertEqual(result["kmdb_id"], "K1")

    def test_review_retained_connection_cannot_be_replaced_by_repeated_observation(self):
        first_review = initialize_kmdb_connection([
            {"connection_status": "MATCHED", "kmdb_id": "K2", "source_kind": "API"}
        ], previous_active={"status": "CONFIRMED", "kmdb_id": "K1", "confidence": "CONFIRMED"})
        second_review = initialize_kmdb_connection([
            {"connection_status": "MATCHED", "kmdb_id": "K2", "source_kind": "API"}
        ], previous_active=first_review)
        self.assertEqual((second_review["status"], second_review["kmdb_id"]),
                         ("REVIEW_REQUIRED", "K1"))

    def test_empty_observation_between_conflicts_does_not_drop_confirmed_connection(self):
        active = {"status": "CONFIRMED", "active_status": "CONFIRMED",
                  "kmdb_id": "K1", "confidence": "CONFIRMED"}
        review = initialize_kmdb_connection([
            {"connection_status": "MATCHED", "kmdb_id": "K2", "source_kind": "API"}
        ], previous_active=active)
        empty = initialize_kmdb_connection([], previous_active=review)
        again = initialize_kmdb_connection([
            {"connection_status": "MATCHED", "kmdb_id": "K2", "source_kind": "API"}
        ], previous_active=empty)
        self.assertEqual((empty["active_status"], empty["kmdb_id"]), ("CONFIRMED", "K1"))
        self.assertEqual((again["status"], again["kmdb_id"]), ("REVIEW_REQUIRED", "K1"))

    def test_unmatched_can_progress_to_provisional_and_confirmed(self):
        unmatched = initialize_kmdb_connection([])
        provisional = initialize_kmdb_connection([
            {"source_kind": "LEGACY", "kmdb_id": "K1", "kmdb_matched": True}
        ], previous_active=unmatched)
        confirmed = initialize_kmdb_connection([
            {"source_kind": "API", "connection_status": "MATCHED", "kmdb_id": "K1"}
        ], previous_active=provisional)

        self.assertEqual((unmatched["active_status"], unmatched["kmdb_id"]), (None, None))
        self.assertEqual((provisional["status"], provisional["kmdb_id"]),
                         ("PROVISIONAL", "K1"))
        self.assertEqual((confirmed["status"], confirmed["active_status"], confirmed["kmdb_id"]),
                         ("CONFIRMED", "CONFIRMED", "K1"))

    def test_initial_conflict_without_active_connection_does_not_block_later_single_candidate(self):
        conflict = initialize_kmdb_connection([
            {"source_kind": "LEGACY", "kmdb_id": "K1", "kmdb_matched": True},
            {"source_kind": "SERVICE_PRESERVED", "kmdb_id": "K2", "kmdb_matched": True},
        ])
        resolved = initialize_kmdb_connection([
            {"source_kind": "LEGACY", "kmdb_id": "K1", "kmdb_matched": True}
        ], previous_active=conflict)

        self.assertEqual((conflict["status"], conflict["active_status"],
                          conflict["retained_previous"]),
                         ("REVIEW_REQUIRED", None, False))
        self.assertEqual((resolved["status"], resolved["kmdb_id"]),
                         ("PROVISIONAL", "K1"))

    def _attribute(self, identity, state, value, observed_at, sequence, source="API"):
        return AttributeObservation(
            observation_id=f"o{sequence}", movie_key="m", attribute_name="poster_url",
            value=value, value_state=state, source_kind=source, source_identity=identity,
            source_run_id=f"r{sequence}", source_observed_at=observed_at,
            source_observed_at_known=observed_at is not None, decision_sequence=sequence,
        )

    def test_latest_api_missing_keeps_earlier_api_present_before_legacy(self):
        observations = [
            self._attribute("kofic:A", "PRESENT", "api-old", "2026-09-01T00:00:00Z", 1),
            self._attribute("kofic:A", "NOT_COLLECTED", None, "2026-09-02T00:00:00Z", 2),
            self._attribute("kofic:A", "PRESENT", "legacy", None, 1, "LEGACY"),
        ]
        self.assertEqual(select_attribute_current(observations).value, "api-old")

    def test_same_kmdb_identity_keeps_earlier_present_but_replacement_excludes_it(self):
        observations = [
            self._attribute("kmdb:K1", "PRESENT", "old-poster", "2026-09-01T00:00:00Z", 1),
            self._attribute("kmdb:K1", "NOT_COLLECTED", None, "2026-09-02T00:00:00Z", 2),
            self._attribute("kmdb:K2", "PRESENT", "new-poster", "2026-09-03T00:00:00Z", 3),
        ]
        self.assertEqual(select_attribute_current(observations, active_source_identity="kmdb:K1").value,
                         "old-poster")
        self.assertEqual(select_attribute_current(observations, active_source_identity="kmdb:K2").value,
                         "new-poster")

    def test_cancelled_identity_does_not_revive_provisional_value(self):
        observation = self._attribute("kmdb:K1", "PRESENT", "poster", None, 1, "LEGACY")
        self.assertIsNone(select_attribute_current(
            [observation], cancelled_source_identities=frozenset({"kmdb:K1"})
        ))
        self.assertEqual(cancel_connection({"kmdb_id": "K1", "confidence": "PROVISIONAL_LEGACY"})["status"],
                         "CANCELLED")

    def test_same_time_conflict_requires_review_and_retains_previous(self):
        first = self._attribute("kmdb:K1", "PRESENT", "one", "2026-09-01T00:00:00Z", 1)
        second = self._attribute("kmdb:K1", "PRESENT", "two", "2026-09-01T00:00:00Z", 2)
        previous = self._attribute("kmdb:K1", "PRESENT", "previous", "2026-08-31T00:00:00Z", 0)
        result = resolve_attribute_current(
            [first, second], active_source_identity="kmdb:K1", previous_current=previous,
        )
        self.assertEqual(result["status"], "REVIEW_REQUIRED")
        self.assertEqual(result["selected"].value, "previous")

    def test_timestamp_order_uses_utc_instant(self):
        older = self._attribute("kmdb:K1", "PRESENT", "older",
                                "2026-09-10T09:00:00+09:00", 1)
        newer = self._attribute("kmdb:K1", "PRESENT", "newer",
                                "2026-09-10T01:00:00Z", 2)
        self.assertEqual(select_attribute_current([older, newer]).value, "newer")

    def test_same_instant_different_timezone_is_conflict(self):
        one = self._attribute("kmdb:K1", "PRESENT", "one",
                              "2026-09-10T09:00:00+09:00", 1)
        two = self._attribute("kmdb:K1", "PRESENT", "two",
                              "2026-09-10T00:00:00Z", 2)
        self.assertEqual(resolve_attribute_current([one, two])["status"], "REVIEW_REQUIRED")

    def test_attribute_group_and_previous_group_must_match(self):
        one = self._attribute("kmdb:K1", "PRESENT", "one", None, 1, "LEGACY")
        other_movie = AttributeObservation(**{**one.__dict__, "movie_key": "other"})
        with self.assertRaisesRegex(ValueError, "one movie"):
            resolve_attribute_current([one, other_movie])
        other_attribute = AttributeObservation(**{**one.__dict__, "attribute_name": "plot"})
        with self.assertRaisesRegex(ValueError, "Previous current"):
            resolve_attribute_current([one], previous_current=other_attribute)

    def test_zero_candidate_match_still_has_evaluation(self):
        evaluation, candidates = build_match_evaluation(
            source_run_id="r", canonical_movie_key="kofic:A", rule_version="match-v1",
            candidates=[], status="UNMATCHED", selected_source_identity=None,
            evidence_hash="e" * 64,
        )
        self.assertEqual(evaluation["candidate_count"], 0)
        self.assertEqual(candidates, [])

    def test_unmatched_can_preserve_rejected_candidates_without_selection(self):
        evaluation, candidates = build_match_evaluation(
            source_run_id="r", canonical_movie_key="kofic:A", rule_version="match-v1",
            candidates=[{"source_identity": "kmdb:K1", "score": 0}],
            status="UNMATCHED", selected_source_identity=None, evidence_hash="e" * 64,
        )
        self.assertEqual(evaluation["candidate_count"], 1)
        self.assertEqual(candidates[0]["evaluation_id"], evaluation["evaluation_id"])

    def test_same_evaluation_id_with_different_outcome_is_conflict(self):
        common = dict(
            source_run_id="r", canonical_movie_key="kofic:A", rule_version="match-v1",
            candidates=[{"source_identity": "kmdb:K1"}], evidence_hash="e" * 64,
        )
        matched, _ = build_match_evaluation(
            **common, status="MATCHED", selected_source_identity="kmdb:K1",
        )
        review, _ = build_match_evaluation(
            **common, status="REVIEW_REQUIRED", selected_source_identity=None,
        )
        self.assertEqual(matched["evaluation_id"], review["evaluation_id"])
        with self.assertRaisesRegex(ValueError, "payload conflict"):
            assert_evaluation_compatible(matched, review)

    def test_candidate_content_is_part_of_evaluation_identity(self):
        common = dict(
            source_run_id="r", canonical_movie_key="kofic:A", rule_version="match-v1",
            status="UNMATCHED", selected_source_identity=None, evidence_hash="e" * 64,
        )
        first, _ = build_match_evaluation(
            **common, candidates=[{"source_identity": "kmdb:K1", "score": 0}],
        )
        changed, _ = build_match_evaluation(
            **common, candidates=[{"source_identity": "kmdb:K2", "score": 99}],
        )
        self.assertNotEqual(first["evaluation_id"], changed["evaluation_id"])

    def test_match_evaluation_rejects_candidate_fk_injection_and_invalid_selection(self):
        common = dict(source_run_id="r", canonical_movie_key="kofic:A",
                      rule_version="match-v1", evidence_hash="e" * 64)
        with self.assertRaisesRegex(ValueError, "generated evaluation ID"):
            build_match_evaluation(
                **common, candidates=[{"source_identity": "kmdb:K1", "evaluation_id": "wrong"}],
                status="MATCHED", selected_source_identity="kmdb:K1",
            )
        with self.assertRaisesRegex(ValueError, "reference one candidate"):
            build_match_evaluation(
                **common, candidates=[{"source_identity": "kmdb:K1"}],
                status="MATCHED", selected_source_identity="kmdb:K2",
            )
        with self.assertRaisesRegex(ValueError, "unique"):
            build_match_evaluation(
                **common,
                candidates=[{"source_identity": "kmdb:K1"}, {"source_identity": "kmdb:K1"}],
                status="REVIEW_REQUIRED", selected_source_identity=None,
            )

    def test_exchange_time_v1_and_v2_contracts(self):
        v1 = normalize_exchange_time({"source_observed_at": "2026-09-10T00:00:00Z"},
                                     exchange_contract_version=1, source_kind="API")
        self.assertTrue(v1["source_observed_at_known"])
        legacy = normalize_exchange_time({
            "source_observed_at": None, "source_observed_at_known": False,
            "source_time_basis": "LEGACY_UNKNOWN",
        }, exchange_contract_version=2, source_kind="LEGACY")
        self.assertEqual(legacy["source_time_basis"], "LEGACY_UNKNOWN")
        with self.assertRaises(ValueError):
            normalize_exchange_time({
                "source_observed_at": None, "source_observed_at_known": True,
                "source_time_basis": "API_COLLECTED_AT",
            }, exchange_contract_version=2, source_kind="API")
        with self.assertRaises(ValueError):
            normalize_exchange_time({"source_observed_at": "garbage"},
                                    exchange_contract_version=1, source_kind="API")
        with self.assertRaises(ValueError):
            normalize_exchange_time({
                "source_observed_at": None, "source_observed_at_known": False,
                "source_time_basis": "LEGACY_UNKNOWN",
            }, exchange_contract_version=2, source_kind="API")


if __name__ == "__main__":
    unittest.main()
