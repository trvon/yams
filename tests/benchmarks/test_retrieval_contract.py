import unittest

from retrieval_contract import validate_retrieval_configuration, validate_retrieval_metrics


class RetrievalConfigurationTests(unittest.TestCase):
    def test_phss_pool_cannot_be_smaller_than_chunk_request(self):
        issues = validate_retrieval_configuration(
            {"YAMS_VECTOR_MAX_RESULTS": "32", "YAMS_VECTOR_VEC0_PHSS_ENABLED": "1",
             "YAMS_VECTOR_VEC0_PHSS_CANDIDATES": "16"},
            {}, {"ann_candidate_budget": 16})
        self.assertTrue(any("96" in issue for issue in issues))

    def test_distinct_effective_pool_is_accepted(self):
        self.assertEqual(validate_retrieval_configuration(
            {"YAMS_VECTOR_MAX_RESULTS": "32", "YAMS_VECTOR_VEC0_PHSS_ENABLED": "1",
             "YAMS_VECTOR_VEC0_PHSS_CANDIDATES": "128"},
            {}, {"ann_candidate_budget": 128}), [])

    def test_vec0_cannot_validate_fast_narrowing(self):
        issues = validate_retrieval_configuration(
            {"YAMS_VECTOR_SEARCH_ENGINE": "vec0"},
            {"require_narrowing": True}, {})
        self.assertTrue(any("simeon_pq_adc" in issue for issue in issues))

    def test_narrowing_requires_bound_calibration(self):
        issues = validate_retrieval_configuration(
            {}, {"vector_search_engine": "simeon_pq_adc", "require_narrowing": True}, {})
        self.assertTrue(any("calibration" in issue for issue in issues))

    def test_artifact_name_alone_cannot_enable_unimplemented_calibration_loading(self):
        issues = validate_retrieval_configuration(
            {}, {"vector_search_engine": "simeon_pq_adc", "require_narrowing": True,
                 "route_calibration_artifact": "claimed-calibration.json"}, {})
        self.assertTrue(any("calibration" in issue for issue in issues))

    def test_product_latency_rejects_trace_work(self):
        self.assertTrue(validate_retrieval_configuration(
            {"YAMS_SEARCH_STAGE_TRACE": "1"}, {"latency_mode": "product"}, {}))

    def test_product_latency_must_explicitly_disable_worker_trace_default(self):
        self.assertTrue(validate_retrieval_configuration({}, {"latency_mode": "product"}, {}))

    def test_shadow_cost_requires_actual_shadow_work(self):
        for rate in (0, float("nan"), 2):
            self.assertTrue(validate_retrieval_metrics(
                {"topology_shadow_evaluation_rate": rate}, {"require_shadow_evaluation": True}))
        self.assertEqual(validate_retrieval_metrics(
            {"topology_shadow_evaluation_rate": 0.5,
             "topology_candidate_rescue_attempt_rate": 0.5,
             "topology_route_work_observation_rate": 0.5},
            {"require_shadow_evaluation": True}), [])

    def test_load_attempt_is_not_routed_work_or_exact_control(self):
        self.assertTrue(validate_retrieval_metrics(
            {"topology_shadow_evaluation_rate": 1.0}, {"require_shadow_evaluation": True}))
        self.assertTrue(validate_retrieval_metrics({}, {"require_exact_shadow_control": True}))
        self.assertEqual(validate_retrieval_metrics(
            {"topology_candidate_rescue_exact_distance_evaluations_sum": 34},
            {"require_exact_shadow_control": True}), [])


if __name__ == "__main__":
    unittest.main()
