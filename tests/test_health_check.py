import sys, os, time, unittest
from pathlib import Path

_REPO_ROOT = Path(r"C:\Users\Administrator\bug_bounty_work\kickama35\thanhle74-kickama-94e0fb0")
sys.path.insert(0, str(_REPO_ROOT / "tools"))

import health_check


class TestCircuitBreaker(unittest.TestCase):
    def test_initial_state_closed(self):
        cb = health_check.CircuitBreaker(threshold=3)
        self.assertEqual(cb.get_state("svc"), "CLOSED")
        self.assertFalse(cb.is_open("svc"))

    def test_opens_after_threshold(self):
        cb = health_check.CircuitBreaker(threshold=3)
        for _ in range(3):
            cb.record_failure("svc")
        self.assertEqual(cb.get_state("svc"), "OPEN")
        self.assertTrue(cb.is_open("svc"))

    def test_resets_on_success(self):
        cb = health_check.CircuitBreaker(threshold=2)
        cb.record_failure("svc")
        cb.record_failure("svc")
        cb.record_success("svc")
        self.assertEqual(cb.get_state("svc"), "CLOSED")
        self.assertFalse(cb.is_open("svc"))

    def test_below_threshold_stays_closed(self):
        cb = health_check.CircuitBreaker(threshold=3)
        cb.record_failure("svc")
        cb.record_failure("svc")
        self.assertEqual(cb.get_state("svc"), "CLOSED")

    def test_cooldown_restores(self):
        cb = health_check.CircuitBreaker(threshold=2, cooldown=0.05)
        cb.record_failure("svc")
        cb.record_failure("svc")
        self.assertTrue(cb.is_open("svc"))
        time.sleep(0.06)
        self.assertFalse(cb.is_open("svc"))
        self.assertEqual(cb.get_state("svc"), "HALF_OPEN")

    def test_multiple_services_isolated(self):
        cb = health_check.CircuitBreaker(threshold=1)
        cb.record_failure("svc_a")
        self.assertTrue(cb.is_open("svc_a"))
        self.assertFalse(cb.is_open("svc_b"))


class TestHealthCheckStats(unittest.TestCase):
    def test_records_all_statuses(self):
        s = health_check.HealthCheckStats()
        s.record("OK")
        s.record("WARNING")
        s.record("CRITICAL")
        d = s.to_dict()
        self.assertEqual(d["passed"], 1)
        self.assertEqual(d["warnings"], 1)
        self.assertEqual(d["critical"], 1)
        self.assertEqual(d["total_checks"], 3)

    def test_records_retries(self):
        s = health_check.HealthCheckStats()
        s.record("OK", retries=2)
        s.record("CRITICAL", retries=3)
        self.assertEqual(s.total_retries, 5)

    def test_to_dict_keys(self):
        keys = {"total_checks", "passed", "warnings", "critical", "total_retries"}
        s = health_check.HealthCheckStats()
        self.assertTrue(keys.issubset(s.to_dict().keys()))


class TestHTTPRetryBackoff(unittest.TestCase):
    def test_http_probe_returns_tuple(self):
        result = health_check.check_http_service(
            "localhost", 19996, "/", timeout=1, max_retries=1,
        )
        self.assertEqual(len(result), 3)

    def test_http_max_retries_zero(self):
        start = time.time()
        result = health_check.check_http_service(
            "localhost", 19995, "/", timeout=1, max_retries=0,
        )
        elapsed = time.time() - start
        self.assertLess(elapsed, 3)

    def test_circuit_breaker_rejects_when_open(self):
        cb = health_check.CircuitBreaker(threshold=2)
        cb.record_failure("svc")
        cb.record_failure("svc")
        result = health_check.check_http_service(
            "localhost", 8080, "/", timeout=1,
            circuit_breaker=cb, service_name="svc",
        )
        self.assertEqual(result[0], "CRITICAL")
        self.assertIn("Circuit breaker", result[1])

    def test_circuit_breaker_closes_after_success(self):
        cb = health_check.CircuitBreaker(threshold=1)
        cb.record_failure("svc")
        self.assertTrue(cb.is_open("svc"))
        cb.record_success("svc")
        self.assertFalse(cb.is_open("svc"))


class TestCLIFlags(unittest.TestCase):
    def test_new_flags_exist(self):
        args = health_check.parse_args([])
        for attr in ["max_retries", "backoff_factor", "circuit_threshold"]:
            self.assertTrue(hasattr(args, attr), f"Missing flag: {attr}")

    def test_default_values(self):
        args = health_check.parse_args([])
        self.assertEqual(args.max_retries, health_check.DEFAULT_MAX_RETRIES)
        self.assertEqual(args.backoff_factor, health_check.DEFAULT_BACKOFF_FACTOR)
        self.assertEqual(args.circuit_threshold, health_check.DEFAULT_CIRCUIT_THRESHOLD)


class TestEndToEnd(unittest.TestCase):
    def test_run_with_params(self):
        result = health_check.run_health_checks(
            max_retries=1, backoff_factor=1.5, circuit_threshold=5,
        )
        self.assertIn("summary", result)
        self.assertIn("circuit_breakers", result)

    def test_summary_structure(self):
        result = health_check.run_health_checks()
        s = result.get("summary", {})
        self.assertIn("total_checks", s)
        self.assertIn("passed", s)

    def test_circuit_breakers_keys(self):
        result = health_check.run_health_checks()
        for name in health_check.SERVICES:
            self.assertIn(name, result.get("circuit_breakers", {}))


if __name__ == "__main__":
    unittest.main()
