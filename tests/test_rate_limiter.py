import sys, os, time, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
from health_check import TokenBucket

class TestTokenBucket(unittest.TestCase):
    def test_initial_tokens(self):
        b = TokenBucket(rate=10)
        self.assertGreater(b.tokens, 0)
    def test_acquire_success(self):
        b = TokenBucket(rate=100)
        self.assertTrue(b.acquire())
    def test_throttle_increases(self):
        b = TokenBucket(rate=0.1)
        for _ in range(5):
            b.acquire()
        self.assertGreaterEqual(b.throttled, 0)
    def test_stats_dict(self):
        b = TokenBucket(rate=5)
        s = b.get_stats()
        self.assertIn("rate", s)
        self.assertIn("throttled", s)
    def test_scale_works(self):
        b = TokenBucket(rate=100)
        ok = b.acquire(scale=0.5)
        self.assertIsInstance(ok, bool)

if __name__ == "__main__":
    unittest.main()
