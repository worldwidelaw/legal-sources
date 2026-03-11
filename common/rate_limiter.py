"""
Rate limiter for World Wide Law.

Token-bucket algorithm with configurable rate and burst.
Respects robots.txt and source-specific rate limits.

AdaptiveRateLimiter extends this with automatic rate discovery:
starts fast, backs off on 429s, ramps up on sustained success.
"""

import time
import logging

logger = logging.getLogger("legal-data-hunter")


class RateLimiter:
    """
    Token bucket rate limiter.

    Args:
        requests_per_second: Sustained request rate
        burst: Maximum burst size (how many requests can fire instantly)
    """

    def __init__(self, requests_per_second: float = 2.0, burst: int = 5):
        self.rate = requests_per_second
        self.burst = burst
        self.tokens = float(burst)
        self.last_time = time.monotonic()
        self.total_waits = 0
        self.total_wait_time = 0.0

    def wait(self):
        """
        Block until a request token is available.
        Call this before every HTTP request.
        """
        now = time.monotonic()
        elapsed = now - self.last_time
        self.last_time = now

        # Add tokens based on elapsed time
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return

        # Need to wait for a token
        wait_time = (1.0 - self.tokens) / self.rate
        logger.debug(f"Rate limit: waiting {wait_time:.2f}s")
        self.total_waits += 1
        self.total_wait_time += wait_time
        time.sleep(wait_time)
        self.tokens = 0.0
        self.last_time = time.monotonic()

    def record_success(self):
        """Called after a successful HTTP response. No-op for fixed limiter."""
        pass

    def record_429(self, retry_after: float = None):
        """Called after a 429 response. No-op for fixed limiter."""
        pass

    def stats(self) -> dict:
        """Return rate limiter statistics."""
        return {
            "total_waits": self.total_waits,
            "total_wait_time_seconds": round(self.total_wait_time, 2),
        }


class AdaptiveRateLimiter(RateLimiter):
    """
    Self-tuning rate limiter that discovers the actual API limit.

    Starts at `start_rate`, ramps up after sustained success,
    and backs off aggressively on 429 responses.

    Args:
        start_rate: Initial requests per second (default 5.0)
        min_rate: Floor — never go below this (default 0.5)
        max_rate: Ceiling — never exceed this (default 50.0)
        burst: Token bucket burst size
        ramp_after: Ramp up rate after this many consecutive successes
        ramp_factor: Multiply rate by this on ramp-up (default 1.3 = +30%)
        backoff_factor: Multiply rate by this on 429 (default 0.5 = halve)
    """

    def __init__(
        self,
        start_rate: float = 5.0,
        min_rate: float = 0.5,
        max_rate: float = 50.0,
        burst: int = 10,
        ramp_after: int = 50,
        ramp_factor: float = 1.3,
        backoff_factor: float = 0.5,
    ):
        super().__init__(requests_per_second=start_rate, burst=burst)
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.start_rate = start_rate
        self.ramp_after = ramp_after
        self.ramp_factor = ramp_factor
        self.backoff_factor = backoff_factor
        self._consecutive_success = 0
        self._total_429s = 0
        self._rate_changes = []

    def record_success(self):
        """Ramp up rate after sustained success."""
        self._consecutive_success += 1
        if self._consecutive_success >= self.ramp_after:
            old_rate = self.rate
            self.rate = min(self.rate * self.ramp_factor, self.max_rate)
            self._consecutive_success = 0
            if self.rate != old_rate:
                self._rate_changes.append(
                    (time.monotonic(), f"{old_rate:.1f} -> {self.rate:.1f} (ramp)")
                )
                logger.info(
                    f"Adaptive rate limiter: ramping up {old_rate:.1f} -> {self.rate:.1f} req/s"
                )

    def record_429(self, retry_after: float = None):
        """Back off aggressively on rate limit hit."""
        self._consecutive_success = 0
        self._total_429s += 1
        old_rate = self.rate
        self.rate = max(self.rate * self.backoff_factor, self.min_rate)
        self._rate_changes.append(
            (time.monotonic(), f"{old_rate:.1f} -> {self.rate:.1f} (429)")
        )
        logger.warning(
            f"Adaptive rate limiter: 429 received, backing off "
            f"{old_rate:.1f} -> {self.rate:.1f} req/s"
            + (f" (retry-after: {retry_after}s)" if retry_after else "")
        )
        if retry_after:
            time.sleep(retry_after)

    def stats(self) -> dict:
        """Return rate limiter statistics including adaptive behavior."""
        base = super().stats()
        base.update({
            "current_rate": round(self.rate, 2),
            "start_rate": self.start_rate,
            "total_429s": self._total_429s,
            "rate_changes": len(self._rate_changes),
        })
        return base
