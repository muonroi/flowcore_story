"""Test Harness Utilities for Profile and Challenge Testing.

This module provides common utilities for testing:
- Budget tracking
- Result aggregation
- Statistical analysis
- Report generation
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class OutcomeType(Enum):
    """Test outcome types."""
    SUCCESS = "success"
    RATE_LIMITED = "rate_limited"
    BLOCKED = "blocked"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass
class BudgetTracker:
    """Budget tracker for testing."""

    max_requests: int = 1000
    max_cost_usd: float = 10.0
    max_duration_seconds: float = 3600.0

    # Current state
    requests_made: int = 0
    cost_incurred: float = 0.0
    start_time: float = field(default_factory=time.time)

    def check_budget(self) -> bool:
        """Check if budget allows more tests.

        Returns:
            True if budget allows more tests
        """
        if self.requests_made >= self.max_requests:
            return False

        if self.cost_incurred >= self.max_cost_usd:
            return False

        elapsed = time.time() - self.start_time
        if elapsed >= self.max_duration_seconds:
            return False

        return True

    def add_request(self, cost: float = 0.0) -> None:
        """Record a request.

        Args:
            cost: Cost of the request
        """
        self.requests_made += 1
        self.cost_incurred += cost

    def get_remaining(self) -> dict[str, Any]:
        """Get remaining budget.

        Returns:
            Dict with remaining budget
        """
        elapsed = time.time() - self.start_time

        return {
            'requests_remaining': max(0, self.max_requests - self.requests_made),
            'cost_remaining': max(0.0, self.max_cost_usd - self.cost_incurred),
            'time_remaining': max(0.0, self.max_duration_seconds - elapsed),
        }

    def get_summary(self) -> dict[str, Any]:
        """Get budget summary.

        Returns:
            Dict with budget summary
        """
        elapsed = time.time() - self.start_time

        return {
            'requests_made': self.requests_made,
            'max_requests': self.max_requests,
            'requests_pct': self.requests_made / self.max_requests * 100 if self.max_requests > 0 else 0,
            'cost_incurred': self.cost_incurred,
            'max_cost': self.max_cost_usd,
            'cost_pct': self.cost_incurred / self.max_cost_usd * 100 if self.max_cost_usd > 0 else 0,
            'duration': elapsed,
            'max_duration': self.max_duration_seconds,
            'duration_pct': elapsed / self.max_duration_seconds * 100 if self.max_duration_seconds > 0 else 0,
        }


@dataclass
class ResultAggregator:
    """Aggregator for test results."""

    total: int = 0
    outcomes: dict[OutcomeType, int] = field(default_factory=dict)
    status_codes: dict[int, int] = field(default_factory=dict)
    response_times: list[float] = field(default_factory=list)

    def add_outcome(
        self,
        outcome: OutcomeType,
        status_code: int | None = None,
        response_time: float | None = None,
    ) -> None:
        """Add an outcome.

        Args:
            outcome: Outcome type
            status_code: HTTP status code
            response_time: Response time
        """
        self.total += 1
        self.outcomes[outcome] = self.outcomes.get(outcome, 0) + 1

        if status_code:
            self.status_codes[status_code] = self.status_codes.get(status_code, 0) + 1

        if response_time is not None:
            self.response_times.append(response_time)

    def get_statistics(self) -> dict[str, Any]:
        """Get statistics.

        Returns:
            Dict with statistics
        """
        if self.total == 0:
            return {
                'total': 0,
                'success_rate': 0.0,
            }

        success_count = self.outcomes.get(OutcomeType.SUCCESS, 0)
        rate_limited_count = self.outcomes.get(OutcomeType.RATE_LIMITED, 0)
        blocked_count = self.outcomes.get(OutcomeType.BLOCKED, 0)
        error_count = self.outcomes.get(OutcomeType.ERROR, 0)
        timeout_count = self.outcomes.get(OutcomeType.TIMEOUT, 0)

        # Response time statistics
        avg_response_time = sum(self.response_times) / len(self.response_times) if self.response_times else 0.0
        min_response_time = min(self.response_times) if self.response_times else 0.0
        max_response_time = max(self.response_times) if self.response_times else 0.0

        # Percentiles
        sorted_times = sorted(self.response_times)
        p50 = sorted_times[len(sorted_times) // 2] if sorted_times else 0.0
        p95 = sorted_times[int(len(sorted_times) * 0.95)] if sorted_times else 0.0
        p99 = sorted_times[int(len(sorted_times) * 0.99)] if sorted_times else 0.0

        return {
            'total': self.total,
            'success_count': success_count,
            'success_rate': success_count / self.total,
            'rate_limited_count': rate_limited_count,
            'rate_limited_rate': rate_limited_count / self.total,
            'blocked_count': blocked_count,
            'blocked_rate': blocked_count / self.total,
            'error_count': error_count,
            'error_rate': error_count / self.total,
            'timeout_count': timeout_count,
            'timeout_rate': timeout_count / self.total,
            'status_code_distribution': dict(sorted(self.status_codes.items())),
            'response_time': {
                'avg': avg_response_time,
                'min': min_response_time,
                'max': max_response_time,
                'p50': p50,
                'p95': p95,
                'p99': p99,
            }
        }


def print_statistics_table(stats: dict[str, Any], title: str = "Statistics") -> None:
    """Print statistics in a formatted table.

    Args:
        stats: Statistics dict
        title: Table title
    """
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)

    print(f"\nTotal Tests: {stats['total']}")

    print("\nOutcome Distribution:")
    print(f"  ✓ Success: {stats['success_count']} ({stats['success_rate']:.1%})")
    print(f"  ⚠ Rate Limited: {stats['rate_limited_count']} ({stats['rate_limited_rate']:.1%})")
    print(f"  ✗ Blocked: {stats['blocked_count']} ({stats['blocked_rate']:.1%})")
    print(f"  ⚠ Errors: {stats['error_count']} ({stats['error_rate']:.1%})")
    print(f"  ⏱ Timeouts: {stats['timeout_count']} ({stats['timeout_rate']:.1%})")

    if stats['status_code_distribution']:
        print("\nStatus Code Distribution:")
        for code, count in stats['status_code_distribution'].items():
            pct = count / stats['total'] * 100
            print(f"  {code}: {count} ({pct:.1f}%)")

    if 'response_time' in stats:
        rt = stats['response_time']
        print("\nResponse Time:")
        print(f"  Avg: {rt['avg']:.2f}s")
        print(f"  Min: {rt['min']:.2f}s")
        print(f"  Max: {rt['max']:.2f}s")
        print(f"  P50: {rt['p50']:.2f}s")
        print(f"  P95: {rt['p95']:.2f}s")
        print(f"  P99: {rt['p99']:.2f}s")

    print("=" * 60)


def calculate_confidence_interval(
    success_count: int,
    total_count: int,
    confidence_level: float = 0.95
) -> tuple[float, float]:
    """Calculate confidence interval for success rate.

    Uses Wilson score interval for binomial proportion.

    Args:
        success_count: Number of successes
        total_count: Total number of trials
        confidence_level: Confidence level (default 0.95)

    Returns:
        Tuple of (lower_bound, upper_bound)
    """
    if total_count == 0:
        return (0.0, 0.0)

    import math

    p = success_count / total_count
    n = total_count

    # Z-score for confidence level
    z_scores = {
        0.90: 1.645,
        0.95: 1.96,
        0.99: 2.576,
    }
    z = z_scores.get(confidence_level, 1.96)

    # Wilson score interval
    denominator = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denominator
    margin = z * math.sqrt((p * (1 - p) + z**2 / (4 * n)) / n) / denominator

    lower = max(0.0, center - margin)
    upper = min(1.0, center + margin)

    return (lower, upper)


def compare_success_rates(
    success_a: int,
    total_a: int,
    success_b: int,
    total_b: int,
    confidence_level: float = 0.95
) -> dict[str, Any]:
    """Compare two success rates statistically.

    Args:
        success_a: Successes in group A
        total_a: Total in group A
        success_b: Successes in group B
        total_b: Total in group B
        confidence_level: Confidence level

    Returns:
        Dict with comparison results
    """
    rate_a = success_a / total_a if total_a > 0 else 0.0
    rate_b = success_b / total_b if total_b > 0 else 0.0

    ci_a = calculate_confidence_interval(success_a, total_a, confidence_level)
    ci_b = calculate_confidence_interval(success_b, total_b, confidence_level)

    difference = rate_a - rate_b
    relative_improvement = (rate_a - rate_b) / rate_b if rate_b > 0 else 0.0

    # Check if confidence intervals overlap
    overlapping = not (ci_a[1] < ci_b[0] or ci_b[1] < ci_a[0])

    return {
        'rate_a': rate_a,
        'rate_b': rate_b,
        'confidence_interval_a': ci_a,
        'confidence_interval_b': ci_b,
        'absolute_difference': difference,
        'relative_improvement': relative_improvement,
        'statistically_significant': not overlapping,
        'confidence_level': confidence_level,
    }


def export_to_csv(results: list[dict[str, Any]], filename: str) -> None:
    """Export results to CSV file.

    Args:
        results: List of result dicts
        filename: Output filename
    """
    import csv

    if not results:
        print("[Export] No results to export")
        return

    # Get all keys
    keys = set()
    for result in results:
        keys.update(result.keys())

    keys = sorted(keys)

    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)

    print(f"[Export] Results exported to {filename}")


def create_test_summary(
    test_name: str,
    stats: dict[str, Any],
    budget: BudgetTracker | None = None,
) -> str:
    """Create a test summary string.

    Args:
        test_name: Name of the test
        stats: Statistics dict
        budget: Optional budget tracker

    Returns:
        Summary string
    """
    lines = []
    lines.append(f"Test: {test_name}")
    lines.append(f"Total: {stats['total']}")
    lines.append(f"Success: {stats['success_count']} ({stats['success_rate']:.1%})")
    lines.append(f"Blocked: {stats['blocked_count']} ({stats['blocked_rate']:.1%})")

    if budget:
        summary = budget.get_summary()
        lines.append(f"Requests: {summary['requests_made']}/{summary['max_requests']}")
        lines.append(f"Cost: ${summary['cost_incurred']:.2f}/${summary['max_cost']:.2f}")

    return " | ".join(lines)


if __name__ == "__main__":
    # Test utilities
    print("Testing Test Harness Utilities\n")

    # Test budget tracker
    budget = BudgetTracker(max_requests=100, max_cost_usd=1.0)

    for _ in range(50):
        budget.add_request(cost=0.003)

    print("Budget Summary:")
    summary = budget.get_summary()
    print(f"  Requests: {summary['requests_made']}/{summary['max_requests']} ({summary['requests_pct']:.1f}%)")
    print(f"  Cost: ${summary['cost_incurred']:.2f}/${summary['max_cost']:.2f} ({summary['cost_pct']:.1f}%)")
    print(f"  Can continue: {budget.check_budget()}")

    # Test result aggregator
    aggregator = ResultAggregator()

    for i in range(100):
        if i < 70:
            aggregator.add_outcome(OutcomeType.SUCCESS, status_code=200, response_time=1.5)
        elif i < 85:
            aggregator.add_outcome(OutcomeType.RATE_LIMITED, status_code=429, response_time=0.5)
        else:
            aggregator.add_outcome(OutcomeType.BLOCKED, status_code=403, response_time=0.2)

    stats = aggregator.get_statistics()
    print_statistics_table(stats, "Test Results")

    # Test confidence intervals
    ci = calculate_confidence_interval(70, 100, confidence_level=0.95)
    print(f"\nConfidence Interval (70/100): {ci[0]:.1%} - {ci[1]:.1%}")

    # Test comparison
    comparison = compare_success_rates(70, 100, 50, 100)
    print("\nComparison:")
    print(f"  Rate A: {comparison['rate_a']:.1%} {comparison['confidence_interval_a']}")
    print(f"  Rate B: {comparison['rate_b']:.1%} {comparison['confidence_interval_b']}")
    print(f"  Improvement: {comparison['relative_improvement']:.1%}")
    print(f"  Significant: {comparison['statistically_significant']}")

