#!/usr/bin/env python
"""
Test script to verify state storage optimizations.
Simulates high-load scenarios and reports metrics.

This is an integration/diagnostic suite that requires a live database and
background workers. It is skipped by default in automated pytest runs.
"""

import asyncio
import os
import sys
import time

import pytest

# Skip by default unless explicitly enabled
pytestmark = pytest.mark.skip(reason="Integration diagnostic; requires live DB/state services")

# Add src to path for standalone execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from flowcore_story.storage.db_health import check_db_health, get_optimization_summary  # noqa: E402


async def test_simple_connectivity():
    """Test basic database connectivity."""
    print("\n" + "="*60)
    print("TEST 1: Database Connectivity")
    print("="*60)

    try:
        from flowcore_story.storage.db_pool import get_db_pool

        pool = await get_db_pool()

        if pool:
            async with pool.acquire() as conn:
                version = await conn.fetchval("SELECT version()")
                print(f"[OK] Connected to: {version.split(',')[0]}")
                print(f"[OK] Pool size: {pool.get_size()}")
                print(f"[OK] Idle connections: {pool.get_idle_size()}")
                return True
        else:
            print("[WARN] Connection pool not available (using file storage)")
            return False
    except Exception as e:
        print(f"[FAIL] Connection failed: {e}")
        return False


async def test_state_save_performance():
    """Test state save performance with optimizations."""
    print("\n" + "="*60)
    print("TEST 2: State Save Performance")
    print("="*60)

    try:
        from flowcore_story.storage.state_metrics import get_state_metrics
        from flowcore_story.utils.state_utils import save_crawl_state

        # Reset metrics
        metrics = get_state_metrics()
        metrics.reset()

        # Simulate 100 state saves
        num_saves = 100
        print(f"Simulating {num_saves} state save operations...")

        start_time = time.monotonic()

        for i in range(num_saves):
            state = {
                "site_key": f"site_{i % 10}",  # 10 different sites
                "current_page": i,
                "processed_urls": [f"url_{j}" for j in range(100)],
                "current_genre_url": f"https://example.com/genre/{i % 5}",
            }

            await save_crawl_state(
                state=state,
                state_file=f"/tmp/test_state_site_{i % 10}.json",
                site_key=f"site_{i % 10}"
            )

            # Small delay to allow debouncing/batching to work
            if i % 10 == 0:
                await asyncio.sleep(0.1)

        duration = time.monotonic() - start_time

        # Wait for batch flush
        print("Waiting for batch flush...")
        await asyncio.sleep(12)  # Wait for batch flush interval

        # Print results
        summary = metrics.get_summary()
        print(f"\n[OK] Test completed in {duration:.2f}s")
        print("\nMetrics:")
        print(f"  - Total save attempts: {num_saves}")
        print(f"  - Actual saves: {summary['total_saves']}")
        print(f"  - DB saves: {summary['db_saves']}")
        print(f"  - Batch saves: {summary['batch_saves']}")
        print(f"  - File saves: {summary['file_saves']}")
        print(f"  - Skipped (debounce): {summary['skipped_debounce']}")
        print(f"  - Skipped (unchanged): {summary['skipped_unchanged']}")
        print(f"  - Failed: {summary['failed']}")
        print(f"  - Success rate: {summary['success_rate']:.1f}%")
        print("\nTiming:")
        print(f"  - Avg: {summary['timing_ms']['avg']:.2f}ms")
        print(f"  - Min: {summary['timing_ms']['min']:.2f}ms")
        print(f"  - Max: {summary['timing_ms']['max']:.2f}ms")

        reduction = (1 - summary['total_saves'] / num_saves) * 100
        print(f"\n[OK] Write reduction: {reduction:.1f}%")

        return True

    except Exception as e:
        print(f"[FAIL] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_circuit_breaker():
    """Test circuit breaker functionality."""
    print("\n" + "="*60)
    print("TEST 3: Circuit Breaker")
    print("="*60)

    try:
        from flowcore_story.storage.db_circuit_breaker import get_circuit_breaker

        breaker = get_circuit_breaker()
        state = breaker.get_state()

        print(f"Circuit breaker state: {state['state']}")
        print(f"Available: {state['available']}")
        print(f"Failure count: {state['failure_count']}")
        print(f"Config: {state['config']}")

        if state['state'] == 'closed' and state['available']:
            print("[OK] Circuit breaker is healthy")
            return True
        else:
            print(f"[WARN] Circuit breaker in {state['state']} state")
            return True

    except Exception as e:
        print(f"[FAIL] Test failed: {e}")
        return False


async def test_batch_writer():
    """Test batch writer stats."""
    print("\n" + "="*60)
    print("TEST 4: Batch Writer")
    print("="*60)

    try:
        from flowcore_story.storage.db_batch_writer import get_batch_writer

        writer = await get_batch_writer()
        stats = writer.get_stats()

        print(f"Batch writer running: {stats['running']}")
        print(f"Queue size: {stats['queue_size']}")
        print(f"Total enqueued: {stats['total_enqueued']}")
        print(f"Total flushed: {stats['total_flushed']}")
        print(f"Total batches: {stats['total_batches']}")
        print(f"Last flush size: {stats['last_flush_size']}")
        print(f"Config: {stats['config']}")

        if stats['running']:
            print("[OK] Batch writer is running")
            return True
        else:
            print("[WARN] Batch writer not running")
            return False

    except Exception as e:
        print(f"[INFO] Batch writer not available (may be disabled): {e}")
        return True


async def print_health_report():
    """Print comprehensive health report."""
    print("\n" + "="*60)
    print("HEALTH REPORT")
    print("="*60)

    health = await check_db_health()

    print(f"\nOverall Status: {health['status'].upper()}")

    if health.get('pool'):
        print("\nDatabase Pool:")
        for key, value in health['pool'].items():
            print(f"  - {key}: {value}")

    if health.get('circuit_breaker'):
        print("\nCircuit Breaker:")
        cb = health['circuit_breaker']
        print(f"  - State: {cb.get('state', 'unknown')}")
        print(f"  - Available: {cb.get('available', False)}")
        print(f"  - Failures: {cb.get('failure_count', 0)}")

    if health.get('batch_writer'):
        print("\nBatch Writer:")
        bw = health['batch_writer']
        if bw.get('running'):
            print(f"  - Running: {bw['running']}")
            print(f"  - Queue: {bw['queue_size']}")
            print(f"  - Flushed: {bw['total_flushed']}")
        else:
            print(f"  - Available: {bw.get('available', False)}")

    if health.get('metrics'):
        print("\nMetrics:")
        m = health['metrics']
        if m.get('available') is not False:
            print(f"  - Total saves: {m['total_saves']}")
            print(f"  - Success rate: {m['success_rate']}%")
            print(f"  - Avg time: {m['timing_ms']['avg']}ms")


def print_optimization_config():
    """Print current optimization configuration."""
    print("\n" + "="*60)
    print("OPTIMIZATION CONFIGURATION")
    print("="*60)

    config = get_optimization_summary()

    print("\nState Save Settings:")
    print(f"  - Debounce time: {config['debounce_time']}s")
    print(f"  - Dirty tracking: {config['dirty_tracking_enabled']}")
    print(f"  - Batch writer: {config['batch_writer_enabled']}")
    print(f"  - Use file state: {config['use_file_state']}")

    print("\nConnection:")
    print(f"  - Retry attempts: {config['connection_retries']}")

    print("\nCircuit Breaker:")
    print(f"  - Failure threshold: {config['circuit_breaker']['failure_threshold']}")
    print(f"  - Recovery timeout: {config['circuit_breaker']['recovery_timeout']}s")

    print("\nBatch Writer:")
    print(f"  - Flush interval: {config['batch_config']['flush_interval']}s")
    print(f"  - Max batch size: {config['batch_config']['max_batch_size']}")


async def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("STORYFLOW STATE OPTIMIZATION TEST SUITE")
    print("="*60)

    # Print configuration
    print_optimization_config()

    # Run tests
    results = []

    results.append(("Connectivity", await test_simple_connectivity()))
    results.append(("Circuit Breaker", await test_circuit_breaker()))
    results.append(("Batch Writer", await test_batch_writer()))
    results.append(("Performance", await test_state_save_performance()))

    # Print health report
    await print_health_report()

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    for name, passed in results:
        status = "[OK] PASS" if passed else "[FAIL] FAIL"
        print(f"{status}: {name}")

    total_passed = sum(1 for _, passed in results if passed)
    print(f"\n{total_passed}/{len(results)} tests passed")

    return total_passed == len(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
