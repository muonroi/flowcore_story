#!/usr/bin/env python3
"""
Pool Timeout Diagnosis Script
Dựa trên phân tích từ Codex + Gemini

Mục đích: Chẩn đoán root cause của PoolTimeout errors
"""
import asyncio
import time
import sys
from collections import defaultdict

try:
    import httpx
except ImportError:
    print("httpx not installed. Install with: pip install httpx")
    sys.exit(1)

# Test targets - các sites đang crawl
TEST_URLS = [
    "https://truyencom.com/",
    "https://xtruyen.vn/",
]

# Test configurations
TEST_CONFIGS = [
    # (max_connections, concurrency, pool_timeout, description)
    (20, 10, 5.0, "Low concurrency - baseline"),
    (20, 20, 5.0, "Match pool limit"),
    (20, 30, 5.0, "Over pool limit - should fail"),
    (50, 30, 5.0, "Increased pool limit"),
    (50, 50, 10.0, "High concurrency + longer timeout"),
    (100, 50, 10.0, "Large pool + medium concurrency"),
    (100, 100, 15.0, "Large pool + high concurrency"),
]

async def single_request(client: httpx.AsyncClient, url: str, sema: asyncio.Semaphore, stats: dict):
    """Make a single request with semaphore control."""
    async with sema:
        try:
            start = time.time()
            response = await client.get(url, follow_redirects=True)
            elapsed = time.time() - start

            if response.status_code == 200:
                stats["success"] += 1
                stats["latencies"].append(elapsed)
            else:
                stats["http_error"] += 1
                stats["error_codes"][response.status_code] += 1

        except httpx.PoolTimeout:
            stats["pool_timeout"] += 1
        except httpx.ConnectTimeout:
            stats["connect_timeout"] += 1
        except httpx.ReadTimeout:
            stats["read_timeout"] += 1
        except httpx.ConnectError as e:
            stats["connect_error"] += 1
            stats["connect_error_details"].append(str(e)[:100])
        except Exception as e:
            stats["other_error"] += 1
            stats["other_error_details"].append(str(e)[:100])


async def run_test(url: str, max_conn: int, concurrency: int, pool_timeout: float,
                   total_requests: int = 100) -> dict:
    """Run a single test configuration."""

    limits = httpx.Limits(
        max_connections=max_conn,
        max_keepalive_connections=max_conn // 2,
        keepalive_expiry=30.0
    )

    timeout = httpx.Timeout(
        connect=30.0,
        read=30.0,
        write=30.0,
        pool=pool_timeout
    )

    stats = {
        "success": 0,
        "pool_timeout": 0,
        "connect_timeout": 0,
        "read_timeout": 0,
        "connect_error": 0,
        "http_error": 0,
        "other_error": 0,
        "latencies": [],
        "error_codes": defaultdict(int),
        "connect_error_details": [],
        "other_error_details": [],
    }

    sema = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        tasks = [single_request(client, url, sema, stats) for _ in range(total_requests)]
        start = time.time()
        await asyncio.gather(*tasks)
        total_time = time.time() - start

    # Calculate metrics
    total = total_requests
    success_rate = (stats["success"] / total) * 100 if total > 0 else 0
    pool_timeout_rate = (stats["pool_timeout"] / total) * 100 if total > 0 else 0

    avg_latency = sum(stats["latencies"]) / len(stats["latencies"]) if stats["latencies"] else 0

    return {
        "url": url,
        "config": {
            "max_connections": max_conn,
            "concurrency": concurrency,
            "pool_timeout": pool_timeout,
        },
        "results": {
            "total_requests": total,
            "success": stats["success"],
            "success_rate": round(success_rate, 2),
            "pool_timeout": stats["pool_timeout"],
            "pool_timeout_rate": round(pool_timeout_rate, 2),
            "connect_timeout": stats["connect_timeout"],
            "read_timeout": stats["read_timeout"],
            "connect_error": stats["connect_error"],
            "http_error": stats["http_error"],
            "other_error": stats["other_error"],
            "avg_latency_ms": round(avg_latency * 1000, 2),
            "total_time_s": round(total_time, 2),
            "rps": round(total / total_time, 2) if total_time > 0 else 0,
        },
        "error_details": {
            "http_codes": dict(stats["error_codes"]),
            "connect_errors": stats["connect_error_details"][:3],
            "other_errors": stats["other_error_details"][:3],
        }
    }


async def diagnose_current_settings():
    """Diagnose with settings matching current crawler configuration."""
    print("\n" + "="*60)
    print("PHASE 1: DIAGNOSE CURRENT SETTINGS")
    print("="*60)

    # Current suspected settings (from logs analysis)
    current_config = {
        "max_connections": 20,
        "concurrency": 15,  # ASYNC_SEMAPHORE_LIMIT
        "pool_timeout": 120.0,  # Current pool timeout
    }

    print(f"\nTesting with current settings:")
    print(f"  max_connections: {current_config['max_connections']}")
    print(f"  concurrency: {current_config['concurrency']}")
    print(f"  pool_timeout: {current_config['pool_timeout']}s")

    for url in TEST_URLS:
        print(f"\n--- Testing {url} ---")
        result = await run_test(
            url=url,
            max_conn=current_config["max_connections"],
            concurrency=current_config["concurrency"],
            pool_timeout=current_config["pool_timeout"],
            total_requests=50
        )
        print_result(result)


async def run_optimization_tests():
    """Run tests with different configurations to find optimal settings."""
    print("\n" + "="*60)
    print("PHASE 2: OPTIMIZATION TESTS")
    print("="*60)

    url = TEST_URLS[0]  # Use truyencom for testing

    results = []
    for max_conn, concurrency, pool_timeout, desc in TEST_CONFIGS:
        print(f"\n--- Testing: {desc} ---")
        print(f"  max_connections={max_conn}, concurrency={concurrency}, pool_timeout={pool_timeout}s")

        result = await run_test(
            url=url,
            max_conn=max_conn,
            concurrency=concurrency,
            pool_timeout=pool_timeout,
            total_requests=50
        )
        result["description"] = desc
        results.append(result)
        print_result(result)

    return results


def print_result(result: dict):
    """Print test result in a readable format."""
    r = result["results"]

    # Determine status color
    if r["success_rate"] >= 90:
        status = "✅ GOOD"
    elif r["success_rate"] >= 70:
        status = "⚠️ WARN"
    else:
        status = "❌ BAD"

    print(f"\n  {status}")
    print(f"  Success Rate: {r['success_rate']}% ({r['success']}/{r['total_requests']})")
    print(f"  PoolTimeout:  {r['pool_timeout']} ({r['pool_timeout_rate']}%)")
    print(f"  Other Errors: connect={r['connect_timeout']}, read={r['read_timeout']}, http={r['http_error']}")
    print(f"  Performance:  {r['rps']} req/s, avg latency {r['avg_latency_ms']}ms")

    if result["error_details"]["connect_errors"]:
        print(f"  Connect Errors: {result['error_details']['connect_errors'][:2]}")


def analyze_results(results: list):
    """Analyze results and provide recommendations."""
    print("\n" + "="*60)
    print("PHASE 3: ANALYSIS & RECOMMENDATIONS")
    print("="*60)

    # Find best configuration
    best = max(results, key=lambda x: x["results"]["success_rate"])
    worst = min(results, key=lambda x: x["results"]["success_rate"])

    print(f"\n📊 BEST CONFIG: {best.get('description', 'Unknown')}")
    print(f"   Success Rate: {best['results']['success_rate']}%")
    print(f"   Settings: max_conn={best['config']['max_connections']}, "
          f"concurrency={best['config']['concurrency']}, "
          f"pool_timeout={best['config']['pool_timeout']}s")

    print(f"\n📊 WORST CONFIG: {worst.get('description', 'Unknown')}")
    print(f"   Success Rate: {worst['results']['success_rate']}%")

    # Recommendations
    print("\n🔧 RECOMMENDATIONS:")

    if best["results"]["success_rate"] >= 90:
        print(f"""
1. OPTIMAL SETTINGS FOUND:
   - max_connections: {best['config']['max_connections']}
   - ASYNC_SEMAPHORE_LIMIT: {best['config']['concurrency']}
   - pool_timeout: {best['config']['pool_timeout']}s

2. Update docker-compose.yml:
   environment:
     - ASYNC_SEMAPHORE_LIMIT={best['config']['concurrency']}
     - HTTP_POOL_TIMEOUT={best['config']['pool_timeout']}

3. Update httpx client in code:
   limits = httpx.Limits(
       max_connections={best['config']['max_connections']},
       max_keepalive_connections={best['config']['max_connections'] // 2}
   )
   timeout = httpx.Timeout(pool={best['config']['pool_timeout']})
""")
    else:
        print("""
⚠️ NO OPTIMAL CONFIG FOUND - Possible issues:
1. Network/DNS issues
2. Target site blocking
3. Proxy issues (despite rotation)
4. Connection leak in code

NEXT STEPS:
1. Check if connections are being properly closed
2. Verify response bodies are fully read
3. Add connection leak detection
4. Consider per-site connection pools
""")


async def main():
    """Main entry point."""
    print("="*60)
    print("POOLTIMEOUT DIAGNOSIS SCRIPT")
    print("Based on Codex + Gemini Analysis")
    print("="*60)

    # Phase 1: Test current settings
    await diagnose_current_settings()

    # Phase 2: Run optimization tests
    results = await run_optimization_tests()

    # Phase 3: Analyze and recommend
    analyze_results(results)

    print("\n" + "="*60)
    print("DIAGNOSIS COMPLETE")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
