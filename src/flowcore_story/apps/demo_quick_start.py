"""Quick Start Demo - Try Profile Management and Challenge Harvester.

This is a simple, runnable demo that shows how the systems work together.
No configuration needed - just run and see!
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))


async def demo_1_basic_profile():
    """Demo 1: Get a profile with fingerprinting."""
    print("\n" + "="*70)
    print(" Demo 1: Basic Profile with Fingerprinting")
    print("="*70)

    try:
        from flowcore_story.utils.playwright_fingerprint import get_fingerprint_summary
        from flowcore_story.utils.profile_manager import get_profile_for_request

        # Get a complete profile
        profile = get_profile_for_request(
            site_key="example_site",
            proxy_url=None  # Can add proxy here
        )

        print("\n✅ Profile created successfully!")
        print("\n📋 Profile Details:")
        print(f"   Profile ID: {profile.fingerprint_profile.profile_id}")
        print(f"   User-Agent: {profile.get_user_agent()[:70]}...")
        print(f"   Has cookies: {profile.cookie_selection is not None}")

        # Show fingerprint summary
        summary = get_fingerprint_summary(profile.fingerprint_profile)
        print("\n🔐 Fingerprint Details:")
        print(f"   Platform: {summary['browser_profile']['platform']}")
        print(f"   Screen: {summary['browser_profile']['screen_resolution']}")
        print(f"   WebGL Vendor: {summary['browser_profile']['webgl_vendor']}")
        print(f"   TLS Profile: {summary['tls_profile']}")
        print(f"   HTTP/2: {'Yes' if summary['http2_configured'] else 'No'}")

        print("\n💡 This profile can be used with:")
        print("   - HTTP client (automatic)")
        print("   - Playwright contexts")
        print("   - Challenge harvester")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


async def demo_2_http_client():
    """Demo 2: HTTP client with automatic profile application."""
    print("\n" + "="*70)
    print(" Demo 2: HTTP Client with Auto Profile")
    print("="*70)

    print("\n📖 How it works:")
    print("   1. HTTP client detects profile manager")
    print("   2. Automatically selects best profile")
    print("   3. Applies User-Agent, cookies, TLS fingerprint")
    print("   4. Randomizes headers")
    print("   5. Records results for quality tracking")

    print("\n📝 Example usage:")
    print("""
    from flowcore_story.utils.http_client import fetch

    # Just call fetch - everything is automatic!
    response = await fetch(
        url="https://example.com",
        site_key="example",
        timeout=30
    )

    # That's it! Profile management is transparent.
    """)

    print("\n💡 Benefits:")
    print("   ✓ Zero configuration needed")
    print("   ✓ Automatic fingerprint rotation")
    print("   ✓ Quality-based profile selection")
    print("   ✓ Cookie management")
    print("   ✓ Anti-bot evasion")


async def demo_3_challenge_harvester():
    """Demo 3: Challenge harvester overview."""
    print("\n" + "="*70)
    print(" Demo 3: Challenge Harvester")
    print("="*70)

    try:
        from flowcore_story.utils.challenge_harvester import get_global_harvester
        from flowcore_story.utils.clearance_scheduler import get_global_scheduler

        harvester = get_global_harvester()
        scheduler = get_global_scheduler()

        print("\n✅ Challenge harvester available!")

        # Show stats
        stats = harvester.get_stats()
        print("\n📊 Harvester Stats:")
        print(f"   Active clearances: {stats['active_clearances']}")
        print(f"   Cache hits: {stats['cache_hits']}")
        print(f"   Solve attempts: {stats['solve_attempts']}")
        print(f"   Success rate: {stats['success_rate']*100:.1f}%")

        # Show scheduler
        sched_stats = scheduler.get_stats()
        print("\n📅 Scheduler Stats:")
        print(f"   Total schedules: {sched_stats['total_schedules']}")
        print(f"   Enabled: {sched_stats['enabled_schedules']}")
        print(f"   Running: {sched_stats['running']}")

        print("\n💡 Features:")
        print("   ✓ Automatic challenge detection")
        print("   ✓ Turnstile/Cloudflare support")
        print("   ✓ Human-like interaction")
        print("   ✓ Clearance caching with TTL")
        print("   ✓ Scheduled pre-warming")
        print("   ✓ Auto-refresh before expiry")

        print("\n📝 Usage:")
        print("""
        from flowcore_story.utils.challenge_harvester import get_global_harvester

    harvester = get_global_harvester()

    # Check for cached clearance
    clearance = harvester.get_clearance(
        site_key="example",
        profile_id="profile_123"
    )

    if clearance and not clearance.is_expired():
        # Use cached clearance
        await context.add_cookies(clearance.cookies)
    else:
        # Solve challenge if needed
        result = await harvester.solve_challenge(...)
        """)

    except Exception as e:
        print(f"\n⚠️  Challenge harvester not fully configured: {e}")
        print("\n💡 To enable:")
        print("   1. Set TURNSTILE_SOLVER_API_KEY in config/env/challenge_harvester.env")
        print("   2. Start the service: python -m workers.challenge_harvester_service")


async def demo_4_integration():
    """Demo 4: Complete integration workflow."""
    print("\n" + "="*70)
    print(" Demo 4: Complete Integration Workflow")
    print("="*70)

    print("\n🔄 Complete Workflow:")
    print("""
    ┌─────────────────────────────────────────────────────────┐
    │                                                         │
    │  1. Request comes in                                    │
    │     └─> fetch(url, site_key)                           │
    │                                                         │
    │  2. Profile Manager selects profile                     │
    │     ├─> Get fingerprint from pool                      │
    │     ├─> Match cookies (if available)                   │
    │     └─> Apply TLS + Headers                            │
    │                                                         │
    │  3. Check for cached clearance                          │
    │     └─> Challenge Harvester.get_clearance()            │
    │                                                         │
    │  4. Make request                                        │
    │     ├─> Use httpx-impersonate                          │
    │     ├─> Apply fingerprints                             │
    │     └─> Rotate if needed                               │
    │                                                         │
    │  5. Handle response                                     │
    │     ├─> Record quality metrics                         │
    │     ├─> Save cookies if new                            │
    │     └─> Store clearance if challenge passed            │
    │                                                         │
    │  6. Next request                                        │
    │     └─> Uses quality data to pick best profile         │
    │                                                         │
    └─────────────────────────────────────────────────────────┘
    """)

    print("\n💡 Key Features:")
    print("   ✓ Fully automatic - no manual config needed")
    print("   ✓ Quality-based selection - learns what works")
    print("   ✓ Rotation - changes profiles periodically")
    print("   ✓ Caching - reuses successful clearances")
    print("   ✓ Fallback - legacy mode if needed")


async def demo_5_configuration():
    """Demo 5: Configuration options."""
    print("\n" + "="*70)
    print(" Demo 5: Configuration Options")
    print("="*70)

    print("\n📁 Environment Files:")
    print("   config/env/fingerprint.env        - Fingerprinting settings")
    print("   config/env/challenge_harvester.env - Challenge solver config")
    print("   config/env/common.env             - Shared settings")
    print("   config/env/crawler.env            - Crawler-specific")

    print("\n🔧 Key Settings (config/env/fingerprint.env):")
    print("   ENABLE_FINGERPRINT_POOL=true")
    print("   FINGERPRINT_POOL_SIZE_PER_PROXY=5")
    print("   FINGERPRINT_ROTATION_INTERVAL=3600")
    print("   ENABLE_HTTP2=true")
    print("   ENABLE_HEADERS_RANDOMIZATION=true")

    print("\n🤖 Solver Settings (config/env/challenge_harvester.env):")
    print("   TURNSTILE_SOLVER_SERVICE=2captcha")
    print("   TURNSTILE_SOLVER_API_KEY=your_key_here")
    print("   CHALLENGE_HARVESTER_HEADFUL=false")
    print("   CHALLENGE_HARVESTER_NAVIGATION_TIMEOUT=35")

    print("\n🐋 Docker Deployment:")
    print("   docker-compose.yml already configured!")
    print("   Services:")
    print("   - challenge-harvester (port 8099)")
    print("   - crawler-producer")
    print("   - crawler-consumer")
    print("   - realtime-dashboard (port 8080)")


async def demo_6_testing():
    """Demo 6: Testing framework."""
    print("\n" + "="*70)
    print(" Demo 6: Testing & Validation")
    print("="*70)

    print("\n🧪 Available Tests:")
    print("   tests/test_profile_system.py     - Profile management")
    print("   tests/test_e2e_cloudflare.py     - E2E Cloudflare tests")
    print("   tests/fuzzing/fingerprint_fuzzer.py - Fuzzing tests")
    print("   tests/ab_testing/profile_ab_test.py - A/B testing")

    print("\n▶️  Run tests:")
    print("   pytest tests/test_profile_system.py")
    print("   pytest tests/test_e2e_cloudflare.py")
    print("   python tests/fuzzing/fingerprint_fuzzer.py")
    print("   python tests/ab_testing/profile_ab_test.py")

    print("\n📊 What tests cover:")
    print("   ✓ Profile creation and rotation")
    print("   ✓ Fingerprint diversity")
    print("   ✓ Cookie management")
    print("   ✓ TLS/HTTP2 fingerprinting")
    print("   ✓ Challenge detection")
    print("   ✓ Clearance caching")
    print("   ✓ Success rates by profile type")


async def main():
    """Run all demos."""
    print("\n")
    print("=" * 70)
    print("  StoryFlow-Core: Profile & Challenge Management Demo")
    print("=" * 70)

    print("\n📚 This demo shows you:")
    print("   - How profile management works")
    print("   - How challenge harvester works")
    print("   - How they integrate with your crawler")
    print("   - Configuration options")
    print("   - Testing capabilities")

    try:
        await demo_1_basic_profile()
        await demo_2_http_client()
        await demo_3_challenge_harvester()
        await demo_4_integration()
        await demo_5_configuration()
        await demo_6_testing()

        print("\n" + "="*70)
        print(" ✅ All Demos Completed!")
        print("="*70)

        print("\n📖 Next Steps:")
        print("   1. Review environment files in config/env/")
        print("   2. Run examples:")
        print("      python examples/profile_management_demo.py")
        print("      python examples/challenge_harvester_demo.py")
        print("   3. Run tests:")
        print("      pytest tests/test_profile_system.py")
        print("   4. Start services:")
        print("      docker-compose up -d")
        print("   5. View dashboard:")
        print("      http://localhost:8080")

        print("\n📚 Documentation:")
        print("   docs/PROFILE_MANAGEMENT.md")
        print("   docs/CHALLENGE_HARVESTER.md")
        print("   docs/TESTING_FRAMEWORK.md")
        print("   docs/PROFILE_MANAGEMENT_QUICKSTART.md")

        print("\n💡 Everything is ready to use!")
        print()

    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

