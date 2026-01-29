"""Comprehensive diagnosis of cross-site URL confusion issue."""
import asyncio
import asyncpg
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


async def diagnose():
    """Run comprehensive diagnosis."""
    print("=" * 80)
    print("DIAGNOSING CROSS-SITE URL CONFUSION ISSUE")
    print("=" * 80)

    # Use POSTGRES_STATE_* env vars (used by the app)
    import os
    db_host = os.getenv("POSTGRES_STATE_HOST", "storyflow-postgres")
    db_port = int(os.getenv("POSTGRES_STATE_PORT", "5432"))
    db_user = os.getenv("POSTGRES_STATE_USER", "storyflow")
    db_password = os.getenv("POSTGRES_STATE_PASSWORD", "storyflow_secret")
    db_name = os.getenv("POSTGRES_STATE_DB", "flowcore_story")

    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name,
    )

    try:
        # ===================================================================
        # TEST 1: Check crawl_states for cross-site URLs
        # ===================================================================
        print("\n[TEST 1] Checking crawl_states for cross-site story URLs")
        print("-" * 80)

        rows = await conn.fetch(
            """
            SELECT site_key, worker_id,
                   current_story_url,
                   processed_story_urls,
                   globally_completed_story_urls
            FROM crawl_states
            """
        )

        cross_site_issues = []

        for row in rows:
            site_key = row["site_key"]
            worker_id = row["worker_id"]

            # Check current_story_url
            if row["current_story_url"]:
                url = row["current_story_url"]
                expected_site = detect_site_from_url(url)
                if expected_site != site_key and expected_site != "unknown":
                    cross_site_issues.append({
                        "type": "current_story_url",
                        "site_key": site_key,
                        "worker_id": worker_id,
                        "url": url,
                        "expected_site": expected_site,
                    })

            # Check processed_story_urls
            for url in (row["processed_story_urls"] or []):
                expected_site = detect_site_from_url(url)
                if expected_site != site_key and expected_site != "unknown":
                    cross_site_issues.append({
                        "type": "processed_story_urls",
                        "site_key": site_key,
                        "worker_id": worker_id,
                        "url": url,
                        "expected_site": expected_site,
                    })

            # Check globally_completed_story_urls
            for url in (row["globally_completed_story_urls"] or []):
                expected_site = detect_site_from_url(url)
                if expected_site != site_key and expected_site != "unknown":
                    cross_site_issues.append({
                        "type": "globally_completed_story_urls",
                        "site_key": site_key,
                        "worker_id": worker_id,
                        "url": url,
                        "expected_site": expected_site,
                    })

        if cross_site_issues:
            print(f"[ERROR] Found {len(cross_site_issues)} cross-site URL issues in crawl_states:")
            for i, issue in enumerate(cross_site_issues[:10], 1):  # Show first 10
                print(f"\n  Issue #{i}:")
                print(f"    Type: {issue['type']}")
                print(f"    Site key in state: {issue['site_key']}/{issue['worker_id']}")
                print(f"    URL: {issue['url']}")
                print(f"    Expected site: {issue['expected_site']}")
        else:
            print("[OK] No cross-site URL issues found in crawl_states")

        # ===================================================================
        # TEST 2: Check stories table for mismatched site_key
        # ===================================================================
        print("\n\n[TEST 2] Checking stories table for mismatched site_key")
        print("-" * 80)

        try:
            story_rows = await conn.fetch(
                """
                SELECT id, site_key, story_url, title
                FROM stories
                WHERE story_url IS NOT NULL
                LIMIT 1000
                """
            )
        except Exception as e:
            print(f"[SKIP] Stories table not found in PostgreSQL (data in MongoDB): {e}")
            story_rows = []

        story_mismatch_issues = []

        for row in story_rows:
            site_key = row["site_key"]
            story_url = row["story_url"]
            expected_site = detect_site_from_url(story_url)

            if expected_site != site_key and expected_site != "unknown":
                story_mismatch_issues.append({
                    "id": row["id"],
                    "site_key": site_key,
                    "story_url": story_url,
                    "expected_site": expected_site,
                    "title": row["title"],
                })

        if story_mismatch_issues:
            print(f"[ERROR] Found {len(story_mismatch_issues)} stories with mismatched site_key:")
            for i, issue in enumerate(story_mismatch_issues[:10], 1):
                print(f"\n  Story #{i}:")
                print(f"    ID: {issue['id']}")
                print(f"    Title: {issue['title']}")
                print(f"    Site key in DB: {issue['site_key']}")
                print(f"    Story URL: {issue['story_url']}")
                print(f"    Expected site: {issue['expected_site']}")
        else:
            print("[OK] No site_key mismatches found in stories table")

        # ===================================================================
        # TEST 3: Check for URL pattern issues
        # ===================================================================
        print("\n\n[TEST 3] Checking for URL pattern anomalies")
        print("-" * 80)

        # Find URLs with wrong path patterns
        anomaly_patterns = [
            ("xtruyen.vn", "/doc-truyen/", "tangthuvien pattern on xtruyen domain"),
            ("tangthuvien.net", "/truyen/", "xtruyen pattern on tangthuvien domain"),
            ("truyencom.com", "/doc-truyen/", "tangthuvien pattern on truyencom domain"),
            ("truyencom.com", "/truyen/", "xtruyen pattern on truyencom domain"),
        ]

        pattern_issues = []

        # Check in crawl_states
        for row in rows:
            for url in (row["processed_story_urls"] or []):
                for domain, pattern, desc in anomaly_patterns:
                    if domain in url and pattern in url:
                        pattern_issues.append({
                            "source": "crawl_states",
                            "site_key": row["site_key"],
                            "url": url,
                            "issue": desc,
                        })

        # Check in stories table
        for row in story_rows:
            url = row["story_url"]
            for domain, pattern, desc in anomaly_patterns:
                if domain in url and pattern in url:
                    pattern_issues.append({
                        "source": "stories",
                        "site_key": row["site_key"],
                        "url": url,
                        "issue": desc,
                    })

        if pattern_issues:
            print(f"[ERROR] Found {len(pattern_issues)} URL pattern anomalies:")
            for i, issue in enumerate(pattern_issues[:10], 1):
                print(f"\n  Anomaly #{i}:")
                print(f"    Source: {issue['source']}")
                print(f"    Site key: {issue['site_key']}")
                print(f"    URL: {issue['url']}")
                print(f"    Issue: {issue['issue']}")
        else:
            print("[OK] No URL pattern anomalies found")

        # ===================================================================
        # SUMMARY & RECOMMENDATIONS
        # ===================================================================
        print("\n\n" + "=" * 80)
        print("DIAGNOSIS SUMMARY")
        print("=" * 80)

        total_issues = len(cross_site_issues) + len(story_mismatch_issues) + len(pattern_issues)

        if total_issues == 0:
            print("\n[OK] No issues detected. System appears healthy.")
        else:
            print(f"\n[ERROR] Total issues found: {total_issues}")
            print(f"  - Cross-site URLs in crawl_states: {len(cross_site_issues)}")
            print(f"  - Mismatched site_key in stories: {len(story_mismatch_issues)}")
            print(f"  - URL pattern anomalies: {len(pattern_issues)}")

            print("\n" + "=" * 80)
            print("ROOT CAUSE ANALYSIS")
            print("=" * 80)

            if cross_site_issues:
                print("\n1. CROSS-SITE URL IN STATE:")
                print("   Root cause: Stories from one site are being stored in another site's state.")
                print("   Possible reasons:")
                print("   - Bug in adapter routing logic")
                print("   - State merge from different sites")
                print("   - Missing site validation when saving state")

            if story_mismatch_issues:
                print("\n2. MISMATCHED SITE_KEY IN STORIES TABLE:")
                print("   Root cause: Stories saved with wrong site_key.")
                print("   Possible reasons:")
                print("   - Wrong adapter used to fetch story")
                print("   - Site detection logic failure")
                print("   - Manual data import error")

            if pattern_issues:
                print("\n3. URL PATTERN ANOMALIES:")
                print("   Root cause: URLs contain path patterns from different sites.")
                print("   Possible reasons:")
                print("   - URL normalization bug (e.g., _normalize_story_url)")
                print("   - URL construction using wrong base URL")
                print("   - Missing URL validation")

            print("\n" + "=" * 80)
            print("RECOMMENDED FIXES")
            print("=" * 80)

            print("\n1. IMMEDIATE FIX:")
            print("   - Add URL validation before saving to state/database")
            print("   - Validate that story URL matches the site_key")
            print("   - Log warning if mismatch detected")

            print("\n2. DATA CLEANUP:")
            print("   - Clean up existing cross-site URLs in crawl_states")
            print("   - Fix site_key in stories table where mismatched")
            print("   - Remove URL pattern anomalies")

            print("\n3. PREVENTION:")
            print("   - Add site validation in DatabaseStateStorage.save_crawl_state()")
            print("   - Add validation in adapter routing (get_adapter_for_url)")
            print("   - Add unit tests for cross-site URL detection")

    finally:
        await conn.close()


def detect_site_from_url(url: str) -> str:
    """Detect site key from URL."""
    if not url:
        return "unknown"

    if "tangthuvien.net" in url:
        return "tangthuvien"
    elif "xtruyen.vn" in url:
        return "xtruyen"
    elif "truyencom.com" in url:
        return "truyencom"
    else:
        return "unknown"


if __name__ == "__main__":
    try:
        asyncio.run(diagnose())
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
