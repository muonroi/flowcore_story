"""Check for cross-site story URL confusion in database."""
import asyncio
import json
import os
import asyncpg
from collections import defaultdict


async def check_cross_site():
    """Check cross-site stories status."""
    # Get DB connection details from env or use defaults
    db_host = os.environ.get("POSTGRES_HOST", "postgres")
    db_port = os.environ.get("POSTGRES_PORT", "5432")
    db_user = os.environ.get("POSTGRES_USER", "storyflow")
    db_password = os.environ.get("POSTGRES_PASSWORD", "storyflow")
    db_name = os.environ.get("POSTGRES_DB", "flowcore_story")

    print(f"Connecting to {db_host}:{db_port}/{db_name} as {db_user}...")

    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )

    try:
        # Get all crawl states
        rows = await conn.fetch(
            """
            SELECT site_key, worker_id, processed_story_urls, globally_completed_story_urls
            FROM crawl_states
            """
        )

        print("=" * 80)
        print("Checking for cross-site URL confusion")
        print("=" * 80)

        issues_found = 0

        for row in rows:
            site_key = row["site_key"]
            worker_id = row["worker_id"]

            # Check processed_story_urls
            story_urls = row["processed_story_urls"] or []
            for url in story_urls:
                # Detect site from URL
                if "tangthuvien.net" in url:
                    expected_site = "tangthuvien"
                elif "xtruyen.vn" in url:
                    expected_site = "xtruyen"
                elif "truyencom.com" in url:
                    expected_site = "truyencom"
                else:
                    expected_site = "unknown"

                if expected_site != site_key and expected_site != "unknown":
                    print(f"\n[ERROR] Cross-site story found:")
                    print(f"  Site key: {site_key}")
                    print(f"  Worker: {worker_id}")
                    print(f"  Story URL: {url}")
                    print(f"  Expected site: {expected_site}")
                    issues_found += 1

                    if issues_found >= 10:  # Limit output
                        break

            # Check globally_completed_story_urls
            completed_urls = row["globally_completed_story_urls"] or []
            for url in completed_urls:
                # Detect site from URL
                if "tangthuvien.net" in url:
                    expected_site = "tangthuvien"
                elif "xtruyen.vn" in url:
                    expected_site = "xtruyen"
                elif "truyencom.com" in url:
                    expected_site = "truyencom"
                else:
                    expected_site = "unknown"

                if expected_site != site_key and expected_site != "unknown":
                    print(f"\n[ERROR] Cross-site completed story found:")
                    print(f"  Site key: {site_key}")
                    print(f"  Worker: {worker_id}")
                    print(f"  Story URL: {url}")
                    print(f"  Expected site: {expected_site}")
                    issues_found += 1

                    if issues_found >= 10:  # Limit output
                        break

            if issues_found >= 10:
                break

        print(f"\n\nTotal issues found: {issues_found}")

        if issues_found == 0:
            print("[OK] No cross-site URL confusion detected")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(check_cross_site())
