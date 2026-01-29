"""
Scan all metadata files in completed_stories and check for missing fields
"""
import json
import os
from collections import defaultdict
from typing import Any

# Required fields for metadata.json
REQUIRED_METADATA_FIELDS = [
    "title", "url", "author", "cover", "description",
    "categories", "status", "source"
]

# Required fields for metadata-chapter.json
REQUIRED_CHAPTER_METADATA_FIELDS = [
    "title", "url", "total_chapters", "chapters"
]

def scan_metadata_files(base_path: str = "/app/completed_stories") -> dict[str, Any]:
    """Scan all metadata files and identify missing fields"""

    results = {
        "total_stories": 0,
        "stories_with_issues": 0,
        "by_site": defaultdict(lambda: {
            "total": 0,
            "with_issues": 0,
            "missing_fields": defaultdict(int),
            "examples": []
        })
    }

    if not os.path.exists(base_path):
        print(f"[ERROR] Path not found: {base_path}")
        return results

    print(f"[INFO] Scanning stories in: {base_path}")

    # Iterate through genre folders first
    for genre_folder in os.listdir(base_path):
        genre_path = os.path.join(base_path, genre_folder)
        if not os.path.isdir(genre_path):
            continue

        print(f"[INFO] Scanning genre: {genre_folder}")

        # Iterate through all story folders
        for story_folder in os.listdir(genre_path):
            story_path = os.path.join(genre_path, story_folder)
            if not os.path.isdir(story_path):
                continue

            results["total_stories"] += 1

            # Check metadata.json
            metadata_path = os.path.join(story_path, "metadata.json")
            metadata_issues = []
            site_key = "unknown"

            if os.path.exists(metadata_path):
                try:
                    with open(metadata_path, encoding="utf-8") as f:
                        metadata = json.load(f)

                    # Determine site_key
                    site_key = metadata.get("site_key") or metadata.get("source", "unknown")

                    # Check for missing/empty required fields
                    for field in REQUIRED_METADATA_FIELDS:
                        value = metadata.get(field)
                        if value is None or value == "" or (isinstance(value, list) and len(value) == 0):
                            metadata_issues.append(f"metadata.json:{field}")
                            results["by_site"][site_key]["missing_fields"][f"metadata.{field}"] += 1

                except Exception:
                    metadata_issues.append("metadata.json:parse_error")
                    results["by_site"][site_key]["missing_fields"]["metadata.parse_error"] += 1
            else:
                metadata_issues.append("metadata.json:missing_file")
                results["by_site"][site_key]["missing_fields"]["metadata.missing_file"] += 1

            # Check metadata-chapter.json
            chapter_metadata_path = os.path.join(story_path, "metadata-chapter.json")
            chapter_issues = []

            if os.path.exists(chapter_metadata_path):
                try:
                    with open(chapter_metadata_path, encoding="utf-8") as f:
                        chapter_metadata = json.load(f)

                    # Check for missing/empty required fields
                    for field in REQUIRED_CHAPTER_METADATA_FIELDS:
                        value = chapter_metadata.get(field)
                        if value is None or value == "" or (isinstance(value, list) and len(value) == 0):
                            chapter_issues.append(f"metadata-chapter.json:{field}")
                            results["by_site"][site_key]["missing_fields"][f"chapter_meta.{field}"] += 1

                except Exception:
                    chapter_issues.append("metadata-chapter.json:parse_error")
                    results["by_site"][site_key]["missing_fields"]["chapter_meta.parse_error"] += 1
            else:
                chapter_issues.append("metadata-chapter.json:missing_file")
                results["by_site"][site_key]["missing_fields"]["chapter_meta.missing_file"] += 1

            # Update site statistics
            results["by_site"][site_key]["total"] += 1

            all_issues = metadata_issues + chapter_issues
            if all_issues:
                results["stories_with_issues"] += 1
                results["by_site"][site_key]["with_issues"] += 1

                # Store example (limit to 3 per site)
                if len(results["by_site"][site_key]["examples"]) < 3:
                    results["by_site"][site_key]["examples"].append({
                        "folder": f"{genre_folder}/{story_folder}",
                        "issues": all_issues
                    })

    return results

def main():
    print("=" * 80)
    print("METADATA COMPLETENESS SCAN")
    print("=" * 80)

    results = scan_metadata_files()

    print("\n[SUMMARY]")
    print(f"  Total stories scanned: {results['total_stories']}")
    print(f"  Stories with issues: {results['stories_with_issues']}")
    if results['total_stories'] > 0:
        success_rate = ((results['total_stories'] - results['stories_with_issues']) / results['total_stories'] * 100)
        print(f"  Success rate: {success_rate:.1f}%")

    print("\n[BY SITE]")
    for site_key in sorted(results["by_site"].keys()):
        site_data = results["by_site"][site_key]
        print(f"\n  Site: {site_key}")
        print(f"    Total stories: {site_data['total']}")
        print(f"    With issues: {site_data['with_issues']}")
        if site_data['total'] > 0:
            site_success_rate = ((site_data['total'] - site_data['with_issues']) / site_data['total'] * 100)
            print(f"    Success rate: {site_success_rate:.1f}%")

        if site_data['missing_fields']:
            print("    Missing fields (frequency):")
            sorted_fields = sorted(site_data['missing_fields'].items(), key=lambda x: -x[1])
            for field, count in sorted_fields[:10]:
                percentage = (count / site_data['total'] * 100)
                print(f"      - {field}: {count} stories ({percentage:.1f}%)")

        if site_data['examples']:
            print("    Example stories with issues:")
            for example in site_data['examples'][:2]:
                print(f"      - {example['folder']}")
                issues_str = ', '.join(example['issues'][:5])
                if len(example['issues']) > 5:
                    issues_str += f" ... +{len(example['issues']) - 5} more"
                print(f"        Issues: {issues_str}")

    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
