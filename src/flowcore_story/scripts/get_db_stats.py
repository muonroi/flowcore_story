
import pymysql
import psycopg2
import psycopg2.extras
import os
import json
from datetime import datetime, timedelta

# Config MySQL
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "storyflow"
DB_PASS = "3b4Kv4iE4ZCtnbT3UNU7nR97"
DB_NAME = "storyflow"

# Config PostgreSQL
PG_HOST = os.environ.get("POSTGRES_STATE_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("POSTGRES_STATE_PORT", "5432"))
PG_USER = os.environ.get("POSTGRES_STATE_USER", "storyflow")
PG_PASS = os.environ.get("POSTGRES_STATE_PASSWORD", "storyflow_secret")
PG_DB = os.environ.get("POSTGRES_STATE_DB", "flowcore_story")

def connect_mysql():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

def connect_postgres():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, database=PG_DB,
        cursor_factory=psycopg2.extras.DictCursor
    )

def get_stats():
    stats = {
        "mysql": {
            "connected": False, 
            "total_stories": 0, 
            "total_chapters": 0, 
            "new_1h": 0, 
            "new_24h": 0, 
            "top_sites_1h_str": "",
            "checksum_1h": True,
            "checksum_24h": True
        },
        "postgres": {
            "connected": False, 
            "queue_size": 0, 
            "progress_count": 0, 
            "total_chapters_tracked": 0
        }
    }

    # MySQL Stats
    try:
        conn = connect_mysql()
        with conn.cursor() as cursor:
            stats["mysql"]["connected"] = True
            
            # Total Stories
            cursor.execute("SELECT COUNT(*) as total FROM stories")
            stats["mysql"]["total_stories"] = cursor.fetchone()['total']
            
            # Total Chapters
            cursor.execute("SELECT SUM(crawled_chapters) as total_chapters FROM stories")
            stats["mysql"]["total_chapters"] = int(cursor.fetchone()['total_chapters'] or 0)
            
            # --- 1H Statistics ---
            # Total count
            cursor.execute("SELECT COUNT(*) as count FROM stories WHERE synced_at >= NOW() - INTERVAL 1 HOUR")
            stats["mysql"]["new_1h"] = cursor.fetchone()['count']
            
            # Breakdown by site
            cursor.execute("SELECT site_key, COUNT(*) as count FROM stories WHERE synced_at >= NOW() - INTERVAL 1 HOUR GROUP BY site_key ORDER BY count DESC")
            rows_1h = cursor.fetchall()
            
            # Checksum 1h
            sum_1h = sum(row['count'] for row in rows_1h)
            stats["mysql"]["checksum_1h"] = (sum_1h == stats["mysql"]["new_1h"])
            
            # Format top sites (1h)
            top_sites_1h = rows_1h[:5]
            stats["mysql"]["top_sites_1h_str"] = ", ".join([f"{r['site_key'] or 'Unknown'} ({r['count']})" for r in top_sites_1h])

            # --- 24H Statistics ---
            # Total count
            cursor.execute("SELECT COUNT(*) as count FROM stories WHERE synced_at >= NOW() - INTERVAL 24 HOUR")
            stats["mysql"]["new_24h"] = cursor.fetchone()['count']
            
            # Breakdown by site
            cursor.execute("SELECT site_key, COUNT(*) as count FROM stories WHERE synced_at >= NOW() - INTERVAL 24 HOUR GROUP BY site_key")
            rows_24h = cursor.fetchall()
            
            # Checksum 24h
            sum_24h = sum(row['count'] for row in rows_24h)
            stats["mysql"]["checksum_24h"] = (sum_24h == stats["mysql"]["new_24h"])

        conn.close()
    except Exception as e:
        stats["mysql"]["error"] = str(e)

    # Postgres Stats
    try:
        pg_conn = connect_postgres()
        with pg_conn.cursor() as cursor:
            stats["postgres"]["connected"] = True
            
            # Queue
            cursor.execute("SELECT COUNT(*) as count FROM story_queue")
            stats["postgres"]["queue_size"] = cursor.fetchone()['count']
            
            # Progress Total
            cursor.execute("SELECT COUNT(*) as count FROM story_progress")
            stats["postgres"]["progress_count"] = cursor.fetchone()['count']

            cursor.execute("SELECT SUM(crawled_chapters) as count FROM story_progress")
            stats["postgres"]["total_chapters_tracked"] = int(cursor.fetchone()['count'] or 0)
            
            # --- Created vs Updated (1h) ---
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE started_at >= NOW() - INTERVAL '1 hour') as created,
                    COUNT(*) FILTER (WHERE updated_at >= NOW() - INTERVAL '1 hour' AND started_at < NOW() - INTERVAL '1 hour') as updated
                FROM story_progress
            """)
            row_1h = cursor.fetchone()
            stats["postgres"]["created_1h"] = row_1h['created'] or 0
            stats["postgres"]["updated_1h"] = row_1h['updated'] or 0

            # --- Created vs Updated (24h) ---
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE started_at >= NOW() - INTERVAL '24 hours') as created,
                    COUNT(*) FILTER (WHERE updated_at >= NOW() - INTERVAL '24 hours' AND started_at < NOW() - INTERVAL '24 hours') as updated
                FROM story_progress
            """)
            row_24h = cursor.fetchone()
            stats["postgres"]["created_24h"] = row_24h['created'] or 0
            stats["postgres"]["updated_24h"] = row_24h['updated'] or 0

            # --- Last Activity ---
            cursor.execute("SELECT EXTRACT(EPOCH FROM (NOW() - MAX(updated_at)))/60 as minutes_ago FROM story_progress")
            res = cursor.fetchone()
            stats["postgres"]["last_active_minutes"] = float(round(res['minutes_ago'], 1)) if res and res['minutes_ago'] is not None else -1

        pg_conn.close()
    except Exception as e:
        stats["postgres"]["error"] = str(e)

    return stats

if __name__ == "__main__":
    print(json.dumps(get_stats()))
