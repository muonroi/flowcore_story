
import pymysql
import psycopg2
import psycopg2.extras
import os
from datetime import datetime

# Config MySQL
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "storyflow"
DB_PASS = "3b4Kv4iE4ZCtnbT3UNU7nR97"
DB_NAME = "storyflow"

# Config PostgreSQL
PG_HOST = os.environ.get("POSTGRES_STATE_HOST", "host.docker.internal")
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

def main():
    print("=== 📊 BÁO CÁO TIẾN ĐỘ HỆ THỐNG (LIVE DB) ===")
    
    # --- MYSQL SECTION ---
    try:
        conn = connect_mysql()
        with conn.cursor() as cursor:
            print(f"\n[MySQL] Kết nối thành công tới {DB_NAME}")
            
            # 1. Tổng quan
            cursor.execute("SELECT COUNT(*) as total FROM stories")
            total_stories = cursor.fetchone()['total']
            
            cursor.execute("SELECT SUM(crawled_chapters) as total_chapters FROM stories")
            total_chapters = cursor.fetchone()['total_chapters'] or 0
            
            print(f"\n🌍 **Tổng quan (MySQL - Content Store):**")
            print(f"- Tổng số truyện: {total_stories}")
            print(f"- Tổng số chương đã crawl: {total_chapters:,}")

            # 2. Thống kê theo Site
            print(f"\n📈 **Tiến độ theo Site (MySQL):**")
            cursor.execute("""
                SELECT site_key, COUNT(*) as count, SUM(crawled_chapters) as chapters, MAX(synced_at) as last_update 
                FROM stories GROUP BY site_key ORDER BY count DESC
            """)
            for row in cursor.fetchall():
                last_up = row['last_update'].strftime('%H:%M %d/%m') if row['last_update'] else "N/A"
                print(f"- **{row['site_key']}**: {row['count']} truyện ({row['chapters']:,} chương) - Cập nhật cuối: {last_up}")

            # 3. Top 5 Thể loại
            print(f"\n📚 **Top 5 Thể loại (MySQL):**")
            cursor.execute("""
                SELECT genre_folder, COUNT(*) as count 
                FROM stories GROUP BY genre_folder ORDER BY count DESC LIMIT 5
            """)
            for row in cursor.fetchall():
                print(f"- {row['genre_folder']}: {row['count']} truyện")

    except Exception as e:
        print(f"❌ Lỗi truy vấn MySQL: {e}")
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()

    # --- POSTGRES SECTION ---
    try:
        pg_conn = connect_postgres()
        with pg_conn.cursor() as cursor:
            print(f"\n[PostgreSQL] Kết nối thành công tới {PG_DB}")

            # 1. Crawl States (Metadata Progress)
            cursor.execute("SELECT COUNT(*) as count FROM crawl_states")
            total_states = cursor.fetchone()['count']
            
            cursor.execute("""
                SELECT site_key, COUNT(*) as count, 
                       MIN(updated_at) as oldest_update, MAX(updated_at) as newest_update 
                FROM crawl_states GROUP BY site_key ORDER BY count DESC
            """)
            
            print(f"\n🔄 **Trạng thái Crawl (PostgreSQL - Crawl States):**")
            print(f"- Tổng số state được track: {total_states}")
            for row in cursor.fetchall():
                newest = row['newest_update'].strftime('%H:%M %d/%m') if row['newest_update'] else "N/A"
                print(f"- **{row['site_key']}**: {row['count']} states (Mới nhất: {newest})")

            # 2. Story Queue (Pending)
            cursor.execute("SELECT COUNT(*) as count FROM story_queue")
            queue_count = cursor.fetchone()['count']
            
            cursor.execute("""
                SELECT site_key, status, COUNT(*) as count 
                FROM story_queue GROUP BY site_key, status ORDER BY site_key, status
            """)
            
            print(f"\n⏳ **Hàng đợi (PostgreSQL - Story Queue: {queue_count}):**")
            for row in cursor.fetchall():
                print(f"- {row['site_key']} [{row['status']}]: {row['count']}")

            # 3. Story Progress (Active Tracking)
            cursor.execute("SELECT COUNT(*) as count FROM story_progress")
            progress_count = cursor.fetchone()['count']
            
            print(f"\n🚀 **Tiến độ chi tiết (PostgreSQL - Story Progress: {progress_count}):**")
            cursor.execute("""
                SELECT site_key, COUNT(*) as count, SUM(total_chapters) as total_chapters, SUM(crawled_chapters) as crawled
                FROM story_progress GROUP BY site_key ORDER BY count DESC
            """)
            for row in cursor.fetchall():
                total = row['total_chapters'] or 0
                crawled = row['crawled'] or 0
                percent = (crawled / total * 100) if total > 0 else 0
                print(f"- **{row['site_key']}**: {row['count']} truyện ({crawled}/{total} chương - {percent:.1f}%)")

    except Exception as e:
        print(f"❌ Lỗi truy vấn PostgreSQL: {e}")
    finally:
        if 'pg_conn' in locals() and not pg_conn.closed:
            pg_conn.close()

if __name__ == "__main__":
    main()
