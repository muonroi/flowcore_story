import os
import time
from sqlalchemy import create_engine, text

# Load Database Config from Environment
PG_USER = os.environ.get('POSTGRES_STATE_USER', 'storyflow')
PG_PASS = os.environ.get('POSTGRES_STATE_PASSWORD', 'storyflow_secret')
PG_HOST = os.environ.get('POSTGRES_STATE_HOST', 'host.docker.internal')
PG_PORT = os.environ.get('POSTGRES_STATE_PORT', '5432')
PG_DB = os.environ.get('POSTGRES_STATE_DB', 'flowcore_story')

DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

def reset_truyencom():
    print("=== RESETTING PLAN FOR: TRUYENCOM ===")
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # 1. Clean Story Queue (Pending/Processing only)
            print("[1/2] Cleaning story_queue (pending/processing)...")
            sql_clean_queue = text("""
                DELETE FROM story_queue 
                WHERE site_key = 'truyencom' 
                AND status IN ('pending', 'processing')
            """)
            result_queue = conn.execute(sql_clean_queue)
            print(f"   -> Deleted {result_queue.rowcount} rows from story_queue.")

            # 2. Reset Genre Progress
            print("[2/2] Resetting genre_progress to trigger re-planning...")
            sql_reset_genre = text("""
                UPDATE genre_progress
                SET status = 'pending',
                    current_page = 0,
                    crawled_pages = 0,
                    processed_stories = 0,
                    total_pages = NULL,
                    updated_at = NOW()
                WHERE site_key = 'truyencom'
            """)
            result_genre = conn.execute(sql_reset_genre)
            conn.commit()
            print(f"   -> Reset {result_genre.rowcount} genres in genre_progress.")
            
            print("\nSUCCESS: TruyenCom is ready to be re-planned with new Adapter logic!")
            
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    # Wait a bit to ensure DB is accessible if just restarted
    time.sleep(1)
    reset_truyencom()
