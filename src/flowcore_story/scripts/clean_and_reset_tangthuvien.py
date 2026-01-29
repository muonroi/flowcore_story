import os
import time
import glob
from sqlalchemy import create_engine, text

# Load Database Config from Environment
PG_USER = os.environ.get('POSTGRES_STATE_USER', 'storyflow')
PG_PASS = os.environ.get('POSTGRES_STATE_PASSWORD', 'storyflow_secret')
PG_HOST = os.environ.get('POSTGRES_STATE_HOST', 'host.docker.internal')
PG_PORT = os.environ.get('POSTGRES_STATE_PORT', '5432')
PG_DB = os.environ.get('POSTGRES_STATE_DB', 'flowcore_story')

DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

def clean_and_reset_tangthuvien():
    print("=== CLEANING AND RESETTING PLAN FOR: TANGTHUVIEN (LEGACY MODE) ===")
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # 1. Delete NEW incorrect genre_progress entries (/the-loai/)
            print("[1/5] Deleting '/the-loai/' entries from genre_progress...")
            delete_sql_new = text("""
                DELETE FROM genre_progress
                WHERE site_key = 'tangthuvien'
                AND genre_url LIKE '%/the-loai/%'
            """)
            delete_result_new = conn.execute(delete_sql_new)
            print(f"   -> Deleted {delete_result_new.rowcount} '/the-loai/' entries.")

            # 2. Delete OLD incorrect genre_progress entries (xtruyen leak)
            print("[2/5] Deleting 'xtruyen' entries from genre_progress...")
            delete_sql_old = text("""
                DELETE FROM genre_progress
                WHERE site_key = 'tangthuvien'
                AND genre_url LIKE '%xtruyen.vn%'
            """)
            delete_result_old = conn.execute(delete_sql_old)
            print(f"   -> Deleted {delete_result_old.rowcount} 'xtruyen' entries.")

            # 3. Clean Story Queue (Pending/Processing only) for tangthuvien
            print("[3/5] Cleaning story_queue (pending/processing) for tangthuvien...")
            sql_clean_queue = text("""
                DELETE FROM story_queue 
                WHERE site_key = 'tangthuvien' 
                AND status IN ('pending', 'processing')
            """ )
            result_queue = conn.execute(sql_clean_queue)
            print(f"   -> Deleted {result_queue.rowcount} rows from story_queue.")

            # 4. Reset VALID genre_progress entries (tong-hop) for tangthuvien
            print("[4/5] Resetting VALID 'tong-hop' genre_progress entries to trigger re-planning...")
            reset_sql = text("""
                UPDATE genre_progress
                SET status = 'pending',
                    current_page = 0,
                    crawled_pages = 0,
                    processed_stories = 0,
                    total_pages = NULL,
                    updated_at = NOW()
                WHERE site_key = 'tangthuvien'
                AND genre_url LIKE '%/tong-hop%'
            """ )
            reset_result = conn.execute(reset_sql)
            conn.commit()
            print(f"   -> Reset {reset_result.rowcount} valid genres in genre_progress.")
            
        # 5. Remove local state files
        print("[5/5] Removing local state files for tangthuvien...")
        deleted_count = 0
        # Matches tangthuvien_crawl_state.json, .json.bak, etc.
        for state_file in glob.glob("/app/state/tangthuvien*state.json*"):
            try:
                os.remove(state_file)
                print(f"   -> Deleted {state_file}")
                deleted_count += 1
            except Exception as e:
                print(f"   -> Error deleting {state_file}: {e}")
        
        if deleted_count == 0:
            print("   -> No local state files found to delete.")

        print("\nSUCCESS: TangThuVien is cleaned and ready to be re-planned (Legacy URL Mode)!")
            
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    time.sleep(1)
    clean_and_reset_tangthuvien()