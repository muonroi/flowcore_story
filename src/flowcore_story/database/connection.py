"""
Database connection manager
Hỗ trợ MySQL, PostgreSQL, MS SQL Server
"""
import os
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from flowcore_story.utils.logger import logger


class DatabaseManager:
    """Quản lý kết nối database"""

    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self._initialized = False

    def initialize(self):
        """Khởi tạo database connection"""
        if self._initialized:
            logger.warning("[DB] Database đã được khởi tạo trước đó")
            return

        db_type = os.getenv("DB_TYPE", "mysql").lower()
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "3306")
        db_name = os.getenv("DB_NAME", "storyflow")
        db_user = os.getenv("DB_USER", "root")
        db_password = os.getenv("DB_PASSWORD", "")

        if db_type == "mysql":
            # Prefer MySQL-specific env to avoid clashing with PostgreSQL state config.
            db_host = os.getenv("DB_HOST_MYSQL", db_host)
            db_port = os.getenv("DB_PORT_MYSQL", db_port)
            db_name = os.getenv("MYSQL_DB", db_name)
            db_user = os.getenv("MYSQL_USER", db_user)
            db_password = os.getenv("MYSQL_PASSWORD", db_password)
        connect_args = {}

        # Xây dựng connection string
        if db_type == "mysql":
            connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
            connect_args = {
                "charset": "utf8mb4",
                "init_command": "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
            }
        elif db_type == "postgresql":
            connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            connect_args = {
                "client_encoding": "utf8"
            }
        elif db_type == "mssql":
            # MS SQL Server với pyodbc driver
            connection_string = f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
        elif db_type == "sqlite":
            # Cho testing
            db_path = os.getenv("DB_PATH", "/app/state/storyflow.db")
            connection_string = f"sqlite:///{db_path}"
        else:
            raise ValueError(f"Unsupported DB_TYPE: {db_type}")

        logger.info(f"[DB] Đang kết nối tới {db_type} database: {db_host}:{db_port}/{db_name}")

        # Tạo engine với connection pool
        self.engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
            pool_pre_ping=True,  # Kiểm tra connection trước khi sử dụng
            pool_recycle=3600,  # Recycle connection sau 1 giờ
            echo=os.getenv("DB_ECHO", "false").lower() == "true",
            connect_args=connect_args
        )

        # Tạo session factory
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )

        self._initialized = True
        logger.info("[DB] Kết nối database thành công!")

    def create_tables(self):
        """Tạo tất cả tables nếu chưa tồn tại"""
        if not self._initialized:
            raise RuntimeError("Database chưa được khởi tạo. Hãy gọi initialize() trước.")

        from flowcore_story.database.models import Base
        logger.info("[DB] Đang tạo database tables...")
        Base.metadata.create_all(bind=self.engine)
        logger.info("[DB] Tạo tables thành công!")

        # Apply compression to existing tables if using MySQL
        db_type = os.getenv("DB_TYPE", "mysql").lower()
        if db_type == "mysql":
            self._apply_mysql_compression()

    def _apply_mysql_compression(self):
        """Apply InnoDB Page Compression to tables with large text columns"""
        # OPTIMIZATION: Allow disabling compression via ENV
        enable_compression = os.getenv("DB_ENABLE_COMPRESSION", "true").lower() == "true"
        if not enable_compression:
            logger.info("[DB] MySQL compression DISABLED via DB_ENABLE_COMPRESSION=false")
            return

        try:
            logger.info("[DB] Kiểm tra và apply compression cho MySQL tables...")

            # Tables cần compression (có MEDIUMTEXT columns)
            tables_to_compress = ['stories', 'chapters']

            with self.engine.connect() as conn:
                for table_name in tables_to_compress:
                    # Check current compression status (ROW_FORMAT)
                    result = conn.execute(
                        text("SELECT ROW_FORMAT, CREATE_OPTIONS FROM information_schema.TABLES "
                             "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"),
                        {"table_name": table_name}
                    ).fetchone()

                    if result:
                        row_format = result[0]
                        create_options = str(result[1] or '')

                        # Check if compression is already enabled via ROW_FORMAT or COMPRESSION
                        if row_format == 'Compressed' or 'COMPRESSION' in create_options:
                            logger.info(f"[DB] ✓ Table '{table_name}' đã có compression: ROW_FORMAT={row_format}, OPTIONS={create_options}")
                        else:
                            logger.info(f"[DB] Table '{table_name}' chưa có compression, đang apply...")
                            # Try transparent page compression first (MySQL 8.0+)
                            try:
                                conn.execute(text(f"ALTER TABLE {table_name} COMPRESSION='ZLIB'"))
                                conn.commit()
                                logger.info(f"[DB] ✓ Table '{table_name}' compression enabled (COMPRESSION='ZLIB')")
                            except Exception as e:
                                # Fallback to row compression if page compression not supported
                                logger.info(f"[DB] COMPRESSION='ZLIB' không được hỗ trợ, sử dụng ROW_FORMAT=COMPRESSED")
                                conn.execute(text(f"ALTER TABLE {table_name} ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"))
                                conn.commit()
                                logger.info(f"[DB] ✓ Table '{table_name}' compression enabled (ROW_FORMAT=COMPRESSED)")

            logger.info("[DB] MySQL compression check completed!")
        except Exception as e:
            logger.warning(f"[DB] Không thể apply compression (có thể do version MySQL cũ): {e}")

    @contextmanager
    def get_session(self) -> Session:
        """Context manager để lấy database session"""
        if not self._initialized:
            raise RuntimeError("Database chưa được khởi tạo. Hãy gọi initialize() trước.")

        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Lỗi trong session: {e}")
            raise
        finally:
            session.close()

    def close(self):
        """Đóng database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("[DB] Đã đóng kết nối database")
            self._initialized = False


# Global database manager instance
db_manager = DatabaseManager()


def get_db_session():
    """Helper function để lấy database session"""
    return db_manager.get_session()


