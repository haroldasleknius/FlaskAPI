import os
import pymysql
import time
from logsetup import setup_logging, get_logger

setup_logging()
log = get_logger(__name__)


class DB:
    def __init__(self):
        self.host = os.environ.get("DB_HOST")
        self.port = int(os.environ.get("DB_PORT"))
        self.name = os.environ.get("DB_NAME")
        self.user = os.environ.get("DB_USER")
        self.password = os.environ.get("DB_PASSWORD")

        time_start = time.monotonic()
        self.connection = None  # ensure defined even if all retries fail
        last = None
        for _ in range(20):
            try:
                self.connection = pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.name,
                    autocommit=True,
                    charset="utf8mb4",
                    cursorclass=pymysql.cursors.DictCursor,
                )
                break
            except Exception as e:
                last = e
                time.sleep(0.5)

        if self.connection is None:
            time_diff = (time.monotonic() - time_start) * 1000
            log.error(
                "action=db.connect component=db outcome=error host=%s port=%s last_error=%s duration_ms=%.1f",
                self.host,
                self.port,
                last,
                time_diff,
            )
            raise RuntimeError(
                f"Could not connect to MySQL at {self.host}:{self.port}: {last}"
            )
        else:
            time_diff = (time.monotonic() - time_start) * 1000
            log.info(
                "action=db.connect component=db outcome=success host=%s port=%s duration_ms=%.1f",
                self.host,
                self.port,
                time_diff,
            )

    def init_schema(self):
        time_start = time.monotonic()
        self.execute("""
            CREATE TABLE IF NOT EXISTS `schemas` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(255) NOT NULL UNIQUE,
            `fields` JSON NOT NULL
            );
        """)
        time_diff = (time.monotonic() - time_start) * 1000
        log.info(
            "action=db.init_schema component=db outcome=success duration_ms=%.1f",
            time_diff,
        )

    def close(self):
        try:
            self.connection.close()
            log.info("action=db.close component=db outcome=success")
        except Exception:
            log.exception("action=db.close component=db outcome=error")

    def execute(self, sql, params=None):
        time_start = time.monotonic()
        try:
            with self.connection.cursor() as cur:
                cur.execute(sql, params) if params is not None else cur.execute(sql)
                rows = cur.rowcount
            time_diff = (time.monotonic() - time_start) * 1000
            log.info(
                "action=db.execute component=db outcome=success rows=%s duration_ms=%.1f",
                rows,
                time_diff,
            )
            return rows
        except Exception:
            time_diff = (time.monotonic() - time_start) * 1000
            log.exception(
                "action=db.execute component=db outcome=error duration_ms=%.1f",
                time_diff,
            )
            raise

    def query_one(self, sql, params=None):
        time_start = time.monotonic()
        try:
            with self.connection.cursor() as cur:
                cur.execute(sql, params) if params is not None else cur.execute(sql)
                row = cur.fetchone()
            time_diff = (time.monotonic() - time_start) * 1000
            log.info(
                "action=db.query_one component=db outcome=success found=%s duration_ms=%.1f",
                bool(row),
                time_diff,
            )
            return row
        except Exception:
            time_diff = (time.monotonic() - time_start) * 1000
            log.exception(
                "action=db.query_one component=db outcome=error duration_ms=%.1f",
                time_diff,
            )
            raise

    def query_all(self, sql, params=None):
        time_start = time.monotonic()
        try:
            with self.connection.cursor() as cur:
                cur.execute(sql, params) if params is not None else cur.execute(sql)
                rows = cur.fetchall()
            time_diff = (time.monotonic() - time_start) * 1000
            log.info(
                "action=db.query_all component=db outcome=success rows=%s duration_ms=%.1f",
                len(rows),
                time_diff,
            )
            return rows
        except Exception:
            time_diff = (time.monotonic() - time_start) * 1000
            log.exception(
                "action=db.query_all component=db outcome=error duration_ms=%.1f",
                time_diff,
            )
            raise
