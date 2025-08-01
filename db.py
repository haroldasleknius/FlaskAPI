import os
import pymysql
import time


class DB:
    def __init__(self):
        self.host = os.environ.get("DB_HOST")
        self.port = int(os.environ.get("DB_PORT"))
        self.name = os.environ.get("DB_NAME")
        self.user = os.environ.get("DB_USER")
        self.password = os.environ.get("DB_PASSWORD")

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
            raise RuntimeError(
                f"Could not connect to MySQL at {self.host}:{self.port}: {last}"
            )

    def init_schema(self):
        self.execute("""
                        CREATE TABLE IF NOT EXISTS `schemas` (
                            `id` INT AUTO_INCREMENT PRIMARY KEY,
                            `name` VARCHAR(255) NOT NULL UNIQUE,
                            `fields` JSON NOT NULL
                     );
                     """)

    def close(self):
        try:
            self.connection.close()
        except Exception:
            pass

    def execute(self, sql, params=None):
        with self.connection.cursor() as cur:
            if params is not None:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
        return cur.rowcount

    def query_one(self, sql, params=None):
        with self.connection.cursor() as cur:
            if params is not None:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
        return cur.fetchone()

    def query_all(self, sql, params=None):
        with self.connection.cursor() as cur:
            if params is not None:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
        return cur.fetchall()
