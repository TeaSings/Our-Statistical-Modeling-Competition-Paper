from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Iterable

from adapters.base import JobRecord
from config import StorageConfig


CSV_HEADERS = [
    "site",
    "page",
    "job_url",
    "job_title",
    "company_name",
    "city",
    "salary",
    "summary",
    "raw_payload",
]


class JobStorage:
    def __init__(self, storage_config: StorageConfig) -> None:
        self.storage_config = storage_config
        self.storage_config.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_sqlite()
        self._ensure_csv()

    def save_jobs(self, jobs: Iterable[JobRecord]) -> int:
        new_jobs: list[JobRecord] = []
        with sqlite3.connect(self.storage_config.sqlite_path) as connection:
            for job in jobs:
                if self._insert_job(connection, job):
                    new_jobs.append(job)
            connection.commit()

        if new_jobs:
            self._append_csv(new_jobs)

        return len(new_jobs)

    def _ensure_sqlite(self) -> None:
        with sqlite3.connect(self.storage_config.sqlite_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    site TEXT NOT NULL,
                    page INTEGER NOT NULL,
                    job_url TEXT NOT NULL UNIQUE,
                    job_title TEXT NOT NULL,
                    company_name TEXT,
                    city TEXT,
                    salary TEXT,
                    summary TEXT,
                    raw_payload TEXT
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_site ON jobs(site)"
            )
            connection.commit()

    def _ensure_csv(self) -> None:
        csv_path = self.storage_config.csv_path
        if csv_path.exists() and csv_path.stat().st_size > 0:
            return

        with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
            writer.writeheader()

    def _insert_job(self, connection: sqlite3.Connection, job: JobRecord) -> bool:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO jobs (
                site,
                page,
                job_url,
                job_title,
                company_name,
                city,
                salary,
                summary,
                raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job.site,
                job.page,
                job.job_url,
                job.job_title,
                job.company_name,
                job.city,
                job.salary,
                job.summary,
                job.raw_payload,
            ),
        )
        return cursor.rowcount > 0

    def _append_csv(self, jobs: list[JobRecord]) -> None:
        with self.storage_config.csv_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
            for job in jobs:
                writer.writerow(job.to_row())
