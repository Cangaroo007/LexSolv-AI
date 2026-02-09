"""
Run Alembic migrations if DATABASE_URL is set.
Used as Railway release command — runs once per deploy before the web process starts.
"""

import os
import subprocess
import sys


def main():
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url or "localhost" in db_url:
        print("DATABASE_URL not configured or points to localhost — skipping migrations.")
        return

    print("Running Alembic migrations…")
    result = subprocess.run(["alembic", "upgrade", "head"], capture_output=False)
    if result.returncode != 0:
        print(f"WARNING: Alembic migration failed (exit code {result.returncode}). Continuing startup.")
    else:
        print("Migrations complete.")


if __name__ == "__main__":
    main()
