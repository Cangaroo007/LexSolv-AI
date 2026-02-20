"""
One-time Railway DB fix script.

Problem: Tables were created by create_all() but Alembic has no record of migrations.
Solution: Stamp alembic_version at head (0007) and add any missing columns/tables/indexes.
Usage: Run ONCE as Railway start command, then revert to normal start command.

    Procfile (temporary):
        release: python fix_railway_db.py
        web: uvicorn main:app --host 0.0.0.0 --port $PORT

    Then revert Procfile to:
        release: python run_migrations.py
        web: uvicorn main:app --host 0.0.0.0 --port $PORT
"""
import os
import sys


def fix():
    from sqlalchemy import create_engine, text

    url = os.environ.get("DATABASE_URL", "")
    if not url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

    # Convert async URL to sync for this script
    sync_url = url.replace("+asyncpg", "").replace("postgresql+asyncpg", "postgresql")
    engine = create_engine(sync_url)

    with engine.connect() as conn:
        # ── 1. Create alembic_version table if missing ────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS alembic_version (
                version_num VARCHAR(32) NOT NULL,
                CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
            )
        """))

        # ── 2. Stamp at migration 0007 (latest head) ─────────────────────
        # Migration chain: 0001 → 0002 → 51a7d87a3bda → 0003 → 0004 → 0005 → 0006 → 0007
        conn.execute(text("DELETE FROM alembic_version"))
        conn.execute(text("INSERT INTO alembic_version (version_num) VALUES ('0007')"))

        # ── 3. Add missing enum values ────────────────────────────────────
        # Migration 0002: add 'forgiven' to creditor_status enum
        print("Adding missing enum values...")
        try:
            conn.execute(text("ALTER TYPE creditor_status ADD VALUE IF NOT EXISTS 'forgiven'"))
            print("  OK: added 'forgiven' to creditor_status")
        except Exception as e:
            print(f"  SKIP: creditor_status forgiven ({e})")

        # Need to commit after ALTER TYPE before DDL that uses it
        conn.commit()

        # ── 4. Add missing columns ───────────────────────────────────────
        missing_columns = [
            # Migration 0002: SBR fields on creditors
            "ALTER TABLE creditors ADD COLUMN IF NOT EXISTS is_related_party BOOLEAN DEFAULT false",
            "ALTER TABLE creditors ADD COLUMN IF NOT EXISTS is_secured BOOLEAN DEFAULT false",
            "ALTER TABLE creditors ADD COLUMN IF NOT EXISTS can_vote BOOLEAN DEFAULT true",
            "ALTER TABLE creditors ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'manual'",
            # Migration 0005: custom glossary on companies
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS custom_glossary JSONB",
            # Migration 0007: SBR engagement fields on companies
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS appointment_date DATE",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS practitioner_name VARCHAR(255)",
            "ALTER TABLE companies ADD COLUMN IF NOT EXISTS industry VARCHAR(100)",
        ]

        print("Adding missing columns...")
        for sql in missing_columns:
            try:
                conn.execute(text(sql))
                print(f"  OK: {sql[:70]}...")
            except Exception as e:
                print(f"  SKIP: {sql[:70]}... ({e})")

        # ── 5. Create missing tables ─────────────────────────────────────
        missing_tables = [
            # Migration 51a7d87a3bda: assets table
            (
                "assets",
                """CREATE TABLE IF NOT EXISTS assets (
                    id UUID NOT NULL PRIMARY KEY,
                    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    asset_type VARCHAR(50),
                    description TEXT,
                    book_value FLOAT,
                    liquidation_recovery_pct FLOAT,
                    liquidation_value FLOAT,
                    notes TEXT,
                    source VARCHAR(20),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
                )""",
            ),
            # Migration 51a7d87a3bda: plan_parameters table
            (
                "plan_parameters",
                """CREATE TABLE IF NOT EXISTS plan_parameters (
                    id UUID NOT NULL PRIMARY KEY,
                    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    total_contribution FLOAT,
                    practitioner_fee_pct FLOAT,
                    num_initial_payments INTEGER,
                    initial_payment_amount FLOAT,
                    num_ongoing_payments INTEGER,
                    ongoing_payment_amount FLOAT,
                    est_liquidator_fees FLOAT,
                    est_legal_fees FLOAT,
                    est_disbursements FLOAT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
                )""",
            ),
            # Migration 0003: entity_maps table
            (
                "entity_maps",
                """CREATE TABLE IF NOT EXISTS entity_maps (
                    id UUID NOT NULL PRIMARY KEY,
                    engagement_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    entity_map JSONB NOT NULL,
                    section VARCHAR(100),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
                )""",
            ),
            # Migration 0004: narratives table
            (
                "narratives",
                """CREATE TABLE IF NOT EXISTS narratives (
                    id UUID NOT NULL PRIMARY KEY,
                    engagement_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    section VARCHAR NOT NULL,
                    content TEXT NOT NULL,
                    status VARCHAR DEFAULT 'draft',
                    metadata_ JSON,
                    entity_map JSON,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
                )""",
            ),
            # Migration 0006: document_outputs table
            (
                "document_outputs",
                """CREATE TABLE IF NOT EXISTS document_outputs (
                    id UUID NOT NULL PRIMARY KEY,
                    engagement_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    document_type VARCHAR NOT NULL,
                    version INTEGER DEFAULT 1,
                    filename VARCHAR NOT NULL,
                    generated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
                    generated_by VARCHAR,
                    metadata_ JSON
                )""",
            ),
        ]

        print("Creating missing tables...")
        for table_name, sql in missing_tables:
            try:
                conn.execute(text(sql))
                print(f"  OK: {table_name}")
            except Exception as e:
                print(f"  SKIP: {table_name} ({e})")

        # ── 6. Create missing indexes ─────────────────────────────────────
        missing_indexes = [
            # Migration 51a7d87a3bda
            ("ix_assets_company_id", "CREATE INDEX IF NOT EXISTS ix_assets_company_id ON assets (company_id)"),
            ("ix_plan_parameters_company_id", "CREATE UNIQUE INDEX IF NOT EXISTS ix_plan_parameters_company_id ON plan_parameters (company_id)"),
            # Migration 0003
            ("ix_entity_maps_engagement_id", "CREATE INDEX IF NOT EXISTS ix_entity_maps_engagement_id ON entity_maps (engagement_id)"),
            ("ix_entity_maps_engagement_section", "CREATE INDEX IF NOT EXISTS ix_entity_maps_engagement_section ON entity_maps (engagement_id, section)"),
            # Migration 0004
            ("ix_narratives_engagement_id", "CREATE INDEX IF NOT EXISTS ix_narratives_engagement_id ON narratives (engagement_id)"),
            ("ix_narratives_engagement_section", "CREATE INDEX IF NOT EXISTS ix_narratives_engagement_section ON narratives (engagement_id, section)"),
            # Migration 0006
            ("ix_document_outputs_engagement_type", "CREATE INDEX IF NOT EXISTS ix_document_outputs_engagement_type ON document_outputs (engagement_id, document_type)"),
        ]

        print("Creating missing indexes...")
        for index_name, sql in missing_indexes:
            try:
                conn.execute(text(sql))
                print(f"  OK: {index_name}")
            except Exception as e:
                print(f"  SKIP: {index_name} ({e})")

        conn.commit()

        # ── 7. Verify ────────────────────────────────────────────────────
        print("\n--- Verification ---")

        result = conn.execute(text("SELECT version_num FROM alembic_version"))
        version = result.fetchone()
        print(f"Alembic version stamped: {version[0] if version else 'NONE'}")

        # Check companies columns
        result = conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'companies'
            ORDER BY ordinal_position
        """))
        columns = [r[0] for r in result]
        print(f"\nCompanies columns: {columns}")

        critical_company_cols = ["custom_glossary", "appointment_date", "practitioner_name", "industry"]
        all_ok = True
        for col in critical_company_cols:
            if col in columns:
                print(f"  OK: companies.{col}")
            else:
                print(f"  MISSING: companies.{col}")
                all_ok = False

        # Check creditors columns
        result = conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'creditors'
            ORDER BY ordinal_position
        """))
        cred_columns = [r[0] for r in result]

        critical_creditor_cols = ["is_related_party", "is_secured", "can_vote", "source"]
        for col in critical_creditor_cols:
            if col in cred_columns:
                print(f"  OK: creditors.{col}")
            else:
                print(f"  MISSING: creditors.{col}")
                all_ok = False

        # Check all expected tables exist
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """))
        tables = [r[0] for r in result]
        print(f"\nTables: {tables}")

        expected_tables = [
            "companies", "creditors", "transactions",
            "integration_connections", "oauth_tokens",
            "assets", "plan_parameters", "entity_maps",
            "narratives", "document_outputs", "alembic_version",
        ]
        for t in expected_tables:
            if t in tables:
                print(f"  OK: {t}")
            else:
                print(f"  MISSING: {t}")
                all_ok = False

        if all_ok:
            print("\nDatabase fixed successfully. Change start command back to normal.")
        else:
            print("\nSome items are still missing — check output above.")
            sys.exit(1)


if __name__ == "__main__":
    fix()
