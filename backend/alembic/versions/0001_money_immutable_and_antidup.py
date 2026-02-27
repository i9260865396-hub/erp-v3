"""Money ledger: immutable facts trigger + anti-duplicate indexes

Revision ID: 0001_money_immutable_and_antidup
Revises: 
Create Date: 2026-02-13
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0001_money_immutable_and_antidup"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1) Stronger anti-duplicate when external_id is NULL: fingerprint unique per (source, account)
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes WHERE indexname = 'uq_moneyop_fingerprint'
            ) THEN
                CREATE UNIQUE INDEX uq_moneyop_fingerprint
                ON money_operations (source, account_id, hash_fingerprint)
                WHERE hash_fingerprint IS NOT NULL;
            END IF;
        END $$;
        """
    )

    # 2) Immutable facts on money_operations (DB-level protection)
    op.execute(
        """
        CREATE OR REPLACE FUNCTION trg_moneyop_immutable()
        RETURNS TRIGGER AS $$
        BEGIN
            -- allow toggling is_void + void_reason only
            IF (
                NEW.account_id IS DISTINCT FROM OLD.account_id OR
                NEW.posted_at IS DISTINCT FROM OLD.posted_at OR
                NEW.amount IS DISTINCT FROM OLD.amount OR
                NEW.currency IS DISTINCT FROM OLD.currency OR
                NEW.counterparty IS DISTINCT FROM OLD.counterparty OR
                NEW.description IS DISTINCT FROM OLD.description OR
                NEW.operation_type IS DISTINCT FROM OLD.operation_type OR
                NEW.external_id IS DISTINCT FROM OLD.external_id OR
                NEW.source IS DISTINCT FROM OLD.source OR
                NEW.raw_payload IS DISTINCT FROM OLD.raw_payload OR
                NEW.hash_fingerprint IS DISTINCT FROM OLD.hash_fingerprint
            ) THEN
                RAISE EXCEPTION 'MoneyOperation facts are immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger WHERE tgname = 'moneyop_immutable'
            ) THEN
                CREATE TRIGGER moneyop_immutable
                BEFORE UPDATE ON money_operations
                FOR EACH ROW
                EXECUTE FUNCTION trg_moneyop_immutable();
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS moneyop_immutable ON money_operations;")
    op.execute("DROP FUNCTION IF EXISTS trg_moneyop_immutable();")
    op.execute("DROP INDEX IF EXISTS uq_moneyop_fingerprint;")
