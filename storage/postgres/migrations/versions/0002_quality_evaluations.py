"""Durable plans, executions, attempt history and immutable completed evaluations."""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE quality_evaluations (
            sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
            evaluation_id text PRIMARY KEY,
            dataset_id text NOT NULL REFERENCES datasets(dataset_id),
            scope_id text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            snapshot jsonb NOT NULL
        );
        CREATE INDEX quality_evaluations_scope_idx
            ON quality_evaluations(dataset_id, scope_id, sequence DESC);
        CREATE TABLE quality_executions (
            evaluation_id text NOT NULL REFERENCES quality_evaluations(evaluation_id) ON DELETE CASCADE,
            check_id text NOT NULL,
            snapshot jsonb NOT NULL,
            PRIMARY KEY (evaluation_id, check_id)
        );
    """)


def downgrade():
    op.execute("DROP TABLE quality_executions; DROP TABLE quality_evaluations")
