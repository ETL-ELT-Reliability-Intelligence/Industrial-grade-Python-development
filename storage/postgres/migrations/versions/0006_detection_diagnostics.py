"""Persist rule detector evidence and training diagnostics without deleting history."""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE rule_detection_results (
            evaluation_id text PRIMARY KEY REFERENCES quality_evaluations(evaluation_id) ON DELETE CASCADE,
            result jsonb NOT NULL
        );
        CREATE TABLE order_volume_training_runs (
            run_id bigserial PRIMARY KEY,
            dataset_id text NOT NULL,
            cutoff timestamptz NOT NULL,
            recorded_at timestamptz NOT NULL DEFAULT now(),
            result jsonb NOT NULL
        );
        CREATE INDEX training_latest ON order_volume_training_runs(dataset_id, run_id DESC);
    """)


def downgrade():
    op.execute("DROP TABLE rule_detection_results, order_volume_training_runs")
