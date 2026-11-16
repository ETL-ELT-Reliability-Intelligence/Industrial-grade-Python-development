"""Store regression models and repeatable hourly predictions."""

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE order_volume_models (
            model_version text PRIMARY KEY,
            dataset_id text NOT NULL REFERENCES datasets(dataset_id),
            source_id text NOT NULL,
            cutoff timestamptz NOT NULL,
            metadata jsonb NOT NULL,
            artifact bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        );
        CREATE INDEX order_volume_model_lookup
            ON order_volume_models(dataset_id, source_id, cutoff DESC);
        CREATE TABLE order_volume_predictions (
            evaluation_id text PRIMARY KEY REFERENCES quality_evaluations(evaluation_id) ON DELETE CASCADE,
            result jsonb NOT NULL
        );
    """)


def downgrade():
    op.execute("DROP TABLE order_volume_predictions, order_volume_models")
