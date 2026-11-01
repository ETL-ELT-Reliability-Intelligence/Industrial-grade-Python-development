"""Replace quality evidence without legacy compatibility; add batch profiles.

Old quality evaluations/results/decisions are deliberately discarded. Raw batches,
schemas and unrelated operational history are retained. Replay ingestion events
to evaluate old raw data under the new configuration.
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        DELETE FROM quality_executions;
        DELETE FROM quality_evaluations;
        DELETE FROM quality_check_results;
        DELETE FROM quality_decisions;
        DELETE FROM dataset_metrics WHERE metric = 'freshness_seconds';
        ALTER TABLE quality_check_results DROP CONSTRAINT quality_check_results_kind_check;
        ALTER TABLE quality_check_results ADD CONSTRAINT quality_check_results_kind_check
            CHECK (kind IN ('null_rate', 'row_count', 'schema', 'cardinality', 'distribution', 'value_rule'));
        ALTER TABLE dataset_metrics DROP CONSTRAINT dataset_metrics_metric_check;
        ALTER TABLE dataset_metrics DROP CONSTRAINT dataset_metrics_check;
        ALTER TABLE dataset_metrics ADD CONSTRAINT dataset_metrics_metric_check
            CHECK (metric IN ('row_count', 'null_rate', 'distinct_count', 'distinct_ratio', 'non_null_count', 'null_count'));
        ALTER TABLE dataset_metrics ADD CONSTRAINT dataset_metrics_check
            CHECK ((metric <> 'row_count') = (column_name IS NOT NULL));
        ALTER TABLE dataset_metrics ADD CONSTRAINT dataset_metrics_ratio_check
            CHECK (metric <> 'distinct_ratio' OR value IS NULL OR value <= 1);
        CREATE TABLE dataset_profiles (
            dataset_id text NOT NULL REFERENCES datasets(dataset_id),
            scope_id text NOT NULL,
            profile jsonb NOT NULL,
            PRIMARY KEY (dataset_id, scope_id)
        );
    """)


def downgrade():
    raise RuntimeError(
        "Quality evidence replacement is irreversible; restore a database backup to downgrade"
    )
