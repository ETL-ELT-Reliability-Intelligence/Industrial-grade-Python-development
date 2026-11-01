"""Add immutable feature snapshots without replacing existing quality evidence."""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE feature_inputs (
            dataset_id text NOT NULL REFERENCES datasets(dataset_id),
            batch_id text NOT NULL,
            raw_sha256 text NOT NULL,
            PRIMARY KEY (dataset_id, batch_id),
            UNIQUE (dataset_id, batch_id, raw_sha256)
        );
        CREATE TABLE feature_profiles (
            profile_id text PRIMARY KEY,
            dataset_id text NOT NULL,
            batch_id text NOT NULL,
            raw_sha256 text NOT NULL,
            definition_hash text NOT NULL,
            implementation_version text NOT NULL,
            profile jsonb NOT NULL,
            FOREIGN KEY (dataset_id, batch_id, raw_sha256)
                REFERENCES feature_inputs(dataset_id, batch_id, raw_sha256),
            UNIQUE (dataset_id, batch_id, raw_sha256, definition_hash, implementation_version)
        );
        CREATE TABLE feature_profile_evaluations (
            evaluation_id text NOT NULL REFERENCES quality_evaluations(evaluation_id),
            profile_id text NOT NULL REFERENCES feature_profiles(profile_id),
            eligible boolean NOT NULL,
            PRIMARY KEY (evaluation_id, profile_id)
        );
        CREATE TABLE profiling_attempts (
            attempt_id text PRIMARY KEY,
            evaluation_id text NOT NULL REFERENCES quality_evaluations(evaluation_id),
            attempt jsonb NOT NULL
        );
        CREATE INDEX profiling_attempts_evaluation ON profiling_attempts(evaluation_id);
        CREATE FUNCTION reject_feature_snapshot_update() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'feature snapshots and input identities are immutable';
        END;
        $$ LANGUAGE plpgsql;
        CREATE TRIGGER immutable_feature_profile BEFORE UPDATE ON feature_profiles
            FOR EACH ROW EXECUTE FUNCTION reject_feature_snapshot_update();
        CREATE TRIGGER immutable_feature_input BEFORE UPDATE ON feature_inputs
            FOR EACH ROW EXECUTE FUNCTION reject_feature_snapshot_update();
    """)


def downgrade():
    op.execute("""
        DROP TABLE profiling_attempts, feature_profile_evaluations, feature_profiles, feature_inputs;
        DROP FUNCTION reject_feature_snapshot_update();
    """)
