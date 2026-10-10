"""Initial state store schema."""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

_UPGRADE = """
CREATE TABLE datasets (
    dataset_id text PRIMARY KEY CHECK (btrim(dataset_id) <> ''),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE dataset_schemas (
    dataset_id text NOT NULL REFERENCES datasets (dataset_id),
    schema_version integer NOT NULL CHECK (schema_version >= 1),
    fields jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (dataset_id, schema_version)
);

CREATE TABLE batches (
    dataset_id text NOT NULL REFERENCES datasets (dataset_id),
    batch_id text NOT NULL CHECK (btrim(batch_id) <> ''),
    records bigint NOT NULL CHECK (records >= 0),
    received_at timestamptz NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version >= 1),
    object_key text,
    PRIMARY KEY (dataset_id, batch_id)
);
CREATE INDEX batches_received_idx ON batches (dataset_id, received_at DESC);

CREATE TABLE pipeline_runs (
    pipeline_id text NOT NULL CHECK (btrim(pipeline_id) <> ''),
    run_id text NOT NULL CHECK (btrim(run_id) <> ''),
    status text NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    error text,
    PRIMARY KEY (pipeline_id, run_id),
    CHECK ((status = 'running') = (finished_at IS NULL)),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK (error IS NULL OR status = 'failed')
);
CREATE INDEX pipeline_runs_started_idx ON pipeline_runs (pipeline_id, started_at DESC);

CREATE TABLE dataset_metrics (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id text NOT NULL REFERENCES datasets (dataset_id),
    scope_id text NOT NULL CHECK (btrim(scope_id) <> ''),
    metric text NOT NULL
        CHECK (metric IN ('row_count', 'null_rate', 'distinct_count', 'freshness_seconds')),
    column_name text,
    value double precision,
    observed_at timestamptz NOT NULL,
    stats_version text NOT NULL CHECK (btrim(stats_version) <> ''),
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((metric IN ('null_rate', 'distinct_count')) = (column_name IS NOT NULL)),
    CHECK (value IS NULL OR value >= 0),
    CHECK (metric <> 'null_rate' OR value IS NULL OR value <= 1)
);
CREATE UNIQUE INDEX dataset_metrics_identity_idx ON dataset_metrics
    (dataset_id, scope_id, metric, (COALESCE(column_name, '')), stats_version);
CREATE INDEX dataset_metrics_history_idx ON dataset_metrics (dataset_id, metric, observed_at DESC);

CREATE TABLE quality_check_results (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    check_id text NOT NULL CHECK (btrim(check_id) <> ''),
    dataset_id text NOT NULL REFERENCES datasets (dataset_id),
    scope_id text NOT NULL CHECK (btrim(scope_id) <> ''),
    column_name text,
    kind text NOT NULL CHECK (kind IN ('freshness', 'null_rate', 'row_count', 'schema')),
    status text NOT NULL CHECK (status IN ('PASS', 'FAIL', 'UNKNOWN')),
    reason_code text NOT NULL,
    message text NOT NULL,
    observation jsonb NOT NULL,
    constraint_spec jsonb NOT NULL,
    details jsonb NOT NULL DEFAULT '[]',
    recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX quality_check_results_identity_idx ON quality_check_results
    (check_id, dataset_id, scope_id, (COALESCE(column_name, '')));
CREATE INDEX quality_check_results_scope_idx ON quality_check_results (dataset_id, scope_id);

CREATE TABLE quality_decisions (
    dataset_id text NOT NULL REFERENCES datasets (dataset_id),
    scope_id text NOT NULL CHECK (btrim(scope_id) <> ''),
    status text NOT NULL CHECK (status IN ('PASS', 'WARN', 'BLOCK')),
    reasons jsonb NOT NULL,
    decided_at timestamptz NOT NULL,
    PRIMARY KEY (dataset_id, scope_id)
);

CREATE TABLE anomalies (
    anomaly_id text PRIMARY KEY CHECK (btrim(anomaly_id) <> ''),
    dataset_id text NOT NULL REFERENCES datasets (dataset_id),
    batch_id text,
    metric text NOT NULL,
    expected double precision NOT NULL,
    actual double precision NOT NULL,
    score double precision NOT NULL CHECK (score >= 0 AND score <= 1),
    detector text NOT NULL,
    model_version text,
    detected_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX anomalies_dataset_idx ON anomalies (dataset_id, detected_at DESC);

CREATE TABLE incidents (
    incident_id text PRIMARY KEY CHECK (btrim(incident_id) <> ''),
    title text NOT NULL,
    summary text NOT NULL,
    status text NOT NULL CHECK (status IN ('open', 'investigating', 'resolved')),
    severity text NOT NULL CHECK (severity IN ('critical', 'warning', 'info')),
    detected_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX incidents_detected_idx ON incidents (detected_at DESC);
CREATE INDEX incidents_status_idx ON incidents (status);

CREATE TABLE incident_affected_datasets (
    incident_id text NOT NULL REFERENCES incidents (incident_id) ON DELETE CASCADE,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    dataset_id text NOT NULL REFERENCES datasets (dataset_id),
    PRIMARY KEY (incident_id, ordinal),
    UNIQUE (incident_id, dataset_id)
);

CREATE TABLE incident_observations (
    observation_id text PRIMARY KEY CHECK (btrim(observation_id) <> ''),
    incident_id text NOT NULL REFERENCES incidents (incident_id) ON DELETE CASCADE,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    dataset_id text NOT NULL REFERENCES datasets (dataset_id),
    metric text NOT NULL,
    expected double precision NOT NULL,
    actual double precision NOT NULL,
    unit text NOT NULL,
    observed_at timestamptz NOT NULL,
    anomaly_id text REFERENCES anomalies (anomaly_id) ON DELETE SET NULL,
    UNIQUE (incident_id, ordinal)
);

CREATE TABLE incident_root_causes (
    incident_id text NOT NULL REFERENCES incidents (incident_id) ON DELETE CASCADE,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    dataset_id text NOT NULL REFERENCES datasets (dataset_id),
    description text NOT NULL,
    score double precision NOT NULL CHECK (score >= 0 AND score <= 1),
    PRIMARY KEY (incident_id, ordinal)
);

CREATE TABLE incident_timeline (
    incident_id text NOT NULL REFERENCES incidents (incident_id) ON DELETE CASCADE,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    at timestamptz NOT NULL,
    description text NOT NULL,
    PRIMARY KEY (incident_id, ordinal)
);

CREATE TABLE incident_impact (
    incident_id text NOT NULL REFERENCES incidents (incident_id) ON DELETE CASCADE,
    kind text NOT NULL CHECK (kind IN ('dataset', 'pipeline', 'dashboard')),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    entity_id text NOT NULL CHECK (btrim(entity_id) <> ''),
    PRIMARY KEY (incident_id, kind, ordinal)
);

CREATE TABLE lineage_nodes (
    node_id text PRIMARY KEY CHECK (btrim(node_id) <> ''),
    node_type text NOT NULL CHECK (node_type IN ('dataset', 'pipeline', 'dashboard'))
);

CREATE TABLE lineage_edges (
    upstream_id text NOT NULL REFERENCES lineage_nodes (node_id),
    downstream_id text NOT NULL REFERENCES lineage_nodes (node_id),
    source text NOT NULL CHECK (btrim(source) <> ''),
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    PRIMARY KEY (upstream_id, downstream_id),
    CHECK (upstream_id <> downstream_id),
    CHECK (last_seen_at >= first_seen_at)
);
CREATE INDEX lineage_edges_downstream_idx ON lineage_edges (downstream_id);

CREATE TABLE ml_model_metadata (
    model_name text NOT NULL CHECK (btrim(model_name) <> ''),
    model_version text NOT NULL CHECK (btrim(model_version) <> ''),
    task text NOT NULL,
    trained_at timestamptz,
    metrics jsonb NOT NULL DEFAULT '{}',
    artifact_key text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model_name, model_version)
);

CREATE TABLE user_actions (
    action_id text PRIMARY KEY CHECK (btrim(action_id) <> ''),
    actor text NOT NULL,
    action_type text NOT NULL,
    target_type text NOT NULL,
    target_id text NOT NULL,
    performed_at timestamptz NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX user_actions_target_idx ON user_actions (target_type, target_id, performed_at DESC);
"""

_TABLES = (
    "user_actions", "ml_model_metadata", "lineage_edges", "lineage_nodes",
    "incident_impact", "incident_timeline", "incident_root_causes",
    "incident_observations", "incident_affected_datasets", "incidents",
    "anomalies", "quality_decisions", "quality_check_results", "dataset_metrics",
    "pipeline_runs", "batches", "dataset_schemas", "datasets",
)


def upgrade() -> None:
    op.execute(_UPGRADE)


def downgrade() -> None:
    for table in _TABLES:
        op.execute(f"DROP TABLE {table}")
