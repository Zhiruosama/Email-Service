-- +goose Up
CREATE TABLE tenants (
    id uuid PRIMARY KEY,
    key varchar(128) NOT NULL,
    name varchar(255) NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'ACTIVE',
    default_locale varchar(35) NOT NULL DEFAULT 'en',
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT tenants_key_unique UNIQUE (key),
    CONSTRAINT tenants_key_not_blank CHECK (btrim(key) <> ''),
    CONSTRAINT tenants_name_not_blank CHECK (btrim(name) <> ''),
    CONSTRAINT tenants_status_valid CHECK (status IN ('ACTIVE', 'PAUSED', 'DISABLED')),
    CONSTRAINT tenants_timestamps_ordered CHECK (updated_at >= created_at)
);

CREATE TABLE mail_messages (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE RESTRICT,
    idempotency_key varchar(255) NOT NULL,
    payload_fingerprint bytea NOT NULL,
    category varchar(32) NOT NULL,
    priority smallint NOT NULL DEFAULT 0,
    duplicate_risk_policy varchar(32) NOT NULL,
    status varchar(32) NOT NULL,
    scheduled_at timestamptz,
    dispatch_deadline timestamptz NOT NULL,
    next_attempt_at timestamptz,
    dispatch_generation bigint NOT NULL DEFAULT 0,
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL,
    provider_accepted_at timestamptz,
    provider_message_id varchar(512),
    latest_sequence bigint NOT NULL,
    version bigint NOT NULL DEFAULT 0,
    last_error_category varchar(32),
    last_error_code varchar(128),
    last_error_retryable boolean,
    accepted_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,

    CONSTRAINT mail_messages_tenant_idempotency_unique
        UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT mail_messages_idempotency_key_not_blank
        CHECK (btrim(idempotency_key) <> ''),
    CONSTRAINT mail_messages_payload_fingerprint_sha256
        CHECK (octet_length(payload_fingerprint) = 32),
    CONSTRAINT mail_messages_category_valid
        CHECK (category IN ('CRITICAL', 'TRANSACTIONAL', 'NOTIFICATION', 'BULK')),
    CONSTRAINT mail_messages_priority_valid CHECK (priority BETWEEN 0 AND 9),
    CONSTRAINT mail_messages_duplicate_risk_policy_valid
        CHECK (duplicate_risk_policy IN ('AVOID_DUPLICATE', 'PREFER_DELIVERY')),
    CONSTRAINT mail_messages_status_valid CHECK (
        status IN (
            'ACCEPTED',
            'SCHEDULED',
            'QUEUED',
            'SENDING',
            'RETRY_SCHEDULED',
            'SUBMISSION_UNKNOWN',
            'PROVIDER_ACCEPTED',
            'DELIVERED',
            'BOUNCED',
            'COMPLAINED',
            'CANCELED',
            'EXPIRED',
            'PERMANENTLY_FAILED',
            'DEAD_LETTERED',
            'UNKNOWN_FINAL'
        )
    ),
    CONSTRAINT mail_messages_schedule_before_deadline
        CHECK (scheduled_at IS NULL OR scheduled_at < dispatch_deadline),
    CONSTRAINT mail_messages_retry_before_deadline
        CHECK (next_attempt_at IS NULL OR next_attempt_at < dispatch_deadline),
    CONSTRAINT mail_messages_deadline_after_acceptance
        CHECK (dispatch_deadline > accepted_at),
    CONSTRAINT mail_messages_timestamps_ordered CHECK (updated_at >= accepted_at),
    CONSTRAINT mail_messages_attempts_valid
        CHECK (max_attempts > 0 AND attempt_count >= 0 AND attempt_count <= max_attempts),
    CONSTRAINT mail_messages_counters_nonnegative
        CHECK (dispatch_generation >= 0 AND latest_sequence > 0 AND version >= 0),
    CONSTRAINT mail_messages_retry_time_present CHECK (
        status <> 'RETRY_SCHEDULED' OR next_attempt_at IS NOT NULL
    ),
    CONSTRAINT mail_messages_dispatch_generation_present CHECK (
        status NOT IN (
            'QUEUED',
            'SENDING',
            'RETRY_SCHEDULED',
            'SUBMISSION_UNKNOWN',
            'PROVIDER_ACCEPTED',
            'DELIVERED',
            'BOUNCED',
            'COMPLAINED',
            'PERMANENTLY_FAILED',
            'DEAD_LETTERED',
            'UNKNOWN_FINAL'
        ) OR dispatch_generation > 0
    ),
    CONSTRAINT mail_messages_attempt_present CHECK (
        status NOT IN (
            'SENDING',
            'RETRY_SCHEDULED',
            'SUBMISSION_UNKNOWN',
            'PROVIDER_ACCEPTED',
            'DELIVERED',
            'BOUNCED',
            'COMPLAINED',
            'PERMANENTLY_FAILED',
            'DEAD_LETTERED',
            'UNKNOWN_FINAL'
        ) OR attempt_count > 0
    ),
    CONSTRAINT mail_messages_failure_fields_consistent CHECK (
        (
            last_error_category IS NULL
            AND last_error_code IS NULL
            AND last_error_retryable IS NULL
        ) OR (
            last_error_category IS NOT NULL
            AND last_error_code IS NOT NULL
            AND btrim(last_error_code) <> ''
            AND last_error_retryable IS NOT NULL
        )
    ),
    CONSTRAINT mail_messages_failure_category_valid CHECK (
        last_error_category IS NULL OR last_error_category IN (
            'VALIDATION',
            'AUTHENTICATION',
            'RATE_LIMITED',
            'RECIPIENT_REJECTED',
            'CONTENT_REJECTED',
            'PROVIDER_UNAVAILABLE',
            'NETWORK',
            'TIMEOUT_BEFORE_SEND',
            'SUBMISSION_UNKNOWN',
            'INTERNAL'
        )
    )
);

CREATE INDEX mail_messages_scheduled_due_idx
    ON mail_messages (scheduled_at, id)
    WHERE status = 'SCHEDULED';

CREATE INDEX mail_messages_retry_due_idx
    ON mail_messages (next_attempt_at, id)
    WHERE status = 'RETRY_SCHEDULED';

CREATE INDEX mail_messages_tenant_accepted_idx
    ON mail_messages (tenant_id, accepted_at DESC, id);

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY,
    aggregate_type varchar(64) NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type varchar(128) NOT NULL,
    aggregate_sequence bigint NOT NULL,
    dispatch_generation bigint NOT NULL DEFAULT 0,
    payload jsonb NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'PENDING',
    available_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_owner varchar(255),
    lease_until timestamptz,
    attempt_count integer NOT NULL DEFAULT 0,
    last_error_code varchar(128),
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at timestamptz,

    CONSTRAINT outbox_events_identity_unique UNIQUE (
        aggregate_type,
        aggregate_id,
        event_type,
        aggregate_sequence,
        dispatch_generation
    ),
    CONSTRAINT outbox_events_aggregate_type_not_blank CHECK (btrim(aggregate_type) <> ''),
    CONSTRAINT outbox_events_event_type_not_blank CHECK (btrim(event_type) <> ''),
    CONSTRAINT outbox_events_counters_nonnegative CHECK (
        aggregate_sequence > 0
        AND dispatch_generation >= 0
        AND attempt_count >= 0
    ),
    CONSTRAINT outbox_events_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT outbox_events_status_valid
        CHECK (status IN ('PENDING', 'PUBLISHED', 'DEAD_LETTERED')),
    CONSTRAINT outbox_events_lease_fields_consistent CHECK (
        (lease_owner IS NULL AND lease_until IS NULL)
        OR (lease_owner IS NOT NULL AND btrim(lease_owner) <> '' AND lease_until IS NOT NULL)
    ),
    CONSTRAINT outbox_events_published_fields_consistent CHECK (
        (status = 'PUBLISHED' AND published_at IS NOT NULL AND lease_owner IS NULL AND lease_until IS NULL)
        OR (status <> 'PUBLISHED' AND published_at IS NULL)
    )
);

CREATE INDEX outbox_events_pending_available_idx
    ON outbox_events (available_at, created_at, id)
    WHERE status = 'PENDING';

CREATE INDEX outbox_events_pending_lease_idx
    ON outbox_events (lease_until, id)
    WHERE status = 'PENDING' AND lease_until IS NOT NULL;

-- +goose Down
DROP TABLE outbox_events;
DROP TABLE mail_messages;
DROP TABLE tenants;
