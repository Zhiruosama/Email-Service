-- +goose Up
CREATE TABLE delivery_events (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE RESTRICT,
    message_id uuid NOT NULL REFERENCES mail_messages (id) ON DELETE RESTRICT,
    idempotency_key varchar(255) NOT NULL,
    status varchar(32) NOT NULL,
    sequence bigint NOT NULL,
    attempt_number integer NOT NULL DEFAULT 0,
    provider_message_id varchar(512),
    failure_category varchar(32),
    failure_code varchar(128),
    failure_retryable boolean,
    occurred_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),

    CONSTRAINT delivery_events_message_sequence_unique UNIQUE (message_id, sequence),
    CONSTRAINT delivery_events_idempotency_key_not_blank CHECK (btrim(idempotency_key) <> ''),
    CONSTRAINT delivery_events_status_valid CHECK (
        status IN (
            'ACCEPTED', 'SCHEDULED', 'QUEUED', 'SENDING', 'RETRY_SCHEDULED',
            'SUBMISSION_UNKNOWN', 'PROVIDER_ACCEPTED', 'DELIVERED', 'BOUNCED',
            'COMPLAINED', 'CANCELED', 'EXPIRED', 'PERMANENTLY_FAILED',
            'DEAD_LETTERED', 'UNKNOWN_FINAL'
        )
    ),
    CONSTRAINT delivery_events_counters_valid CHECK (sequence > 0 AND attempt_number >= 0),
    CONSTRAINT delivery_events_provider_message_id_not_blank CHECK (
        provider_message_id IS NULL OR btrim(provider_message_id) <> ''
    ),
    CONSTRAINT delivery_events_failure_fields_consistent CHECK (
        (
            failure_category IS NULL
            AND failure_code IS NULL
            AND failure_retryable IS NULL
        ) OR (
            failure_category IS NOT NULL
            AND failure_code IS NOT NULL
            AND btrim(failure_code) <> ''
            AND failure_retryable IS NOT NULL
        )
    ),
    CONSTRAINT delivery_events_failure_category_valid CHECK (
        failure_category IS NULL OR failure_category IN (
            'VALIDATION', 'AUTHENTICATION', 'RATE_LIMITED', 'RECIPIENT_REJECTED',
            'CONTENT_REJECTED', 'PROVIDER_UNAVAILABLE', 'NETWORK',
            'TIMEOUT_BEFORE_SEND', 'SUBMISSION_UNKNOWN', 'INTERNAL'
        )
    )
);

CREATE INDEX delivery_events_tenant_observed_idx
    ON delivery_events (tenant_id, observed_at DESC, id);

CREATE INDEX delivery_events_message_sequence_idx
    ON delivery_events (message_id, sequence ASC);

-- +goose Down
DROP TABLE delivery_events;
