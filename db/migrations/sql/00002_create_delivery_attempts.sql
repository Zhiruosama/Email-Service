-- +goose Up
CREATE TABLE delivery_attempts (
    id uuid PRIMARY KEY,
    message_id uuid NOT NULL REFERENCES mail_messages (id) ON DELETE RESTRICT,
    attempt_no integer NOT NULL,
    dispatch_generation bigint NOT NULL,
    provider_key varchar(128) NOT NULL,
    status varchar(32) NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    provider_message_id varchar(512),
    error_category varchar(32),
    error_code varchar(128),
    error_retryable boolean,

    CONSTRAINT delivery_attempts_message_attempt_unique
        UNIQUE (message_id, attempt_no),
    CONSTRAINT delivery_attempts_message_generation_unique
        UNIQUE (message_id, dispatch_generation),
    CONSTRAINT delivery_attempts_attempt_no_positive CHECK (attempt_no > 0),
    CONSTRAINT delivery_attempts_generation_positive CHECK (dispatch_generation > 0),
    CONSTRAINT delivery_attempts_provider_key_not_blank CHECK (btrim(provider_key) <> ''),
    CONSTRAINT delivery_attempts_status_valid CHECK (
        status IN ('STARTED', 'PROVIDER_ACCEPTED', 'FAILED', 'SUBMISSION_UNKNOWN')
    ),
    CONSTRAINT delivery_attempts_timestamps_valid CHECK (
        finished_at IS NULL OR finished_at >= started_at
    ),
    CONSTRAINT delivery_attempts_result_consistent CHECK (
        (
            status = 'STARTED'
            AND finished_at IS NULL
            AND provider_message_id IS NULL
            AND error_category IS NULL
            AND error_code IS NULL
            AND error_retryable IS NULL
        ) OR (
            status = 'PROVIDER_ACCEPTED'
            AND finished_at IS NOT NULL
            AND provider_message_id IS NOT NULL
            AND btrim(provider_message_id) <> ''
            AND error_category IS NULL
            AND error_code IS NULL
            AND error_retryable IS NULL
        ) OR (
            status = 'FAILED'
            AND finished_at IS NOT NULL
            AND provider_message_id IS NULL
            AND error_category IS NOT NULL
            AND error_category <> 'SUBMISSION_UNKNOWN'
            AND error_code IS NOT NULL
            AND btrim(error_code) <> ''
            AND error_retryable IS NOT NULL
        ) OR (
            status = 'SUBMISSION_UNKNOWN'
            AND finished_at IS NOT NULL
            AND provider_message_id IS NULL
            AND error_category = 'SUBMISSION_UNKNOWN'
            AND error_code IS NOT NULL
            AND btrim(error_code) <> ''
            AND error_retryable IS NOT NULL
        )
    ),
    CONSTRAINT delivery_attempts_error_category_valid CHECK (
        error_category IS NULL OR error_category IN (
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

CREATE INDEX delivery_attempts_message_started_idx
    ON delivery_attempts (message_id, started_at DESC, id);

CREATE INDEX delivery_attempts_unfinished_started_idx
    ON delivery_attempts (started_at, id)
    WHERE status = 'STARTED';

-- +goose Down
DROP TABLE delivery_attempts;
