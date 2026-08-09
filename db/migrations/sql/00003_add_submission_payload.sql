-- +goose Up
ALTER TABLE mail_messages
    ADD COLUMN sender_identity_key varchar(64),
    ADD COLUMN template_key varchar(128),
    ADD COLUMN template_version integer,
    ADD COLUMN locale varchar(35),
    ADD COLUMN recipient_masked varchar(254),
    ADD COLUMN payload_key_id varchar(128),
    ADD COLUMN encrypted_payload bytea,
    ADD COLUMN submission_metadata jsonb;

ALTER TABLE mail_messages
    ADD CONSTRAINT mail_messages_submission_fields_consistent CHECK (
        (
            sender_identity_key IS NULL
            AND template_key IS NULL
            AND template_version IS NULL
            AND locale IS NULL
            AND recipient_masked IS NULL
            AND payload_key_id IS NULL
            AND encrypted_payload IS NULL
            AND submission_metadata IS NULL
        ) OR (
            sender_identity_key IS NOT NULL
            AND btrim(sender_identity_key) <> ''
            AND template_key IS NOT NULL
            AND btrim(template_key) <> ''
            AND template_version IS NOT NULL
            AND template_version > 0
            AND locale IS NOT NULL
            AND btrim(locale) <> ''
            AND recipient_masked IS NOT NULL
            AND btrim(recipient_masked) <> ''
            AND payload_key_id IS NOT NULL
            AND btrim(payload_key_id) <> ''
            AND encrypted_payload IS NOT NULL
            AND octet_length(encrypted_payload) >= 29
            AND submission_metadata IS NOT NULL
            AND jsonb_typeof(submission_metadata) = 'object'
        )
    );

-- +goose Down
ALTER TABLE mail_messages
    DROP CONSTRAINT mail_messages_submission_fields_consistent,
    DROP COLUMN submission_metadata,
    DROP COLUMN encrypted_payload,
    DROP COLUMN payload_key_id,
    DROP COLUMN recipient_masked,
    DROP COLUMN locale,
    DROP COLUMN template_version,
    DROP COLUMN template_key,
    DROP COLUMN sender_identity_key;
