package postgres

const lockDueMessagesQuery = `
	SELECT ` + messageColumns + `, transaction_timestamp()
	FROM mail_messages
	WHERE
		EXISTS (
			SELECT 1
			FROM tenants
			WHERE tenants.id = mail_messages.tenant_id
			  AND tenants.status = 'ACTIVE'
		)
		AND (
			(status = 'SCHEDULED' AND scheduled_at <= transaction_timestamp())
			OR (status = 'RETRY_SCHEDULED' AND next_attempt_at <= transaction_timestamp())
		)
	ORDER BY
		CASE
			WHEN status = 'SCHEDULED' THEN scheduled_at
			ELSE next_attempt_at
		END ASC,
		priority DESC,
		id ASC
	LIMIT $1
	FOR UPDATE SKIP LOCKED
`
