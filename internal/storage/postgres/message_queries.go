package postgres

const messageColumns = `
	id,
	tenant_id,
	idempotency_key,
	payload_fingerprint,
	category,
	priority,
	duplicate_risk_policy,
	status,
	scheduled_at,
	dispatch_deadline,
	next_attempt_at,
	dispatch_generation,
	attempt_count,
	max_attempts,
	provider_accepted_at,
	provider_message_id,
	latest_sequence,
	version,
	last_error_category,
	last_error_code,
	last_error_retryable,
	accepted_at,
	updated_at
	,sender_identity_key
	,template_key
	,template_version
	,locale
	,recipient_masked
	,payload_key_id
	,encrypted_payload
	,submission_metadata
`

const insertMessageQuery = `
	INSERT INTO mail_messages (` + messageColumns + `)
	VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
		$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23,
		$24, $25, $26, $27, $28, $29, $30, $31
	)
	ON CONFLICT ON CONSTRAINT mail_messages_tenant_idempotency_unique DO NOTHING
	RETURNING version
`

const getMessageByIDQuery = `
	SELECT ` + messageColumns + `
	FROM mail_messages
	WHERE id = $1
`

const getMessageByIdempotencyKeyQuery = `
	SELECT ` + messageColumns + `
	FROM mail_messages
	WHERE tenant_id = $1 AND idempotency_key = $2
`

const updateMessageQuery = `
	UPDATE mail_messages
	SET
		status = $1,
		scheduled_at = $2,
		dispatch_deadline = $3,
		next_attempt_at = $4,
		dispatch_generation = $5,
		attempt_count = $6,
		max_attempts = $7,
		provider_accepted_at = $8,
		provider_message_id = $9,
		latest_sequence = $10,
		last_error_category = $11,
		last_error_code = $12,
		last_error_retryable = $13,
		updated_at = $14,
		version = version + 1
	WHERE id = $15 AND version = $16
	RETURNING version
`
