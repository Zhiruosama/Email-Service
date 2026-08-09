package postgres

const insertStartedDeliveryAttemptQuery = `
	INSERT INTO delivery_attempts (
		id,
		message_id,
		attempt_no,
		dispatch_generation,
		provider_key,
		status,
		started_at
	) VALUES ($1, $2, $3, $4, $5, 'STARTED', $6)
	RETURNING id
`

const completeDeliveryAttemptQuery = `
	UPDATE delivery_attempts
	SET
		status = $2,
		finished_at = $3,
		provider_message_id = $4,
		error_category = $5,
		error_code = $6,
		error_retryable = $7
	WHERE id = $1 AND status = 'STARTED'
	RETURNING id
`
