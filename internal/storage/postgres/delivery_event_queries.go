package postgres

const insertDeliveryEventQuery = `
	INSERT INTO delivery_events (
		id,
		tenant_id,
		message_id,
		idempotency_key,
		status,
		sequence,
		attempt_number,
		provider_message_id,
		failure_category,
		failure_code,
		failure_retryable,
		occurred_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
	)
	ON CONFLICT ON CONSTRAINT delivery_events_message_sequence_unique DO NOTHING
	RETURNING id
`

const deliveryEventMatchesQuery = `
	SELECT
		id = $1
		AND tenant_id = $2
		AND idempotency_key = $4
		AND status = $5
		AND attempt_number = $7
		AND provider_message_id IS NOT DISTINCT FROM $8
		AND failure_category IS NOT DISTINCT FROM $9
		AND failure_code IS NOT DISTINCT FROM $10
		AND failure_retryable IS NOT DISTINCT FROM $11
		AND occurred_at = $12
	FROM delivery_events
	WHERE message_id = $3 AND sequence = $6
`

const getDeliveryEventByIDQuery = `
	SELECT
		id,
		tenant_id,
		message_id,
		idempotency_key,
		status,
		sequence,
		attempt_number,
		provider_message_id,
		failure_category,
		failure_code,
		failure_retryable,
		occurred_at,
		observed_at
	FROM delivery_events
	WHERE id = $1
`
