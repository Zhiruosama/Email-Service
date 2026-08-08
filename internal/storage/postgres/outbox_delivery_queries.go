package postgres

const claimPendingOutboxQuery = `
	WITH candidates AS (
		SELECT id
		FROM outbox_events
		WHERE status = 'PENDING'
		  AND available_at <= transaction_timestamp()
		  AND (lease_until IS NULL OR lease_until <= transaction_timestamp())
		ORDER BY available_at ASC, created_at ASC, id ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE outbox_events AS event
	SET
		lease_owner = $2,
		lease_until = transaction_timestamp() + ($3 * INTERVAL '1 millisecond')
	FROM candidates
	WHERE event.id = candidates.id
	RETURNING
		event.id,
		event.aggregate_type,
		event.aggregate_id,
		event.event_type,
		event.aggregate_sequence,
		event.dispatch_generation,
		event.payload,
		event.lease_owner,
		event.lease_until,
		event.attempt_count,
		transaction_timestamp()
`

const markOutboxPublishedQuery = `
	UPDATE outbox_events
	SET
		status = 'PUBLISHED',
		attempt_count = $3,
		last_error_code = NULL,
		lease_owner = NULL,
		lease_until = NULL,
		published_at = transaction_timestamp()
	WHERE id = $1
	  AND status = 'PENDING'
	  AND lease_owner = $2
	  AND attempt_count = $3 - 1
	RETURNING published_at
`

const rescheduleOutboxQuery = `
	UPDATE outbox_events
	SET
		attempt_count = $3,
		last_error_code = $5,
		available_at = transaction_timestamp() + ($4 * INTERVAL '1 millisecond'),
		lease_owner = NULL,
		lease_until = NULL
	WHERE id = $1
	  AND status = 'PENDING'
	  AND lease_owner = $2
	  AND attempt_count = $3 - 1
	RETURNING available_at
`

const deadLetterOutboxQuery = `
	UPDATE outbox_events
	SET
		status = 'DEAD_LETTERED',
		attempt_count = $3,
		last_error_code = $4,
		lease_owner = NULL,
		lease_until = NULL
	WHERE id = $1
	  AND status = 'PENDING'
	  AND lease_owner = $2
	  AND attempt_count = $3 - 1
	RETURNING id
`
