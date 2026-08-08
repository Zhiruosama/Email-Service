package postgres

const insertOutboxEventQuery = `
	INSERT INTO outbox_events (
		id,
		aggregate_type,
		aggregate_id,
		event_type,
		aggregate_sequence,
		dispatch_generation,
		payload
	) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
	ON CONFLICT ON CONSTRAINT outbox_events_identity_unique DO NOTHING
	RETURNING id
`

const outboxPayloadMatchesQuery = `
	SELECT payload = $6::jsonb
	FROM outbox_events
	WHERE aggregate_type = $1
	  AND aggregate_id = $2
	  AND event_type = $3
	  AND aggregate_sequence = $4
	  AND dispatch_generation = $5
`
