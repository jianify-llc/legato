# Legato execution contract

PostgreSQL is the only owner of execution rights, checkpoints and receipts. Queue messages only notify consumers. The package owns the complete `queue_tasks` core columns and transition SQL; products add domain foreign keys/indexes and keep their own domain relations outside that table.

- One successful claim increments `run_attempts` (the slice count). Normal yield does not spend failure budget. Existing bounded failure history determines failed slices, not total claims.
- `created_at`, immutable `first_started_at`, current `started_at` and `finished_at` derive total task and initial queue durations. `execution_ms` settles each owned slice once; an expired slice ends at the earlier of its confirmed lease expiry and hard deadline. Unknown legacy duration remains null.
- A slice asks its handler to yield after 12 minutes and loses ordinary write rights at 14 minutes. A voluntary yield saves its checkpoint and returns to pending atomically. A forced timeout is a failure, not a continuation. An AbortSignal cannot kill arbitrary external work; database fences reject all late ordinary writes.
- Voluntary continuation belongs to Queue tasks. Inline execution keeps its existing caller-owned recovery contract and must not enter pending without a consumer.
- Domain code chooses safe checkpoints. A model round is safe only after its response and all parallel tools finish. Store the response identity, new native tool results and necessary domain state, not a second full conversation history. Resuming revalidates scope, config, facts and targets. A completed Agent checkpoint skips the model and only finishes domain commit.
- A model call starts with Flex. Standard fallback must fit the remaining soft budget; otherwise the handler yields. The next slice starts with Flex again.
- Checkpoint/yield and final business effects/task success use the caller's same short transaction. No transaction spans model or network waits. A verified external receipt may use a separate explicit late-success entry, which never accepts an old checkpoint or counters.
- Products own business policy, authorization, encryption, idempotent side effects and dispatch transport. They must not put plaintext chat, credentials or private keys into generic checkpoints.

Memoia retains Operation/Blob domain identities and evidence constraints; only Legato owns maintenance execution. Legacy execution columns are migration input and stopped-writer rollback output, not a live second owner. Luvel preserves existing domain transaction hooks and external receipt recovery.

Validation uses isolated PostgreSQL, deterministic timers and synthetic HTTP responses: duplicate claims, rollback, multiple yields, lease takeover, hard timeout, delayed reaping, exact-once duration settlement, stale writes, native tool continuation and scoped deletion. No shared database, deployment or paid provider is required for local acceptance.
