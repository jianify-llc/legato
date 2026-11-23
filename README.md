# Legato

Legato provides checkpointable task execution using PostgreSQL as the owner of leases, checkpoints and results. Queue messages are notifications; duplicate delivery does not grant execution rights. The package supplies Drizzle core columns and constraints, SQL transitions, slice budgets and a runner. Applications own domain authorization, safe checkpoints, idempotent effects, dispatch and business data. Commit checkpoint/yield or final business effects/task success in the same short database transaction; never hold it open during network work.

Each claim is a slice (`run_attempts`); voluntary yields do not spend the failure budget. The soft deadline is 12 minutes and the hard deadline is 14 minutes. `execution_ms` sums settled owned slices; task elapsed time derives from creation to finish, and initial queue time from creation to first start. A legacy unknown execution duration stays null. `acceptLateSuccessInTx` is reserved for an independently verified external receipt and cannot replace the checkpoint.

The runner signals cancellation but cannot forcibly stop arbitrary JavaScript or external work. Database lease and deadline checks fence late ordinary writes. `renewInTx` returns the new lease expiry or `null` when the current owner cannot renew. Domain handlers must cooperate with the budget and pick safe continuation boundaries. A Standard fallback should only begin when its timeout and closing reserve fit in the remaining soft budget.

Automatic continuation applies to Queue tasks. Inline tasks remain single invocations with caller-owned receipt recovery; yielding an inline task is rejected instead of leaving an unconsumable pending task.

For an ID-only Queue notification, `claimQueuedInTx` takes the task types available to the consumer and each type's failure limit. It checks the locked row and database time before granting a lease. An unsupported queued type can be reported to the caller without locking the task or charging an attempt.

`reserveDispatchByTypeInTx` applies the configured budget for the locked task's type and returns the previous status with the reservation. The caller sends the notification after confirming the reservation. The fixed-budget `reserveDispatchInTx` uses the same write.

`yieldInTx` keeps the current checkpoint when `next` is undefined and clears it when `next` is null. An optional `minimumDelayMs` schedules the next slice no earlier than the requested delay after the Task row lock is acquired. A later absolute `nextRunAt` still takes precedence.

## Installation

The package has not yet been published. From a local checkout, use `pnpm install`, `pnpm build`, and a workspace or local package dependency. It requires PostgreSQL and Drizzle ORM 0.45.x.

## Local verification

Follow the [Jianify development guide](https://github.com/jianify-llc/Jianify-LLC/blob/main/docs/engineering/development.md) for code review, testing and long-running handoffs.

Run `pnpm check`, `pnpm build`, and `LEGATO_TEST_DATABASE_URL=<isolated-postgres-url> pnpm test`. Integration tests create and drop an isolated schema in that database; the configured account needs schema creation privileges. Without the URL, database tests are skipped. The tests use synthetic data and do not call paid providers.
