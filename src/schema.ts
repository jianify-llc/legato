/** Core columns have one owner. Applications only compose schema, domain foreign keys and indexes. */
import { sql } from "drizzle-orm";
import {
  bigint,
  check,
  integer,
  jsonb,
  text,
  timestamp,
  uuid,
  varchar,
  type AnyPgColumn,
} from "drizzle-orm/pg-core";
import type { Json, TaskStatus } from "./types.js";
export type { Json, TaskStatus } from "./types.js";
export function taskColumns(options: {
  id: "uuid";
}): ReturnType<typeof uuidColumns>;
export function taskColumns(options: {
  id: "bigint";
}): ReturnType<typeof bigintColumns>;
export function taskColumns(options: { id: "uuid" | "bigint" }) {
  return options.id === "uuid" ? uuidColumns() : bigintColumns();
}
const uuidId = () => uuid("id").defaultRandom().primaryKey();
const bigintId = () =>
  bigint("id", { mode: "number" })
    .generatedAlwaysAsIdentity({
      startWith: 1,
      increment: 1,
      minValue: 1,
      maxValue: Number.MAX_SAFE_INTEGER,
      cache: 1,
      cycle: false,
    })
    .primaryKey();
const uuidColumns = () => ({
  ...columns(),
  id: uuidId(),
  retryOfTaskId: uuid("retry_of_task_id"),
});
const bigintColumns = () => ({
  ...columns(),
  id: bigintId(),
  retryOfTaskId: bigint("retry_of_task_id", { mode: "number" }),
});
function columns() {
  return {
    requestKey: text("request_key")
      .notNull()
      .unique("queue_tasks_request_key_key"),
    businessKey: text("business_key").notNull(),
    type: varchar("type", { length: 64 }).notNull(),
    executionMode: varchar("execution_mode", { length: 8 })
      .$type<"queue" | "inline">()
      .notNull()
      .default("queue"),
    userId: uuid("user_id"),
    operationId: bigint("operation_id", { mode: "number" }),
    bizParams: jsonb("biz_params").$type<Json>().notNull(),
    status: varchar("status", { length: 16 })
      .$type<TaskStatus>()
      .notNull()
      .default("pending"),
    runAttempts: integer("run_attempts").notNull().default(0),
    executionMs: bigint("execution_ms", { mode: "number" }).default(0),
    firstStartedAt: timestamp("first_started_at", { withTimezone: true }),
    startedAt: timestamp("started_at", { withTimezone: true }),
    finishedAt: timestamp("finished_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    leaseId: uuid("lease_id"),
    leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true }),
    nextRunAt: timestamp("next_run_at", { withTimezone: true })
      .notNull()
      .default(sql`clock_timestamp()`),
    expiresAt: timestamp("expires_at", { withTimezone: true }),
    executionState: jsonb("execution_state").$type<Json>(),
    result: jsonb("result").$type<Json>(),
    resultCode: varchar("result_code", { length: 64 }),
    failReasons: jsonb("fail_reasons").$type<Json[]>().notNull().default([]),
    lastFailedRunAttempt: integer("last_failed_run_attempt"),
    lastFailedAt: timestamp("last_failed_at", { withTimezone: true }),
    attentionEnteredAt: timestamp("attention_entered_at", {
      withTimezone: true,
    }),
    dispatchAttempts: integer("dispatch_attempts").notNull().default(0),
    dispatchFailures: integer("dispatch_failures").notNull().default(0),
    nextDispatchAt: timestamp("next_dispatch_at", { withTimezone: true }),
    lastDispatchAttemptAt: timestamp("last_dispatch_attempt_at", {
      withTimezone: true,
    }),
  };
}
/** The callback receives the already-built table, just like Drizzle's native constraints API. */
export function taskConstraints(t: {
  [K in keyof ReturnType<typeof columns>]: AnyPgColumn;
}) {
  return [
    check(
      "queue_task_status",
      sql`${t.status} IN ('pending','queued','running','succeeded','retry_wait','failed','manual')`,
    ),
    check("queue_task_mode", sql`${t.executionMode} IN ('queue','inline')`),
    check("queue_task_rounds", sql`${t.runAttempts}>=0`),
    check(
      "queue_task_execution_ms",
      sql`${t.executionMs} IS NULL OR ${t.executionMs}>=0`,
    ),
    check(
      "queue_task_retries",
      sql`${t.dispatchAttempts}>=0 AND ${t.dispatchFailures}>=0`,
    ),
    check(
      "queue_task_fail_reasons_array",
      sql`jsonb_typeof(${t.failReasons})='array'`,
    ),
    check(
      "queue_task_lease_pair",
      sql`(${t.leaseId} IS NULL)=(${t.leaseExpiresAt} IS NULL)`,
    ),
    check(
      "queue_task_lease_state",
      sql`${t.leaseId} IS NULL OR ${t.status}='running'`,
    ),
    check(
      "queue_task_running_lease",
      sql`${t.status}<>'running' OR (${t.leaseId} IS NOT NULL AND ${t.startedAt} IS NOT NULL)`,
    ),
    // Imported terminal receipts can have genuinely unknown completion time; new tasks still require it.
    check(
      "queue_task_terminal",
      sql`((${t.status} IN ('succeeded','failed'))=(${t.finishedAt} IS NOT NULL)) OR (${t.status} IN ('succeeded','failed') AND ${t.executionMs} IS NULL AND ${t.firstStartedAt} IS NULL)`,
    ),
    check(
      "queue_task_failed_round_pair",
      sql`(${t.lastFailedRunAttempt} IS NULL)=(${t.lastFailedAt} IS NULL) AND (${t.lastFailedRunAttempt} IS NULL OR (${t.lastFailedRunAttempt}>0 AND ${t.lastFailedRunAttempt}<=${t.runAttempts}))`,
    ),
  ];
}
