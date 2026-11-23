/** Single execution writer; every method uses the caller's existing PostgreSQL transaction. */
import type {
  AdministrativeDecision,
  ClaimOptions,
  Completion,
  Failure,
  Json,
  RetryPolicy,
  Sql,
  TaskId,
  TaskInput,
  TaskRecord,
  TaskStatus,
} from "./types.js";
import {
  CHECKPOINT_MAX_BYTES,
  HARD_SLICE_MS,
  TASK_LEASE_MS,
  TaskLeaseLost,
} from "./policy.js";

export const TASK_RECOVERED = "TASK_RECOVERED";
export const TASK_FAILURE_BUDGET_BASELINE = "TASK_FAILURE_BUDGET_BASELINE";
const names = [
  "id",
  "request_key",
  "business_key",
  "type",
  "execution_mode",
  "user_id",
  "operation_id",
  "biz_params",
  "status",
  "run_attempts",
  "execution_ms",
  "first_started_at",
  "started_at",
  "finished_at",
  "created_at",
  "updated_at",
  "lease_id",
  "lease_expires_at",
  "next_run_at",
  "expires_at",
  "execution_state",
  "result",
  "result_code",
  "dispatch_attempts",
  "dispatch_failures",
  "next_dispatch_at",
  "last_dispatch_attempt_at",
  "last_failed_run_attempt",
  "last_failed_at",
  "attention_entered_at",
  "retry_of_task_id",
];
const leaseTimes = "id, lease_expires_at, started_at";
const camel = (name: string) =>
  name.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());
const numeric = new Set([
  "operation_id",
  "run_attempts",
  "execution_ms",
  "dispatch_attempts",
  "dispatch_failures",
  "last_failed_run_attempt",
  "failure_count",
]);
function integer(value: unknown): number {
  const n = Number(value);
  if (!Number.isSafeInteger(n))
    throw new RangeError("Task integer exceeds the safe range");
  return n;
}
function checkpoint(value: Json | null) {
  if (value === null) return null;
  const encoded = JSON.stringify(value);
  if (new TextEncoder().encode(encoded).length > CHECKPOINT_MAX_BYTES)
    throw new RangeError("Task checkpoint exceeds its byte budget");
  return encoded;
}
const identifier = (value: string) => {
  if (!/^[a-z_][a-z0-9_]*$/.test(value))
    throw new TypeError("Invalid task schema identifier");
  return `"${value}"`;
};
/** Historical failed slices after the last explicit reset/baseline; yield is never an error. */
export function failureCountSql(alias = "t"): string {
  identifier(alias);
  const history = `${alias}.fail_reasons`;
  return `(SELECT COALESCE(max(CASE WHEN x.pos=b.pos THEN COALESCE((x.item->>'failureCount')::integer,0) END),0)
    +count(DISTINCT x.item->>'runAttempt') FILTER (WHERE x.pos>b.pos AND x.item->>'phase' IN ('execution','scheduling') AND (x.item->>'runAttempt') ~ '^[1-9][0-9]*$')
    FROM jsonb_array_elements(${history}) WITH ORDINALITY AS x(item,pos)
    CROSS JOIN (SELECT COALESCE(max(p),0) AS pos FROM jsonb_array_elements(${history}) WITH ORDINALITY e(v,p)
      WHERE v->>'resultCode' IN ('${TASK_RECOVERED}','${TASK_FAILURE_BUDGET_BASELINE}')) b)`;
}

export class TaskRepository<I extends TaskId = string> {
  readonly table: string;
  constructor(
    schema: string,
    readonly idType: "uuid" | "bigint" = "uuid",
  ) {
    this.table = `${identifier(schema)}."queue_tasks"`;
  }
  private projection() {
    return `${names.map((n) => `t.${n}`).join(",")},${failureCountSql()} AS failure_count,t.fail_reasons->-1 AS last_failure`;
  }
  private decode(raw: Record<string, unknown>): TaskRecord<I> {
    const value: Record<string, unknown> = {};
    for (const [name, rawValue] of Object.entries(raw)) {
      let v = rawValue;
      if (
        v != null &&
        (numeric.has(name) ||
          (this.idType === "bigint" &&
            ["id", "retry_of_task_id"].includes(name)))
      )
        v = integer(v);
      if (v != null && (name.endsWith("_at") || name === "lease_expires_at"))
        v = v instanceof Date ? v : new Date(String(v));
      value[camel(name)] = v;
    }
    return value as unknown as TaskRecord<I>;
  }
  private async one(
    tx: Sql,
    text: string,
    values: unknown[],
  ): Promise<TaskRecord<I> | null> {
    const result = await tx.query(text, values);
    return result.rows[0] ? this.decode(result.rows[0]) : null;
  }
  private afterLockSql(text: string, columns: string, candidate = "t.id=$1") {
    // A write can still see its old snapshot after waiting. Qualify on the row
    // returned by the lock and evaluate deadlines with the post-lock clock.
    return `WITH legato_locked AS MATERIALIZED (
       SELECT ${columns} FROM ${this.table} AS t WHERE ${candidate} FOR NO KEY UPDATE
     ), legato_clock AS MATERIALIZED (
       SELECT clock_timestamp() AS now FROM legato_locked
     )
     ${text.replaceAll("clock_timestamp()", "(SELECT now FROM legato_clock)")}`;
  }
  private oneAfterLock(
    tx: Sql,
    text: string,
    values: unknown[],
    columns = "id",
    candidate?: string,
  ) {
    return this.one(tx, this.afterLockSql(text, columns, candidate), values);
  }
  private runningOwner() {
    return "t.id=$1 AND t.status='running' AND t.lease_id=$2";
  }
  private unexpiredLease(row: string) {
    return `${row}.lease_expires_at>clock_timestamp()
      AND ${row}.started_at+${HARD_SLICE_MS}*interval '1 millisecond'>clock_timestamp()`;
  }
  private lockedRowWhere(predicate = this.unexpiredLease("locked")) {
    return `t.id=(SELECT locked.id FROM legato_locked AS locked WHERE ${predicate})`;
  }
  private elapsed() {
    // NULL is a legacy unknown duration and remains unknown after later slices.
    return `t.execution_ms+CASE WHEN t.status='running' AND t.lease_id IS NOT NULL AND t.started_at IS NOT NULL
      THEN greatest(0,floor(extract(epoch FROM (least(clock_timestamp(),t.lease_expires_at,t.started_at+${HARD_SLICE_MS}*interval '1 millisecond')-t.started_at))*1000))::bigint ELSE 0 END`;
  }
  async readInTx(tx: Sql, id: I, lock = false) {
    return this.one(
      tx,
      `SELECT ${this.projection()} FROM ${this.table} t WHERE t.id=$1 ${lock ? "FOR UPDATE" : ""}`,
      [id],
    );
  }
  async readManyInTx(tx: Sql, ids: readonly I[]): Promise<TaskRecord<I>[]> {
    if (!ids.length) return [];
    if (ids.length > 500)
      throw new RangeError("Task read page exceeds its limit");
    const result = await tx.query(
      `SELECT ${this.projection()} FROM ${this.table} t WHERE t.id=ANY($1::${this.idType === "uuid" ? "uuid" : "bigint"}[])`,
      [[...new Set(ids)]],
    );
    return result.rows.map((row) => this.decode(row));
  }
  async assertOwnedInTx(
    tx: Sql,
    id: I,
    owner: string,
    lock = false,
  ): Promise<TaskRecord<I>> {
    const statement = lock
      ? `WITH legato_locked AS MATERIALIZED (
           SELECT t.* FROM ${this.table} t WHERE ${this.runningOwner()} FOR UPDATE
         ), legato_clock AS MATERIALIZED (SELECT clock_timestamp() AS now FROM legato_locked)
         SELECT ${this.projection()} FROM legato_locked t
         WHERE ${this.unexpiredLease("t").replaceAll("clock_timestamp()", "(SELECT now FROM legato_clock)")}`
      : `SELECT ${this.projection()} FROM ${this.table} t
         WHERE ${this.runningOwner()} AND ${this.unexpiredLease("t")}`;
    const row = await this.one(tx, statement, [id, owner]);
    if (!row) throw new TaskLeaseLost();
    return row;
  }
  async createInTx(tx: Sql, input: TaskInput<I>) {
    const columns = [
      "request_key",
      "business_key",
      "type",
      "biz_params",
      "user_id",
      "operation_id",
      "execution_mode",
      "execution_state",
      "next_run_at",
      "expires_at",
      "retry_of_task_id",
    ];
    const values: unknown[] = [
      input.requestKey,
      input.businessKey,
      input.type,
      JSON.stringify(input.bizParams),
      input.userId ?? null,
      input.operationId ?? null,
      input.executionMode ?? "queue",
      checkpoint(input.executionState ?? null),
      input.nextRunAt ?? null,
      input.expiresAt ?? null,
      input.retryOfTaskId ?? null,
    ];
    if (input.id !== undefined) {
      columns.push("id");
      values.push(input.id);
    }
    const binds = values.map((_, i) =>
      i === 8
        ? `COALESCE($${i + 1}::timestamptz,clock_timestamp())`
        : `$${i + 1}`,
    );
    return this.one(
      tx,
      `INSERT INTO ${this.table} AS t (${columns.join(",")}) ${input.id !== undefined && this.idType === "bigint" ? "OVERRIDING SYSTEM VALUE" : ""}
      VALUES (${binds.join(",")}) ON CONFLICT DO NOTHING RETURNING ${this.projection()}`,
      values,
    );
  }
  private claimParameters(id: I, leaseMs: number) {
    if (!Number.isSafeInteger(leaseMs) || leaseMs <= 0)
      throw new RangeError("Invalid task lease duration");
    return [id, crypto.randomUUID(), leaseMs];
  }
  private claimUpdate(now = "clock_timestamp()") {
    return `UPDATE ${this.table} AS t SET status='running',lease_id=$2,lease_expires_at=least(${now}+$3*interval '1 millisecond',${now}+${HARD_SLICE_MS}*interval '1 millisecond'),
      first_started_at=CASE WHEN run_attempts=0 THEN ${now} ELSE first_started_at END,
      started_at=${now},run_attempts=run_attempts+1,dispatch_failures=0,next_dispatch_at=NULL,updated_at=${now}`;
  }
  private claimTimeCondition(row: string, now = "clock_timestamp()") {
    return `${row}.next_run_at<=${now} AND (${row}.expires_at IS NULL OR ${row}.expires_at>${now})`;
  }
  async claimInTx(tx: Sql, id: I, options: ClaimOptions = {}) {
    const candidate = `t.id=$1 AND t.status=ANY($4::text[]) AND t.lease_id IS NULL
      AND ($5::text IS NULL OR t.type=$5) AND ($6::text IS NULL OR t.execution_mode=$6)`;
    const eligible = `($7::integer IS NULL OR ${failureCountSql("locked")}<$7)
      AND ${this.claimTimeCondition("locked")}`;
    return this.oneAfterLock(
      tx,
      `${this.claimUpdate()} WHERE ${this.lockedRowWhere(eligible)} RETURNING ${this.projection()}`,
      [
        ...this.claimParameters(id, options.leaseMs ?? TASK_LEASE_MS),
        options.statuses ?? ["pending", "queued", "retry_wait"],
        options.type ?? null,
        options.executionMode ?? null,
        options.limit ?? null,
      ],
      "id, fail_reasons, next_run_at, expires_at",
      candidate,
    );
  }
  /** Claim an ID-only queue notification using the deployed handlers' type budgets. */
  async claimQueuedInTx(
    tx: Sql,
    id: I,
    typeLimits: ReadonlyMap<string, number | null>,
    leaseMs = TASK_LEASE_MS,
  ): Promise<{ task: TaskRecord<I> | null; unsupportedType: string | null }> {
    const policies = [...typeLimits];
    const now = "(SELECT now FROM legato_clock)";
    const update = `${this.claimUpdate(now)}
      FROM legato_locked AS locked
      WHERE t.id=locked.id AND ${this.claimTimeCondition("locked", now)}
        AND (locked.failure_limit IS NULL OR ${failureCountSql("locked")}<locked.failure_limit)
      RETURNING ${this.projection()}`;
    // Only eligible routes lock the task. This update leaves foreign-key references usable.
    // The diagnostic read never locks the task or consumes budget.
    const result = await tx.query(
      `WITH claim_types AS (
         SELECT * FROM unnest($4::text[], $5::integer[]) AS policy(type, failure_limit)
       ), legato_locked AS MATERIALIZED (
         SELECT t.id, t.next_run_at, t.expires_at, t.fail_reasons, policy.failure_limit FROM ${this.table} AS t
         JOIN claim_types AS policy ON policy.type=t.type
         WHERE t.id=$1 AND t.status='queued' AND t.execution_mode='queue' AND t.lease_id IS NULL
           AND (t.expires_at IS NULL OR t.expires_at>statement_timestamp())
         FOR NO KEY UPDATE OF t
       ), legato_clock AS MATERIALIZED (
         SELECT clock_timestamp() AS now FROM legato_locked
       ), claimed AS (${update}), unsupported AS (
         SELECT t.type FROM ${this.table} AS t
         WHERE NOT EXISTS (SELECT 1 FROM claimed) AND t.id=$1 AND t.status='queued'
           AND t.execution_mode='queue'
           AND (t.expires_at IS NULL OR t.expires_at>statement_timestamp())
           AND NOT EXISTS (SELECT 1 FROM claim_types WHERE claim_types.type=t.type)
       )
       SELECT claimed.*, unsupported.type AS unsupported_type
       FROM (VALUES (1)) AS request(id)
       LEFT JOIN claimed ON true LEFT JOIN unsupported ON true`,
      [
        ...this.claimParameters(id, leaseMs),
        policies.map(([type]) => type),
        policies.map(([, limit]) => limit),
      ],
    );
    const { unsupported_type, ...row } = result.rows[0] ?? {};
    return {
      task: row.id == null ? null : this.decode(row),
      unsupportedType: typeof unsupported_type === "string" ? unsupported_type : null,
    };
  }
  async renewInTx(tx: Sql, id: I, owner: string, leaseMs = TASK_LEASE_MS): Promise<Date | null> {
    if (!Number.isSafeInteger(leaseMs) || leaseMs <= 0)
      throw new RangeError("Invalid task lease duration");
    const update = `UPDATE ${this.table} AS t
      SET lease_expires_at=least(clock_timestamp()+$3*interval '1 millisecond',started_at+${HARD_SLICE_MS}*interval '1 millisecond'),
        updated_at=clock_timestamp()
      WHERE ${this.lockedRowWhere()} RETURNING t.lease_expires_at`;
    const { rows: [row] } = await tx.query<{ lease_expires_at: Date | string }>(
      this.afterLockSql(update, leaseTimes, this.runningOwner()),
      [id, owner, leaseMs],
    );
    return row ? new Date(row.lease_expires_at) : null;
  }
  async saveCheckpointInTx(
    tx: Sql,
    id: I,
    owner: string,
    previous: Json | null,
    next: Json,
  ) {
    const sameCheckpoint = "NULLIF(locked.execution_state,'null'::jsonb) IS NOT DISTINCT FROM NULLIF($3::jsonb,'null'::jsonb)";
    return this.oneAfterLock(
      tx,
      `UPDATE ${this.table} AS t SET execution_state=$4::jsonb,updated_at=clock_timestamp()
      WHERE ${this.lockedRowWhere(`${this.unexpiredLease("locked")} AND ${sameCheckpoint}`)} RETURNING ${this.projection()}`,
      [id, owner, checkpoint(previous), checkpoint(next)],
      `${leaseTimes}, execution_state`,
      this.runningOwner(),
    );
  }
  async yieldInTx(
    tx: Sql,
    id: I,
    owner: string,
    next: Json | null | undefined,
    nextRunAt?: Date,
    options?: Partial<Pick<Completion, "result" | "resultCode">> & {
      minimumDelayMs?: number;
      type?: string;
    },
  ) {
    const delay = options?.minimumDelayMs;
    if (delay !== undefined && (!Number.isSafeInteger(delay) || delay < 0))
      throw new RangeError("Minimum yield delay must be a non-negative integer");
    const candidate = `${this.runningOwner()} AND t.execution_mode='queue'
      AND ($10::text IS NULL OR t.type=$10)`;
    return this.oneAfterLock(
      tx,
      `UPDATE ${this.table} AS t SET status='pending',execution_state=CASE WHEN $8 THEN $3::jsonb ELSE t.execution_state END,
      next_run_at=greatest(COALESCE($4::timestamptz,clock_timestamp()),clock_timestamp()+$9::bigint*interval '1 millisecond'),
      execution_ms=${this.elapsed()},lease_id=NULL,lease_expires_at=NULL,started_at=NULL,next_dispatch_at=NULL,result_code=$5,result=CASE WHEN $6 THEN $7::jsonb ELSE result END,updated_at=clock_timestamp()
      WHERE ${this.lockedRowWhere()}
      RETURNING ${this.projection()}`,
      [
        id,
        owner,
        checkpoint(next ?? null),
        nextRunAt ?? null,
        options?.resultCode ?? "TASK_YIELDED",
        options?.result !== undefined,
        JSON.stringify(options?.result ?? null),
        next !== undefined,
        delay ?? null,
        options?.type ?? null,
      ],
      leaseTimes,
      candidate,
    );
  }
  async completeInTx(tx: Sql, id: I, owner: string, completion: Completion) {
    return this.oneAfterLock(
      tx,
      `UPDATE ${this.table} AS t SET status='succeeded',result=$3::jsonb,result_code=$4,
      execution_state=CASE WHEN $5 THEN $6::jsonb ELSE execution_state END,
      execution_ms=${this.elapsed()},lease_id=NULL,lease_expires_at=NULL,started_at=NULL,next_dispatch_at=NULL,finished_at=clock_timestamp(),updated_at=clock_timestamp()
      WHERE ${this.lockedRowWhere()} RETURNING ${this.projection()}`,
      [
        id,
        owner,
        JSON.stringify(completion.result ?? null),
        completion.resultCode,
        completion.executionState !== undefined,
        checkpoint(completion.executionState ?? null),
      ],
      leaseTimes,
      this.runningOwner(),
    );
  }
  /** Empty/no-op batches finish without pretending a worker/model slice was executed. */
  async completeUnstartedInTx(tx: Sql, id: I, completion: Completion) {
    return this.one(
      tx,
      `UPDATE ${this.table} AS t SET status='succeeded',result=$2::jsonb,result_code=$3,finished_at=clock_timestamp(),
      execution_state=NULL,next_dispatch_at=NULL,updated_at=clock_timestamp()
      WHERE id=$1 AND status='pending' AND run_attempts=0 AND lease_id IS NULL RETURNING ${this.projection()}`,
      [id, JSON.stringify(completion.result ?? null), completion.resultCode],
    );
  }
  /** Caller must prove the authoritative external result in the same transaction. No stale checkpoint input exists here. */
  async acceptLateSuccessInTx(
    tx: Sql,
    id: I,
    completion: Omit<Completion, "executionState">,
  ) {
    if ("executionState" in completion)
      throw new TypeError("Late success cannot replace a checkpoint");
    return this.oneAfterLock(
      tx,
      `UPDATE ${this.table} AS t SET status='succeeded',result=$2::jsonb,result_code=$3,
      execution_ms=${this.elapsed()},lease_id=NULL,lease_expires_at=NULL,started_at=NULL,next_dispatch_at=NULL,finished_at=clock_timestamp(),updated_at=clock_timestamp()
      WHERE id=$1 AND status<>'succeeded' RETURNING ${this.projection()}`,
      [id, JSON.stringify(completion.result ?? null), completion.resultCode],
    );
  }
  /** A caller-reviewed manual result may replace its checkpoint in the review transaction. */
  async confirmReviewedSuccessInTx(
    tx: Sql,
    id: I,
    runAttempts: number,
    completion: Completion & { executionState: NonNullable<Completion["executionState"]> },
  ) {
    return this.one(
      tx,
      `UPDATE ${this.table} AS t SET status='succeeded',result=$3::jsonb,result_code=$4,
      execution_state=$5::jsonb,lease_expires_at=NULL,started_at=NULL,next_dispatch_at=NULL,
      finished_at=clock_timestamp(),updated_at=clock_timestamp()
      WHERE id=$1 AND run_attempts=$2 AND status='manual' AND lease_id IS NULL
      RETURNING ${this.projection()}`,
      [
        id,
        runAttempts,
        JSON.stringify(completion.result ?? null),
        completion.resultCode,
        checkpoint(completion.executionState),
      ],
    );
  }
  private async fail(
    tx: Sql,
    id: I,
    owner: string | null,
    failure: Failure,
    policy: RetryPolicy,
    expired: boolean,
    completion?: Pick<Completion, "result"> & { expectedType?: string },
  ) {
    if (
      !policy.delays.length ||
      policy.delays.some((d) => !Number.isFinite(d) || d < 0)
    )
      throw new RangeError("Invalid retry delays");
    const ownerCandidate = expired
      ? "t.id=$1 AND t.status='running' AND ($2::uuid IS NULL OR t.lease_id=$2)"
      : this.runningOwner();
    const candidate = `${ownerCandidate} AND ($13::text IS NULL OR t.type=$13)`;
    const eligible = expired
      ? `(locked.lease_expires_at<=clock_timestamp() OR locked.started_at+${HARD_SLICE_MS}*interval '1 millisecond'<=clock_timestamp())`
      : this.unexpiredLease("locked");
    const count = `${failureCountSql()}+1`;
    const again = `$4 AND execution_mode='queue' AND ($5::integer IS NULL OR ${count}<$5)`;
    const next = `greatest(clock_timestamp()+($6::double precision[])[least(array_length($6::double precision[],1),${count})]*interval '1 minute',COALESCE($7::timestamptz,clock_timestamp()))`;
    const retry = `${again} AND (expires_at IS NULL OR ${next}<expires_at)`;
    return this.oneAfterLock(
      tx,
      `UPDATE ${this.table} AS t SET
      status=CASE WHEN $8 THEN 'manual' WHEN ${retry} THEN 'retry_wait' ELSE 'failed' END,
      next_run_at=CASE WHEN ${retry} THEN ${next} ELSE clock_timestamp() END,
      finished_at=CASE WHEN $8 OR ${retry} THEN NULL ELSE clock_timestamp() END,
      fail_reasons=fail_reasons||jsonb_build_array(jsonb_build_object('failedAt',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),'runAttempt',run_attempts,'phase',$9::text,'resultCode',$3::text,'diagnostics',$10::jsonb,'retryable',$4::boolean)),
      last_failed_run_attempt=run_attempts,last_failed_at=clock_timestamp(),result_code=$3,
      result=CASE WHEN $11 THEN $12::jsonb ELSE result END,
      execution_ms=${this.elapsed()},lease_id=NULL,lease_expires_at=NULL,started_at=NULL,next_dispatch_at=NULL,updated_at=clock_timestamp()
      WHERE ${this.lockedRowWhere(eligible)} RETURNING ${this.projection()}`,
      [
        id,
        owner,
        failure.code,
        failure.retryable,
        policy.limit,
        [...policy.delays],
        failure.retryAt ?? null,
        failure.manual ?? false,
        failure.phase ?? (expired ? "scheduling" : "execution"),
        JSON.stringify(failure.diagnostics ?? []),
        completion?.result !== undefined,
        JSON.stringify(completion?.result ?? null),
        completion?.expectedType ?? null,
      ],
      leaseTimes,
      candidate,
    );
  }
  failInTx(
    tx: Sql,
    id: I,
    owner: string,
    failure: Failure,
    policy: RetryPolicy,
    completion?: Pick<Completion, "result"> & { expectedType?: string },
  ) {
    return this.fail(tx, id, owner, failure, policy, false, completion);
  }
  revokeInTx(
    tx: Sql,
    id: I,
    failure: Failure,
    policy: RetryPolicy,
    owner?: string,
  ) {
    return this.fail(tx, id, owner ?? null, failure, policy, true);
  }
  async reserveDispatchInTx(tx: Sql, id: I, limit?: number | null) {
    const reservation = await this.reserveDispatchByTypeInTx(tx, id, new Map(), limit ?? null);
    return reservation?.task ?? null;
  }
  /** Reserve a notification using the locked task's type and return its previous status. */
  async reserveDispatchByTypeInTx(
    tx: Sql,
    id: I,
    typeLimits: ReadonlyMap<string, number | null>,
    defaultLimit: number | null,
  ): Promise<{ task: TaskRecord<I>; previousStatus: TaskStatus } | null> {
    const policies = [...typeLimits];
    const { rows: [row] } = await tx.query<{ previous_status: TaskStatus } & Record<string, unknown>>(
      `WITH legato_locked AS MATERIALIZED (
         SELECT t.id, t.type, t.status, t.next_run_at, t.next_dispatch_at, t.expires_at, t.fail_reasons
         FROM ${this.table} AS t
         WHERE t.id=$1 AND t.execution_mode='queue' AND t.status IN ('pending','queued','retry_wait')
           AND t.lease_id IS NULL AND (t.expires_at IS NULL OR t.expires_at>statement_timestamp())
         FOR NO KEY UPDATE OF t
       ), legato_dispatch AS MATERIALIZED (
         SELECT locked.*, clock_timestamp() AS now,
           CASE WHEN policy.type IS NULL THEN $2::integer ELSE policy.failure_limit END AS failure_limit
         FROM legato_locked AS locked
         LEFT JOIN unnest($3::text[], $4::integer[]) AS policy(type, failure_limit) ON policy.type=locked.type
       )
       UPDATE ${this.table} AS t SET status='queued',dispatch_attempts=t.dispatch_attempts+1,
         dispatch_failures=t.dispatch_failures+1,last_dispatch_attempt_at=date_trunc('milliseconds',candidate.now),
         next_dispatch_at=candidate.now+interval '1 minute',updated_at=candidate.now
       FROM legato_dispatch AS candidate
       WHERE t.id=candidate.id AND ${this.claimTimeCondition("candidate","candidate.now")}
         AND (candidate.next_dispatch_at IS NULL OR candidate.next_dispatch_at<=candidate.now)
         AND (candidate.failure_limit IS NULL OR ${failureCountSql("candidate")}<candidate.failure_limit)
       RETURNING ${this.projection()},candidate.status AS previous_status`,
      [
        id,
        defaultLimit,
        policies.map(([type]) => type),
        policies.map(([, limit]) => limit),
      ],
    );
    if (!row) return null;
    const { previous_status, ...task } = row;
    return { task: this.decode(task), previousStatus: previous_status };
  }
  async acknowledgeDispatchInTx(tx: Sql, id: I, reservedAt: Date) {
    return this.one(
      tx,
      `UPDATE ${this.table} AS t SET dispatch_failures=0 WHERE id=$1 AND last_dispatch_attempt_at=$2 RETURNING ${this.projection()}`,
      [id, reservedAt],
    );
  }
  /** Explicit recovery resets only the failure budget; audit history and slice/time totals stay intact. */
  async recoverInTx(tx: Sql, id: I) {
    return this.one(
      tx,
      `UPDATE ${this.table} AS t SET status='pending',finished_at=NULL,next_run_at=clock_timestamp(),next_dispatch_at=NULL,
      fail_reasons=fail_reasons||jsonb_build_array(jsonb_build_object('failedAt',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),'runAttempt',0,'phase','scheduling','resultCode','${TASK_RECOVERED}','diagnostics','[]'::jsonb,'failureCount',0)),
      result_code='${TASK_RECOVERED}',updated_at=clock_timestamp()
      WHERE id=$1 AND status IN ('failed','manual') AND lease_id IS NULL RETURNING ${this.projection()}`,
      [id],
    );
  }
  /** Domain-authorized decisions on unowned rows; running executions must use the fenced methods instead. */
  async decideInTx(
    tx: Sql,
    id: I,
    expected: { status: TaskStatus; runAttempts: number },
    decision: AdministrativeDecision,
  ) {
    return this.one(
      tx,
      `UPDATE ${this.table} AS t SET status=$4::text,result_code=$5,next_run_at=COALESCE($6::timestamptz,next_run_at),
      finished_at=CASE WHEN $4::text='failed' THEN CASE WHEN t.status='failed' THEN finished_at ELSE clock_timestamp() END ELSE NULL END,
      execution_state=CASE WHEN $7 THEN $8::jsonb ELSE execution_state END,
      result=CASE WHEN $9 THEN $10::jsonb ELSE result END,next_dispatch_at=NULL,updated_at=clock_timestamp()
      WHERE id=$1 AND status=$2 AND run_attempts=$3 AND lease_id IS NULL AND status<>'running' AND status<>'succeeded'
      RETURNING ${this.projection()}`,
      [
        id,
        expected.status,
        expected.runAttempts,
        decision.status,
        decision.resultCode,
        decision.nextRunAt ?? null,
        decision.executionState !== undefined,
        checkpoint(decision.executionState ?? null),
        decision.result !== undefined,
        JSON.stringify(decision.result ?? null),
      ],
    );
  }
  /** A stopped deployment may clear private checkpoints only after all confirmed execution rights have expired. */
  async stopForMigrationInTx(tx: Sql, id: I) {
    const expired = `locked.lease_id IS NULL OR locked.lease_expires_at<=clock_timestamp()
      OR locked.started_at+${HARD_SLICE_MS}*interval '1 millisecond'<=clock_timestamp()`;
    return this.oneAfterLock(
      tx,
      `UPDATE ${this.table} AS t SET execution_state=NULL,execution_ms=${this.elapsed()},
      status=CASE WHEN status IN ('succeeded','failed') THEN status ELSE 'manual' END,
      result_code=CASE WHEN status IN ('succeeded','failed') THEN result_code ELSE 'TASK_MIGRATION_PAUSED' END,
      lease_id=NULL,lease_expires_at=NULL,started_at=NULL,next_dispatch_at=NULL,updated_at=clock_timestamp()
      WHERE ${this.lockedRowWhere(expired)}
      RETURNING ${this.projection()}`,
      [id],
      `${leaseTimes}, lease_id`,
    );
  }
  /** Domain-authorized cancellation (for example account deletion), not ordinary executor recovery. */
  async cancelInTx(tx: Sql, id: I, resultCode: string) {
    return this.oneAfterLock(
      tx,
      `UPDATE ${this.table} AS t SET execution_state=NULL,execution_ms=${this.elapsed()},
      status=CASE WHEN status='succeeded' THEN status ELSE 'failed' END,
      result_code=CASE WHEN status='succeeded' THEN result_code ELSE $2 END,
      finished_at=CASE WHEN status IN ('succeeded','failed') THEN finished_at ELSE clock_timestamp() END,lease_id=NULL,lease_expires_at=NULL,started_at=NULL,next_dispatch_at=NULL,updated_at=clock_timestamp()
      WHERE id=$1 RETURNING ${this.projection()}`,
      [id, resultCode],
    );
  }
}
