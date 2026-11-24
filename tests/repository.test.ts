import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import { SQL } from "drizzle-orm";
import { getTableConfig, pgTable, PgDialect } from "drizzle-orm/pg-core";
import { taskColumns, taskConstraints } from "../src/schema.js";
import {
  TaskRepository,
  TASK_FAILURE_BUDGET_BASELINE,
  TASK_RECOVERED,
} from "../src/repository.js";
import { taskMetrics } from "../src/policy.js";
import type { TaskRecord } from "../src/types.js";

const url = process.env.LEGATO_TEST_DATABASE_URL;
const schema = `legato_test_${process.pid}`;
const repo = new TaskRepository<string>(schema);
const pool = url ? new pg.Pool({ connectionString: url }) : null;
const table = pgTable(
  "queue_tasks",
  taskColumns({ id: "uuid" }),
  taskConstraints,
);
const config = getTableConfig(table);
const dialect = new PgDialect();
const query = (text: string, values?: unknown[]) => pool!.query(text, values);
const literal = (v: unknown) =>
  typeof v === "number"
    ? String(v)
    : `'${(typeof v === "string" ? v : JSON.stringify(v)).replaceAll("'", "''")}'`;
const sqlText = (value: SQL) => dialect.sqlToQuery(value).sql;
const column = (c: (typeof config.columns)[number]) => {
  const def =
    c.default === undefined
      ? ""
      : ` DEFAULT ${c.default instanceof SQL ? sqlText(c.default) : literal(c.default)}`;
  const identity = c.generatedIdentity
    ? ` GENERATED ALWAYS AS IDENTITY (MINVALUE ${c.generatedIdentity.sequenceOptions?.minValue} MAXVALUE ${c.generatedIdentity.sequenceOptions?.maxValue})`
    : "";
  return `"${c.name}" ${c.getSQLType()}${identity}${def}${c.notNull ? " NOT NULL" : ""}${c.primary ? " PRIMARY KEY" : ""}${c.isUnique ? ` CONSTRAINT "${c.uniqueName}" UNIQUE` : ""}`;
};
const tx = async <T>(
  f: (client: pg.PoolClient) => Promise<T>,
  commit = true,
) => {
  const client = await pool!.connect();
  try {
    await client.query("BEGIN");
    const result = await f(client);
    await client.query(commit ? "COMMIT" : "ROLLBACK");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};
const create = async (type = "test") =>
  (await repo.createInTx(pool!, {
    requestKey: crypto.randomUUID(),
    businessKey: "business",
    type,
    bizParams: {},
  }))!;
const claim = async (id: string, limit = 4) =>
  (await repo.claimInTx(pool!, id, { limit, leaseMs: 60_000 }))!;
const policy = { limit: 4, delays: [0] };
const failure = { code: "TIMEOUT", retryable: true };
const integration = (name: string, fn: () => Promise<void>) =>
  test(
    name,
    {
      skip:
        !url &&
        "Set LEGATO_TEST_DATABASE_URL to an isolated PostgreSQL database",
    },
    fn,
  );

before(async () => {
  if (!pool) return;
  await query(`CREATE SCHEMA "${schema}"`);
  const checks = config.checks.map(
    (c) => `CONSTRAINT "${c.name}" CHECK (${sqlText(c.value)})`,
  );
  await query(
    `CREATE TABLE "${schema}"."queue_tasks" (${[...config.columns.map(column), ...checks].join(", ")})`,
  );
  await query(
    `CREATE TABLE "${schema}".business_effects (id uuid PRIMARY KEY)`,
  );
  await query(`CREATE SCHEMA "${schema}_bigint"`);
  const bigintConfig = getTableConfig(
    pgTable("queue_tasks", taskColumns({ id: "bigint" }), taskConstraints),
  );
  const bigintChecks = bigintConfig.checks.map(
    (c) => `CONSTRAINT "${c.name}" CHECK (${sqlText(c.value)})`,
  );
  await query(
    `CREATE TABLE "${schema}_bigint".queue_tasks (${[...bigintConfig.columns.map(column), ...bigintChecks].join(", ")})`,
  );
});
after(async () => {
  if (pool) {
    await query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    await query(`DROP SCHEMA IF EXISTS "${schema}_bigint" CASCADE`);
    await pool.end();
  }
});

integration(
  "pending and queued notifications tolerate duplicate concurrent claims",
  async () => {
    const pending = await create();
    const claims = await Promise.all(
      Array.from({ length: 8 }, () => repo.claimInTx(pool!, pending.id)),
    );
    assert.equal(claims.filter(Boolean).length, 1);
    const queued = await create();
    const reservation = await repo.reserveDispatchInTx(pool!, queued.id);
    assert.equal(reservation?.status, "queued");
    assert.equal(await repo.reserveDispatchInTx(pool!, queued.id), null);
    assert.ok(
      await repo.acknowledgeDispatchInTx(
        pool!,
        queued.id,
        reservation!.lastDispatchAttemptAt!,
      ),
    );
    assert.equal((await claim(queued.id)).runAttempts, 1);
    assert.equal(await repo.claimInTx(pool!, queued.id), null);
  },
);

integration(
  "checkpoint CAS, transaction rollback, and more than four yields",
  async () => {
    const task = await create();
    const first = await claim(task.id);
    assert.ok(first.firstStartedAt);
    assert.ok(
      await repo.saveCheckpointInTx(pool!, task.id, first.leaseId!, null, {
        step: 1,
      }),
    );
    assert.equal(
      await repo.saveCheckpointInTx(pool!, task.id, first.leaseId!, null, {
        step: 99,
      }),
      null,
    );
    await tx(async (client) => {
      await client.query(
        `INSERT INTO "${schema}".business_effects VALUES ($1)`,
        [task.id],
      );
      assert.ok(
        await repo.yieldInTx(client, task.id, first.leaseId!, { step: 2 }),
      );
    }, false);
    assert.deepEqual((await repo.readInTx(pool!, task.id))?.executionState, {
      step: 1,
    });
    assert.equal(
      (
        await query(`SELECT * FROM "${schema}".business_effects WHERE id=$1`, [
          task.id,
        ])
      ).rowCount,
      0,
    );
    await tx(async (client) => {
      assert.ok(
        await repo.saveCheckpointInTx(
          client,
          task.id,
          first.leaseId!,
          { step: 1 },
          { step: 2 },
        ),
      );
      assert.ok(
        await repo.failInTx(client, task.id, first.leaseId!, failure, policy, {
          result: { partial: true },
        }),
      );
    }, false);
    const afterFailedTransaction = (await repo.readInTx(pool!, task.id))!;
    assert.equal(afterFailedTransaction.status, "running");
    assert.deepEqual(afterFailedTransaction.executionState, { step: 1 });
    assert.equal(afterFailedTransaction.result, null);
    assert.equal(afterFailedTransaction.failureCount, 0);
    assert.ok(
      await repo.yieldInTx(pool!, task.id, first.leaseId!, { step: 2 }),
    );
    for (let i = 0; i < 5; i++) {
      const next = await claim(task.id);
      assert.equal(next.failureCount, 0);
      assert.ok(
        await repo.yieldInTx(pool!, task.id, next.leaseId!, { step: i + 3 }),
      );
    }
    const next = await claim(task.id);
    assert.equal(next.runAttempts, 7);
    assert.equal(next.failureCount, 0);
    assert.equal(
      next.firstStartedAt?.getTime(),
      first.firstStartedAt?.getTime(),
    );
    await tx(async (client) => {
      await client.query(
        `INSERT INTO "${schema}".business_effects VALUES ($1)`,
        [task.id],
      );
      assert.ok(
        await repo.completeInTx(client, task.id, next.leaseId!, {
          resultCode: "OK",
        }),
      );
    }, false);
    assert.equal((await repo.readInTx(pool!, task.id))?.status, "running");
    assert.equal(
      (
        await query(`SELECT * FROM "${schema}".business_effects WHERE id=$1`, [
          task.id,
        ])
      ).rowCount,
      0,
    );
    await tx(async (client) => {
      await client.query(
        `INSERT INTO "${schema}".business_effects VALUES ($1)`,
        [task.id],
      );
      assert.ok(
        await repo.completeInTx(client, task.id, next.leaseId!, {
          resultCode: "OK",
        }),
      );
    });
    const done = (await repo.readInTx(pool!, task.id))!;
    assert.equal(done.status, "succeeded");
    assert.ok(done.executionMs! >= 0);
    assert.ok(done.firstStartedAt && done.finishedAt);
    assert.equal(taskMetrics(done).slice_count, 7);
    assert.ok(
      taskMetrics(done).task_elapsed_ms! >= taskMetrics(done).initial_queue_ms!,
    );
  },
);

const writesAfterRenewal = [
  ["renew", (client: pg.PoolClient, id: string, lease: string) =>
    repo.renewInTx(client, id, lease, 60_000), "running"],
  ["checkpoint", (client: pg.PoolClient, id: string, lease: string) =>
    repo.saveCheckpointInTx(client, id, lease, { step: 1 }, { step: 2 }), "running"],
  ["yield", (client: pg.PoolClient, id: string, lease: string) =>
    repo.yieldInTx(client, id, lease, undefined), "pending"],
  ["success", (client: pg.PoolClient, id: string, lease: string) =>
    repo.completeInTx(client, id, lease, { resultCode: "DONE" }), "succeeded"],
  ["failure", (client: pg.PoolClient, id: string, lease: string) =>
    repo.failInTx(client, id, lease, failure, policy), "retry_wait"],
  ["failure with checkpoint", (client: pg.PoolClient, id: string, lease: string) =>
    repo.failInTx(client, id, lease, failure, policy, {
      checkpoint: { previous: { step: 1 }, next: { step: 2 } },
    }), "retry_wait"],
  ["migration pause", (client: pg.PoolClient, id: string) =>
    repo.stopForMigrationInTx(client, id), undefined],
] as const;

for (const [operation, write, status] of writesAfterRenewal) {
  integration(`${operation} waiting for renewal uses the current lease and checkpoint`, async () => {
    const task = await create();
    const running = await claim(task.id);
    const writer = await pool!.connect();
    let pending: Promise<TaskRecord<string> | Date | null> | undefined;
    try {
      const pid = (await writer.query("SELECT pg_backend_pid() AS pid")).rows[0].pid;
      const { rows: [original] } = await query(
        `UPDATE "${schema}".queue_tasks SET lease_expires_at=clock_timestamp()+interval '1 second'
          WHERE id=$1 RETURNING lease_expires_at`,
        [task.id],
      );
      await tx(async (owner) => {
        assert.ok(await repo.renewInTx(owner, task.id, running.leaseId!, 60_000));
        assert.ok(await repo.saveCheckpointInTx(owner, task.id, running.leaseId!, null, { step: 1 }));
        pending = write(writer, task.id, running.leaseId!);
        let blocked = false;
        for (let i = 0; i < 100 && !blocked; i++) {
          const check = await query(
            "SELECT cardinality(pg_blocking_pids($1))>0 AS blocked FROM pg_sleep(0.005)", [pid],
          );
          blocked = check.rows[0].blocked;
        }
        assert.equal(blocked, true);
        await owner.query(
          "SELECT pg_sleep(greatest(0,extract(epoch FROM ($1::timestamptz-clock_timestamp())))+0.02)",
          [original.lease_expires_at],
        );
      });
      const result = await pending;
      const stored = await repo.readInTx(pool!, task.id);
      if (operation === "renew") {
        assert.ok(result instanceof Date);
        assert.equal(result.getTime(), stored?.leaseExpiresAt?.getTime());
      } else {
        assert.ok(!(result instanceof Date));
        assert.equal(result?.status, status);
      }
      assert.equal(stored?.status, status ?? "running");
      const writesCheckpoint = operation === "checkpoint" || operation === "failure with checkpoint";
      assert.deepEqual(stored?.executionState, { step: writesCheckpoint ? 2 : 1 });
      if (operation.startsWith("failure")) assert.equal(stored?.failureCount, 1);
    } finally {
      if (pending) await Promise.allSettled([pending]);
      writer.release();
    }
  });
}

integration("known bad lease, task type, and dispatch state skip a busy row", async () => {
  const running = await claim((await create("compression")).id);
  const owner = running.leaseId!;
  const stale = crypto.randomUUID();
  await tx(async (blocker) => {
    await blocker.query(
      `SELECT id FROM "${schema}".queue_tasks WHERE id=$1 FOR NO KEY UPDATE`,
      [running.id],
    );
    await tx(async (caller) => {
      await caller.query("SET LOCAL lock_timeout='200ms'");
      assert.equal(await repo.renewInTx(caller, running.id, stale), null);
      assert.equal(await repo.saveCheckpointInTx(caller, running.id, stale, null, {}), null);
      assert.equal(await repo.yieldInTx(caller, running.id, stale, null), null);
      assert.equal(await repo.completeInTx(caller, running.id, stale, { resultCode: "DONE" }), null);
      assert.equal(await repo.failInTx(caller, running.id, stale, failure, policy), null);
      assert.equal(await repo.failInTx(caller, running.id, owner, failure, policy, {
        expectedType: "other",
      }), null);
      assert.equal(await repo.revokeInTx(caller, running.id, failure, policy, stale), null);
      assert.equal(await repo.reserveDispatchInTx(caller, running.id), null);
    });
  });
});

integration("failure requires the expected task type when supplied", async () => {
  const task = await create("compression");
  const running = await claim(task.id);
  assert.equal(await repo.failInTx(pool!, task.id, running.leaseId!, failure, policy, {
    expectedType: "other",
  }), null);
  assert.equal((await repo.readInTx(pool!, task.id))?.status, "running");
  const failed = await repo.failInTx(pool!, task.id, running.leaseId!, failure, policy, {
    expectedType: "compression",
  });
  assert.equal(failed?.status, "retry_wait");
  assert.equal(failed?.failureCount, 1);
});

integration("failure saves a matching checkpoint and retry state in one statement", async () => {
  const running = await claim((await create()).id);
  const owner = running.leaseId!;
  assert.equal(await repo.failInTx(pool!, running.id, owner, failure, policy, {
    checkpoint: { previous: { cursor: 8 }, next: { cursor: 9 } },
  }), null);
  const current = await repo.readInTx(pool!, running.id);
  assert.equal(current?.status, "running");
  assert.equal(current?.executionState, null);
  assert.equal(current?.failureCount, 0);

  let statements = 0;
  const failed = await repo.failInTx({
    query(statement, values) {
      statements++;
      return pool!.query(statement, values);
    },
  }, running.id, owner, failure, policy, {
    checkpoint: { previous: null, next: { cursor: 9 } },
  });
  assert.equal(statements, 1);
  assert.equal(failed?.status, "retry_wait");
  assert.equal(failed?.failureCount, 1);
  assert.deepEqual(failed?.executionState, { cursor: 9 });
});

integration("claim reads the actual schedule after a competing write", async () => {
  for (const dueAfterWait of [true, false]) {
    const task = await create();
    const schedule = `UPDATE "${schema}".queue_tasks
      SET next_run_at=clock_timestamp() + CASE WHEN $2 THEN INTERVAL '-1 minute'
        ELSE INTERVAL '1 hour' END WHERE id=$1`;
    await query(schedule, [task.id, !dueAfterWait]);
    const writer = await pool!.connect();
    let pending: ReturnType<typeof repo.claimInTx> | undefined;
    try {
      const pid = (await writer.query("SELECT pg_backend_pid() AS pid")).rows[0].pid;
      await tx(async (editor) => {
        await editor.query(schedule, [task.id, dueAfterWait]);
        pending = repo.claimInTx(writer, task.id);
        let blocked = false;
        for (let i = 0; i < 100 && !blocked; i++) {
          const check = await query(
            "SELECT cardinality(pg_blocking_pids($1))>0 AS blocked FROM pg_sleep(0.005)", [pid],
          );
          blocked = check.rows[0].blocked;
        }
        assert.equal(blocked, true);
      });
      assert.equal((await pending)?.status, dueAfterWait ? "running" : undefined);
      assert.equal((await repo.readInTx(pool!, task.id))?.runAttempts, dueAfterWait ? 1 : 0);
    } finally {
      if (pending) await Promise.allSettled([pending]);
      writer.release();
    }
  }
});

integration(
  "expiry, delayed reaping, takeover, and stale ordinary writes are fenced",
  async () => {
    const task = await create();
    const old = await claim(task.id);
    await query(
      `UPDATE "${schema}".queue_tasks SET started_at=clock_timestamp()-interval '20 minutes', lease_expires_at=clock_timestamp()-interval '6 minutes' WHERE id=$1`,
      [task.id],
    );
    assert.equal(
      await repo.saveCheckpointInTx(pool!, task.id, old.leaseId!, null, {
        stale: true,
      }),
      null,
    );
    assert.equal(
      await repo.completeInTx(pool!, task.id, old.leaseId!, {
        resultCode: "STALE",
      }),
      null,
    );
    assert.equal(
      await repo.yieldInTx(pool!, task.id, old.leaseId!, { stale: true }),
      null,
    );
    const reaped = (await repo.revokeInTx(
      pool!,
      task.id,
      failure,
      policy,
      old.leaseId!,
    ))!;
    assert.equal(reaped.status, "retry_wait");
    assert.equal(reaped.failureCount, 1);
    assert.ok(
      reaped.executionMs! >= 839_000 && reaped.executionMs! <= 840_000,
      `elapsed ${reaped.executionMs}`,
    );
    assert.equal(
      await repo.revokeInTx(pool!, task.id, failure, policy, old.leaseId!),
      null,
    );
    const current = await claim(task.id);
    assert.equal(current.runAttempts, 2);
    assert.equal(
      await repo.saveCheckpointInTx(pool!, task.id, old.leaseId!, null, {
        stale: true,
      }),
      null,
    );
    assert.equal(
      await repo.completeInTx(pool!, task.id, old.leaseId!, {
        resultCode: "STALE",
      }),
      null,
    );
    assert.equal(
      await repo.yieldInTx(pool!, task.id, old.leaseId!, null),
      null,
    );
    assert.ok(
      await repo.saveCheckpointInTx(pool!, task.id, current.leaseId!, null, {
        progress: 2,
      }),
    );
    await assert.rejects(
      () =>
        repo.acceptLateSuccessInTx(pool!, task.id, {
          resultCode: "OK",
          executionState: { stale: true },
        } as never),
      /checkpoint/,
    );
    const done = (await repo.acceptLateSuccessInTx(pool!, task.id, {
      resultCode: "RECEIPT",
      result: { receipt: "verified" },
    }))!;
    assert.deepEqual(done.executionState, { progress: 2 });
    assert.equal(done.failureCount, 1);
    assert.equal(done.runAttempts, 2);
    assert.equal(
      await repo.completeInTx(pool!, task.id, current.leaseId!, {
        resultCode: "STALE",
      }),
      null,
    );
  },
);

integration(
  "failure budget is reset explicitly without erasing history and legacy duration stays unknown",
  async () => {
    const task = await create();
    await query(
      `UPDATE "${schema}".queue_tasks SET execution_ms=NULL WHERE id=$1`,
      [task.id],
    );
    for (let i = 0; i < 4; i++) {
      const running = await claim(task.id);
      const failed = (await repo.failInTx(
        pool!,
        task.id,
        running.leaseId!,
        failure,
        policy,
      ))!;
      assert.equal(failed.failureCount, i + 1);
      assert.equal(failed.executionMs, null);
    }
    const before = await query(
      `SELECT fail_reasons FROM "${schema}".queue_tasks WHERE id=$1`,
      [task.id],
    );
    assert.equal(before.rows[0].fail_reasons.length, 4);
    const recovered = (await repo.recoverInTx(pool!, task.id))!;
    assert.equal(recovered.failureCount, 0);
    assert.equal(recovered.runAttempts, 4);
    assert.equal(recovered.executionMs, null);
    const after = await query(
      `SELECT fail_reasons FROM "${schema}".queue_tasks WHERE id=$1`,
      [task.id],
    );
    assert.deepEqual(
      after.rows[0].fail_reasons.slice(0, 4),
      before.rows[0].fail_reasons,
    );
    const running = await claim(task.id);
    assert.equal(running.runAttempts, 5);
    const done = (await repo.completeInTx(pool!, task.id, running.leaseId!, {
      resultCode: "OK",
    }))!;
    assert.equal(done.executionMs, null);
  },
);

integration(
  "an uncooperative slice cannot write after hard-timeout revocation",
  async () => {
    const { runSlice } = await import("../src/runner.js");
    const { HARD_SLICE_MS, TaskTimeout } = await import("../src/policy.js");
    const task = await create();
    const running = await claim(task.id);
    let release!: () => void;
    const wait = new Promise<void>((resolve) => {
      release = resolve;
    });
    let lateWrite: Promise<unknown> | undefined;
    const run = runSlice({
      startedAt: new Date(Date.now() - HARD_SLICE_MS + 50),
      leaseExpiresAt: new Date(Date.now() + 60_000),
      renew: async () => null,
      execute: async () => {
        await wait;
        return (lateWrite = repo.yieldInTx(pool!, task.id, running.leaseId!, {
          stale: true,
        }));
      },
      onTimeout: async () => {
        await query(
          `UPDATE "${schema}".queue_tasks SET started_at=clock_timestamp()-interval '15 minutes' WHERE id=$1`,
          [task.id],
        );
        assert.ok(
          await repo.revokeInTx(
            pool!,
            task.id,
            failure,
            policy,
            running.leaseId!,
          ),
        );
      },
    });
    await assert.rejects(run, TaskTimeout);
    release();
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(await lateWrite, null);
    assert.equal((await repo.readInTx(pool!, task.id))?.failureCount, 1);
  },
);

integration(
  "lease expiry caps elapsed time even before the hard deadline",
  async () => {
    const task = await create();
    const running = await claim(task.id);
    await query(
      `UPDATE "${schema}".queue_tasks SET started_at=clock_timestamp()-interval '10 minutes', lease_expires_at=clock_timestamp()-interval '8 minutes' WHERE id=$1`,
      [task.id],
    );
    const reaped = (await repo.revokeInTx(
      pool!,
      task.id,
      failure,
      policy,
      running.leaseId!,
    ))!;
    assert.ok(
      reaped.executionMs! >= 119_000 && reaped.executionMs! <= 120_000,
      `elapsed ${reaped.executionMs}`,
    );
  },
);

integration(
  "a pure row lock wait cannot authorize a write using the earlier clock",
  async () => {
    const task = await create();
    const running = (await repo.claimInTx(pool!, task.id, { leaseMs: 250 }))!;
    const blocker = await pool!.connect();
    const writer = await pool!.connect();
    try {
      await blocker.query("BEGIN");
      await blocker.query(
        `SELECT id FROM "${schema}".queue_tasks WHERE id=$1 FOR UPDATE`,
        [task.id],
      );
      const pid = (await writer.query("SELECT pg_backend_pid() AS id")).rows[0]
        .id;
      const pending = repo.completeInTx(writer, task.id, running.leaseId!, {
        resultCode: "STALE",
      });
      let blocked = false;
      for (let i = 0; i < 100; i++) {
        const r = await query(
          "SELECT cardinality(pg_blocking_pids($1))>0 AS blocked",
          [pid],
        );
        if (r.rows[0].blocked) {
          blocked = true;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      assert.equal(blocked, true);
      await new Promise((resolve) =>
        setTimeout(
          resolve,
          Math.max(0, running.leaseExpiresAt!.getTime() - Date.now() + 20),
        ),
      );
      await blocker.query("COMMIT");
      assert.equal(await pending, null);
      const current = (await repo.readInTx(pool!, task.id))!;
      assert.equal(current.status, "running");
      assert.equal(current.finishedAt, null);
    } finally {
      await blocker.query("ROLLBACK");
      blocker.release();
      writer.release();
    }
  },
);

integration(
  "waiting uses post-lock database time and retains the stored checkpoint",
  async () => {
    const task = await create();
    const running = await claim(task.id);
    const owner = running.leaseId!;
    assert.ok(
      await repo.saveCheckpointInTx(pool!, task.id, owner, null, { cursor: 7 }),
    );
    assert.equal(
      await repo.yieldInTx(pool!, task.id, owner, undefined, undefined, {
        type: "another-task",
      }),
      null,
    );
    const blocker = await pool!.connect();
    const writer = await pool!.connect();
    let pending: ReturnType<typeof repo.yieldInTx> | undefined;
    try {
      await blocker.query("BEGIN");
      await blocker.query(
        `SELECT id FROM "${schema}".queue_tasks WHERE id=$1 FOR UPDATE`,
        [task.id],
      );
      const pid = (await writer.query("SELECT pg_backend_pid() AS id")).rows[0]
        .id;
      const minimumDelayMs = 500;
      pending = repo.yieldInTx(writer, task.id, owner, undefined, new Date(0), {
        resultCode: "CHILD_PENDING",
        minimumDelayMs,
        type: "test",
      });
      let blocked = false;
      for (let i = 0; i < 100 && !blocked; i++) {
        const check = await query(
          "SELECT cardinality(pg_blocking_pids($1))>0 AS blocked FROM pg_sleep(0.005)",
          [pid],
        );
        blocked = check.rows[0].blocked;
      }
      assert.equal(blocked, true);
      const releasedAt: Date = (
        await blocker.query("SELECT clock_timestamp() AS ts FROM pg_sleep(0.1)")
      ).rows[0].ts;
      await blocker.query("COMMIT");
      const result = await pending;
      assert.ok(result);
      assert.ok(+result.nextRunAt >= +releasedAt + minimumDelayMs);
      assert.equal(result.status, "pending");
      assert.equal(result.resultCode, "CHILD_PENDING");
      assert.equal(result.failureCount, running.failureCount);
      assert.equal(result.runAttempts, running.runAttempts);
      assert.equal(result.leaseId, null);
      assert.deepEqual(result.executionState, { cursor: 7 });
      assert.equal(await repo.yieldInTx(pool!, task.id, owner, null), null);
    } finally {
      await blocker.query("ROLLBACK");
      if (pending) await Promise.allSettled([pending]);
      blocker.release();
      writer.release();
    }
  },
);

integration(
  "waiting honors a later absolute time and retains existing yield behavior",
  async () => {
    const former = await claim((await create()).id);
    const later: Date = (
      await query("SELECT date_trunc('milliseconds',clock_timestamp()) + interval '5 minutes' AS ts")
    ).rows[0].ts;
    const delayed = await repo.yieldInTx(
      pool!, former.id, former.leaseId!, { cursor: 3 }, later,
      { minimumDelayMs: 600, type: "test" },
    );
    assert.equal(delayed?.nextRunAt.getTime(), later.getTime());
    assert.deepEqual(delayed?.executionState, { cursor: 3 });

    const ordinary = await claim((await create()).id);
    await repo.saveCheckpointInTx(pool!, ordinary.id, ordinary.leaseId!, null, { cursor: 4 });
    const immediate = await repo.yieldInTx(pool!, ordinary.id, ordinary.leaseId!, null, new Date(0));
    assert.equal(immediate?.nextRunAt.getTime(), 0);
    assert.equal(immediate?.executionState, null);
    assert.equal(immediate?.resultCode, "TASK_YIELDED");
  },
);

integration(
  "SQL and legacy JSON null checkpoints share the same empty-state contract",
  async () => {
    const task = await create();
    const running = await claim(task.id);
    assert.equal(
      (
        await query(
          `SELECT execution_state IS NULL AS empty FROM "${schema}".queue_tasks WHERE id=$1`,
          [task.id],
        )
      ).rows[0].empty,
      true,
    );
    assert.ok(
      await repo.saveCheckpointInTx(pool!, task.id, running.leaseId!, null, {
        step: 1,
      }),
    );
    await query(
      `UPDATE "${schema}".queue_tasks SET execution_state='null'::jsonb WHERE id=$1`,
      [task.id],
    );
    assert.ok(
      await repo.saveCheckpointInTx(pool!, task.id, running.leaseId!, null, {
        step: 2,
      }),
    );
    assert.ok(await repo.yieldInTx(pool!, task.id, running.leaseId!, null));
    assert.equal(
      (
        await query(
          `SELECT execution_state IS NULL AS empty FROM "${schema}".queue_tasks WHERE id=$1`,
          [task.id],
        )
      ).rows[0].empty,
      true,
    );
  },
);

integration(
  "inline tasks cannot yield into an orphaned pending queue state",
  async () => {
    const task = (await repo.createInTx(pool!, {
      requestKey: crypto.randomUUID(),
      businessKey: "inline",
      type: "test",
      bizParams: {},
      executionMode: "inline",
    }))!;
    const running = (await repo.claimInTx(pool!, task.id, {
      executionMode: "inline",
    }))!;
    assert.equal(
      await repo.yieldInTx(pool!, task.id, running.leaseId!, { step: 1 }),
      null,
    );
    assert.equal((await repo.readInTx(pool!, task.id))?.status, "running");
    assert.ok(
      await repo.completeInTx(pool!, task.id, running.leaseId!, {
        resultCode: "OK",
      }),
    );
  },
);

integration(
  "cancellation clears private progress, revokes the old owner, and settles only once",
  async () => {
    const task = await create();
    const running = await claim(task.id);
    assert.ok(
      await repo.saveCheckpointInTx(pool!, task.id, running.leaseId!, null, {
        private: "synthetic-memory",
      }),
    );
    const stopped = (await repo.cancelInTx(pool!, task.id, "ACCOUNT_DELETED"))!;
    assert.equal(stopped.executionState, null);
    assert.equal(stopped.status, "failed");
    assert.equal(stopped.failureCount, 0);
    await assert.rejects(
      () => repo.assertOwnedInTx(pool!, task.id, running.leaseId!, true),
      /no longer owns/,
    );
    assert.equal(
      await repo.completeInTx(pool!, task.id, running.leaseId!, {
        resultCode: "LATE",
      }),
      null,
    );
    const repeated = (await repo.cancelInTx(
      pool!,
      task.id,
      "ACCOUNT_DELETED",
    ))!;
    assert.equal(repeated.executionMs, stopped.executionMs);
    assert.equal(repeated.finishedAt?.getTime(), stopped.finishedAt?.getTime());
  },
);

integration(
  "legacy terminal timestamps remain unknown while empty new batches finish without a fake slice",
  async () => {
    const legacy = await create();
    await query(
      `UPDATE "${schema}".queue_tasks SET status='failed',execution_ms=NULL,finished_at=NULL WHERE id=$1`,
      [legacy.id],
    );
    const changed = (await repo.decideInTx(
      pool!,
      legacy.id,
      { status: "failed", runAttempts: 0 },
      { status: "failed", resultCode: "REVIEWED" },
    ))!;
    assert.equal(changed.finishedAt, null);
    assert.equal(
      (await repo.cancelInTx(pool!, legacy.id, "CLEANED"))?.finishedAt,
      null,
    );
    assert.equal(taskMetrics(changed).task_elapsed_ms, null);
    const empty = await create();
    const done = (await repo.completeUnstartedInTx(pool!, empty.id, {
      resultCode: "EMPTY",
      result: { items: [] },
    }))!;
    assert.equal(done.runAttempts, 0);
    assert.equal(done.firstStartedAt, null);
    assert.equal(done.executionMs, 0);
    assert.ok(done.finishedAt);
    assert.equal(
      await repo.completeUnstartedInTx(pool!, empty.id, {
        resultCode: "EMPTY",
      }),
      null,
    );
  },
);

integration(
  "unique request keys prevent concurrent duplicate tasks and UUID retry identity persists",
  async () => {
    const requestKey = crypto.randomUUID();
    const retryOfTaskId = crypto.randomUUID();
    const input = {
      requestKey,
      businessKey: "business",
      type: "test",
      bizParams: {},
      retryOfTaskId,
    };
    const results = await Promise.all(
      Array.from({ length: 6 }, () => repo.createInTx(pool!, input)),
    );
    assert.equal(results.filter(Boolean).length, 1);
    assert.equal(results.find(Boolean)?.retryOfTaskId, retryOfTaskId);
    assert.equal(
      (
        await query(
          `SELECT * FROM "${schema}".queue_tasks WHERE request_key=$1`,
          [requestKey],
        )
      ).rowCount,
      1,
    );
  },
);

integration(
  "old dispatch acknowledgment cannot acknowledge a newer reservation",
  async () => {
    const task = await create();
    const old = (await repo.reserveDispatchInTx(pool!, task.id))!;
    await query(
      `UPDATE "${schema}".queue_tasks SET next_dispatch_at=clock_timestamp()-interval '1 minute' WHERE id=$1`,
      [task.id],
    );
    await query("SELECT pg_sleep(0.005)");
    const oldToken = old.lastDispatchAttemptAt!;
    const next = (await repo.reserveDispatchInTx(pool!, task.id))!;
    assert.equal(next.dispatchFailures, 2);
    assert.equal(
      await repo.acknowledgeDispatchInTx(pool!, task.id, oldToken),
      null,
    );
    assert.equal((await repo.readInTx(pool!, task.id))?.dispatchFailures, 2);
    assert.equal(
      (
        await repo.acknowledgeDispatchInTx(
          pool!,
          task.id,
          next.lastDispatchAttemptAt!,
        )
      )?.dispatchFailures,
      0,
    );
  },
);

integration(
  "administrative decisions cannot cross a running lease and use status and slice CAS",
  async () => {
    const task = await create();
    const running = await claim(task.id);
    const decision = { status: "failed" as const, resultCode: "ADMIN" };
    assert.equal(
      await repo.decideInTx(
        pool!,
        task.id,
        { status: "running", runAttempts: 1 },
        decision,
      ),
      null,
    );
    assert.equal(
      await repo.decideInTx(
        pool!,
        task.id,
        { status: "pending", runAttempts: 0 },
        decision,
      ),
      null,
    );
    assert.ok(
      await repo.yieldInTx(pool!, task.id, running.leaseId!, { safe: true }),
    );
    assert.equal(
      await repo.decideInTx(
        pool!,
        task.id,
        { status: "pending", runAttempts: 0 },
        decision,
      ),
      null,
    );
    const decided = (await repo.decideInTx(
      pool!,
      task.id,
      { status: "pending", runAttempts: 1 },
      decision,
    ))!;
    assert.equal(decided.status, "failed");
    assert.ok(decided.finishedAt);
    assert.deepEqual(decided.executionState, { safe: true });
  },
);

integration(
  "migration stop rejects active owners, clears expired checkpoint and does not revive execution",
  async () => {
    const task = await create();
    const running = await claim(task.id);
    assert.ok(
      await repo.saveCheckpointInTx(pool!, task.id, running.leaseId!, null, {
        private: "checkpoint",
      }),
    );
    assert.equal(await repo.stopForMigrationInTx(pool!, task.id), null);
    await query(
      `UPDATE "${schema}".queue_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`,
      [task.id],
    );
    const stopped = (await repo.stopForMigrationInTx(pool!, task.id))!;
    assert.equal(stopped.status, "manual");
    assert.equal(stopped.executionState, null);
    assert.equal(stopped.leaseId, null);
    assert.equal(await repo.claimInTx(pool!, task.id), null);
    assert.equal(
      await repo.saveCheckpointInTx(pool!, task.id, running.leaseId!, null, {
        resurrected: true,
      }),
      null,
    );
    const again = (await repo.stopForMigrationInTx(pool!, task.id))!;
    assert.equal(again.executionMs, stopped.executionMs);
    assert.equal(again.executionState, null);
  },
);

integration(
  "baseline and recovery markers set failure budget while yields do not append failures",
  async () => {
    const task = await create();
    await query(
      `UPDATE "${schema}".queue_tasks SET run_attempts=5, fail_reasons=$2::jsonb WHERE id=$1`,
      [
        task.id,
        JSON.stringify([
          { runAttempt: 1, phase: "execution", resultCode: "OLD" },
          {
            runAttempt: 0,
            phase: "scheduling",
            resultCode: TASK_FAILURE_BUDGET_BASELINE,
            failureCount: 2,
          },
          { runAttempt: 4, phase: "execution", resultCode: "FAIL" },
          { runAttempt: 4, phase: "scheduling", resultCode: "DUPLICATE" },
        ]),
      ],
    );
    assert.equal((await repo.readInTx(pool!, task.id))?.failureCount, 3);
    const running = await claim(task.id);
    const yielded = (await repo.yieldInTx(
      pool!,
      task.id,
      running.leaseId!,
      null,
    ))!;
    assert.equal(yielded.failureCount, 3);
    assert.equal(yielded.lastFailure?.resultCode, "DUPLICATE");
    const next = await claim(task.id);
    const failed = (await repo.failInTx(
      pool!,
      task.id,
      next.leaseId!,
      { code: "FINAL", retryable: false },
      policy,
    ))!;
    assert.equal(failed.failureCount, 4);
    assert.equal(failed.lastFailure?.retryable, false);
    const recovered = (await repo.recoverInTx(pool!, task.id))!;
    assert.equal(recovered.failureCount, 0);
    assert.equal(recovered.lastFailure?.resultCode, TASK_RECOVERED);
    assert.equal(
      (
        await query(
          `SELECT jsonb_array_length(fail_reasons) AS n FROM "${schema}".queue_tasks WHERE id=$1`,
          [task.id],
        )
      ).rows[0].n,
      6,
    );
  },
);

integration(
  "bigint schema factory identity stays safe and retry identity round-trips",
  async () => {
    const bigintRepo = new TaskRepository<number>(`${schema}_bigint`, "bigint");
    const input = {
      requestKey: crypto.randomUUID(),
      businessKey: "business",
      type: "test",
      bizParams: {},
    };
    const first = (await bigintRepo.createInTx(pool!, input))!;
    assert.equal(first.id, 1);
    const second = (await bigintRepo.createInTx(pool!, {
      ...input,
      requestKey: crypto.randomUUID(),
      retryOfTaskId: first.id,
    }))!;
    assert.equal(second.retryOfTaskId, first.id);
    const identity = await query(
      `SELECT seqmax FROM pg_sequence WHERE seqrelid=pg_get_serial_sequence($1, 'id')::regclass`,
      [`${schema}_bigint.queue_tasks`],
    );
    assert.equal(identity.rows[0].seqmax, String(Number.MAX_SAFE_INTEGER));
  },
);

integration(
  "an expired unowned task cannot dispatch or claim and can be closed administratively",
  async () => {
    const expired = (await repo.createInTx(pool!, {
      requestKey: crypto.randomUUID(),
      businessKey: "business",
      type: "test",
      bizParams: {},
      expiresAt: new Date(Date.now() - 60_000),
    }))!;
    assert.equal(await repo.reserveDispatchInTx(pool!, expired.id), null);
    assert.equal(await repo.claimInTx(pool!, expired.id), null);
    const closed = (await repo.decideInTx(
      pool!,
      expired.id,
      { status: "pending", runAttempts: 0 },
      { status: "failed", resultCode: "EXPIRED" },
    ))!;
    assert.equal(closed.status, "failed");
    assert.ok(closed.finishedAt);
    assert.equal(await repo.claimInTx(pool!, expired.id), null);
  },
);

integration(
  "ID-only queue claims apply individual failure budgets and report unsupported types",
  async () => {
    const finite = await create("finite");
    const unlimited = await create("unlimited");
    const unknown = await create("unknown");
    const allowed = new Map<string, number | null>([["finite", 1], ["unlimited", null]]);
    for (const task of [finite, unlimited, unknown]) {
      await repo.reserveDispatchInTx(pool!, task.id);
      await query(
        `UPDATE "${schema}".queue_tasks SET fail_reasons=$2::jsonb WHERE id=$1`,
        [task.id, JSON.stringify([{ phase: "execution", runAttempt: 1, resultCode: "FAILED" }])],
      );
    }
    assert.deepEqual(await repo.claimQueuedInTx(pool!, finite.id, allowed), {
      task: null, unsupportedType: null,
    });
    assert.deepEqual(await repo.claimQueuedInTx(pool!, unknown.id, allowed), {
      task: null, unsupportedType: "unknown",
    });
    const claims = await Promise.all(Array.from({ length: 4 }, () =>
      repo.claimQueuedInTx(pool!, unlimited.id, allowed)
    ));
    const winners = claims.flatMap(({ task }) => task ? [task] : []);
    assert.equal(winners.length, 1);
    assert.equal(winners[0]?.type, "unlimited");
    assert.equal(winners[0]?.runAttempts, 1);
    assert.equal(winners[0]?.failureCount, 1);
    assert.equal((await repo.readInTx(pool!, finite.id))?.runAttempts, 0);
    assert.equal((await repo.readInTx(pool!, unknown.id))?.status, "queued");
  },
);

integration(
  "only a supported route waits for the task lock and then observes expiry",
  async () => {
    const task = await create();
    await repo.reserveDispatchInTx(pool!, task.id);
    await query(
      `UPDATE "${schema}".queue_tasks SET expires_at=clock_timestamp()+interval '550 milliseconds' WHERE id=$1`,
      [task.id],
    );
    const blocker = await pool!.connect();
    const writer = await pool!.connect();
    let pending: ReturnType<typeof repo.claimQueuedInTx> | undefined;
    try {
      await blocker.query("BEGIN");
      await blocker.query(`SELECT id FROM "${schema}".queue_tasks WHERE id=$1 FOR UPDATE`, [task.id]);
      await writer.query("SET statement_timeout=200");
      assert.deepEqual(await repo.claimQueuedInTx(writer, task.id, new Map()), {
        task: null, unsupportedType: "test",
      });
      await writer.query("RESET statement_timeout");
      const pid = (await writer.query("SELECT pg_backend_pid() AS id")).rows[0].id;
      pending = repo.claimQueuedInTx(writer, task.id, new Map([["test", null]]));
      let blocked = false;
      for (let i = 0; i < 100 && !blocked; i++) {
        const check = await query(
          "SELECT cardinality(pg_blocking_pids($1))>0 AS blocked FROM pg_sleep(0.005)", [pid],
        );
        blocked = check.rows[0].blocked;
      }
      assert.equal(blocked, true);
      await blocker.query(`SELECT pg_sleep(greatest(0,extract(epoch FROM (expires_at-clock_timestamp())))+0.02)
        FROM "${schema}".queue_tasks WHERE id=$1`, [task.id]);
      await blocker.query("COMMIT");
      assert.deepEqual(await pending, { task: null, unsupportedType: null });
      assert.deepEqual(await repo.claimQueuedInTx(writer, task.id, new Map()), {
        task: null, unsupportedType: null,
      });
      await blocker.query("BEGIN");
      await blocker.query(`SELECT id FROM "${schema}".queue_tasks WHERE id=$1 FOR UPDATE`, [task.id]);
      await writer.query("SET statement_timeout=200");
      assert.deepEqual(await repo.claimQueuedInTx(writer, task.id, new Map([["test", null]])), {
        task: null, unsupportedType: null,
      });
      await writer.query("RESET statement_timeout");
      assert.equal((await repo.readInTx(pool!, task.id))?.runAttempts, 0);
    } finally {
      await blocker.query("ROLLBACK");
      if (pending) await Promise.allSettled([pending]);
      blocker.release();
      writer.release();
    }
  },
);
integration(
  "queue claims coexist with key-share locks on the task",
  async () => {
    const task = await create();
    await repo.reserveDispatchInTx(pool!, task.id);
    const writer = await pool!.connect();
    try {
      await writer.query("SET statement_timeout=300");
      await tx(async (reference) => {
        await reference.query(`SELECT id FROM "${schema}".queue_tasks WHERE id=$1 FOR KEY SHARE`, [task.id]);
        const result = await repo.claimQueuedInTx(writer, task.id, new Map([["test", null]]));
        assert.equal(result.task?.status, "running");
      });
    } finally {
      await writer.query("RESET statement_timeout");
      writer.release();
    }
  },
);

integration("completion and recovery coexist with key-share locks", async () => {
  const task = await create();
  const running = await claim(task.id);
  const failed = await create();
  assert.ok(await repo.decideInTx(pool!, failed.id, { status: "pending", runAttempts: 0 }, {
    status: "failed",
    resultCode: "FAILED",
  }));
  const writer = await pool!.connect();
  try {
    await writer.query("SET statement_timeout=250");
    await tx(async (reader) => {
      await reader.query(
        `SELECT id FROM "${schema}".queue_tasks WHERE id=ANY($1::uuid[]) FOR KEY SHARE`,
        [[task.id, failed.id]],
      );
      assert.equal(
        (await repo.completeInTx(writer, task.id, running.leaseId!, { resultCode: "DONE" }))?.status,
        "succeeded",
      );
      assert.equal((await repo.recoverInTx(writer, failed.id))?.status, "pending");
    });
  } finally {
    await writer.query("RESET statement_timeout");
    writer.release();
  }
});

integration(
  "dispatch reservations use the actual type budget and return the locked previous status",
  async () => {
    const finite = await create("finite");
    const unlimited = await create("unlimited");
    const unknown = await create("unknown");
    const limits = new Map<string, number | null>([["finite", 1], ["unlimited", null]]);
    for (const task of [finite, unlimited, unknown]) {
      await query(
        `UPDATE "${schema}".queue_tasks SET fail_reasons=$2::jsonb WHERE id=$1`,
        [task.id, JSON.stringify([{ phase: "execution", runAttempt: 1, resultCode: "FAILED" }])],
      );
    }
    const reserve = (id: string) => repo.reserveDispatchByTypeInTx(pool!, id, limits, 1);
    assert.equal(await reserve(finite.id), null);
    assert.equal(await reserve(unknown.id), null);
    const competing = await Promise.all([reserve(unlimited.id), reserve(unlimited.id)]);
    const winners = competing.filter((receipt) => receipt !== null);
    assert.equal(winners.length, 1);
    assert.equal(winners[0]?.previousStatus, "pending");
    assert.equal(winners[0]?.task.runAttempts, 0);
    await query(
      `UPDATE "${schema}".queue_tasks SET next_dispatch_at=clock_timestamp() WHERE id=$1`,
      [unlimited.id],
    );
    const retry = await reserve(unlimited.id);
    assert.equal(retry?.previousStatus, "queued");
    assert.equal(retry?.task.dispatchAttempts, 2);
  },
);

integration("reviewed success only updates the expected unleased manual attempt", async () => {
  const task = await create();
  const running = await claim(task.id);
  const lease = running.leaseId!;
  const review = {
    resultCode: "REVIEWED",
    result: { operationId: 42 },
    executionState: { report: "confirmed" },
  };
  assert.equal(await repo.confirmReviewedSuccessInTx(pool!, task.id, running.runAttempts, review), null);
  assert.ok(await repo.saveCheckpointInTx(pool!, task.id, lease, null, { report: "submitting" }));
  const manual = (await repo.failInTx(pool!, task.id, lease, { ...failure, manual: true }, policy))!;
  assert.equal(manual.status, "manual");
  assert.equal(await repo.confirmReviewedSuccessInTx(pool!, task.id, manual.runAttempts + 1, review), null);
  const reviewed = (await repo.confirmReviewedSuccessInTx(pool!, task.id, manual.runAttempts, review))!;
  assert.equal(reviewed.status, "succeeded");
  assert.equal(reviewed.resultCode, review.resultCode);
  assert.deepEqual(reviewed.result, review.result);
  assert.deepEqual(reviewed.executionState, review.executionState);
  assert.deepEqual(reviewed.failReasons, manual.failReasons);
  assert.equal(reviewed.executionMs, manual.executionMs);
  assert.equal(reviewed.nextRunAt.getTime(), manual.nextRunAt.getTime());
  assert.ok(reviewed.finishedAt);
  assert.equal(await repo.confirmReviewedSuccessInTx(pool!, task.id, manual.runAttempts, review), null);
});
