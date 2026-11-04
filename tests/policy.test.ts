import { test } from "node:test";
import assert from "node:assert/strict";
import {
  HARD_SLICE_MS,
  SOFT_SLICE_MS,
  SliceBudget,
  TaskTimeout,
  TaskYield,
  taskMetrics,
} from "../src/policy.js";
import { runSlice } from "../src/runner.js";

test("soft and hard boundaries, request reserve and fallback fit", () => {
  let now = 0;
  const b = new SliceBudget(0, undefined, () => now);
  assert.equal(b.shouldYield(), false);
  assert.equal(b.requestTimeout(900_000, 30_000), SOFT_SLICE_MS - 30_000);
  now = SOFT_SLICE_MS - 30_000;
  assert.throws(() => b.requestTimeout(1000, 30_000), TaskYield);
  assert.equal(b.shouldYield(30_000), true);
  now = SOFT_SLICE_MS;
  assert.equal(b.shouldYield(), true);
  now = HARD_SLICE_MS;
  assert.throws(() => b.assertActive(), TaskTimeout);
});

test("runner invokes timeout even with an uncooperative promise", async () => {
  let revoked = false;
  await assert.rejects(
    runSlice({
      startedAt: new Date(Date.now() - HARD_SLICE_MS + 40),
      leaseExpiresAt: new Date(Date.now() + 10_000),
      renew: async () => null,
      execute: async () => new Promise(() => {}),
      onTimeout: async () => {
        revoked = true;
      },
    }),
    TaskTimeout,
  );
  assert.equal(revoked, true);
});

test("time metrics preserve unknown values and prevent negative durations", () => {
  const t = {
    createdAt: new Date(100),
    firstStartedAt: null,
    finishedAt: null,
    executionMs: null,
    runAttempts: 3,
  };
  assert.deepEqual(taskMetrics(t), {
    slice_count: 3,
    execution_ms: null,
    task_elapsed_ms: null,
    initial_queue_ms: null,
  });
  assert.equal(
    taskMetrics({
      ...t,
      firstStartedAt: new Date(50),
      finishedAt: new Date(75),
    }).task_elapsed_ms,
    0,
  );
  assert.equal(
    taskMetrics({ ...t, firstStartedAt: new Date(50) }).initial_queue_ms,
    0,
  );
});

test("abort rejection waits for timeout settlement before returning", async () => {
  let release!: () => void;
  let started!: () => void;
  const settlementStarted = new Promise<void>((resolve) => {
    started = resolve;
  });
  const settlementGate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let completed = false;
  const execution = runSlice({
    startedAt: new Date(Date.now() - HARD_SLICE_MS + 50),
    leaseExpiresAt: new Date(Date.now() + 60_000),
    renew: async () => null,
    execute: async (budget) =>
      new Promise((_, reject) => {
        budget.signal!.addEventListener(
          "abort",
          () => reject(budget.signal!.reason),
          { once: true },
        );
      }),
    onTimeout: async () => {
      started();
      await settlementGate;
    },
  });
  const observed = execution.then(
    () => {
      completed = true;
    },
    () => {
      completed = true;
    },
  );
  await settlementStarted;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(completed, false);
  release();
  await observed;
  assert.equal(completed, true);
});

test("cooperative hard checks and already-expired entries revoke exactly once", async () => {
  for (const alreadyExpired of [true, false]) {
    let revoked = 0;
    await assert.rejects(
      runSlice({
        startedAt: new Date(
          Date.now() - HARD_SLICE_MS + (alreadyExpired ? -1 : 20),
        ),
        leaseExpiresAt: new Date(Date.now() + 60_000),
        renew: async () => null,
        execute: async (budget) => {
          const end = Date.now() + 30;
          while (Date.now() < end) {} // Deliberately prevent timer callbacks from hiding a synchronous guard failure.
          budget.assertActive();
        },
        onTimeout: async () => {
          revoked++;
        },
      }),
      TaskTimeout,
    );
    assert.equal(revoked, 1);
  }
});

test("a handler returning after a blocked hard timer still triggers revocation", async () => {
  let revoked = 0;
  await assert.rejects(
    runSlice({
      startedAt: new Date(Date.now() - HARD_SLICE_MS + 20),
      leaseExpiresAt: new Date(Date.now() + 60_000),
      renew: async () => null,
      execute: async () => {
        const end = Date.now() + 35;
        while (Date.now() < end) {}
        return "ignored";
      },
      onTimeout: async () => {
        revoked++;
      },
    }),
    TaskTimeout,
  );
  assert.equal(revoked, 1);
});

test("finishing during renewal drains it without leaving an expiry timer", async (t) => {
  for (const outcome of ["success", "error"] as const) {
    await t.test(outcome, async (t) => {
      t.mock.timers.enable({ apis: ["setTimeout", "Date"], now: 1_000_000 });
      let finish!: () => void;
      let renewed!: (value: Date) => void;
      const executionGate = new Promise<void>((resolve) => {
        finish = resolve;
      });
      const renewalGate = new Promise<Date>((resolve) => {
        renewed = resolve;
      });
      let signal!: AbortSignal;
      let renewals = 0;
      let returned = false;
      const running = runSlice({
        startedAt: new Date(),
        leaseExpiresAt: new Date(Date.now() + 60_000),
        renew: async () => {
          renewals++;
          return renewalGate;
        },
        execute: async (budget) => {
          signal = budget.signal!;
          await executionGate;
          if (outcome === "error") throw new Error("business");
          return "done";
        },
        onTimeout: async () => {
          assert.fail("Completed execution must not time out");
        },
      });
      const observed = running.then(
        (value) => {
          returned = true;
          return value;
        },
        (error: Error) => {
          returned = true;
          return error;
        },
      );
      t.mock.timers.tick(20_000);
      assert.equal(renewals, 1);
      finish();
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(
        returned,
        false,
        "Execution must await its in-flight renewal",
      );
      renewed(new Date(Date.now() + 60_000));
      const result = await observed;
      if (outcome === "error")
        assert.equal((result as Error).message, "business");
      else assert.equal(result, "done");
      t.mock.timers.tick(HARD_SLICE_MS);
      assert.equal(renewals, 1, "No heartbeat may restart after completion");
      assert.equal(
        signal.aborted,
        false,
        "No expiry timer may abort the ended execution",
      );
    });
  }
});
