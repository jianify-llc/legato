import type { TaskRecord } from "./types.js";
export const SOFT_SLICE_MS = 12 * 60_000;
export const HARD_SLICE_MS = 14 * 60_000;
export const TASK_LEASE_MS = 60_000;
export const TASK_RENEW_MS = 20_000;
export const CHECKPOINT_MAX_BYTES = 1_048_576;
export class TaskYield extends Error {
  constructor() {
    super("Task reached a safe continuation boundary");
    this.name = "TaskYield";
  }
}
export class TaskTimeout extends Error {
  constructor() {
    super("Task slice exceeded its hard deadline");
    this.name = "TaskTimeout";
  }
}
export class TaskLeaseLost extends Error {
  constructor() {
    super("Task no longer owns this slice");
    this.name = "TaskLeaseLost";
  }
}
/** Pure time budget; a fresh instance is created from each database claim. */
export class SliceBudget {
  readonly softDeadlineAt: number;
  readonly hardDeadlineAt: number;
  constructor(
    startedAt: Date | number,
    readonly signal?: AbortSignal,
    private readonly now: () => number = Date.now,
    private readonly assertLease?: () => void,
  ) {
    const start = startedAt instanceof Date ? startedAt.getTime() : startedAt;
    this.softDeadlineAt = start + SOFT_SLICE_MS;
    this.hardDeadlineAt = start + HARD_SLICE_MS;
  }
  assertActive() {
    this.signal?.throwIfAborted();
    if (this.now() >= this.hardDeadlineAt) throw new TaskTimeout();
    this.assertLease?.();
  }
  shouldYield(reserveMs = 0) {
    this.assertActive();
    return this.now() + reserveMs >= this.softDeadlineAt;
  }
  /** Never start a model call without a positive soft budget and closing reserve. */
  requestTimeout(maxMs: number, reserveMs = 30_000) {
    this.assertActive();
    const timeout = Math.min(
      maxMs,
      this.softDeadlineAt - this.now() - reserveMs,
    );
    if (timeout <= 0) throw new TaskYield();
    return timeout;
  }
}
export function taskMetrics(
  task: Pick<
    TaskRecord,
    | "createdAt"
    | "firstStartedAt"
    | "finishedAt"
    | "executionMs"
    | "runAttempts"
  >,
) {
  return {
    slice_count: task.runAttempts,
    execution_ms: task.executionMs,
    task_elapsed_ms: task.finishedAt
      ? Math.max(0, task.finishedAt.getTime() - task.createdAt.getTime())
      : null,
    initial_queue_ms: task.firstStartedAt
      ? Math.max(0, task.firstStartedAt.getTime() - task.createdAt.getTime())
      : null,
  };
}
