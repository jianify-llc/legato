/** Managed heartbeat and hard revocation. Domain code still chooses and atomically saves safe checkpoints. */
import {
  HARD_SLICE_MS,
  SliceBudget,
  TASK_RENEW_MS,
  TaskLeaseLost,
  TaskTimeout,
} from "./policy.js";

export interface SliceOptions<T> {
  startedAt: Date;
  leaseExpiresAt: Date;
  signal?: AbortSignal;
  /** Return only a database-confirmed expiry; a failed/unknown renewal grants no extra time. */
  renew: () => Promise<Date | null>;
  execute: (budget: SliceBudget) => Promise<T>;
  /** Persist fenced timeout failure before returning. A lease-lost result must not affect the new owner. */
  onTimeout: () => Promise<void>;
  onRenewError?: (error: unknown) => void;
}
export async function runSlice<T>(options: SliceOptions<T>): Promise<T> {
  const controller = new AbortController();
  const signal = options.signal
    ? AbortSignal.any([options.signal, controller.signal])
    : controller.signal;
  let expiresAt = options.leaseExpiresAt.getTime();
  const budget = new SliceBudget(options.startedAt, signal, Date.now, () => {
    // Event-loop stalls can postpone timers: every synchronous boundary checks the latest confirmed expiry.
    if (Date.now() >= expiresAt) {
      controller.abort(new TaskLeaseLost());
      signal.throwIfAborted();
    }
  });
  let stopped = false;
  let heartbeat: ReturnType<typeof setTimeout> | undefined;
  let expiry: ReturnType<typeof setTimeout> | undefined;
  let renewing: Promise<void> | undefined;
  const lose = () => controller.abort(new TaskLeaseLost());
  const scheduleExpiry = () => {
    if (stopped || signal.aborted) return;
    clearTimeout(expiry);
    expiry = setTimeout(
      () => {
        if (Date.now() >= expiresAt) lose();
        else scheduleExpiry();
      },
      Math.max(1, expiresAt - Date.now()),
    );
  };
  const scheduleRenew = () => {
    if (stopped || signal.aborted) return;
    heartbeat = setTimeout(
      () => {
        renewing = renew();
      },
      Math.max(0, Math.min(TASK_RENEW_MS, expiresAt - Date.now())),
    );
  };
  const renew = async () => {
    try {
      budget.assertActive();
      if (Date.now() >= expiresAt) {
        lose();
        return;
      }
      const next = await options.renew();
      if (!next) lose();
      else {
        expiresAt = next.getTime();
        scheduleExpiry();
      }
    } catch (error) {
      if (Date.now() >= expiresAt) lose();
      options.onRenewError?.(error);
    } finally {
      scheduleRenew();
    }
  };
  let hardTimer: ReturnType<typeof setTimeout> | undefined;
  let timingOut: Promise<void> | undefined;
  const settleTimeout = () => {
    timingOut ??= Promise.resolve().then(options.onTimeout);
    return timingOut;
  };
  const timeout = new Promise<never>((_, reject) => {
    hardTimer = setTimeout(
      () => {
        const error = new TaskTimeout();
        controller.abort(error);
        // Revocation uses an independent bounded database operation, never the aborted model call's connection.
        settleTimeout().then(() => reject(error), reject);
      },
      Math.max(0, options.startedAt.getTime() + HARD_SLICE_MS - Date.now()),
    );
  });
  try {
    budget.assertActive();
    if (Date.now() >= expiresAt) {
      lose();
      signal.throwIfAborted();
    }
    scheduleExpiry();
    scheduleRenew();
    // All persistence methods fence the lease in PostgreSQL. Abort cannot kill an uncooperative HTTP promise.
    const result = await Promise.race([options.execute(budget), timeout]);
    // A handler can swallow cancellation, or block the event loop past the timer. Neither extends the slice.
    if (Date.now() >= budget.hardDeadlineAt) throw new TaskTimeout();
    return result;
  } catch (error) {
    // Cooperative deadline checks can beat a delayed timer. Both paths revoke through one settlement.
    if (error instanceof TaskTimeout || Date.now() >= budget.hardDeadlineAt) {
      const timeoutError =
        error instanceof TaskTimeout ? error : new TaskTimeout();
      controller.abort(timeoutError);
      await settleTimeout();
      throw timeoutError;
    }
    throw error;
  } finally {
    stopped = true;
    clearTimeout(heartbeat);
    clearTimeout(expiry);
    clearTimeout(hardTimer);
    await renewing;
    await timingOut;
  }
}
