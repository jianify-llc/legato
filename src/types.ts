/** Portable persistence contract. Transactions and domain lock order belong to the caller. */
export type TaskId = string | number;
export type TaskStatus =
  | "pending"
  | "queued"
  | "running"
  | "succeeded"
  | "retry_wait"
  | "failed"
  | "manual";
export type Json = Record<string, unknown>;
export interface Sql {
  query<T extends Record<string, unknown> = Record<string, unknown>>(
    text: string,
    values?: unknown[],
  ): Promise<{ rows: T[]; rowCount: number | null }>;
}
export interface TaskRecord<I extends TaskId = TaskId> {
  id: I;
  requestKey: string;
  businessKey: string;
  type: string;
  executionMode: "queue" | "inline";
  userId: string | null;
  operationId: number | null;
  bizParams: Json;
  status: TaskStatus;
  runAttempts: number;
  /** Computed from failure history, never a second persisted slice counter. */
  failureCount: number;
  lastFailure: Json | null;
  executionMs: number | null;
  firstStartedAt: Date | null;
  startedAt: Date | null;
  finishedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
  leaseId: string | null;
  leaseExpiresAt: Date | null;
  nextRunAt: Date;
  expiresAt: Date | null;
  executionState: Json | null;
  result: Json | null;
  resultCode: string | null;
  dispatchAttempts: number;
  dispatchFailures: number;
  nextDispatchAt: Date | null;
  lastDispatchAttemptAt: Date | null;
  lastFailedRunAttempt: number | null;
  lastFailedAt: Date | null;
  attentionEnteredAt: Date | null;
  retryOfTaskId: I | null;
}
export interface RetryPolicy {
  limit: number | null;
  delays: readonly number[];
}
export interface Failure {
  code: string;
  retryable: boolean;
  diagnostics?: readonly Record<string, unknown>[];
  phase?: "execution" | "scheduling";
  retryAt?: Date;
  manual?: boolean;
}
export interface ClaimOptions {
  type?: string;
  executionMode?: "queue" | "inline";
  statuses?: readonly ("pending" | "queued" | "retry_wait")[];
  limit?: number | null;
  leaseMs?: number;
}
export interface Completion {
  resultCode: string;
  result?: Json | null;
  executionState?: Json | null;
}
export interface AdministrativeDecision {
  status: "pending" | "retry_wait" | "manual" | "failed";
  resultCode: string | null;
  nextRunAt?: Date | string;
  executionState?: Json | null;
  result?: Json | null;
}
export interface TaskInput<I extends TaskId = TaskId> {
  id?: I;
  requestKey: string;
  businessKey: string;
  type: string;
  bizParams: Json;
  userId?: string | null;
  operationId?: number | null;
  retryOfTaskId?: I | null;
  executionMode?: "queue" | "inline";
  executionState?: Json | null;
  nextRunAt?: Date | string;
  expiresAt?: Date | null;
}
