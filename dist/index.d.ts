import * as _prisma_client_runtime_library from '@prisma/client/runtime/library';
import { Prisma, QueueJob, PrismaClient } from '@prisma/client';
import { EventEmitter } from 'events';

type Simplify<T> = {
    [KeyType in keyof T]: T[KeyType];
} & {};
type JobPayload = Prisma.InputJsonValue;
type JobResult = Prisma.InputJsonValue;
type DatabaseJob<T, U> = Simplify<Omit<QueueJob, "payload" | "result"> & {
    payload: T;
    result: U;
}>;
type JobCreator<T extends JobPayload> = (client: PrismaLightClient) => Promise<T>;
type JobWorker<T extends JobPayload = JobPayload, U extends JobResult = JobResult> = (job: PrismaJob<T, U>, client: PrismaClient) => Promise<U>;
type PrismaLightClient = Omit<PrismaClient, "$connect" | "$disconnect" | "$on" | "$transaction" | "$use" | "$extends">;

type PrismaJobOptions = {
    model: Prisma.QueueJobDelegate;
    client: PrismaLightClient;
};
/**
 * Represents a job within a Prisma-managed queue.
 */
declare class PrismaJob<T, U> {
    #private;
    readonly id: bigint;
    /**
     * Constructs a new PrismaJob instance with the provided job record and database access objects.
     * @param record - The initial database job record.
     * @param model - The Prisma delegate used for database operations related to the job.
     * @param client - The Prisma client for executing arbitrary queries.
     */
    constructor(record: DatabaseJob<T, U>, { model, client }: PrismaJobOptions);
    /**
     * Gets the current job record.
     */
    get record(): DatabaseJob<T, U>;
    /**
     * Gets the job's unique key if any.
     */
    get key(): string | null;
    /**
     * Gets the CRON expression associated with the job for recurring scheduling.
     */
    get cron(): string | null;
    /**
     * Gets the job's priority level.
     */
    get priority(): number;
    /**
     * Gets the payload associated with the job.
     */
    get payload(): T;
    /**
     * Gets the timestamp when the job was finished.
     */
    get finishedAt(): Date | null;
    /**
     * Gets the error record if the job failed.
     */
    get error(): Prisma.JsonValue;
    /**
     * Updates the job's progress percentage.
     * @param progress - The new progress percentage.
     */
    progress(progress: number): Promise<DatabaseJob<T, U>>;
    /**
     * Fetches the latest job record from the database and updates the internal state.
     */
    fetch(): Promise<DatabaseJob<T, U>>;
    /**
     * Updates the job record in the database with new data.
     * @param data - The new data to be merged with the existing job record.
     */
    update(data: Prisma.QueueJobUpdateInput): Promise<DatabaseJob<T, U>>;
    /**
     * Deletes the job from the database.
     */
    delete(): Promise<DatabaseJob<T, U>>;
    /**
     * Checks if the job is currently locked by another transaction.
     * @returns {Promise<boolean>} True if the job is locked, false otherwise.
     */
    isLocked(): Promise<boolean>;
}

type PrismaQueueOptions = {
    prisma?: PrismaClient;
    name?: string;
    maxAttempts?: number | null;
    maxConcurrency?: number;
    pollInterval?: number;
    jobInterval?: number;
    modelName?: string;
    tableName?: string;
    deleteOn?: "success" | "failure" | "always" | "never";
    alignTimeZone?: boolean;
};
type EnqueueOptions = {
    cron?: string;
    runAt?: Date;
    key?: string;
    maxAttempts?: number;
    priority?: number;
};
type ScheduleOptions = Omit<EnqueueOptions, "key" | "cron"> & {
    key: string;
    cron: string;
};
type PrismaQueueEvents<T extends JobPayload = JobPayload, U extends JobResult = JobResult> = {
    enqueue: (job: PrismaJob<T, U>) => void;
    dequeue: (job: PrismaJob<T, U>) => void;
    success: (result: U, job: PrismaJob<T, U>) => void;
    error: (error: unknown, job?: PrismaJob<T, U>) => void;
};
interface PrismaQueue<T extends JobPayload = JobPayload, U extends JobResult = JobResult> {
    on<E extends keyof PrismaQueueEvents<T, U>>(event: E, listener: PrismaQueueEvents<T, U>[E]): this;
    once<E extends keyof PrismaQueueEvents<T, U>>(event: E, listener: PrismaQueueEvents<T, U>[E]): this;
    emit<E extends keyof PrismaQueueEvents<T, U>>(event: E, ...args: Parameters<PrismaQueueEvents<T, U>[E]>): boolean;
}
declare class PrismaQueue<T extends JobPayload = JobPayload, U extends JobResult = JobResult> extends EventEmitter {
    #private;
    private options;
    worker: JobWorker<T, U>;
    private name;
    private config;
    private concurrency;
    private stopped;
    /**
     * Constructs a PrismaQueue object with specified options and a worker function.
     * @param options - Configuration options for the queue.
     * @param worker - The worker function that processes jobs.
     */
    constructor(options: PrismaQueueOptions | undefined, worker: JobWorker<T, U>);
    /**
     * Gets the Prisma delegate associated with the queue job model.
     */
    private get model();
    /**
     * Starts the job processing in the queue.
     */
    start(): Promise<void>;
    /**
     * Stops the job processing in the queue.
     */
    stop(): Promise<void>;
    /**
     * Adds a job to the queue.
     * @param payloadOrFunction - The job payload or a function that returns a job payload.
     * @param options - Options for the job, such as scheduling and attempts.
     */
    add: (payloadOrFunction: T | JobCreator<T>, options?: EnqueueOptions) => Promise<PrismaJob<T, U>>;
    /**
     * Adds a job to the queue.
     * @param payloadOrFunction - The job payload or a function that returns a job payload.
     * @param options - Options for the job, such as scheduling and attempts.
     */
    enqueue(payloadOrFunction: T | JobCreator<T>, options?: EnqueueOptions): Promise<PrismaJob<T, U>>;
    /**
     * Schedules a job according to the cron expression or a specific run time.
     * @param options - Scheduling options including cron, key, and run time.
     * @param payloadOrFunction - The job payload or a function that returns a job payload.
     */
    schedule(options: ScheduleOptions, payloadOrFunction: T | JobCreator<T>): Promise<PrismaJob<T, U>>;
    /**
     * Polls the queue and processes jobs according to the configured intervals and concurrency settings.
     */
    private poll;
    /**
     * Processes a dequeued job by running the worker and handling success/failure.
     * @param job - The job to process
     */
    private processJob;
    /**
     * Handles post-dequeue logic like emitting events and scheduling next cron run.
     * @param job - The dequeued job
     */
    private handleDequeueResult;
    /**
     * Dequeues and processes the next job in the queue. Handles locking and error management internally.
     * @returns {Promise<PrismaJob<T, U> | null>} The job that was processed or null if no job was available.
     */
    private dequeue;
    private dequeueByProvider;
    /**
     * Dequeues using FOR UPDATE SKIP LOCKED (PostgreSQL, MySQL, etc.).
     * @returns {Promise<PrismaJob<T, U> | null>} The job that was processed or null if no job was available.
     */
    private dequeueWithSkipLocked;
    /**
     * Dequeues a job using optimistic locking for SQLite (no SKIP LOCKED support).
     * @returns {Promise<PrismaJob<T, U> | null>} The job that was processed or null if no job was available.
     */
    private dequeueWithOptimisticLocking;
    /**
     * Counts the number of jobs in the queue, optionally only those available for processing.
     * @param {boolean} onlyAvailable - If true, counts only jobs that are ready to be processed.
     * @returns {Promise<number>} The number of jobs.
     */
    size(onlyAvailable?: boolean): Promise<number>;
}

type InputJsonValue = Prisma.InputJsonValue;
declare function prepareForJson<T>(originalValue: T): InputJsonValue;
declare function restoreFromJson<T = unknown>(preparedValue: InputJsonValue): T;

/**
 * Factory function to create a new PrismaQueue instance.
 * This function simplifies the instantiation of a PrismaQueue by wrapping it into a function call.
 *
 * @param options - The configuration options for the PrismaQueue. These options configure how the queue interacts with the database and controls job processing behavior.
 * @param worker - The worker function that will process each job. The worker function is called with each dequeued job and is responsible for executing the job's logic.
 *
 * @returns An instance of PrismaQueue configured with the provided options and worker.
 *
 * @template T - The type of the job payload. It extends JobPayload which can be further extended to include more specific data types as needed.
 * @template U - The type of the result expected from the worker function after processing a job. It extends JobResult which can be specialized based on the application's needs.
 *
 * @example
 * // Create a new queue for email sending jobs
 * const emailQueue = createQueue<EmailPayload, void>({
 *   name: 'emails',
 *   prisma: new PrismaClient(),
 *   pollInterval: 5000,
 * }, async (job) => {
 *   await sendEmail(job.payload);
 * });
 */
declare const createQueue: <T extends JobPayload = _prisma_client_runtime_library.InputJsonValue, U extends JobResult = _prisma_client_runtime_library.InputJsonValue>(options: PrismaQueueOptions, worker: JobWorker<T, U>) => PrismaQueue<T, U>;

export { type DatabaseJob, type EnqueueOptions, type JobCreator, type JobPayload, type JobResult, type JobWorker, PrismaJob, type PrismaJobOptions, type PrismaLightClient, PrismaQueue, type PrismaQueueEvents, type PrismaQueueOptions, type ScheduleOptions, type Simplify, createQueue, prepareForJson, restoreFromJson };
