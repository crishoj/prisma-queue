// src/PrismaQueue.ts
import { Prisma as Prisma2, PrismaClient } from "@prisma/client";
import { Cron } from "croner";
import { EventEmitter } from "events";
import assert2 from "node:assert";

// src/utils/debug.ts
import createDebug from "debug";
var debug = createDebug("prisma-queue");

// src/utils/database.ts
var cachedProvider = null;
async function detectDatabaseProvider(prisma) {
  try {
    await prisma.$queryRaw`SELECT 1`;
    await prisma.$queryRaw`SHOW server_version`;
    return "postgresql";
  } catch {
    try {
      await prisma.$queryRaw`SELECT sqlite_version()`;
      return "sqlite";
    } catch {
      try {
        await prisma.$queryRaw`SELECT VERSION()`;
        return "mysql";
      } catch {
        try {
          await prisma.$queryRaw`SELECT @@VERSION`;
          return "sqlserver";
        } catch {
          return "postgresql";
        }
      }
    }
  }
}
async function databaseProvider(prisma) {
  if (cachedProvider) {
    return cachedProvider;
  }
  cachedProvider = await detectDatabaseProvider(prisma);
  debug(`detected database provider: ${cachedProvider}`);
  return cachedProvider;
}

// src/utils/error.ts
var serializeError = (err) => {
  if (err instanceof Error) {
    return {
      name: err.name,
      message: err.message,
      stack: err.stack
    };
  }
  return {
    name: "UnknownError",
    message: String(err),
    stack: void 0
  };
};
var isPrismaError = (error) => {
  return error instanceof Error && "code" in error;
};

// src/utils/prisma.ts
import { Prisma } from "@prisma/client";
import assert from "assert";
var getTableName = (modelName) => {
  const model = Prisma.dmmf.datamodel.models.find((model2) => model2.name === modelName);
  assert(model?.dbName, `Did not find model=${modelName} in Prisma.dmmf!`);
  return model.dbName;
};

// src/utils/string.ts
var escape = (name) => '"' + name.replace(/"/g, '""') + '"';
var uncapitalize = (string) => string.charAt(0).toLowerCase() + string.slice(1);

// src/utils/stringify.ts
function prepareForJson(originalValue) {
  if (typeof originalValue === "undefined") {
    return {
      $type: "undefined"
    };
  } else if (typeof originalValue === "bigint") {
    return {
      $type: "bigint",
      $value: `0x${originalValue.toString(16)}`
    };
  } else if (originalValue instanceof Map) {
    return {
      $type: "Map",
      $value: Array.from(originalValue.entries())
    };
  } else if (originalValue instanceof Set) {
    return {
      $type: "Set",
      $value: Array.from(originalValue.values())
    };
  } else if (originalValue instanceof Date) {
    return {
      $type: "Date",
      $value: originalValue.getTime()
    };
  } else if (typeof originalValue === "object" && originalValue !== null) {
    if (Array.isArray(originalValue)) {
      return originalValue.map(prepareForJson);
    } else {
      const copy = {};
      for (const key in originalValue) {
        copy[key] = prepareForJson(originalValue[key]);
      }
      return copy;
    }
  }
  return originalValue;
}
function restoreFromJson(preparedValue) {
  if (typeof preparedValue === "object" && preparedValue !== null && "$type" in preparedValue) {
    if (preparedValue["$type"] === "undefined") {
      return void 0;
    } else if (preparedValue["$type"] === "bigint") {
      return BigInt(preparedValue["$value"]);
    } else if (preparedValue["$type"] === "Map") {
      return new Map(preparedValue["$value"]);
    } else if (preparedValue["$type"] === "Set") {
      return new Set(preparedValue["$value"]);
    } else if (preparedValue["$type"] === "Date") {
      return new Date(preparedValue["$value"]);
    }
  } else if (typeof preparedValue === "object" && preparedValue !== null) {
    if (Array.isArray(preparedValue)) {
      return preparedValue.map(restoreFromJson);
    } else {
      const copy = {};
      for (const key in preparedValue) {
        copy[key] = restoreFromJson(preparedValue[key]);
      }
      return copy;
    }
  }
  return preparedValue;
}

// src/utils/time.ts
var waitFor = async (ms) => new Promise((resolve) => {
  setTimeout(resolve, ms);
});
var calculateDelay = (attempts) => Math.min(1e3 * Math.pow(2, Math.max(1, attempts)) + Math.random() * 100, Math.pow(2, 31) - 1);
var getCurrentTimeZone = () => Intl.DateTimeFormat().resolvedOptions().timeZone;

// src/PrismaJob.ts
var PrismaJob = class {
  #model;
  #client;
  #record;
  id;
  /**
   * Constructs a new PrismaJob instance with the provided job record and database access objects.
   * @param record - The initial database job record.
   * @param model - The Prisma delegate used for database operations related to the job.
   * @param client - The Prisma client for executing arbitrary queries.
   */
  constructor(record, { model, client }) {
    this.#model = model;
    this.#client = client;
    this.#record = record;
    this.id = record.id;
  }
  /**
   * Internal method to assign a new record to the job.
   * @param record - Optional new record to assign.
   */
  #assign(record) {
    if (record) {
      this.#record = record;
    }
  }
  /**
   * Gets the current job record.
   */
  get record() {
    return this.#record;
  }
  /**
   * Gets the job's unique key if any.
   */
  get key() {
    return this.#record.key;
  }
  /**
   * Gets the CRON expression associated with the job for recurring scheduling.
   */
  get cron() {
    return this.#record.cron;
  }
  /**
   * Gets the job's priority level.
   */
  get priority() {
    return this.#record.priority;
  }
  /**
   * Gets the payload associated with the job.
   */
  get payload() {
    return this.#record.payload;
  }
  /**
   * Gets the timestamp when the job was finished.
   */
  get finishedAt() {
    return this.#record.finishedAt;
  }
  /**
   * Gets the error record if the job failed.
   */
  get error() {
    return this.#record.error;
  }
  /**
   * Updates the job's progress percentage.
   * @param progress - The new progress percentage.
   */
  async progress(progress) {
    return await this.update({ progress: Math.max(0, Math.min(100, progress)) });
  }
  /**
   * Fetches the latest job record from the database and updates the internal state.
   */
  async fetch() {
    const record = await this.#model.findUnique({
      where: { id: this.id }
    });
    this.#assign(record);
    return record;
  }
  /**
   * Updates the job record in the database with new data.
   * @param data - The new data to be merged with the existing job record.
   */
  async update(data) {
    const record = await this.#model.update({
      where: { id: this.id },
      data
    });
    this.#assign(record);
    return record;
  }
  /**
   * Deletes the job from the database.
   */
  async delete() {
    const record = await this.#model.delete({
      where: { id: this.id }
    });
    return record;
  }
  /**
   * Checks if the job is currently locked by another transaction.
   * @returns {Promise<boolean>} True if the job is locked, false otherwise.
   */
  async isLocked() {
    try {
      await this.#client.$executeRawUnsafe(
        `SELECT "id" FROM "public"."queue_jobs" WHERE "id" = $1 FOR UPDATE NOWAIT`,
        this.id
      );
      return false;
    } catch (error) {
      if (isPrismaError(error) && error.meta?.["code"] === "55P03") {
        return true;
      }
      throw error;
    }
  }
};

// src/PrismaQueue.ts
var DEFAULT_MAX_CONCURRENCY = 1;
var DEFAULT_POLL_INTERVAL = 10 * 1e3;
var DEFAULT_JOB_INTERVAL = 50;
var DEFAULT_DELETE_ON = "never";
var PrismaQueue = class extends EventEmitter {
  /**
   * Constructs a PrismaQueue object with specified options and a worker function.
   * @param options - Configuration options for the queue.
   * @param worker - The worker function that processes jobs.
   */
  constructor(options = {}, worker) {
    super();
    this.options = options;
    this.worker = worker;
    const {
      prisma = new PrismaClient(),
      name = "default",
      modelName = "QueueJob",
      tableName = getTableName(modelName),
      maxAttempts = null,
      maxConcurrency = DEFAULT_MAX_CONCURRENCY,
      pollInterval = DEFAULT_POLL_INTERVAL,
      jobInterval = DEFAULT_JOB_INTERVAL,
      deleteOn = DEFAULT_DELETE_ON,
      alignTimeZone = false
    } = this.options;
    assert2(name.length <= 255, "name must be less or equal to 255 chars");
    assert2(pollInterval >= 100, "pollInterval must be more than 100 ms");
    assert2(jobInterval >= 10, "jobInterval must be more than 10 ms");
    this.name = name;
    this.#prisma = prisma;
    this.config = {
      modelName,
      tableName,
      maxAttempts,
      maxConcurrency,
      pollInterval,
      jobInterval,
      deleteOn,
      alignTimeZone
    };
    this.on("error", (error, job) => {
      debug(
        job ? `Job with id=${job.id} failed for queue named="${this.name}" with error` : `Queue named="${this.name}" encountered an unexpected error`,
        error
      );
    });
  }
  #prisma;
  name;
  config;
  concurrency = 0;
  stopped = true;
  /**
   * Gets the Prisma delegate associated with the queue job model.
   */
  get model() {
    const queueJobKey = uncapitalize(this.config.modelName);
    return this.#prisma[queueJobKey];
  }
  /**
   * Starts the job processing in the queue.
   */
  async start() {
    debug(`starting queue named="${this.name}"...`);
    if (!this.stopped) {
      debug(`queue named="${this.name}" is already running, skipping...`);
      return;
    }
    this.stopped = false;
    return this.poll();
  }
  /**
   * Stops the job processing in the queue.
   */
  async stop() {
    const { pollInterval } = this.config;
    debug(`stopping queue named="${this.name}"...`);
    this.stopped = true;
    await waitFor(pollInterval);
  }
  /**
   * Adds a job to the queue.
   * @param payloadOrFunction - The job payload or a function that returns a job payload.
   * @param options - Options for the job, such as scheduling and attempts.
   */
  // eslint-disable-next-line @typescript-eslint/unbound-method
  add = this.enqueue;
  /**
   * Adds a job to the queue.
   * @param payloadOrFunction - The job payload or a function that returns a job payload.
   * @param options - Options for the job, such as scheduling and attempts.
   */
  async enqueue(payloadOrFunction, options = {}) {
    debug(`enqueue`, this.name, payloadOrFunction, options);
    const { name: queueName, config } = this;
    const { key = null, cron = null, maxAttempts = config.maxAttempts, priority = 0, runAt } = options;
    const record = await this.#prisma.$transaction(async (client) => {
      const payload = payloadOrFunction instanceof Function ? await payloadOrFunction(client) : payloadOrFunction;
      const data = { queue: queueName, cron, payload, maxAttempts, priority, key };
      if (key && runAt) {
        const { count } = await this.model.deleteMany({
          where: {
            queue: queueName,
            key,
            runAt: {
              gte: /* @__PURE__ */ new Date(),
              not: runAt
            }
          }
        });
        if (count > 0) {
          debug(`deleted ${count} conflicting upcoming queue jobs`);
        }
        const update = { ...data, ...runAt ? { runAt } : {} };
        return await this.model.upsert({
          where: { key_runAt: { key, runAt } },
          create: { ...update },
          update
        });
      }
      return await this.model.create({ data });
    });
    const job = new PrismaJob(record, { model: this.model, client: this.#prisma });
    this.emit("enqueue", job);
    return job;
  }
  /**
   * Schedules a job according to the cron expression or a specific run time.
   * @param options - Scheduling options including cron, key, and run time.
   * @param payloadOrFunction - The job payload or a function that returns a job payload.
   */
  async schedule(options, payloadOrFunction) {
    debug(`schedule`, this.name, options, payloadOrFunction);
    const { key, cron, runAt: firstRunAt, ...otherOptions } = options;
    const runAt = firstRunAt ?? new Cron(cron).nextRun();
    assert2(runAt, `Failed to find a future occurence for given cron`);
    return this.enqueue(payloadOrFunction, { key, cron, runAt, ...otherOptions });
  }
  /**
   * Polls the queue and processes jobs according to the configured intervals and concurrency settings.
   */
  async poll() {
    const { maxConcurrency, pollInterval, jobInterval } = this.config;
    debug(
      `polling queue named="${this.name}" with pollInterval=${pollInterval} maxConcurrency=${maxConcurrency}...`
    );
    while (!this.stopped) {
      if (this.concurrency >= maxConcurrency) {
        await waitFor(pollInterval);
        continue;
      }
      let estimatedQueueSize = await this.size(true);
      if (estimatedQueueSize === 0) {
        await waitFor(pollInterval);
        continue;
      }
      while (estimatedQueueSize > 0 && !this.stopped) {
        while (estimatedQueueSize > 0 && !this.stopped && this.concurrency < maxConcurrency) {
          debug(`processing job from queue named="${this.name}"...`);
          this.concurrency++;
          setImmediate(() => {
            this.dequeue().then((job) => {
              if (job) {
                debug(`dequeued job({id: ${job.id}, payload: ${JSON.stringify(job.payload)}})`);
                estimatedQueueSize--;
              } else {
                estimatedQueueSize = 0;
              }
            }).catch((error) => {
              this.emit("error", error);
            }).finally(() => {
              this.concurrency--;
            });
          });
          await waitFor(jobInterval);
        }
        await waitFor(jobInterval * 2);
      }
    }
  }
  /**
   * Processes a dequeued job by running the worker and handling success/failure.
   * @param job - The job to process
   */
  async processJob(job) {
    const { deleteOn } = this.config;
    const { id, payload, attempts, maxAttempts } = job.record;
    try {
      assert2(this.worker, "Missing queue worker to process job");
      debug(`starting worker for job({id: ${id}, payload: ${JSON.stringify(payload)}})`);
      const result = await this.worker(job, this.#prisma);
      debug(`finished worker for job({id: ${id}, payload: ${JSON.stringify(payload)}})`);
      const date = /* @__PURE__ */ new Date();
      await job.update({ finishedAt: date, progress: 100, result, error: Prisma2.DbNull });
      this.emit("success", result, job);
      if (deleteOn === "success" || deleteOn === "always") {
        await job.delete();
      }
    } catch (error) {
      const date = /* @__PURE__ */ new Date();
      debug(
        `failed finishing job({id: ${id}, payload: ${JSON.stringify(payload)}}) with error="${String(error)}"`
      );
      const isFinished = maxAttempts && attempts >= maxAttempts;
      const notBefore = new Date(date.getTime() + calculateDelay(attempts));
      if (!isFinished) {
        debug(`will retry at notBefore=${notBefore.toISOString()} (attempts=${attempts})`);
      }
      await job.update({
        finishedAt: isFinished ? date : null,
        failedAt: date,
        error: serializeError(error),
        notBefore: isFinished ? null : notBefore
      });
      this.emit("error", error, job);
      if (deleteOn === "failure" || deleteOn === "always") {
        await job.delete();
      }
    }
  }
  /**
   * Handles scheduling next cron run.
   */
  async scheduleNextCronRun(job) {
    const { key, cron, payload, finishedAt } = job;
    if (finishedAt && cron && key) {
      debug(`scheduling next cron job({key: ${key}, cron: ${cron}}) with payload=${JSON.stringify(payload)}`);
      await this.schedule({ key, cron }, payload);
    }
  }
  /**
   * Dequeues and processes the next job in the queue. Handles locking and error management internally.
   * @returns {Promise<PrismaJob<T, U> | null>} The job that was processed or null if no job was available.
   */
  async dequeue() {
    if (this.stopped) {
      return null;
    }
    const job = await this.dequeueByProvider();
    if (job) {
      this.emit("dequeue", job);
      await this.processJob(job);
      await this.scheduleNextCronRun(job);
    }
    return job;
  }
  async dequeueByProvider() {
    const provider = await databaseProvider(this.#prisma);
    switch (provider) {
      case "postgresql":
        return await this.dequeueWithSkipLocked();
      case "sqlite":
        return await this.dequeueWithOptimisticLocking();
      default:
        throw Error(`Unsupported provider:  ${provider}`);
    }
  }
  /**
   * Dequeues using FOR UPDATE SKIP LOCKED (PostgreSQL, MySQL, etc.).
   * @returns {Promise<PrismaJob<T, U> | null>} The acquired job or null if no job was available.
   */
  async dequeueWithSkipLocked() {
    debug(`dequeuing from queue named="${this.name}"...`);
    const { name: queueName } = this;
    const { tableName: tableNameRaw, alignTimeZone } = this.config;
    const tableName = escape(tableNameRaw);
    const jobRecord = await this.#prisma.$transaction(
      async (client) => {
        if (alignTimeZone) {
          const [{ TimeZone: dbTimeZone }] = await client.$queryRawUnsafe("SHOW TIME ZONE");
          const localTimeZone = getCurrentTimeZone();
          if (dbTimeZone !== localTimeZone) {
            debug(`aligning database timezone from ${dbTimeZone} to ${localTimeZone}!`);
            await client.$executeRawUnsafe(`SET LOCAL TIME ZONE '${localTimeZone}';`);
          }
        }
        const rows = await client.$queryRawUnsafe(
          `UPDATE ${tableName} SET "processedAt" = NOW(), "attempts" = "attempts" + 1
           WHERE id = (
             SELECT id
             FROM ${tableName}
             WHERE (${tableName}."queue" = $1)
               AND (${tableName}."finishedAt" IS NULL)
               AND (${tableName}."runAt" < NOW())
               AND (${tableName}."notBefore" IS NULL OR ${tableName}."notBefore" < NOW())
             ORDER BY ${tableName}."priority" ASC, ${tableName}."runAt" ASC
             FOR UPDATE SKIP LOCKED
             LIMIT 1
           )
           RETURNING *;`,
          queueName
        );
        if (!rows.length || !rows[0]) {
          debug(`no jobs found in queue named="${this.name}"`);
          return null;
        }
        return rows[0];
      },
      // Short timeout for job acquisition only
      { timeout: 3e4 }
      // 30 seconds
    );
    if (!jobRecord) {
      return null;
    }
    return new PrismaJob(jobRecord, { model: this.model, client: this.#prisma });
  }
  /**
   * Dequeues a job using optimistic locking for SQLite (no SKIP LOCKED support).
   * @returns {Promise<PrismaJob<T, U> | null>} The acquired job or null if no job was available.
   */
  async dequeueWithOptimisticLocking() {
    if (this.stopped) {
      return null;
    }
    debug(`dequeuing from queue named="${this.name}"...`);
    const { name: queueName } = this;
    const { alignTimeZone } = this.config;
    const queueJobKey = uncapitalize(this.config.modelName);
    const jobRecord = await this.#prisma.$transaction(
      async (client) => {
        if (alignTimeZone) {
          debug(`timezone alignment not supported for provider, skipping...`);
        }
        const availableJob = await client[queueJobKey].findFirst({
          where: {
            queue: queueName,
            finishedAt: null,
            runAt: { lte: /* @__PURE__ */ new Date() },
            OR: [
              { notBefore: null },
              { notBefore: { lte: /* @__PURE__ */ new Date() } }
            ]
          },
          orderBy: [
            { priority: "asc" },
            { runAt: "asc" }
          ]
        });
        if (!availableJob) {
          debug(`no jobs found in queue named="${this.name}"`);
          return null;
        }
        const updatedJob = await client[queueJobKey].updateMany({
          where: {
            id: availableJob.id,
            processedAt: null
            // Only update if not already being processed
          },
          data: {
            processedAt: /* @__PURE__ */ new Date(),
            attempts: { increment: 1 }
          }
        });
        if (updatedJob.count === 0) {
          debug(`job ${availableJob.id} was already claimed by another worker`);
          return null;
        }
        const jobRecord2 = await client[queueJobKey].findUnique({
          where: { id: availableJob.id }
        });
        if (!jobRecord2) {
          debug(`job ${availableJob.id} not found after update`);
          return null;
        }
        return jobRecord2;
      },
      // Short timeout for job acquisition only
      { timeout: 3e4 }
      // 30 seconds
    );
    if (!jobRecord) {
      return null;
    }
    return new PrismaJob(jobRecord, { model: this.model, client: this.#prisma });
  }
  /**
   * Counts the number of jobs in the queue, optionally only those available for processing.
   * @param {boolean} onlyAvailable - If true, counts only jobs that are ready to be processed.
   * @returns {Promise<number>} The number of jobs.
   */
  async size(onlyAvailable) {
    const { name: queueName } = this;
    const date = /* @__PURE__ */ new Date();
    const where = { queue: queueName, finishedAt: null };
    if (onlyAvailable) {
      where.runAt = { lte: date };
      where.AND = { OR: [{ notBefore: { lte: date } }, { notBefore: null }] };
    }
    return await this.model.count({
      where
    });
  }
};

// src/index.ts
var createQueue = (options, worker) => {
  return new PrismaQueue(options, worker);
};
export {
  PrismaJob,
  PrismaQueue,
  createQueue,
  prepareForJson,
  restoreFromJson
};
//# sourceMappingURL=index.js.map