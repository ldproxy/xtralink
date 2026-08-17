import type { Config } from "genffi";

export default {
  outDir: ".",
  verbose: true,

  go: {
    path: ".",
    module: "github.com/ldproxy/xtralink",
    api: {
      pkg: "ffi/api",
    },
    cgo: {
      pkg: "ffi/clib",
      init: true,
    },
    impl: {
      pkg: "ffi/impl",
    },
    model: {
      pkg: "model",
    },
  },

  //schema: { label: "Example JSON Schema" },

  java: {
    path: "ffi/java/src/main/java",
    pkg: "de.ii.xtralink.jobs",
    api: {
      pkg: "de.ii.xtralink.jobs.internal",
    },
  },
} satisfies Config;

export namespace GenModel {
  export namespace Enums {
    export enum Status {
      ACCEPTED = "ACCEPTED",
      RUNNING = "RUNNING",
      SUCCESSFUL = "SUCCESSFUL",
      FAILED = "FAILED",
      DISMISSED = "DISMISSED",
    }

    export enum ProgressOperation {
      ADD = "ADD",
      SUBTRACT = "SUBTRACT",
    }

    export enum Result {
      SUCCESS = "SUCCESS",
      FAILURE = "FAILURE",
      RETRY = "RETRY",
      ONHOLD = "ONHOLD",
    }

    export enum Queue {
      LOCAL = "LOCAL",
      REDIS = "REDIS",
    }
  }

  export namespace Config {
    export type JobProgress = {
      /** @TJS-type integer */
      current: number;
      /** @TJS-type integer */
      total: number;
      /** @TJS-type integer */
      percent: number;
      details: { [key: string]: any };
    };
    export type InitProgress = {
      /** @TJS-type integer */
      total: number;
      details: { [key: string]: any };
    };
    export type JobSequence = {
      /** @TJS-type integer */
      current: number;
      /** @TJS-type integer */
      remaining: number;
    };
    export type OutputValue = {
      value: any;
      href: string;
      kind: string;
    };
    export type ProgressUpdate = {
      path: string;
      operation: Enums.ProgressOperation;
    };
    export type QueueConfiguration = {
      /** @TJS-type integer */
      concurrency: number;
      executor: string;
      queue: Enums.Queue;
      /** @optional */
      cluster: string;
      redis: string[];
    };

    export type BaseJob = {
      id: string;
      kind: string;
      createdAt: bigint;
      startedAt: bigint;
      updatedAt: bigint;
      finishedAt: bigint;
      /** @TJS-type integer */
      priority: number;
      progress: JobProgress;
      status: Enums.Status;
      errors: string[];
      context: { [key: string]: any };
    };

    export type PartialJob = BaseJob & {
      partOf: string;
      executor: string;
      onHold: boolean;
      progressUpdates: ProgressUpdate[];
      /**
       * @TJS-type integer
       * @optional
       */
      sequence: number;
    };

    export type Job = BaseJob & {
      label: string;
      description: string;
      inputs: { [key: string]: any };
      outputs: { [key: string]: OutputValue };
      /** @optional */
      setup: PartialJob;
      /** @optional */
      cleanup: PartialJob;
      followUps: Job[];
      /** @optional */
      sequence: JobSequence;
    };

    export type JobConfiguration = {
      kind: string;
      /** @TJS-type integer */
      priority: number;
      label: string;
      description: string;
      inputs: { [key: string]: any };
      context: { [key: string]: any };
      progress: JobProgress;
      setup: boolean;
      cleanup: boolean;
      followUps: JobConfiguration[];
    };

    export type PartialJobConfiguration = {
      kind: string;
      /** @TJS-type integer */
      priority: number;
      partOf: string;
      progress: JobProgress;
      progressUpdates: ProgressUpdate[];
      /**
       * @TJS-type integer
       * @optional
       */
      sequence: number;
      context: { [key: string]: any };
    };

    export type JobResult = {
      status: Enums.Result;
      messages: string[];
    };
  }
}

export namespace GenApi {
  export type int = number & { readonly __int: unique symbol };

  /**
   * @listener
   */
  export interface JobListener {
    onProgress: (job: GenModel.Config.Job) => void;
  }

  /**
   * @listener
   */
  export interface JobProcessor {
    /**
     * @throws
     */
    process: (
      partialJob: GenModel.Config.PartialJob,
      job: GenModel.Config.Job,
    ) => GenModel.Config.JobResult;
  }

  /**
   * The one globally reachable object. `InitLibrary()` creates it by calling
   * `clib.NewInit()`, which you have to write by hand — it is the single obligation
   * genffi places on the implementation side.
   * @singleton
   */
  export interface JobQueue {
    /**
     * @throws
     */
    start: (cfg: GenModel.Config.QueueConfiguration) => void;

    stop: () => void;

    /**
     * @scoped
     */
    push: (
      job: GenModel.Config.JobConfiguration,
      onProgress: JobListener,
    ) => Promise<GenModel.Config.Job>;

    pushPartial: (
      partialJob: GenModel.Config.PartialJobConfiguration,
    ) => Promise<GenModel.Config.PartialJob>;

    repushPartial: (id: string) => Promise<GenModel.Config.PartialJob>;

    init(id: string, progress: GenModel.Config.InitProgress): void;

    updatePartial(id: string, delta: int): void;

    cancel(id: string): boolean;

    /**
     * @optional
     */
    get: (id: string) => GenModel.Config.Job;

    /**
     * @optional
     */
    getPartial: (id: string) => GenModel.Config.PartialJob;

    /**
     * @throws
     */
    register: (jobType: string, priority: int, processor: JobProcessor) => void;
  }
}
