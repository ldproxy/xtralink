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
      //pkg: "de.ii.xtraplatform.jobs",
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

    export type JobResult = {
      message: string;
      status: Enums.Status;
      errors: string[];
    };
  }
}

export namespace GenApi {
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
    process: (job: GenModel.Config.Job) => GenModel.Config.JobResult;
  }

  /**
   * The one globally reachable object. `InitLibrary()` creates it by calling
   * `clib.NewInit()`, which you have to write by hand — it is the single obligation
   * genffi places on the implementation side.
   * @singleton
   */
  export interface JobQueue {
    create: (jobType: string) => GenModel.Config.Job;

    /**
     * The same thing through a `@scoped` listener, whose Java overload takes the interface
     * itself and manages the registration around the call. A single-method listener, so the
     * call site can be a lambda — which is the whole point of the tag and the one thing a
     * compile cannot prove is *usable*.
     * @scoped
     */
    push: (
      cfg: GenModel.Config.Job,
      onProgress: JobListener,
    ) => Promise<GenModel.Config.Job>;

    get: (id: string) => GenModel.Config.Job;
  }

  /**
   * The one globally reachable object. `InitLibrary()` creates it by calling
   * `clib.NewInit()`, which you have to write by hand — it is the single obligation
   * genffi places on the implementation side.
   * @singleton
   */
  export interface JobProcessors {
    register: (jobType: string, processor: JobProcessor) => boolean;
  }
}
