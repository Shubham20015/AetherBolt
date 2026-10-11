# ADR 0002: Begin with trusted built-in handlers, then move execution to Docker

**Status:** Accepted.

Phase 1 uses trusted in-process handlers only to validate the HTTP contract, registry behavior, error mapping, and invocation lifecycle cheaply. They implement the same small runtime-facing interface expected by later phases.

From Phase 2, uploaded or user-authored code must execute in containers, never in the API process. This adds startup and Docker operational complexity but establishes the necessary trust boundary. Process-only execution is rejected for untrusted artifacts; microVMs are deferred due to local-project complexity.
