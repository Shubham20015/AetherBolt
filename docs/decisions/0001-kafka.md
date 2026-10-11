# ADR 0001: Use Kafka for asynchronous invocation delivery

**Status:** Accepted for Phase 4 design.

Kafka is selected for its durable, partitioned event log, replay capability, consumer groups, and direct relevance to distributed-systems workloads. Invocation work will be keyed by function/version as appropriate, with the consumer group providing horizontal processing.

The trade-off is greater local and operational complexity than NATS JetStream: a Kafka-compatible broker needs more memory, topic/retention configuration, partition planning, consumer-lag monitoring, and careful offset handling. This is acceptable because asynchronous execution is a later, explicitly distributed-systems phase. The platform’s delivery guarantee remains at-least-once; offsets are committed only after the durable invocation transition, and business handlers still need idempotency.

NATS JetStream remains a viable lower-operations alternative for a compact deployment. A narrow `queue` interface preserves the adapter boundary, but does not hide delivery semantics.
