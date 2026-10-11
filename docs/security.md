# Security model and limitations

## Trust boundaries

- Clients are untrusted: authenticate/authorize them, validate requests, cap bodies, and rate-limit.
- Function artifacts and invocation payloads are untrusted: never execute uploaded code in the API process.
- PostgreSQL, Kafka, Docker, metrics, and dashboards are private infrastructure with credentials managed outside source control.

## Required controls by later phase

- API authentication and role-based authorization: deploy/manage versus invoke/read logs; audit sensitive mutations.
- Artifact allowlisting/provenance, checksums, immutable references, malware/dependency scanning policy, and bounded storage/retention.
- Docker workloads: non-root user, non-privileged, no Docker socket mount, dropped capabilities, `no-new-privileges`, read-only filesystem when compatible, memory/CPU/pid limits, scoped writable temp directory, and egress restrictions.
- Runtime: deadline propagation, forced cleanup, output-size limits, redaction of configured secret values, and no event payload logging by default.
- Infrastructure: TLS/auth for external deployment, least-privilege database/broker identities, configuration through environment or a secret provider, never image layers/repository/logs.

## Non-claims

Containers are a defense layer, not a complete tenant-isolation guarantee. A hostile multi-tenant production design needs stronger sandboxing, kernel hardening, network policy, image provenance, monitoring, and independent security review. Docker daemon access is privileged; it must remain exclusively on trusted platform components.
