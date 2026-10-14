# Changes in this fork

## Unreleased — floating-point command snapshots

- Compare floating-point monitoring snapshots by representation, preserving unchanged NaN payloads while rejecting changed bits, quality or timestamps. Public numeric equality remains unchanged.
- Add real-TCP immediate-command recovery and concurrent-update rejection tests for all three floating-point monitoring types.

## Unreleased — framing and sequence serialization

- Reject mismatched APDU lengths, reserved S-frame bits, ambiguous U-frame functions and out-of-range sequence counters.
- Serialize sequence-addressed objects with one initial address; reject nonconsecutive or overflowing addresses and mismatched payload types.
- Validate object counts in the public information-object parser and avoid zero-sized chunk panics.
- Add bounded deterministic parser mutations, all-type VSQ/short-input coverage, and real TCP fragmentation interleaved with outgoing work. These checks supplement, not replace, external interoperability or conformance testing.


## Unreleased — authenticated TLS admission

- Add required client certificate verification with a configured PEM trust bundle on rustls. Reject unsupported client-authentication policy on native_tls.
- Expose verified peer certificate identity to an optional low-level authorization callback before protocol admission. Bound admission callbacks per connection.
- Exercise actual TLS trust, hostname, missing/expired/untrusted certificate and authorization failures with public test credentials.
- TlsServerConfig gains client_ca_certificates; serialized configurations default to None, while exhaustive Rust literals need the new field.

## Unreleased — command lifecycle

- Add bounded deferred command preparation/completion without blocking telemetry or interrogation; positive ACT_TERM is sent only on success.
- Add permanent session cancellation, guarded replies and joined shutdown of command futures. Accepted failures/timeouts close the peer instead of claiming success.
- Add optional bounded SelectBeforeExecute/SelectionState policies with expiry, ownership, exact matching and one-use execution.
- Validate model updates atomically; reject stale immediate updates, unsupported multi-object/test/Request process commands and concurrent commands from the same peer.
- Add command_timeout configuration and synchronous nonblocking connection-close notifications for cancellation.

## Unreleased — session continuity

- Retain sequence, acknowledgement and pending-output state across STOPDT/STARTDT on one TCP connection, including coalesced input.
- Continue link supervision while stopped; separate transfer-confirmation and TESTFR deadlines.
- Add distinct disconnect callbacks. Server stopped callbacks now mean STOPDT, not TCP loss.
- Send RTU initialization once per connection and reject stale client commands across reconnects.
- Add independent wire regressions for both peers, queued output, initialization and deadline isolation.

## Unreleased — bounded delivery

- Bound the RTU mailbox, server command queues and concurrent delivery tasks with configurable defaults and explicit overload errors.
- Attempt broadcast delivery to every started peer independently; abort peers that miss the delivery deadline and reclaim their admission slots.
- Add fallible callback admission hooks while preserving existing callback implementations.
- Add callback-safe shutdown requests and reject self-joins (follow-up to the review on #8).
- Add real-TCP overload, slow-peer and callback-shutdown regressions. Public configuration struct literals require new fields or defaults.


## 0.5.2-dev.0 — unreleased development revision

Based on upstream 0.5.1, commit `162cbead586de10d2a22687ab9d4f5b30946091c`.

- Add opt-in clock/reset rejection and read-only counter presets, with a monitoring-only composition, example and wire-level regression tests. Existing defaults remain unchanged.
- Isolate TLS handshakes from the listener loop, enforce a handshake deadline, and keep failed peers from stopping the listener. Client TLS setup also has a deadline.
- Bound accepted sockets (including handshakes) to 64 by default. `ServerConfig` adds `max_connections` and `tls_handshake_timeout`; serialized configs have defaults, while exhaustive Rust literals must be updated.
- Add awaited, idempotent shutdown to server, client and RTU handles. Cancel and join I/O, callbacks and reconnect work; last-handle drop requests cancellation. Pending output is discarded.
- Validate the RTU model before binding so invalid models cannot leave a listener behind.
- Add real TLS transport and lifecycle/admission regression tests with certificate verification enabled.
- Acknowledge received I-frames at each configured `w` threshold, including coalesced reads; add three independent raw-TCP regressions. Includes upstream contribution #30.
- Establish independent repository ownership, issue reporting and pinned CI/tooling.
- Address Rust 1.99 diagnostics: use early-return macros as statements and reborrow mutable connection references. Narrow expectations document redundant attributes generated by async-trait 0.1.89.
- Repair public documentation links, including optional time-library references.
- Retain the original MIT license and Git history; mark the fork as Git-only and prevent accidental registry publication.

This is not a production-ready release. Lifecycle, bounded-resource behavior, command completion, interoperability and security work remains tracked in this fork. Full protocol conformance has not been established.
