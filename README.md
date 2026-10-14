# iec104 — independently maintained fork

[![Rust checks](https://github.com/nkgotcode/iec104/actions/workflows/rust.yaml/badge.svg?branch=main)](https://github.com/nkgotcode/iec104/actions/workflows/rust.yaml)

Development repository: **[nkgotcode/iec104](https://github.com/nkgotcode/iec104)**. Based on [mzaniolo/iec104](https://github.com/mzaniolo/iec104); the original MIT copyright and license are preserved. This fork owns its fixes, CI and development revisions independently of upstream review.

**Development status:** experimental. See [changes](CHANGELOG.md), [contribution and release checks](CONTRIBUTING.md), [security reporting](SECURITY.md), and [our issue tracker](https://github.com/nkgotcode/iec104/issues). The crate name remains `iec104` for compatibility, but this fork is Git-only (`publish = false`). Consume a tested full commit SHA, not a moving branch.

The docs.rs links below describe the upstream API, not separately published fork documentation. Generate the exact fork docs with `cargo doc --locked --no-deps`.


Rust library for **[IEC 60870-5-104](https://en.wikipedia.org/wiki/IEC_60870-5#IEC_60870-5-104)** (IEC 104 over TCP): APDUs, ASDUs, type IDs, and async I/O on **Tokio**. The focus is **balanced** (station-to-station) communication as used in energy and automation SCADA.

**Status:** usable for client work, low-level servers, and the bundled RTU-style server layer, but the API and behavior are still evolving. Report issues and try the examples before relying on it in production.


## What this crate provides

| Area | Module | Role |
| ---- | ------ | ---- |
| **Client** | [`iec104::client`](https://docs.rs/iec104/latest/iec104/client/index.html) | Connect to a remote IEC 104 station, send commands, receive ASDUs via [`ClientCallback`](https://docs.rs/iec104/latest/iec104/client/trait.ClientCallback.html). Optional **TLS** is configured on [`ClientConfig`](https://docs.rs/iec104/latest/iec104/config/struct.ClientConfig.html) ([`TlsClientConfig`](https://docs.rs/iec104/latest/iec104/config/struct.TlsClientConfig.html)). |
| **Server (low level)** | [`iec104::server`](https://docs.rs/iec104/latest/iec104/server/index.html) | Accept TCP connections, run the 104 link layer (STARTDT / TESTFR / …), decode ingress ASDUs, and deliver them to your [`ServerCallback`](https://docs.rs/iec104/latest/iec104/server/trait.ServerCallback.html). You implement all application logic (interrogations, spontaneous data, command handling). TLS is optional via [`ServerConfig`](https://docs.rs/iec104/latest/iec104/config/struct.ServerConfig.html) / [`TlsServerConfig`](https://docs.rs/iec104/latest/iec104/config/struct.TlsServerConfig.html). |
| **RTU server (high level)** | [`iec104::rtu_server`](https://docs.rs/iec104/latest/iec104/rtu_server/index.html) | In-memory **typed points**, general and group interrogation, counter interrogation, pluggable **process commands** ([`RtuCommandHandler`](https://docs.rs/iec104/latest/iec104/rtu_server/trait.RtuCommandHandler.html)), and default **system ASDUs** (clock sync, test, read, reset process, etc.). Built on the low-level `Server`. |
| **Types & parsing** | [`iec104::types`](https://docs.rs/iec104/latest/iec104/types/index.html), [`iec104::asdu`](https://docs.rs/iec104/latest/iec104/asdu/index.html), [`iec104::types_id`](https://docs.rs/iec104/latest/iec104/types_id/index.html) | ASDU building blocks, information objects, and type IDs used by client and server paths. |

This is **IEC 60870-5-104 only** (not 101 serial profiles). File transfer, integrated totals beyond what the RTU layer models, and every optional standard feature are not goals for v1 completeness—check [`TypeId`](https://docs.rs/iec104/latest/iec104/types_id/enum.TypeId.html) and module docs for what is encoded and parsed today.

## Connection limits and shutdown

`ServerConfig` now defaults to at most 64 accepted connections, counting pending
TLS handshakes, with a 10-second TLS handshake deadline. Both values must be
nonzero. Set `max_connections` and `tls_handshake_timeout` explicitly when needed;
existing serialized configurations receive the defaults. Rust struct literals
must add these fields or use `..ServerConfig::default()`.

TLS setup runs independently for each admitted peer. A failed or stalled handshake
does not stop the listener or block another admitted peer. Client TLS setup uses
`ClientConfig::protocol.t0` as its handshake deadline.

Call `Server::shutdown().await`, `RtuServerHandle::shutdown().await`, or
`Client::shutdown().await` to cancel and join their owned work. Server/RTU shutdown
applies to all clones. Pending I/O, callbacks and queued output are cancelled,
without flushing or sending STOPDT; callbacks must be cancellation-safe and must
not block executor threads. After successful shutdown, owned tasks are joined and
sockets released. Repeated calls are safe; a client can reconnect afterward.
Dropping the last server/RTU handle or dropping a client requests cancellation
without waiting. Retain a handle and await shutdown when completion matters.
`command_capacity` (default 256) bounds server commands, per-peer commands and
in-flight deliveries. `delivery_timeout` (default 1 second) bounds delivery to a
peer's protocol task; expiry disconnects that peer. A broadcast attempts every
started peer before waiting for results, so a stalled or full peer cannot prevent
another peer from receiving it. Success means local protocol admission, not a
remote acknowledgement. A failed broadcast may have delivered to some peers;
blindly retrying it can duplicate data.

`rtu_queue_capacity` (default 1024) bounds the model mailbox. Local handle calls
return `RtuHandleError::Overloaded` without enqueueing when it is full. Network
ingress closes the overflowing peer, rather than acknowledging application
acceptance or silently dropping requests. All three new settings must be nonzero.
`max_pending_outgoing_asdu` still bounds the separate protocol-window queue;
its existing explicit zero/unlimited option is unchanged.

Use `Server::request_shutdown()` or `RtuServerHandle::request_shutdown()` inside
callbacks, then await `shutdown()` from an external task. A shutdown call from
the task it would join requests cancellation and returns an error instead of
self-joining. Callbacks must not retain handles in permanent ownership cycles.

## Data-transfer sessions

STOPDT pauses data transfer on the existing TCP connection. Sequence counters,
acknowledgements, protocol-window output queues and coalesced input survive a
subsequent STARTDT. No queued I-frame is transmitted while stopped. TESTFR and
S-frame acknowledgements continue to be processed. STARTDT/STOPDT confirmation
deadlines are independent from TESTFR deadlines. Unsolicited confirmations fail
the session instead of clearing unrelated timers.

`on_connection_stopped` now specifically reports STOPDT; TCP loss is reported
through `on_connection_disconnected`. Update callbacks that previously treated
server `on_connection_stopped` as a disconnect notification. Explicit shutdown
still cancels callbacks; it does not promise a final notification. An RTU emits
end-of-initialization only on the first STARTDT of each TCP connection.

Client reconnection creates fresh sequence state and rejects queued commands
from the previous connection. There is no implicit command replay across a
connection loss. A successful local send is not proof of remote execution.

## Deferred commands and selection

Override `RtuCommandHandler::prepare_command` to return either an immediate
`CommandHandling` result or `CommandDispatch::Deferred(DeferredCommand::new(...))`.
Preparation must not actuate. The deferred future is first polled after a positive
ACT_CON enters the link queue; successful completion emits ACT_TERM. This is local
admission, not proof that the peer has received the confirmation. Existing
`handle_command` implementations remain supported as immediate handlers.

Command work is bounded by `command_capacity`, with at most one in flight per
connection. Telemetry and interrogation continue while a command future waits.
`command_timeout` (default 10 seconds) applies separately to preparation and
execution. Preparation timeout rejects the command. Failure, panic, invalid
completion updates or timeout after acceptance closes the connection; it never
claims successful completion or invents a type-independent negative ACT_TERM.
Connection loss means the external outcome may be unknown. The application must
implement its own recovery and idempotency policy before retrying an operation.

`CommandContext::session` is permanently cancelled on STOPDT, disconnect or
shutdown. Restart creates a fresh session token. Owned futures are cancelled and
joined during shutdown, and guarded replies queued behind a full transmit window
cannot leak into a restarted session. Dropping a future cannot undo an external
action already performed: adapters must honour cancellation and must not detach
untracked actuator tasks.

Preparation sees a model snapshot. Immediate updates are rejected if their target
has changed since that snapshot; all updates are validated before mutation.
Negative replies never apply updates. Commands must contain one object; test,
negative and Request-COT process commands are rejected without invoking execution.

Wrap a handler with `SelectBeforeExecute::new(handler, timeout, capacity)` for
mandatory selection. The handler must explicitly authorize `select` without
actuating. Reservations are bounded, expire, belong to one data-transfer session,
and are consumed once. Type, common address, IOA, originator, value, qualifier and
timestamp must match exactly except for the S/E bit. Wrong-master/value/address
executions, expired selections and duplicate executes are rejected. Deactivation
cancels a matching reservation. Bitstring commands have no S/E field and are
rejected by this wrapper. `SelectionState` is also available for custom policies.

These mechanisms provide protocol lifecycle management, not application
permission checks, interlocks or equipment safety logic.

## Explicit system-command policy

`RtuSystemHandlers::monitoring_only()` allows test/read requests and ordinary
counter reads, while rejecting clock sync, reset process and counter freeze/reset.
Combine it with `RejectAllCommands` for process commands. Individual opt-in presets
`RejectClockSync`, `RejectResetProcess` and `ReadOnlyCounterInterrogation` can also
be composed with custom handlers. Defaults remain unchanged.

A positive default clock-sync reply only stores the supplied timestamp; it does
not set the operating-system clock. A positive default reset reply does not
perform an external reset. Implementations must define and implement their own
acceptance boundary before acknowledging those operations.

See `cargo run --example monitoring_server` for explicit capability selection.

## Examples

Build and run from the repository root:

```bash
cargo run --example client
cargo run --example server
cargo run --example rtu_server
```

| Example | Description |
| ------- | ----------- |
| [`examples/client.rs`](examples/client.rs) | Minimal **master/client**: connect, start receiving, demonstrate sending a process command. |
| [`examples/server.rs`](examples/server.rs) | Minimal **slave/server** callback: accept connections and react to incoming ASDUs yourself. |
| [`examples/rtu_server.rs`](examples/rtu_server.rs) | **RTU-style station** with a documented point table (common address **47**), interrogation, counter interrogation, clock sync, and command presets—intended for learning and for the Python harness below. |

## Python client / integration harness

End-to-end checks against `examples/rtu_server` live under [`tests/`](tests/):

- Start `cargo run --example rtu_server`, then run [`tests/test_client.py`](tests/test_client.py) in another terminal.
- The Python harness expects the **[iec104-python](https://pypi.org/project/c104/)** module (`c104`) in the environment (for example in a project `.venv`).

The RTU example is aligned with that client for general interrogation, clock sync, counter interrogation, group interrogation, **C_TS_TA_1** test command, and process commands. Note: interoperability always depends on both stacks’ choices (type IDs, COTs, timeouts).

## Cargo features

| Feature | Default | Effect |
| ------- | ------- | ------ |
| `chrono` | no | Conversions between IEC time tags ([`Cp24Time2a`](https://docs.rs/iec104/latest/iec104/types/time/struct.Cp24Time2a.html), [`Cp56Time2a`](https://docs.rs/iec104/latest/iec104/types/time/struct.Cp56Time2a.html)) and **chrono** `DateTime`. |
| `time` | no | Conversions between IEC time tags and the **time** crate. |
| `native_tls` | no | TLS via **tokio-native-tls** (OpenSSL/system TLS). Enable this to use [`TlsClientConfig`](https://docs.rs/iec104/latest/iec104/config/struct.TlsClientConfig.html) / [`TlsServerConfig`](https://docs.rs/iec104/latest/iec104/config/struct.TlsServerConfig.html). |
| `rustls` | no | TLS via **tokio-rustls** (pure Rust, no OpenSSL). Mutually exclusive with `native_tls`. |

By default the crate has **no TLS dependencies** and accepts plain TCP only. If `tls` is set in config without enabling a TLS feature, connection setup returns an error.

For a deployed application, add `rev` with the tested full commit SHA. The examples below show the source and feature selection only.

**Breaking change (0.5+):** TLS is no longer enabled implicitly. Downstream crates that use TLS must opt in:

```toml
iec104 = { git = "https://github.com/nkgotcode/iec104", features = ["native_tls"] }
# or
iec104 = { git = "https://github.com/nkgotcode/iec104", features = ["rustls"] }
```

## Requirements

- **Rust** with support for the edition and features declared in `Cargo.toml` (currently **Edition 2024**).
- **Tokio** runtime for async networking.

## Limitations and unsupported areas

- **Work in progress:** public APIs and default RTU behavior may change between minor releases until the crate stabilises.
- **Coverage:** not every standard type ID or edge case is implemented or battle-tested; prefer explicit tests for your IO list.
- **Low-level `Server`:** does not implement an RTU process image—you get raw ASDUs and must send replies yourself.
- **Security:** TLS is available for transport when the `native_tls` or `rustls` feature is enabled; application-layer security (authentication, authorisation) is outside IEC 104 and not provided here.
- **101 / serial:** out of scope.

## Contributing

Contributions are welcome and **encouraged**. Please run tests and formatters locally before opening a pull request.

## Pre-commit

Hooks are optional but useful for consistent style:

1. Install [pre-commit](https://pre-commit.com) (package manager or `pip install --user pre-commit`).
2. `pre-commit autoupdate` — refresh hook revisions.
3. `pre-commit install` — enable hooks in this clone.
