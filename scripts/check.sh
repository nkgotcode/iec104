#!/bin/sh
# Matches CI. Pass one feature set; native_tls and rustls are mutually exclusive.
set -eu
features=${1:-}
cargo test --locked --workspace --features "$features"
cargo clippy --locked --workspace --all-targets --features "$features" -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --features "$features"
