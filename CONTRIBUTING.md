# Contributing to this fork

Development and releases are maintained in `nkgotcode/iec104`. Open issues and PRs here; upstream acceptance is not a prerequisite. Preserve the original project's MIT attribution and history. Upstream remains configured as a source of changes that can be reviewed and integrated.

Use Rust 1.99.0 from `rust-toolchain.toml`. Run:

```sh
rustup toolchain install nightly-2026-10-13 --profile minimal --component rustfmt
cargo +nightly-2026-10-13 fmt -- --check
sh scripts/check.sh
sh scripts/check.sh native_tls
sh scripts/check.sh rustls
```

The script runs locked tests, strict Clippy and documentation checks. CI also retains typo checking and unused-dependency checking (`cargo-udeps` 0.1.61 on the pinned nightly, installed from its official release binary with a pinned SHA-256 checksum). CI uses an official Rust image pinned by digest, without depending on the upstream author's container. Native TLS requires OpenSSL development libraries, supplied by that image. The TLS features are mutually exclusive. A future change must remove a narrow `clippy::double_must_use` expectation if async-trait no longer generates the redundant attribute; these expectations do not disable warnings globally.

Protocol fixes should include independently constructed wire tests and failing-before/passing-after evidence where practical. State which profiles and peer implementations were exercised. Do not infer complete protocol conformance from unit tests alone. Report security concerns privately using SECURITY.md.

## Development releases

This fork currently retains the library name `iec104` for source compatibility and uses version `0.5.2-dev.0`. It is consumed by a full Git commit SHA; it is not published to crates.io. `publish = false` prevents accidental publication under the upstream package name. Choose a distinct registry name before any future crates.io publication.

Before creating a `fork-v*` tag:

1. Update CHANGELOG.md with behavior changes, compatibility impact and known limitations.
2. Pass the complete check matrix on the exact commit; inspect CI on that commit.
3. Review runtime/security changes and check dependency license notices.
4. Pin downstream users to the tested commit and run their integration tests.
5. Create an annotated tag and release notes identifying this fork and the evidence. Do not describe a development release as production-certified.

Release publication is deliberate; pushes do not automatically publish packages or GitHub releases. Interoperability with independent implementations must be verified separately.
