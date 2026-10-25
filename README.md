# Standalone Amaterasu input source proof

Extracted from committed Amaterasu `a6b0db9e36a1a59328ab4b4785b27b210f8182b7`. This local-only
workspace owns the native input module and its OS companion implementation.
Original module documentation remains in `modules/input/README.md`.

The module/companion source and their service identities are unchanged. Shared
SDK/storage dependencies are pinned to foundation `28da5667f73e26d812b0e485f50f0a5c77c5e4f0`;
GameStream and input policy are pinned to protocol `32c64d07b80fce5779b530057757476cdfae4b3d`. The
protocol repository pins the same foundation revision, avoiding duplicate Rust
ABI/contract crate identities. These `file://` URLs are local proofs only; a
stable approved location and regenerated lockfile are required for portability.

Host-owned `crates/host/src/input_companion.rs` and `linux_input.rs` remain in
the host repository. They validate the selected signed package digest and
construct the keyboard/mouse/gamepad permit. Release profiles own default-off
settings and grants. This repository cannot authorize itself or activate a
device. No live input, host policy, service or permission is changed.
