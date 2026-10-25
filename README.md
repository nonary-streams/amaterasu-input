# Standalone Amaterasu input source (publication candidate)

This workspace holds the native input module and OS companion. The functional
contract is in `modules/input/README.md`. Its proposed portable Cargo pins
reference exact commits of `nonary-streams/amaterasu-sdk` and
`nonary-streams/amaterasu-protocol`; the committed lockfile records the locally
validated resolution. Run `cargo test --workspace --locked --offline --no-run`
for an offline build; socket and device tests need separately authorized
qualification. The current input-lifecycle observation source is included.

The host still owns signed package selection, companion permits and default-off
input grants. Publishing this source neither activates input nor lets an
extension authorize itself. Amaterasu owns this repository; third parties
consume its contracts in their own repositories. The source license remains
pending.
