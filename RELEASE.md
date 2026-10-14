# Releasing

The 0.2.0 source release is licensed under MIT OR Apache-2.0. GitHub tags and release archives are public. Registry publication requires authenticated crates.io access and must be verified separately; a GitHub release does not imply a crates.io release.

## Checks before publication

1. Update version, changelog, and verification record together.
2. Run formatting, Clippy, unit/doctests, and warning-free documentation builds.
3. Run all dedicated-broker tests and examples on each supported server version.
4. Review `cargo package --list`, then run `cargo package --locked` to compile the packaged source.
5. Confirm the compatibility CI matrix passes for the exact release commit.
6. Confirm registry ownership and authenticate locally with `cargo login`; never place tokens in repository files, logs, or issue comments.
7. Run `cargo publish --locked`, verify the published version on crates.io, and change the README installation example to the registry version.
8. Tag the verified release commit and create the corresponding GitHub release. If registry access is unavailable, ship the GitHub release with an explicit registry-pending notice and the tagged Git dependency.

For each release, record actual tests and limitations. Do not imply network fault tolerance or performance results from deterministic backend tests.
