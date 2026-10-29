# Hoplum build

The `hoplum` branch is upstream `main` plus the patches Hoplum runs and has
proposed upstream. Each patch lives on its own branch, merged here:

| Branch | Upstream PR | Purpose |
|---|---|---|
| `libvirt-cpu-topology` | [#46](https://github.com/NetwindHQ/gha-outrunner/pull/46) | Windows guests use every vCPU |
| `libvirt-tpm` | [#47](https://github.com/NetwindHQ/gha-outrunner/pull/47) | Optional emulated TPM 2.0 |
| `libvirt-firmware` | [#48](https://github.com/NetwindHQ/gha-outrunner/pull/48) | Explicit UEFI firmware, so Secure Boot can be off |
| `libvirt-resolve-base-symlink` | [#53](https://github.com/NetwindHQ/gha-outrunner/pull/53) | Symlinked `path` resolved per VM |
| `shutdown-on-sigterm` | not yet opened | Clean shutdown when systemd or launchd stops the service |

## Update

1. `git fetch upstream`, then merge `upstream/main` into each patch branch
   still open upstream, and push it (this updates its PR).
2. On `hoplum`, merge `upstream/main` and every updated patch branch, then
   push. A patch upstream has merged becomes a no-op merge.
3. Drop a patch from the table once upstream has merged its PR.

## Release

`scripts/hoplum-release.sh` tests, builds linux/amd64, linux/arm64 and
darwin/arm64 binaries stamped with the version, and publishes them as a
`<upstream tag>-hoplum.<n>` release on `nel/gha-outrunner`.
