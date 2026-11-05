# ONREZA SlateDB fork

This fork carries four source changes on SlateDB v0.18.0, commit
`66b5672446423a32c63b8ccd73d6e9aad1867011`:

- Make YAML settings optional through `config-yaml`. Default features retain YAML support.
- Release native tasks and owned resources when database setup or recovery fails.
- Retry object-store read conflicts within existing retry limits. Preserve terminal errors.
- Add manifest fencing and bounded SST collection for retired databases.

Retired collection requires caller-proven writer quiescence and a complete set of protected roots.
LIST results stream without full buffering, so authorization is checked between objects.
Read the API safety requirements before calling it. This API cannot establish retirement authority.

The fork retains upstream schemas, generated Rust files, workspace crates, and license.
A patch for a published crate manifest is unnecessary in this source workspace.

## Use with Cargo

Pin a reviewed commit from `fix/onreza-storage` in the application manifest:

```toml
[patch.crates-io]
slatedb = { git = "ssh://git@github.com/ONREZA/slatedb.git", rev = "<full-commit-sha>" }
```

Commit the resulting `Cargo.lock`. Public HTTPS access also works without GitHub credentials.
SSH access requires a GitHub SSH key or agent on each build host.

## Targeted checks

```sh
cargo test --locked -p slatedb --no-default-features --features aws,moka \
  --lib config::tests
cargo test --locked -p slatedb --no-default-features --features aws,moka \
  --lib retrying_object_store::tests
cargo test --locked -p slatedb --no-default-features --features aws,moka \
  --test retired_gc
cargo test --locked -p slatedb --no-default-features --features aws,moka,config-yaml \
  --lib config::tests
```

Application lifecycle tests remain necessary when integrating this fork.
