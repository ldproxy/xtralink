# xtrasync

`xtrasync` synchronizes content from external sources into a local target directory.
Currently supported remote types:

- `GIT`
- `OCI`
- `S3`

Additional command:

- `push` (synchronizes a remote by `id` and pushes the result as an OCI artifact)

## What does the tool do?

- Reads a YAML control configuration.
- Processes all entries under `remotes`.
- Fetches/updates content depending on the remote type.
- Mirrors the result into your target (`targetDir` + `localPath`).

## Where should the control configuration be stored?

You can place the file anywhere; using `config/` is a common convention.

Example:

- `config/exampleConfig.yaml`
- `config/oci-ghcr-test.yaml`

## Configuration schema

```yaml
targetDir: .
remotes:
  - type: GIT|OCI|S3
    id: "..."
    url: "..."
    tag: "..." # optional (e.g. branch/tag/ref)
    user: "..." # optional
    password: "..." # optional
    path: "..." # optional, directory subpath only
    localPath: "..." # optional, target path relative to targetDir (default: id)
```

### Important fields

- `targetDir`: Base target directory for all remotes.
- `localPath`: Optional target path relative to `targetDir`.
  - Default: the remote `id`.
- `path`: optional subdirectory in the remote content.
  - Must point to a **directory** (not a single file).
- `id`: Used to resolve matching credentials from env (`user_<id>`, `password_<id>`).

## Credentials (GIT / OCI / S3)

For all three drivers (`GIT`, `OCI`, `S3`), the same precedence applies:

1. `user` / `password` directly in the corresponding remote block (control config).
2. If empty there: environment variables `user_<id>` / `password_<id>`.

Resolution of `user_<id>` / `password_<id>` is performed centrally while loading
the configuration (`app/load.go`). Drivers then only use already-resolved values
in `remote.user` and `remote.password`.

Example for `id: bplan`:

- `user_bplan`
- `password_bplan`

If a `.env` file exists in the project directory, `xtrasync` loads it automatically.
So you can define credentials there (e.g. `user_bplan=...`) without needing shell `export` commands.

## Examples by type

### GIT

```yaml
targetDir: .
remotes:
  - type: GIT
    id: git-base
    url: https://github.com/example/repo.git
    tag: main
    path: configs/base
    localPath: config/synced/repo-base
```

### OCI

```yaml
targetDir: .
remotes:
  - type: OCI
    id: talos
    url: oci://ghcr.io/interactive-instruments/talos-config
    tag: "0.9.3"
    user: "<github-user>"
    password: "<pat>"
    localPath: config/synced/oci-talos-config
```

OCI note: currently the tool supports artifacts where the **first layer** contains
the relevant payload ZIP.

### S3

```yaml
targetDir: .
remotes:
  - type: S3
    id: bplan
    url: https://s3.example.net/my-bucket
    path: folder/subfolder
    localPath: config/synced/s3-data
```

S3 note: Access Key / Secret must be provided as `user` / `password`.

## Run

```bash
go run . --config config/<your-file>.yaml sync
```

Example:

```bash
go run . --config config/oci-ghcr-test.yaml sync
```

## Push command

With `push`, you select a remote by `id` from the control configuration. That
remote is first synchronized locally (same as `sync`). Then the local content is
packaged as ZIP and pushed as an OCI artifact.

- Target registry: `docker.ci.interactive-instruments.de/xtrasync/<image>`
- Artifact Type: `application/vnd.iide.xtrapkg`

Example:

```bash
go run . --config config/all.yaml push --id bplan --image my-bplan --tag latest
```

Note: Push credentials are resolved the same way as in the drivers:

1. `user` / `password` in the remote block
2. otherwise `user_<id>` / `password_<id>` from env or `.env`
