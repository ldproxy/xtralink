# xtrasync

`xtrasync` synchronizes content from external sources into a local target directory.
Currently supported package types:

- `GIT`
- `OCI`
- `S3`

Main commands live under `pkg`:

- `pkg pull` (synchronize configured packages)
- `pkg push` (synchronize one package by `id` and push it as OCI artifact)

## What does the tool do?

- Reads a YAML control configuration.
- Processes all entries under `packages`.
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
packages:
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
  - Default: the package `id`.
- `path`: optional subdirectory in the remote content.
  - Must point to a **directory** (not a single file).
- `id`: Used to resolve matching credentials from env (`USER_<ID>`, `PASSWORD_<ID>`).

## Credentials (GIT / OCI / S3)

For all three drivers (`GIT`, `OCI`, `S3`), the same precedence applies:

1. `user` / `password` directly in the corresponding remote block (control config).
2. If empty there: environment variables `USER_<ID>` / `PASSWORD_<ID>`.

Resolution of `USER_<ID>` / `PASSWORD_<ID>` is performed centrally while loading
the configuration (`app/settings.go`). Drivers then only use already-resolved values
in `remote.user` and `remote.password`.

Example for `id: bplan`:

- `USER_BPLAN`
- `PASSWORD_BPLAN`

If a `.env` file exists in the project directory, `xtrasync` loads it automatically.
So you can define credentials there (e.g. `USER_BPLAN=...`) without needing shell `export` commands.

## Examples by type

### GIT

```yaml
targetDir: .
packages:
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
packages:
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
packages:
  - type: S3
    id: bplan
    url: https://s3.example.net/my-bucket
    path: folder/subfolder
    localPath: config/synced/s3-data
```

S3 note: Access Key / Secret must be provided as `user` / `password`.

## Run

`--config` is optional. If omitted, the default config file is `.xtrasync.yml`.

Pull all configured packages (default config `.xtrasync.yml`):

```bash
go run . pkg pull
```

Pull only one package by id:

```bash
go run . pkg pull bplan
```

Pull all packages with custom config:

```bash
go run . --config config/exampleConfig.yaml pkg pull
```

Use built binary instead of `go run`:

```bash
go build -o xtrasync . && ./xtrasync pkg pull
```

## Push command (`pkg push`)

With `pkg push`, you select a package by `id` from the control configuration. That
package is first synchronized locally (same as `pkg pull`). Then the local content is
packaged as ZIP and pushed as an OCI artifact.

- Target registry/repository is provided as positional `<image>` argument
  (e.g. `ghcr.io/org/name` or `docker.ci.interactive-instruments.de/xtrasync/name`)
- Artifact Type: `application/vnd.iide.xtrapkg`

Example:

```bash
go run . pkg push bplan ghcr.io/my-org/my-bplan:latest
```

Example for your registry:

```bash
go run . pkg push bplan docker.ci.interactive-instruments.de/xtrasync/test-bplan:latest
```

Important: for `pkg push`, pass image as `registry/path/name[:tag]` **without** `oci://` prefix.
So use `docker.ci.../repo:tag`, not `oci://docker.ci.../repo:tag`.

This push example also uses the default `.xtrasync.yml` config file.

Custom config file example:

```bash
go run . --config config/all.yaml pkg push bplan docker.ci.interactive-instruments.de/xtrasync/my-bplan:latest
```

## Useful paths and test commands

Git cache directory:

```bash
echo "$TMPDIR"xtrasync-cache/git
```

Run integration tests in drivers package:

```bash
go test ./lib/drivers -run TestIntegrationSync_ -v -count=1
```

Note: Push credentials are resolved the same way as in the drivers:

1. `user` / `password` in the remote block
2. otherwise `USER_<ID>` / `PASSWORD_<ID>` from env or `.env`
