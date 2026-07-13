# xtralink

`xtralink` synchronizes content from external sources into a local target directory.
Currently supported package types:

- `GIT`
- `OCI`
- `S3`

Main commands live under `pkg`:

- `pkg pull` (synchronize configured packages)
- `pkg push` (synchronize one package by `id` and push it as OCI artifact)
- `pkg inspect` (analyze one package and print JSON report)

## What does the tool do?

- Reads a YAML control configuration (`.xtralink.yml`).
- Processes all entries under `packages`.
- Fetches/updates content depending on the remote type.
- Mirrors the result into your target (`targetDir` + `localPath`).

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

If a `.env` file exists in the project directory, `xtralink` loads it automatically.
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
    url: oci://ghcr.io/example/repo
    tag: "0.9.3"
    user: "<github-user>"
    password: "<pat>"
    localPath: config/synced/oci-example-repo
```

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

`--config` is optional. If omitted, the default config file is `.xtralink.yml`.

Pull all configured packages (default config `.xtralink.yml`):

```bash
xtralink pkg pull
```

Pull only one package by id:

```bash
xtralink pkg pull bplan
```

Pull all packages with custom config:

```bash
xtralink --config config/exampleConfig.yaml pkg pull
```

Inspect one package and print a JSON report:

```bash
xtralink pkg inspect bplan
```

`pkg inspect` currently reports:

- `entities`
  - service/provider type counts
- `substitutions`
  - all detected `${...}` placeholders in YAML files
  - includes `file`, `path`, `name`, and optional `default`
- `data-sources`
  - detected sources from `resources/features` and `db`
  - usage detection based on provider configs under `entities/instances/providers`
  - source classes currently include `GPKG`, `PGIS/DUMP`, and `PGIS/REF`
  - when resolving data-source related values from YAML, substitution defaults are used if available

## Push command (`pkg push`)

With `pkg push`, you select a package by `id` from the control configuration. That
package is first synchronized locally (same as `pkg pull`). Then the local content is
packaged as ZIP and pushed as an OCI artifact.

- Target registry/repository is provided as positional `<image>` argument (e.g. `ghcr.io/org/name`)
- Artifact Type: `application/vnd.iide.xtrapkg`

Example:

```bash
xtralink pkg push bplan ghcr.io/my-org/my-bplan:latest
```

Important: for `pkg push`, pass image as `registry/path/name[:tag]` **without** `oci://` prefix.
So use `ghcr.io/org/repo:tag`, not `oci://ghcr.io/org/repo:tag`.

## Useful paths and test commands

Git cache directory:

```bash
echo "$TMPDIR"xtralink-cache/git
```

Run integration tests in drivers package:

```bash
go test ./lib/drivers -run TestIntegrationSync_ -v -count=1
```

Note: Push credentials are resolved the same way as in the drivers:

1. `user` / `password` in the remote block
2. otherwise `USER_<ID>` / `PASSWORD_<ID>` from env or `.env`
