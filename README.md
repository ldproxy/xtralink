# xtrasync

`xtrasync` synchronisiert Inhalte aus externen Quellen in ein lokales Zielverzeichnis.
Unterstützte Remote-Typen sind aktuell:

- `GIT`
- `OCI`
- `S3`

## Kurz erklärt: Was macht das Tool?

- Liest eine YAML-Steuerkonfiguration.
- Verarbeitet alle Einträge unter `remotes`.
- Holt/aktualisiert die Inhalte je nach Typ.
- Spiegelt das Ergebnis in dein Ziel (`targetDir` + `localPath`).

## Wo liegt die Steuer-Konfiguration?

Du kannst die Datei frei ablegen, üblich ist z. B. unter `config/`.

Beispiel:

- `config/exampleConfig.yaml`
- `config/oci-ghcr-test.yaml`

## Konfigurationsschema

```yaml
targetDir: .
remotes:
  - type: GIT|OCI|S3
    url: "..."
    tag: "..." # optional (z. B. Branch/Tag/Ref)
    user: "..." # optional
    password: "..." # optional
    path: "..." # optional, nur Verzeichnis-Subpath
    localPath: "..." # Zielpfad relativ zu targetDir
```

### Wichtige Felder

- `targetDir`: Basis-Zielordner für alle Remotes.
- `localPath`: Ziel relativ zu `targetDir` (Pflichtfeld je Remote).
- `path`: optionaler Unterordner im Remote-Inhalt.
  - Muss auf ein **Verzeichnis** zeigen (keine einzelne Datei).

## Beispiele pro Typ

### GIT

```yaml
targetDir: .
remotes:
  - type: GIT
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
    url: oci://ghcr.io/interactive-instruments/talos-config
    tag: "0.9.3"
    user: "<github-user>"
    password: "<pat>"
    localPath: config/synced/oci-talos-config
```

Hinweis OCI: Aktuell wird ein Artefakt unterstützt, bei dem der **erste Layer** das relevante ZIP mit Nutzdaten enthält.

### S3

```yaml
targetDir: .
remotes:
  - type: S3
    url: https://s3.example.net/my-bucket
    path: folder/subfolder
    localPath: config/synced/s3-data
```

## Ausführen

```bash
go run . --config config/<deine-datei>.yaml sync
```

Beispiel:

```bash
go run . --config config/oci-ghcr-test.yaml sync
```
