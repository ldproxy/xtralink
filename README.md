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
    id: "..."
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
- `id`: Mithilfe der ID wird der passende User und das passende Passwort aus .ENV geholt

## Credentials (GIT / OCI / S3)

Für alle drei Treiber (`GIT`, `OCI`, `S3`) gilt dieselbe Reihenfolge:

1. `user` / `password` direkt im jeweiligen Remote-Block (Steuer-Konfig).
2. Falls dort leer: Environment-Variablen `user_<id>` / `password_<id>`.

Beispiel bei `id: bplan`:

- `user_bplan`
- `password_bplan`

Wenn im Projektordner eine `.env`-Datei liegt, liest `xtrasync` sie automatisch ein.
Du kannst deine Zugangsdaten also einfach dort eintragen (z. B. `user_bplan=...`), ohne vorher im Terminal etwas mit `export` setzen zu müssen.

## Beispiele pro Typ

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

Hinweis OCI: Aktuell wird ein Artefakt unterstützt, bei dem der **erste Layer** das relevante ZIP mit Nutzdaten enthält.

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

Hinweis S3: Access Key / Secret müssen als `user` / `password` angegeben werden.

## Ausführen

```bash
go run . --config config/<deine-datei>.yaml sync
```

Beispiel:

```bash
go run . --config config/oci-ghcr-test.yaml sync
```
