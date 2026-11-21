# `config.yaml` reference

## Top level

| Key | Default | Meaning |
|---|---|---|
| `engine` | **Required** | Engine identity. |
| `security` | `{}` | How files are treated. |
| `upload` | **Required** | Object storage target. |

## `engine`

| Key | Default | Meaning |
|---|---|---|
| `name` | **Required** | Engine name. |
| `tenant` | `default` | Tenant. |

## `security`

| Key | Default | Meaning |
|---|---|---|
| `strict_mode` | `false` | Strict. |

## `upload`

| Key | Default | Meaning |
|---|---|---|
| `bucket` | **Required** | Target bucket. |

### `upload.transports`

| Key | Default | Meaning |
|---|---|---|
| `window` | - | Window. |
