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
| `work_cap` | `5` | Staging cap. |
| `pinned_zone` | - | Zone to pin to. |
| `region_id` | **Required** | The region. |
| `labelName` | - | A label. |
| `audit_sink` | - | Where audit records go. An engine before this release ignores the key and writes no audit records. |

## `security`

| Key | Default | Meaning |
|---|---|---|
| `strict_mode` | `false` | Strict. |
| `new_toggle` | `false` | A toggle. |

## `upload`

| Key | Default | Meaning |
|---|---|---|
| `bucket` | **Required** | Target bucket. |
| `lock` | - | The lock block; see [`upload.lock`](#uploadlock). |

### `upload.transports`

| Key | Default | Meaning |
|---|---|---|
| `window` | - | Window. |
| `burst` | `4` | Burst size. |

### `upload.lock`

The lock profile. An engine older than v2 ignores the block and writes
unlocked objects, so [read this](../other.md) first.

| Key | Default | Meaning |
|---|---|---|
| `mode` | **Required** in the block | The mode. |
| `days` | **Required** in the block | The days. |
