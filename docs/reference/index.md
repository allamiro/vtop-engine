# Reference

Exact behaviour, for when you need a specific command, key, state or limit.
The guides link here; these pages do not repeat the guides.

<div class="grid cards" markdown>

-   **CLI (`vtopctl`)**

    ---

    The engine commands, and how `vtopctl run` stops.

    [:octicons-arrow-right-24: CLI](cli.md)

-   **`config.yaml`**

    ---

    Every key, its default and its meaning.

    [:octicons-arrow-right-24: Config reference](../configure/reference.md)

-   **Archive path**

    ---

    Sources, formats, the manifest, upload backends, replay, metrics.

    [:octicons-arrow-right-24: Archive path](archive-path.md)

-   **Known limitations**

    ---

    What VTOP does not do yet.

    [:octicons-arrow-right-24: Limitations](limitations.md)

-   **Protocol**

    ---

    The normative draft: states, the commit rule, naming, verification.

    [:octicons-arrow-right-24: Protocol](../VTOP_PROTOCOL_DRAFT.md)

-   **Design documents**

    ---

    The full [architecture](../ARCHITECTURE.md),
    [Production HA](../PRODUCTION_HA.md) design and status,
    [security model](../SECURITY_MODEL.md),
    [native broker architecture](../NATIVE_BROKER_ARCHITECTURE.md) and
    [comparison](../COMPARISON.md).

</div>

Every `vtopctl` and `vtop-node` command also documents itself:
`vtopctl <command> --help`.
