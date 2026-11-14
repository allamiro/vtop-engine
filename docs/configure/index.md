# Configure

The engine reads `config.yaml`, an optional `streams.yaml` beside it, and the
environment for secrets. Read the pages in order: the basics, per-stream
settings, how to check what will take effect, then the full reference when
you need a specific key.

<div class="grid cards" markdown>

-   **Config basics**

    ---

    The two files, secrets, environment variables, and `vtopctl validate`.

    [:octicons-arrow-right-24: Basics](basics.md)

-   **Streams and per-stream checksum**

    ---

    Tenant, format, storage name and checksum for each source.

    [:octicons-arrow-right-24: Streams](streams.md)

-   **Effective settings**

    ---

    `vtopctl validate --print-effective`: what each stream runs with, and why.

    [:octicons-arrow-right-24: Effective settings](print-effective.md)

-   **`config.yaml` reference**

    ---

    Every key, its default and its meaning.

    [:octicons-arrow-right-24: Reference](reference.md)

</div>
