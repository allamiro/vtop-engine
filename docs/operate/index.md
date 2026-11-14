# Operate

Day-two work for a running engine, and for the cluster plane where it
differs. Set up monitoring first, then the scheduled checks that prove the
archive is complete, then the procedures you will need less often.

<div class="grid cards" markdown>

-   **Metrics and alerts**

    ---

    The endpoint, the metrics that carry the commit rule, and alert rules.

    [:octicons-arrow-right-24: Monitoring](monitoring.md)

-   **Logs and auditing**

    ---

    Log format and levels, where audit evidence lives, and what is planned.

    [:octicons-arrow-right-24: Logs and auditing](logs-and-auditing.md)

-   **Scrub and coverage audit**

    ---

    Prove the archive has no holes and every object is still intact.

    [:octicons-arrow-right-24: Scrub and audit](scrub-and-audit.md)

-   **Rotate the manifest MAC key**

    ---

    Add a key as verify-only, switch the signing key, scrub, and retire a
    withdrawn key.

    [:octicons-arrow-right-24: Key rotation](manifest-key-rotation.md)

-   **S3 Object Lock**

    ---

    Write every object under a retention lock, and have the engine refuse a
    bucket or a batch that does not carry it.

    [:octicons-arrow-right-24: Object Lock profile](../OBJECT_LOCK_DEPLOYMENT.md)

-   **Upgrades**

    ---

    Upgrade order, feature levels, one-way changes.

    [:octicons-arrow-right-24: Upgrades](upgrades.md)

-   **Backup and recovery**

    ---

    What the engine recovers by itself, and what you must back up.

    [:octicons-arrow-right-24: Backup and recovery](backup-and-recovery.md)

-   **Troubleshooting**

    ---

    Error messages, missing data, stuck stops, the cluster plane.

    [:octicons-arrow-right-24: Troubleshooting](troubleshooting.md)

</div>

The engine's runbook and rollback procedures are in
[Production HA §18](../PRODUCTION_HA.md#18-operator-runbook--rollback-procedures).
