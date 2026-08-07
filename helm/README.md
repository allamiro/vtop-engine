# Helm charts

Kubernetes packaging for VTOP.

```
helm/
└── vtop/                     # the one chart: co-located VTOP cluster nodes
    ├── Chart.yaml            # apiVersion v2; appVersion tracks Cargo.toml
    ├── values.yaml           # every knob, exhaustively commented
    ├── values.schema.json    # shape validation for the values above
    ├── README.md             # full docs: TLS Secret contract, all values
    └── templates/
        ├── _helpers.tpl      # labels, naming, and the per-ordinal node config
        ├── configmap.yaml    # one rendered node-<ordinal>.yaml per replica
        ├── statefulset.yaml  # the co-located nodes (vtop-node node)
        ├── service-headless.yaml  # stable peer DNS + per-pod client access
        ├── service.yaml      # admin + observability ClusterIP
        ├── serviceaccount.yaml
        ├── pdb.yaml          # maxUnavailable 1 — quorum protection
        ├── networkpolicy.yaml     # optional, values-gated
        ├── servicemonitor.yaml    # optional, CRD-guarded
        └── NOTES.txt         # what to check first after install
```

Quick start (TLS Secrets and cluster identities are **required** — the chart
ships no defaults for either; see `vtop/README.md` for the Secret contract):

```bash
helm lint helm/vtop
helm template vtop helm/vtop --set tls.metaSecretName=... # (+ identities)
helm install vtop helm/vtop -n vtop --create-namespace -f my-values.yaml
```
