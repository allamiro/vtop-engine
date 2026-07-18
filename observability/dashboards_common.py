"""Constants shared by every dashboard generator.

Kept in one module because a value that must agree across generators but is
defined in each of them independently is a drift bug waiting to happen — which
is exactly what it was: `stat()` in build-dashboards.py and dashboards_kafka.py
referenced GRAFANA_PLUGIN_VERSION while only dashboards_vtop.py defined it, so
generating those dashboards raised NameError.
"""

# Stat panels MUST carry a pluginVersion. Without it Grafana treats the panel as
# pre-schema-migration and runs the stat migration handler, which empties
# `reduceOptions.calcs` — the panel then renders blank until someone opens it in
# the editor and saves, which re-adds the field. Matches the Grafana image in
# docker-compose.observability.yml; bump both together.
GRAFANA_PLUGIN_VERSION = "13.1.0"
