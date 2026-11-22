# Kafka Connect with the Lenses Stream Reactor S3 sink 12.1.2 (#746), on the Connect runtime of the
# source's own image. The connector zip is checked against the sha256 in benchmarks/compare_common/products.toml
# (`connect-lenses.archive_sha256`, GitHub's recorded asset digest); a mismatch fails the build.
FROM apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7
USER root
ADD --checksum=sha256:694d545654dbfd3f64d4644cfaf9bd0e3d40bd30053730ee830adfaec5c4b583 \
    https://github.com/lensesio/stream-reactor/releases/download/12.1.2/kafka-connect-aws-s3-12.1.2.zip /tmp/connector.zip
RUN mkdir -p /opt/connectors \
    && cd /opt/connectors \
    && /opt/java/openjdk/bin/jar xf /tmp/connector.zip \
    && rm -f /tmp/connector.zip \
    && chmod -R a+rX /opt/connectors \
    && mkdir -p /data \
    && chown appuser:appuser /data
USER appuser
