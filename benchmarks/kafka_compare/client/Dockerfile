# The end-to-end consumer (#596, #600): client/E2eLatency.java, compiled once.
#
# The pinned apache/kafka image ships a JRE without jdk.compiler, so it cannot launch a .java source file
# ("Module jdk.compiler not in boot Layer"). The class is compiled here with the pinned JDK, against the same
# kafka-clients jars as the pinned apache/kafka image (its /opt/kafka/libs), and copied onto that same image, so
# it runs in the JVM and with the client library the perf tools use. The compiler stage is not in the result.
# This is how crash/client/Dockerfile builds #599's producer; both bases are pinned by digest here, run.py
# passes the same two values as build arguments, and a test keeps them equal to crash.py's. `--release 17`: the
# class must load in the Kafka image's own JRE, which runs a Kafka 4 broker and so is at least 17.
ARG KAFKA_IMAGE=apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7
ARG JDK_IMAGE=eclipse-temurin:21.0.11_10-jdk@sha256:98155457fcdbeb0987472e8abf721657b698ede407aad52ac24ae42618c0931c

FROM ${KAFKA_IMAGE} AS kafka

FROM ${JDK_IMAGE} AS build
COPY --from=kafka /opt/kafka/libs /kafka-libs
COPY E2eLatency.java /src/E2eLatency.java
RUN mkdir /out && javac --release 17 -Xlint:all -Werror -cp "/kafka-libs/*" -d /out /src/E2eLatency.java

FROM ${KAFKA_IMAGE}
COPY --from=build /out /opt/e2e-latency
