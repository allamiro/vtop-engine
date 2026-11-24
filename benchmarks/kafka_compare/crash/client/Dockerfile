# The crash benchmark's producer (#599): kafka-verifiable-producer's behaviour with a record size.
#
# The same KafkaProducer as the pinned apache/kafka image (its /opt/kafka/libs), compiled with the JDK that image
# lacks (it ships a JRE: no javac, no jshell), and copied onto that same image. The compiler stage is not in the
# result. Both bases are pinned by digest here; crash.py passes the same two values as build arguments, and a test
# keeps the three places equal.
ARG KAFKA_IMAGE=apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7
ARG JDK_IMAGE=eclipse-temurin:21.0.11_10-jdk@sha256:98155457fcdbeb0987472e8abf721657b698ede407aad52ac24ae42618c0931c

FROM ${KAFKA_IMAGE} AS kafka

FROM ${JDK_IMAGE} AS build
COPY --from=kafka /opt/kafka/libs /kafka-libs
COPY AckProducer.java /src/AckProducer.java
RUN mkdir /out && javac --release 21 -Xlint:all -Werror -cp "/kafka-libs/*" -d /out /src/AckProducer.java

FROM ${KAFKA_IMAGE}
COPY --from=build /out /opt/ackproducer
