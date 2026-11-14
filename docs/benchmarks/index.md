<!-- The charts and their captions are the README's "Evidence" section, with
     links re-based for this directory. benchmarks/readme_charts.py draws the
     charts and CI fails if they drift; keep this text in step with README.md. -->

# Measured results

[`benchmarks/readme_charts.py`](../../benchmarks/readme_charts.py) draws every chart
below from measurements committed in this repository. CI redraws them on every
change to the data or the script, and fails if an image differs. All runs are
on a single host, so they show behaviour and relative cost, not capacity.

## Upload throughput on a long, lossy link

![Line charts of the share of a 10 Mbit/s, 100 ms link that VTOP's uploads carried, against the number of objects uploaded at once, for a clean link and one with 1% burst loss. With one part in flight and 1% loss, one upload at a time carries 0.25 of the link and 16 at once carry about all of it. The shipped default carries 0.960 of the clean link and 0.967 of the lossy one.](../assets/readme/egress-utilisation-light.svg#only-light)
![Line charts of the share of a 10 Mbit/s, 100 ms link that VTOP's uploads carried, against the number of objects uploaded at once, for a clean link and one with 1% burst loss. With one part in flight and 1% loss, one upload at a time carries 0.25 of the link and 16 at once carry about all of it. The shipped default carries 0.960 of the clean link and 0.967 of the lossy one.](../assets/readme/egress-utilisation-dark.svg#only-dark)

With 1% burst loss, one upload at a time carries 0.25 of the link. The shipped
default (8 objects in flight, 4 parts each) carries **0.960** of a clean link
and **0.967** of a lossy one, the median of 5 runs each.
Data: [`483-tcp-baseline/points.csv`](../../benchmarks/measurements/483-tcp-baseline/points.csv).
Method and every condition: [EGRESS_TRANSPORT_RESEARCH.md](../EGRESS_TRANSPORT_RESEARCH.md).

## A policed uplink, before and after a fix

![Bar chart of objects committed at the shipped upload defaults. On a 10 Mbit/s policed uplink, 0 of 32 before the fix and 96 of 96 after. On clean and lossy 100 ms links, every object committed before and after.](../assets/readme/policed-uplink-light.svg#only-light)
![Bar chart of objects committed at the shipped upload defaults. On a 10 Mbit/s policed uplink, 0 of 32 before the fix and 96 of 96 after. On clean and lossy 100 ms links, every object committed before and after.](../assets/readme/policed-uplink-dark.svg#only-dark)

On a 10 Mbit/s policed uplink, which drops what exceeds its rate instead of
queueing it, the shipped defaults committed
**0 of 32** objects. Every part upload hit a fixed 300 s deadline and was sent
again from its first byte, so every object ended `failed` and no source
position moved. Since [#666](https://github.com/allamiro/vtop-engine/pull/666),
a request's deadline grows with the size of its body, and the same link
committed **96 of 96**.
Data: [`663-policed-uplink/runs.csv`](../../benchmarks/measurements/663-policed-uplink/runs.csv).

## Leader failover, against Apache Kafka

![Dot plot of the seconds without an acknowledgement after a leader fault, Apache Kafka against VTOP. kill -9: Kafka 10.2 s, VTOP 25.7 s. Orderly stop: Kafka 0.1 s, VTOP 16.7 s. Network partition: Kafka 30.0 s, VTOP 30.1 s. No acknowledged record was lost or duplicated in any repetition.](../assets/readme/kafka-failover-light.svg#only-light)
![Dot plot of the seconds without an acknowledgement after a leader fault, Apache Kafka against VTOP. kill -9: Kafka 10.2 s, VTOP 25.7 s. Orderly stop: Kafka 0.1 s, VTOP 16.7 s. Network partition: Kafka 30.0 s, VTOP 30.1 s. No acknowledged record was lost or duplicated in any repetition.](../assets/readme/kafka-failover-dark.svg#only-dark)

All 30 failovers, 15 per system, read back every acknowledged record:
**0 lost, 0 duplicated**, 14,250,000 records in all. Kafka's producer resumes
sooner after a `kill -9` of the leader (10.2 s, against 25.7 s for VTOP) and
after an orderly stop; a network partition costs both about 30 s. VTOP was
driven through its native client, because a Kafka client cannot yet follow a
VTOP failover.
Data and the full report: [kafka-failover.md](kafka-failover.md).

## Produce throughput, against Apache Kafka

![Bar chart of produce throughput at saturation in MiB/s. 100 B records: Apache Kafka 27.95, VTOP's Kafka gateway 2.33. 1 KiB records: Apache Kafka 32.19, VTOP's Kafka gateway 2.32. Kafka carries 12.0x and 13.9x VTOP's records per second.](../assets/readme/kafka-throughput-light.svg#only-light)
![Bar chart of produce throughput at saturation in MiB/s. 100 B records: Apache Kafka 27.95, VTOP's Kafka gateway 2.33. 1 KiB records: Apache Kafka 32.19, VTOP's Kafka gateway 2.32. Kafka carries 12.0x and 13.9x VTOP's records per second.](../assets/readme/kafka-throughput-dark.svg#only-dark)

Through the same JVM producer, Kafka carries 12.0x to 13.9x the records per
second of VTOP's Kafka gateway. Each system ran as shipped: a Kafka `acks=all`
acknowledgement does not wait for an fsync, and a VTOP quorum acknowledgement
waits for two replicas to fsync.
Data, latency and consume results: [kafka-comparison.md](kafka-comparison.md).

> [!NOTE]
> Both Kafka comparisons ran on one laptop (Apple M5 Max), each three-node
> cluster in turn in the same Docker Desktop VM, at 2 CPUs and 2 GiB per node.
> They are indicative, not a production benchmark.

## The full reports

- [Kafka comparison](kafka-comparison.md) and [Kafka failover](kafka-failover.md):
  method, raw data and every condition.
- [Egress transport research](../EGRESS_TRANSPORT_RESEARCH.md): the upload
  path on long, lossy and policed links.
- [Throughput research](../THROUGHPUT_RESEARCH.md): what the per-stage
  numbers do and do not claim.
