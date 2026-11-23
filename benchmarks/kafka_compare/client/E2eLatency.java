// Metric 3 of the Kafka comparison (#596, #600): end-to-end latency, from the producer's CreateTime (stamped
// at send(), so linger and batching are inside it) to the moment poll() returns the record here. Both clocks
// are the client containers' host clock, at 1 ms resolution.
//
// It runs in the pinned image's JVM with its kafka-clients jars, beside the perf producer, in the same kind
// of client container for both systems:
//
//   java -cp '/opt/kafka/libs/*' /client/E2eLatency.java BOOTSTRAP TOPIC EXPECTED IDLE_MS
//   java -cp '/opt/kafka/libs/*' /client/E2eLatency.java --check      (prints `ok`; proves the source launcher works)
//
// It assigns every partition of TOPIC (no consumer group, no offset commits) and seeks to the END, so only
// records produced after it is ready are counted. It prints `READY` once every position is resolved, which is
// when the harness starts the producer. It then reads until EXPECTED records arrived or IDLE_MS passed without
// one, and prints `consumed N` and one `bucket count` line per non-empty bucket of a 1 ms histogram
// (0..59999 ms, 60000 for 60 s or more, -1 for a negative latency). e2e.py parses and judges these lines.
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class E2eLatency {
    public static void main(String[] args) {
        if (args.length == 1 && args[0].equals("--check")) {
            System.out.println("ok");
            return;
        }
        if (args.length != 4) {
            System.err.println("usage: E2eLatency BOOTSTRAP TOPIC EXPECTED IDLE_MS | --check");
            System.exit(2);
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("group.id", "e2e-latency-" + System.nanoTime());
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        long expected = Long.parseLong(args[2]);
        long idleMs = Long.parseLong(args[3]);
        TreeMap<Long, Long> histogram = new TreeMap<>();
        long consumed = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo info : consumer.partitionsFor(args[1])) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);
            for (TopicPartition partition : partitions) {
                consumer.position(partition); // resolves the seek now, before the producer starts
            }
            System.out.println("READY");
            System.out.flush();
            long lastRecord = System.currentTimeMillis();
            while (consumed < expected && System.currentTimeMillis() - lastRecord < idleMs) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                long now = System.currentTimeMillis();
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    long latency = now - record.timestamp();
                    long bucket = latency < 0 ? -1 : Math.min(latency, 60_000);
                    histogram.merge(bucket, 1L, Long::sum);
                    consumed++;
                }
                if (!records.isEmpty()) {
                    lastRecord = now;
                }
            }
        }
        System.out.println("consumed " + consumed);
        histogram.forEach((bucket, count) -> System.out.println(bucket + " " + count));
    }
}
