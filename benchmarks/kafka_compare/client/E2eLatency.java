// Metric 3 of the Kafka comparison (#596): end-to-end latency, from the
// producer's CreateTime (stamped at send(), so linger and batching are inside
// it) to the moment poll() returns the record here. Both clocks are the
// client container's host clock, at 1 ms resolution.
//
// Run with the pinned image's JDK and its kafka-clients jar, beside the perf
// producer, in the same client container for both systems:
//
//   java -cp '/opt/kafka/libs/*' E2eLatency.java BOOTSTRAP TOPIC GROUP EXPECTED IDLE_MS
//
// It reads from the earliest offset with auto-commit off, until EXPECTED
// records arrived or IDLE_MS passed without one. It then prints one
// `consumed N` line, and `bucket count` lines for the 1 ms histogram
// (0..59999 ms, 60000 for 60 s or more, -1 for a negative latency).
// e2e.py parses these lines and judges them.
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class E2eLatency {
    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("usage: E2eLatency BOOTSTRAP TOPIC GROUP EXPECTED IDLE_MS");
            System.exit(2);
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("group.id", args[2]);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        long expected = Long.parseLong(args[3]);
        long idleMs = Long.parseLong(args[4]);
        TreeMap<Long, Long> histogram = new TreeMap<>();
        long consumed = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(args[1]));
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
