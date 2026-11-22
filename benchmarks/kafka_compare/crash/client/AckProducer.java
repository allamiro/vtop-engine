import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * kafka-verifiable-producer with a record size (#599).
 *
 * The crash benchmark needs the same client for both systems, one line per acknowledgement, and records of about
 * 1 KiB (the issue's size). kafka-verifiable-producer prints what is needed but its value is `<prefix>.<n>` and it has
 * no size option, so this is the same KafkaProducer from the same pinned image, paced the same way (open loop,
 * `--throughput` records a second), printing the same JSON lines (startup_complete, producer_send_success,
 * producer_send_error, tool_data), with the value padded to `--value-size` bytes:
 *
 *     <prefix>.<n>~xxxx...      (ASCII, so characters are bytes; a size below the text's own length pads nothing)
 *
 * Every producer setting is the client's default except the ones --command-config names, as with the stock tool.
 */
public final class AckProducer {
    private static final Object OUT = new Object();

    private AckProducer() {
    }

    private static void emit(String json) {
        synchronized (OUT) {
            System.out.println(json);
            System.out.flush();
        }
    }

    private static String quote(String text) {
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\') {
                out.append('\\').append(c);
            } else if (c < 0x20 || c > 0x7e) {
                out.append(' ');
            } else {
                out.append(c);
            }
        }
        return out.append('"').toString();
    }

    static String value(String prefix, long seq, int size) {
        StringBuilder text = new StringBuilder(Math.max(size, 24));
        text.append(prefix).append('.').append(seq);
        if (size > text.length()) {
            text.append('~');
            while (text.length() < size) {
                text.append('x');
            }
        }
        return text.toString();
    }

    private static String arg(String[] args, String name, String fallback) {
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return fallback;
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = arg(args, "--bootstrap-server", null);
        String topic = arg(args, "--topic", null);
        if (bootstrap == null || topic == null) {
            System.err.println("usage: AckProducer --bootstrap-server HOST:PORT --topic T [--acks N] "
                    + "[--throughput R] [--max-messages M] [--value-prefix P] [--value-size BYTES] "
                    + "[--command-config FILE]");
            System.exit(2);
        }
        long max = Long.parseLong(arg(args, "--max-messages", "-1"));
        long rate = Long.parseLong(arg(args, "--throughput", "-1"));
        int size = Integer.parseInt(arg(args, "--value-size", "0"));
        String prefix = arg(args, "--value-prefix", "0");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, arg(args, "--acks", "-1"));
        String config = arg(args, "--command-config", null);
        if (config != null) {
            try (InputStream in = new FileInputStream(config)) {
                props.load(in);
            } catch (IOException e) {
                System.err.println("cannot read " + config + ": " + e);
                System.exit(2);
            }
        }

        AtomicLong acked = new AtomicLong();
        long sent = 0;
        long began = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            emit("{\"timestamp\":" + System.currentTimeMillis() + ",\"name\":\"startup_complete\"}");
            long start = System.nanoTime();
            for (long seq = 0; max < 0 || seq < max; seq++) {
                if (rate > 0) {
                    long due = start + seq * 1_000_000_000L / rate;
                    long wait;
                    while ((wait = due - System.nanoTime()) > 0) {
                        Thread.sleep(wait / 1_000_000L, (int) (wait % 1_000_000L));
                    }
                }
                final String text = value(prefix, seq, size);
                producer.send(new ProducerRecord<>(topic, null, text), (meta, error) -> {
                    long now = System.currentTimeMillis();
                    if (error == null) {
                        acked.incrementAndGet();
                        emit("{\"timestamp\":" + now + ",\"name\":\"producer_send_success\",\"key\":null,\"value\":"
                                + quote(text) + ",\"offset\":" + meta.offset() + ",\"topic\":" + quote(meta.topic())
                                + ",\"partition\":" + meta.partition() + "}");
                    } else {
                        emit("{\"timestamp\":" + now + ",\"name\":\"producer_send_error\",\"key\":null,\"value\":"
                                + quote(text) + ",\"topic\":" + quote(topic) + ",\"exception\":"
                                + quote(error.getClass().getName()) + ",\"message\":"
                                + quote(String.valueOf(error.getMessage())) + "}");
                    }
                });
                sent++;
            }
            producer.flush();
        }
        double seconds = Math.max((System.nanoTime() - began) / 1e9, 1e-9);
        emit("{\"timestamp\":" + System.currentTimeMillis() + ",\"name\":\"shutdown_complete\"}");
        emit("{\"timestamp\":" + System.currentTimeMillis() + ",\"name\":\"tool_data\",\"sent\":" + sent
                + ",\"acked\":" + acked.get() + ",\"target_throughput\":" + rate + ",\"avg_throughput\":"
                + (sent / seconds) + "}");
    }
}
