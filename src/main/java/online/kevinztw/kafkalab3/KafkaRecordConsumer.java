package online.kevinztw.kafkalab3;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRecordConsumer implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(KafkaRecordConsumer.class);
  private final List<String> logs;
  private KafkaConsumer<String, String> consumer;
  private boolean running = true;

  KafkaRecordConsumer(List<String> logs) {
    setUpKafkaConsumer();
    this.logs = logs;
  }

  private void setUpKafkaConsumer() {
    String bootstrapServers = "127.0.0.1:8097";
    String groupId = "my-group-id";
    String topic = "lab-3";

    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(List.of(topic));
  }

  @Override
  public void run() {
    while (running) {
      consumer
          .poll(java.time.Duration.ofMillis(100))
          .forEach(
              r -> {
                log.info(
                    String.format(
                        "Key: %s, Value: %s, Partition: %s, Offset: %s",
                        r.key(), r.value(), r.partition(), r.offset()));
                logs.add(r.value());
              });
    }
    consumer.close();
  }

  public void terminate() {
    log.info("Closing consumer");
    running = false;
  }
}
