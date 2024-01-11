package online.kevinztw.kafkalab3;

import java.util.ArrayList;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRecordProducer implements KafkaInorderRecordProducer {

  private static final Logger log = LoggerFactory.getLogger(KafkaRecordProducer.class);
  private final KafkaKeyStrategy keyStrategy;
  private KafkaProducer<String, String> producer;

  public KafkaRecordProducer(KafkaKeyStrategy keyStrategy) {
    this.keyStrategy = keyStrategy;
    setUpKafkaProducer();
  }

  private void setUpKafkaProducer() {
    String bootstrapServers = "127.0.0.1:8097";
    Properties properties = new Properties();

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.toString(true));

    this.producer = new KafkaProducer<>(properties);
  }

  @Override
  public void sendDataInorder(ArrayList<String> data) {
    data.forEach(
        value -> {
          ProducerRecord<String, String> producerRecord =
              new ProducerRecord<>("lab-3", keyStrategy.generateKey(), value);
          try {
            producer.send(producerRecord);
          } catch (Exception e) {
            log.error("Producer encountered error: ", e);
          }
        });
  }

  @Override
  public void cleanUp() {
    producer.flush();
    producer.close();
  }
}
