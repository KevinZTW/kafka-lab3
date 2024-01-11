package online.kevinztw.kafkalab3;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws InterruptedException {
    KafkaInorderRecordProducer inorderRecordProducer =
        new KafkaRecordProducer(new ConstantKafkaKeyStrategy());

    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      data.add(String.valueOf(i));
    }

    inorderRecordProducer.sendDataInorder(data);

    inorderRecordProducer.cleanUp();

    KafkaInorderRecordValidator validator = new KafkaInorderRecordValidator();
    boolean isKafkaRecordInorder = validator.validate();
    log.info("Kafka record in order: {}", isKafkaRecordInorder);
  }
}

//    inorderRecordProducer.sendDataInorder(
//        new ArrayList<>(List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")));
