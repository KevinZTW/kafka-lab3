package online.kevinztw.kafkalab3;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaInorderRecordValidator {
  private static final Logger log = LoggerFactory.getLogger(KafkaInorderRecordValidator.class);
  private final List<String> logs;
  private final KafkaRecordConsumer kafkaConsumer;

  public KafkaInorderRecordValidator() {
    this.logs = new ArrayList<>();
    this.kafkaConsumer = new KafkaRecordConsumer(logs);
  }

  public boolean validate() throws InterruptedException {
    Thread consumerThread = new Thread(kafkaConsumer);
    consumerThread.start();

    Thread.sleep(5000);
    kafkaConsumer.terminate();

    consumerThread.join();

    return isInorder(logs);
  }

  private boolean isInorder(List<String> logs) {
    boolean dataIsNumeric = logs.getFirst().chars().allMatch(Character::isDigit);

    for (int i = 0; i < logs.size() - 1; i++) {
      if (dataIsNumeric) {
        if (Integer.parseInt(logs.get(i)) > Integer.parseInt(logs.get(i + 1))) {
          log.warn("----------------------------------------");
          log.warn("Record out of order: {} {}", logs.get(i), logs.get(i + 1));
          log.warn("----------------------------------------");
          return false;
        }
      } else if (logs.get(i).compareTo(logs.get(i + 1)) > 0) {
        log.warn("----------------------------------------");
        log.warn("Record out of order: {} {}", logs.get(i), logs.get(i + 1));
        log.warn("----------------------------------------");
        return false;
      }
    }
    return true;
  }
}
