package online.kevinztw.kafkalab3;

import java.util.ArrayList;

public interface KafkaInorderRecordProducer {
  void sendDataInorder(ArrayList<String> data);

  void cleanUp();
}
