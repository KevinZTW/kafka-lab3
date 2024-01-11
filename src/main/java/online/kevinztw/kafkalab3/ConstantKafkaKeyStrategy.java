package online.kevinztw.kafkalab3;

public class ConstantKafkaKeyStrategy implements KafkaKeyStrategy {
  @Override
  public String generateKey() {
    return "constant-key";
  }
}
