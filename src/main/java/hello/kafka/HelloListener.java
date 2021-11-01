package hello.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class HelloListener {

    private static final Logger logger =
            LoggerFactory.getLogger(HelloListener.class);

    @KafkaListener(topics = "json-topic", clientIdPrefix = "json",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject(ConsumerRecord<String, HelloMessage> consumerRecord,
                               @Payload HelloMessage payload) {
        logger.info("Logger1 [JSON] received key {}: ype [{}]:Payload: {}:Record: {}", consumerRecord.key(),
                HelloUtil.typeIdHeader(consumerRecord.headers()), payload, consumerRecord.toString());

    }

    @KafkaListener(topics = "string-topic", clientIdPrefix = "string",
            containerFactory = "kafkaListenerStringContainerFactory")
    public void listenAsString(ConsumerRecord<String, String> cr,
                               @Payload String payload) {
        logger.info("Logger 2 [String] received key {}: Type [{}] : Payload: {}:Record: {}", cr.key(),
                HelloUtil.typeIdHeader(cr.headers()), payload, cr.toString());
    }

    @KafkaListener(topics = "bytearray-topic", clientIdPrefix = "bytearray",
            containerFactory = "kafkaListenerByteArrayContainerFactory")
    public void listenAsByteArray(ConsumerRecord<String, byte[]> cr,
                                  @Payload byte[] payload) {
        logger.info("Logger 3 [ByteArray] received key {}: Type [{}]:Payload: {}:Record: {}", cr.key(),
                HelloUtil.typeIdHeader(cr.headers()), payload, cr.toString());
    }
}
