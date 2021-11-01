package hello.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.stream.IntStream;

@RestController
public class HelloKafkaController {

    private static final Logger logger =  LoggerFactory.getLogger(HelloKafkaController.class);

    private final KafkaTemplate<String, Object> template;

    private final String topicName;

    private final int messagesPerRequest;

    private HelloListener helloListener;

    public HelloKafkaController(
            final KafkaTemplate<String, Object> template,
            @Value("${hello.topic-name}") final String topicName,
            @Value("${hello.messages-per-request}") final int messagesPerRequest,
            HelloListener helloListener) {
        this.template = template;
        this.topicName = topicName;
        this.messagesPerRequest = messagesPerRequest;
        this.helloListener = helloListener;
    }

    @GetMapping("/send")
    public String sendString(@RequestParam String message) throws Exception {
        this.template.send("string-topic", message);
        return "Message sent.";
    }

    @GetMapping("/send-json")
    public String sendJSON(@RequestParam String message) throws Exception {
        this.template.send("json-topic", new HelloMessage(message, 999));
        return "Message sent.";
    }

    @GetMapping("/start")
    public String hello() throws Exception {
        IntStream.range(0, messagesPerRequest)
                .forEach(i -> this.template.send(topicName, String.valueOf(i),
                        new HelloMessage("Message:" + i, i))
                );
        logger.info("All messages processed");
        return "Sent all messages!";
    }


}
