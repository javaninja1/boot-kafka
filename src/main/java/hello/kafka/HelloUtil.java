package hello.kafka;

import org.apache.kafka.common.header.Headers;

import java.util.stream.StreamSupport;

public class HelloUtil {

    static String typeIdHeader(Headers headers) {
        return StreamSupport.stream(headers.spliterator(), false)
                .filter(header -> header.key().equals("__TypeId__"))
                .findFirst().map(header -> new String(header.value())).orElse("N/A");
    }
}
