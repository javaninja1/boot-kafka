package hello.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

record HelloMessage(@JsonProperty("message") String message,
                    @JsonProperty("identifier") int identifier) {
}
