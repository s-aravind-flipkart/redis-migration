package redis.migration.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NonNull;

/**
 * Created by s.aravind on 15/02/18.
 */

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class GroupConfig {

    private final String topic;
    private final String group;

    @JsonCreator
    public GroupConfig(
            @NonNull @JsonProperty("topic") String topic,
            @NonNull @JsonProperty("group") String group) {
        this.topic = topic;
        this.group = group;
    }

}
