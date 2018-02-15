package redis.migration.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NonNull;

/**
 * Created by s.aravind on 15/02/18.
 */

@Getter
public class LagConfig {

    private final String topic;
    private final String group;
    private final Long lag;

    @JsonCreator
    public LagConfig(
            @NonNull @JsonProperty("topic") String topic,
            @NonNull @JsonProperty("group") String group,
            @NonNull @JsonProperty("lag") Long lag) {
        this.topic = topic;
        this.group = group;
        this.lag = lag;
    }

}
