package redis.migration.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;

/**
 * Created by s.aravind on 15/02/18.
 */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerConfig {

    private final String bootstrapServers;
    private final List<GroupConfig> groupConfigs;
    private final long delayMillis;

    @JsonCreator
    public ConsumerConfig(
            @NonNull @JsonProperty("bootstrap_servers") String bootstrapServers,
            @NonNull @JsonProperty("group_configs") List<GroupConfig> groupConfigs,
            @NonNull @JsonProperty("delay_millis") long delayMillis) {
        this.bootstrapServers = bootstrapServers;
        this.groupConfigs = groupConfigs;
        this.delayMillis = delayMillis;
    }

}
