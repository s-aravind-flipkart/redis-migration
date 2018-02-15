package redis.migration.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NonNull;

/**
 * Created by s.aravind on 08/08/17.
 */
@Getter
public class DeleteConfig {

  private final String srcHost;
  private final Long srcPort;
  private final String fileName;
  private final String suffix;

  @JsonCreator
  public DeleteConfig(
      @JsonProperty("srcHost") @NonNull String srcHost,
      @JsonProperty("srcPort") @NonNull Long srcPort,
      @JsonProperty("fileName") @NonNull String fileName,
      @JsonProperty("suffix") @NonNull String suffix) {
    this.srcHost = srcHost;
    this.srcPort = srcPort;
    this.fileName = fileName;
    this.suffix = suffix;
  }

}
