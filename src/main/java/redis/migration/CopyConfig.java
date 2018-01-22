package redis.migration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NonNull;

/**
 * Created by s.aravind on 08/08/17.
 */
@Getter
public class CopyConfig {

  private final String srcHost;
  private final Long srcPort;
  private final String dstHost;
  private final Long dstPort;
  private final String fileName;

  @JsonCreator
  public CopyConfig(
      @JsonProperty("srcHost") @NonNull String srcHost,
      @JsonProperty("srcPort") @NonNull Long srcPort,
      @JsonProperty("dstHost") @NonNull String dstHost,
      @JsonProperty("dstPort") @NonNull Long dstPort,
      @JsonProperty("fileName") @NonNull String fileName) {
    this.srcHost = srcHost;
    this.srcPort = srcPort;
    this.dstHost = dstHost;
    this.dstPort = dstPort;
    this.fileName = fileName;
  }



}
