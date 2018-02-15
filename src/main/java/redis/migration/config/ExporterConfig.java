package redis.migration.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.NonNull;

/**
 * Created by s.aravind on 08/08/17.
 */
public class ExporterConfig {

  private final String srcHost;
  private final Long srcPort;
  private final String destHost;
  private final Long destPort;
  private final String keyPattern;
  private final Long fetchSize;

  @JsonCreator
  public ExporterConfig(
      @JsonProperty("srcHost") @NonNull String srcHost,
      @JsonProperty("srcPort") @NonNull Long srcPort,
      @JsonProperty("destHost") @NonNull String destHost,
      @JsonProperty("destPort") @NonNull Long destPort,
      @JsonProperty("keyPattern") @NonNull String keyPattern,
      @JsonProperty("fetchSize") @NonNull Long fetchSize) {
    this.srcHost = srcHost;
    this.srcPort = srcPort;
    this.destHost = destHost;
    this.destPort = destPort;
    this.keyPattern = keyPattern;
    this.fetchSize = fetchSize;
  }

  public String getSrcHost() {
    return srcHost;
  }

  public Long getSrcPort() {
    return srcPort;
  }

  public String getDestHost() {
    return destHost;
  }

  public Long getDestPort() {
    return destPort;
  }

  public String getKeyPattern() {
    return keyPattern;
  }

  public Long getFetchSize() {
    return fetchSize;
  }
}
