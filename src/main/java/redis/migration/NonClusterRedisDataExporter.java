package redis.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.lambdaworks.redis.KeyScanCursor;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.ScanArgs;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.cluster.ClusterClientOptions;
import com.lambdaworks.redis.cluster.ClusterTopologyRefreshOptions;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import java.io.File;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.rnorth.ducttape.unreliables.Unreliables;

/**
 * Created by s.aravind on 08/08/17.
 */
@Slf4j
public class NonClusterRedisDataExporter {

  private static final int MAX_TRIES = 10;

  private RedisClient redisClient;
  private StatefulRedisConnection<String, String> connection;
  private RedisClusterClient redisClusterClient;
  private StatefulRedisClusterConnection<String, String> clusterConnection;

  public NonClusterRedisDataExporter(
      Pair<String, Integer> nonCluster, Pair<String, Integer> cluster) {
    Preconditions.checkNotNull(nonCluster);
    Preconditions.checkNotNull(cluster);
    redisClient = RedisClient.create(RedisURI.create(nonCluster.getKey(), nonCluster.getValue()));
    redisClusterClient =
        RedisClusterClient.create(RedisURI.create(cluster.getKey(), cluster.getValue()));
    connection = redisClient.connect();
    ClusterTopologyRefreshOptions topologyRefreshOptions =
        ClusterTopologyRefreshOptions.builder()
            .enablePeriodicRefresh(5, TimeUnit.SECONDS)
            .enableAllAdaptiveRefreshTriggers()
            .build();
    ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder()
        .topologyRefreshOptions(topologyRefreshOptions)
        .build();
    redisClusterClient.setOptions(clusterClientOptions);
    clusterConnection = redisClusterClient.connect();
    checkHostsAreReachable();
  }

  private void checkHostsAreReachable() throws RedisException {
    connection.sync().ping();
    clusterConnection.sync().ping();
  }

  public void migrate(String keyPattern, int limit) {
    RedisCommands<String, String> commands = connection.sync();
    KeyScanCursor<String> keyScanCursor =
        Unreliables.<KeyScanCursor>retryUntilSuccess(MAX_TRIES, () -> {
              return commands.scan(ScanArgs.Builder.matches(keyPattern).limit(limit));
            }
        );
    try {
      clusterConnection.setAutoFlushCommands(false);
      while (!keyScanCursor.isFinished()) {
        /**
         * Fetch the dump value for each key and commit in async manner to cluster
         * with flush disabled
         */
        int count = 0;
        for (String key : keyScanCursor.getKeys()) {
          Unreliables.retryUntilSuccess(MAX_TRIES, () -> {
                byte[] value = commands.dump(key);
                clusterConnection.async().restore(key, 0, value);
                return null;
              }
          );
          ++count;
        }

        log.info("Retrieved [{}] keys from the host port with scan range cursor [{}] with pattern"
                + " [{}]",
            count, keyScanCursor.getCursor(), keyPattern);

        /**
         * Flush the batched commands with retry
         */
        Unreliables.retryUntilSuccess(MAX_TRIES, () -> {
          clusterConnection.flushCommands();
          log.info("Successfully flushed the commands to destination cluster");
          return null;
        });
        keyScanCursor = commands
            .scan(keyScanCursor, ScanArgs.Builder.matches(keyPattern).limit(limit));
      }
    } finally {
      clusterConnection.setAutoFlushCommands(true);
    }
  }

  public void close() throws Exception {
    connection.close();
    clusterConnection.close();
    redisClient.shutdown();
    redisClusterClient.shutdown();
  }


  public static void main(String[] args) throws Exception {
    if (args == null || args.length == 0) {
      System.out.println("Specify the export config file name");
      return;
    }

    String configFileName = args[0];
    if (configFileName == null) {
      System.out.println("Specify the export config file name seems to be null");
      return;
    }

    File configFile = new File(configFileName);
    if (!configFile.exists()) {
      System.out.println("File " + configFileName + " does not exists");
      return;
    }

    NonClusterRedisDataExporter nonClusterRedisDataExporter = null;
    try {

      ObjectMapper mapper = new ObjectMapper();
      ExporterConfig config = mapper.readValue(configFile, ExporterConfig.class);

      Pair<String, Integer> srcHostAndPort =
          Pair.of(config.getSrcHost(), config.getSrcPort().intValue());
      Pair<String, Integer> dstHostAndPort =
          Pair.of(config.getDestHost(), config.getDestPort().intValue());

      log.info("Migrating the keys [{}] from {}:{} to {}:{}", config.getKeyPattern(),
          srcHostAndPort.getKey(), srcHostAndPort.getValue(), dstHostAndPort.getKey(),
          dstHostAndPort.getValue());

      nonClusterRedisDataExporter =
          new NonClusterRedisDataExporter(srcHostAndPort, dstHostAndPort);
      nonClusterRedisDataExporter.migrate(config.getKeyPattern(), config.getFetchSize().intValue());

    } catch (Exception ex) {
      System.out.println("Error occured : " + ex.getMessage());
      log.error("Error while migrating ", ex);

    } finally {
      if (nonClusterRedisDataExporter != null) {
        nonClusterRedisDataExporter.close();
      }
    }

  }


}

