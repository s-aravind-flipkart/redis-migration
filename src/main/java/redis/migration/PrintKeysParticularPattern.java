package redis.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.lambdaworks.redis.KeyScanCursor;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.ScanArgs;
import com.lambdaworks.redis.cluster.ClusterClientOptions;
import com.lambdaworks.redis.cluster.ClusterTopologyRefreshOptions;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.rnorth.ducttape.unreliables.Unreliables;
import redis.migration.config.ExporterConfig;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Created by s.aravind on 08/08/17.
 */
@Slf4j
public class PrintKeysParticularPattern {

    private static final int MAX_TRIES = 10;

    private RedisClusterClient redisClusterClient;
    private StatefulRedisClusterConnection<String, String> clusterConnection;

    public PrintKeysParticularPattern(Pair<String, Integer> cluster) {
        Preconditions.checkNotNull(cluster);
        redisClusterClient =
                RedisClusterClient.create(RedisURI.create(cluster.getKey(), cluster.getValue()));
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
        clusterConnection.sync().ping();
    }

    public void migrate(String keyPattern, int limit) {
        RedisAdvancedClusterCommands<String, String> commands = clusterConnection.sync();
        KeyScanCursor<String> keyScanCursor =
                Unreliables.<KeyScanCursor>retryUntilSuccess(MAX_TRIES, () -> {
                            return commands.scan(ScanArgs.Builder.matches(keyPattern).limit(limit));
                        }
                );
        try {
            while (!keyScanCursor.isFinished()) {
                /**
                 * Fetch the dump value for each key and commit in async manner to cluster
                 * with flush disabled
                 */
                int count = 0;
                for (String key : keyScanCursor.getKeys()) {
                    System.out.println(commands.get(key));
                    ++count;
                }

                log.info("Scan [{}] keys from the host with scan range cursor [{}] with pattern"
                                + " [{}]",
                        count, keyScanCursor.getCursor(), keyPattern);

                /**
                 * Flush the batched commands with retry
                 */
                keyScanCursor = commands
                        .scan(keyScanCursor, ScanArgs.Builder.matches(keyPattern).limit(limit));
            }
        } finally {
        }
    }

    public void close() throws Exception {
        clusterConnection.close();
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

        PrintKeysParticularPattern nonClusterRedisDataExporter = null;
        try {

            ObjectMapper mapper = new ObjectMapper();
            ExporterConfig config = mapper.readValue(configFile, ExporterConfig.class);

            Pair<String, Integer> srcHostAndPort = Pair.of(config.getSrcHost(), config.getSrcPort().intValue());

            log.info("Deleting the keys [{}] from {}:{}", config.getKeyPattern(),
                    srcHostAndPort.getKey(), srcHostAndPort.getValue());

            nonClusterRedisDataExporter = new PrintKeysParticularPattern(srcHostAndPort);
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

