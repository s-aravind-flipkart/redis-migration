package redis.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lambdaworks.redis.ReadFrom;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.ClusterClientOptions;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import redis.migration.config.CopyConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created by s.aravind on 21/01/18.
 */
@Slf4j
public class CopyRedisCluster {

    private RedisClusterClient srcRedisClusterClient;
    private RedisClusterClient destRedisClusterClient;
    private StatefulRedisClusterConnection<String, String> srcClusterConnection;
    private StatefulRedisClusterConnection<String, String> destClusterConnection;

    public CopyRedisCluster(Pair<String, Integer> srcCluster, Pair<String, Integer> destinationCluster) {
        srcRedisClusterClient =
                RedisClusterClient.create(RedisURI.create(srcCluster.getKey(), srcCluster.getValue()));
        ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder()
                .build();
        srcRedisClusterClient.setOptions(clusterClientOptions);
        srcClusterConnection = srcRedisClusterClient.connect();
        srcClusterConnection.setReadFrom(ReadFrom.SLAVE);
        destRedisClusterClient =
                RedisClusterClient.create(RedisURI.create(destinationCluster.getKey(), destinationCluster.getValue()));
        destRedisClusterClient.setOptions(clusterClientOptions);
        destClusterConnection = destRedisClusterClient.connect();
        checkHostsAreReachable();
    }

    private void checkHostsAreReachable() throws RedisException {
        srcClusterConnection.sync().ping();
        destClusterConnection.sync().ping();
    }


    private void copy(String listingId) {
        String offerIdKey = "offerId_{" + listingId + "}";
        String metaKey = "meta_{" + listingId + "}";
        String lznKey = "lzn_{" + listingId + "}";

        byte[] offerIdBytes = srcClusterConnection.sync().dump(offerIdKey);
        byte[] metaBytes = srcClusterConnection.sync().dump(metaKey);
        byte[] lznBytes = srcClusterConnection.sync().dump(lznKey);

        if (offerIdBytes != null) {
            Boolean exists = destClusterConnection.sync().exists(offerIdKey);
            if (exists) {
                destClusterConnection.sync().del(offerIdKey);
            }
            destClusterConnection.sync().restore(offerIdKey, 0, offerIdBytes);
        }
        if (metaBytes != null) {
            Boolean exists = destClusterConnection.sync().exists(metaKey);
            if (exists) {
                destClusterConnection.sync().del(metaKey);
            }
            destClusterConnection.sync().restore(metaKey, 0, metaBytes);
        }
        if (lznBytes != null) {
            Boolean exists = destClusterConnection.sync().exists(lznKey);
            if (exists) {
                destClusterConnection.sync().del(lznKey);
            }
            destClusterConnection.sync().restore(lznKey, 0, lznBytes);
        }
    }

    public void close() throws Exception {
        srcClusterConnection.close();
        srcRedisClusterClient.shutdown();
        destClusterConnection.close();
        destRedisClusterClient.shutdown();
    }

    public void copyListings(String fileName) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        String key = null;
        System.out.println("Jsdfas");
        int idx = 0;
        while ((key = bufferedReader.readLine()) != null) {
            if (key != null && key.startsWith("L")) {
                key = key.trim();
                copy(key);
            }
            idx++;
            if (idx % 1000 == 0) {
                System.out.println("key " + key);
                log.info("Completed " + idx + " listings copy");
            }
        }
        log.info("Completed " + idx + " listings copy");
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

        CopyRedisCluster copyRedisCluster = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            CopyConfig config = mapper.readValue(configFile, CopyConfig.class);
            Pair<String, Integer> srcHostAndPort = Pair.of(config.getSrcHost(), config.getSrcPort().intValue());
            Pair<String, Integer> dstHostAndPort = Pair.of(config.getDstHost(), config.getDstPort().intValue());
            copyRedisCluster = new CopyRedisCluster(srcHostAndPort, dstHostAndPort);
            copyRedisCluster.copyListings(config.getFileName());
        } catch (Exception ex) {
            System.out.println("Error occured : " + ex.getMessage());
            log.error("Error while copying ", ex);

        } finally {
            if (copyRedisCluster != null) {
                copyRedisCluster.close();
            }
        }
    }
}
