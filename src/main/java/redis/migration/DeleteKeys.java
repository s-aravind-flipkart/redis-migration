package redis.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.rnorth.ducttape.unreliables.Unreliables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by s.aravind on 08/08/17.
 */
@Slf4j
public class DeleteKeys {

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> redisConnection;

    public DeleteKeys(Pair<String, Integer> cluster) {
        Preconditions.checkNotNull(cluster);
        redisClient = RedisClient.create(RedisURI.create(cluster.getKey(), cluster.getValue()));
        redisConnection = redisClient.connect();
    }

    private void checkHostsAreReachable() throws RedisException {
        redisConnection.sync().ping();
    }

    public void delete(String fileName, String suffix) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        String key = null;
        int count = 1;
        int idx = 0;
        List<String> deleteKeys = new ArrayList<>(100);
        while ((key = bufferedReader.readLine()) != null) {
            deleteKeys.add(key + suffix);
            if (count % 100 == 0) {
                String[] keys = deleteKeys.stream().toArray(String[]::new);
                redisConnection.sync().del(keys);
                log.info("Flushing the keys count " + count);
                deleteKeys = new ArrayList<>(100);
            }
            ++count;
        }
        String[] keys = deleteKeys.stream().toArray(String[]::new);
        redisConnection.sync().del(keys);
    }

    public void close() throws Exception {
        redisConnection.close();
        redisClient.shutdown();
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

        DeleteKeys deleteKeysHandler = null;
        try {

            ObjectMapper mapper = new ObjectMapper();
            DeleteConfig config = mapper.readValue(configFile, DeleteConfig.class);

            Pair<String, Integer> srcHostAndPort = Pair.of(config.getSrcHost(), config.getSrcPort().intValue());

            deleteKeysHandler = new DeleteKeys(srcHostAndPort);
            deleteKeysHandler.delete(config.getFileName(), config.getSuffix());

        } catch (Exception ex) {
            System.out.println("Error occured : " + ex.getMessage());
            log.error("Error while migrating ", ex);

        } finally {
            if (deleteKeysHandler != null) {
                deleteKeysHandler.close();
            }
        }

    }


}

