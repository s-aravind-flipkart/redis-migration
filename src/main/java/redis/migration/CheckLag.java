package redis.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import redis.migration.config.ConsumerLagConfig;
import redis.migration.config.DeleteConfig;
import redis.migration.config.LagConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by s.aravind on 08/08/17.
 */
@Slf4j
public class CheckLag {

    private String bootStrapServers;
    private List<LagConfig> lagConfigs;

    public CheckLag(ConsumerLagConfig consumerLagConfig) {
        this.bootStrapServers = consumerLagConfig.getBootstrapServers();
        this.lagConfigs = consumerLagConfig.getLagConfigs();
    }

    public void checkConsumerLag() {
        for (LagConfig lagConfig : lagConfigs) {
            String group = lagConfig.getGroup();
            String topic = lagConfig.getTopic();
            long lag = lagConfig.getLag();
            try (KafkaConsumer<String, String> consumer =
                         new KafkaConsumer<>(getConsumerProperties(bootStrapServers, group))) {
                List<TopicPartition> topicPartitions = new ArrayList<>();
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                for (PartitionInfo info : partitionInfos) {
                    topicPartitions.add(new TopicPartition(info.topic(), info.partition()));
                }
                consumer.assign(topicPartitions);
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
                long sumOffset = 0;
                for (TopicPartition partition : topicPartitions) {
                    OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
                    if (offsetAndMetadata != null) {
                        Long endOffset = endOffsets.get(partition);
                        if (endOffset != null) {
                            sumOffset += (endOffset - offsetAndMetadata.offset());
                        }
                    }
                }
                log.info("Topic : " + topic + " group: " + group + "Actuallag: " + sumOffset + " ExpectedLag" + lag);
                if (sumOffset >= lag) {
                    System.out.println("Group " + group + "lag is " + sumOffset + " greater than " + lag);
                }
            }
        }
    }


    private static Properties getConsumerProperties(String bootstrap, String group) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
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

        CheckLag checkLagHandler = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            ConsumerLagConfig config = mapper.readValue(configFile, ConsumerLagConfig.class);
            checkLagHandler = new CheckLag(config);
            checkLagHandler.checkConsumerLag();

        } catch (Exception ex) {
            System.out.println("Error occured : " + ex.getMessage());
            log.error("Error while checking lag ", ex);

        } finally {
        }

    }
}

