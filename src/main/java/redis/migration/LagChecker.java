package redis.migration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import redis.migration.config.GroupConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class LagChecker implements LagCheckerMBean {

    private final String bootstrapServers;
    private final GroupConfig groupConfig;
    private Thread backgroundThread;
    private long lag;

    public LagChecker(String bootstrapServers, GroupConfig groupConfig, long delayInMillis) {
        this.bootstrapServers = bootstrapServers;
        this.groupConfig = groupConfig;
        this.backgroundThread = new Thread(() -> {
            try {
                while (true) {
                    lag = computeLag();
                    Thread.sleep(delayInMillis);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        this.backgroundThread.setName(groupConfig.getTopic() + "_" + groupConfig.getGroup());
        this.backgroundThread.start();
    }


    @Override
    public long getLag() {
        return lag;
    }

    private long computeLag() {
        long sumOffset = 0;
        try {
            String group = groupConfig.getGroup();
            String topic = groupConfig.getTopic();
            try (KafkaConsumer<String, String> consumer =
                         new KafkaConsumer<>(getConsumerProperties(bootstrapServers, group))) {
                List<TopicPartition> topicPartitions = new ArrayList<>();
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                for (PartitionInfo info : partitionInfos) {
                    topicPartitions.add(new TopicPartition(info.topic(), info.partition()));
                }
                consumer.assign(topicPartitions);
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
                Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
                for (TopicPartition partition : topicPartitions) {
                    OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
                    Long endOffset = endOffsets.get(partition);
                    if (offsetAndMetadata != null) {
                        if (endOffset != null) {
                            sumOffset += (endOffset - offsetAndMetadata.offset());
                        }
                    } else {
                        Long begOffset = beginningOffsets.get(partition);
                        if (begOffset != null) {
                            sumOffset += (endOffset - begOffset);
                        }
                    }
                }
            }
            return sumOffset;
        } catch (Exception ex) {
            log.debug("Error while computing the offset");
        }
        return sumOffset;
    }

    private Properties getConsumerProperties(String bootstrap, String group) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
