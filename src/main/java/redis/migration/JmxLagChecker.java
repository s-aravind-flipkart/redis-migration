package redis.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import redis.migration.config.ConsumerConfig;
import redis.migration.config.GroupConfig;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.List;


/**
 * Created by s.aravind on 08/08/17.
 */
@Slf4j
public class JmxLagChecker {

    private static final String JMX_NAME = "redis.migration.jmxlagchecker";

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

        try {
            ObjectMapper mapper = new ObjectMapper();
            ConsumerConfig config = mapper.readValue(configFile, ConsumerConfig.class);
            List<GroupConfig> groupConfigs = config.getGroupConfigs();
            if (groupConfigs == null || groupConfigs.size() == 0) {
                System.out.println("Specify the configs zero");
                return;
            }

            MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
            for (GroupConfig groupConfig : groupConfigs) {
                String name = groupConfig.getTopic() + "##" + groupConfig.getGroup();
                ObjectName objectName = new ObjectName(JMX_NAME + ":name=" + name);
                LagChecker lagChecker =
                        new LagChecker(config.getBootstrapServers(), groupConfig, config.getDelayMillis());
                platformMBeanServer.registerMBean(lagChecker, objectName);
            }

            while (true) {
                try {
                    Thread.sleep(config.getDelayMillis() * 100);
                } catch (Exception ex) {
                    log.error("Error while checking lag", ex);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("Error while checking lag", ex);
        } finally {
        }

    }
}

