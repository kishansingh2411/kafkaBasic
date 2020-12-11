package kafkanew;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.utils.Time;
import scala.collection.Seq;

import java.util.Properties;

public class checkandmodify {
    public static void main(String[] args) {

        ZkClient zkClient = null;
        ZkUtils zkUtils = null;

        try {
            //String zookeeperHost = "127.0.0.1:2181";
            String zookeeperHost = "localhost:2181";
            int sessionTimeOut = 15 * 1000;
            int connectionTimeOut = 10 * 1000;
            int maxInFlightRequests = 10;
            Time time = Time.SYSTEM;
            Boolean isSucre = false;
            Properties props1 = new Properties();
            Properties props = new Properties();
            props1.put("zookeeperHost","localhost:2181");
            //props1.put("zookeeperHost","127.0.0.1:2181");
            //props1.put("bootstrap.servers","127.0.0.1:9092");
            props1.put("bootstrap.servers","localhost:9092");
            //KafkaZkClient kafkaZkClient = KafkaZkClient.apply("127.0.0.1:9092",false,15000,
            //       10000,10,Time.SYSTEM,"testMetricGroup", "testMetricType");
            //AdminZkClient ob1 = new AdminZkClient(kafkaZkClient);

            zkClient = new ZkClient(zookeeperHost,sessionTimeOut,connectionTimeOut, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHost), false);

            String metricGroup = "myGroup";
            String metricType = "myType";
            KafkaZkClient zkClient_new = KafkaZkClient.apply(zookeeperHost,isSucre,sessionTimeOut,
                    connectionTimeOut,maxInFlightRequests,time,metricGroup,metricType);
            AdminZkClient adminZkClient = new AdminZkClient(zkClient_new);

            String topicName = "Hello";
            int partitions = 3;
            int replication = 1;
            Properties topicConfiguration = new Properties();
            //topicConfiguration.put("cleanup.policy", "delete");

            // delete topic
            //adminZkClient.deleteTopic(topicName);

            Seq<String> allTopic = zkClient_new.getAllTopicsInCluster();

            System.out.println("Cluset has " + allTopic.length() + " topics");
            System.out.println(allTopic);
            if(!allTopic.contains(topicName)) {
                AdminUtils.createTopic(zkUtils, topicName, partitions, replication, topicConfiguration,
                        RackAwareMode.Disabled$.MODULE$);
                System.out.println("new topic created "+ topicName);
            } else {
                topicConfiguration.put("retention.ms","65700000");
                AdminUtils.changeTopicConfig(zkUtils,topicName,topicConfiguration);
                System.out.println(adminZkClient.getAllTopicConfigs().get(topicName));
            }
            //AdminUtils.changeTopicConfig(zkUtils, topicName, topicConfiguration);
            //Map<String,Properties> props = AdminUtils.fetchAllEntityConfigs(zkUtils, ConfigType.Topic());
            //Properties props = ob1.fetchEntityConfig(zkUtils, ConfigType.Topic(), topicName)
            //scala.collection.Map props = ob1.fetchAllEntityConfigs(topicName);
            //AdminClient ob = AdminClient.create(props1);
            //ListTopicsResult l = ob.listTopics();
            //System.out.println(props);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }
}
