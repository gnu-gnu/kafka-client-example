package com.gnu.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class MainApps {
    private static final Logger LOG = LoggerFactory.getLogger(AdminUtils.class);
    private static final String BROKERS = System.getProperty("bootstrap.server", "192.168.0.201:9092,192.168.0.202:9092,192.168.0.203:9092");

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Scanner sc = new Scanner(System.in);
        try (AdminClient client = AdminUtils.getClient()) {
            DescribeClusterResult cluster = client.describeCluster();
            LOG.info("{} - {}", "cluster id", cluster.clusterId().get());
            String[] arguments;
            while (true) {
                System.out.print("kafka [" + BROKERS + "] ");
                String key = sc.nextLine().toLowerCase();
                if (key.equals("controller")) {
                    cluster.controller().thenApply(AdminUtils::showControllerInfo);
                } else if (key.equals("nodes")) {
                    cluster.nodes().thenApply(AdminUtils::showAllNodesList).get();
                } else if (key.equals("brokers")) {
                    List<ConfigResource> configResources = cluster.nodes().thenApply(AdminUtils::showAllNodesList).get();
                    AdminUtils.showAllBrokersConfigInfo(client, configResources);
                } else if (key.startsWith("broker ")) {
                    arguments = key.split(" ");
                    String brokerId = arguments[1];
                    String metricName = "";
                    if (arguments.length == 3) {
                        metricName = arguments[2];
                        AdminUtils.showBrokerMetricByMetricName(client, brokerId, metricName);
                    } else {
                        AdminUtils.showBrokerConfigInfoById(client, brokerId);
                    }
                } else if (key.startsWith("topic ")) {
                    arguments = key.split(" ");
                    String topicName = arguments[1];
                    String partitionNo = "";
                    if (arguments.length == 3) {
                        partitionNo = arguments[2];
                        // TODO TOPIC의 개별 Partition 정보
                    } else {
                        AdminUtils.showTopicInfoByName(client, topicName);
                    }
                } else if (key.startsWith("topic-conf ")) {
                    arguments = key.split(" ");
                    String topicName = arguments[1];
                    String metricName = "";
                    if (arguments.length == 3) {
                        metricName = arguments[2];
                        // TODO TOPIC의 개별 Metric 조회topic
                    } else {
                        AdminUtils.showTopicConfigInfoByName(client, topicName);
                    }
                } else if (key.equals("topics")) {
                    AdminUtils.showAllTopicsName(client);
                } else if (key.equals("help")) {
                    System.out.println("controller - show controller node");
                    System.out.println("nodes - show all nodes");
                    System.out.println("broker [id] - show broker [id]'s config list");
                    System.out.println("brokers - show all brokers config");
                    System.out.println("topic-conf [name] - show topic [name]'s config list");
                    System.out.println("topics - show all topics");
                    System.out.println("topic [name] - show topic [name]'s information");
                    System.out.println("quit - exit");
                } else if (key.isEmpty()) {

                } else if (key.equals("quit") || key.equals("exit")) {
                    sc.close();
                    break;
                } else {
                    System.out.println("command " + key + " not found - you can use 'help' command");
                }
            }
        }
    }

    private static void exec() throws InterruptedException, ExecutionException {
        try (AdminClient client = AdminUtils.getClient()) {
            DescribeClusterResult cluster = client.describeCluster();
            String clusterId = "";


            Map<ConfigResource, Config> configMap = new HashMap<>();
            ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "2");
            Config config = new Config(Arrays.asList(new ConfigEntry(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG, "30001")));
            configMap.put(resource, config);
            client.alterConfigs(configMap);
            /**
             * Broker 설정을 변경하려고 시도했지만 이 요청은 변화가 없을 것입니다.
             * Document에 따르면  topic is the only resource type with configs that can be updated currently
             * 즉 Topic 에 관련된 설정만 바꿀 수 있습니다.
             * 또한 .alterConfigs 는 2.3.0 부터는 Deprecated 입니다.
             * incrementalAlterConfigs 를 사용하세요
             */
            LOG.info("after change");

            client.listTopics().names().thenApply(topics -> {
                Set<Map.Entry<String, KafkaFuture<TopicDescription>>> topicEntries = client.describeTopics(topics).values().entrySet();
                LOG.info("This cluster has {} topics", topicEntries.size());
                String topicName = "";
                for (Map.Entry<String, KafkaFuture<TopicDescription>> topicEntry : topicEntries) {
                    topicName = topicEntry.getKey();
                    LOG.info("topic name - {}", topicName);
                    final String finalTopicName = topicName;
                    topicEntry.getValue().thenApply(topic -> {
                        List<TopicPartitionInfo> partitions = topic.partitions();
                        LOG.info("{} has {} partitions", finalTopicName, partitions.size());
                        return topic;
                    });
                }
                /*thenApply(singleTopic ->{
                    LOG.info("partition info");
                    for (Map.Entry<String, TopicDescription> topicEntry : singleTopic.entrySet()) {
                        LOG.info("Topic {}'s information", topicEntry.getKey());
                        TopicDescription topicDesc = topicEntry.getValue();
                        LOG.info("Topic name : {}", topicDesc.name());
                        List<TopicPartitionInfo> partitions = topicDesc.partitions();
                        LOG.info("Partition number : {}", partitions.size());
                        partitions.forEach(partition -> {
                            LOG.info("-- partition {}",partition.partition());
                            LOG.info("partiton leader is {}", partition.leader().id());
                            for(Node isrNode : partition.isr()){
                                LOG.info("{} is ISR",isrNode.id());
                            }
                        });

                    }
                   return singleTopic;
                });*/
                return topics;
            });
        }
    }


}
