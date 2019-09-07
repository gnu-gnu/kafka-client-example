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
                } else if (key.equals("create-topic")){
                    System.out.print("topic name? ");
                    String topicName = sc.nextLine().toLowerCase();
                    System.out.print("partitions? ");
                    int partitions = sc.nextInt();
                    System.out.print("replication? ");
                    short replicas = (short)sc.nextInt();
                    AdminUtils.createTopic(client, topicName, partitions, replicas);
                }
                else if (key.equals("help")) {
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
}
