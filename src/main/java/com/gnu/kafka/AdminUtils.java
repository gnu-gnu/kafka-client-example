package com.gnu.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MainApps.class);
    private static final String BROKERS = System.getProperty("bootstrap.server", "192.168.0.201:9092,192.168.0.202:9092,192.168.0.203:9092");

    /**
     * 모든 Topic의 이름을 출력한다.
     *
     * @param client
     */
    protected static void showAllTopicsName(AdminClient client) {
        client.listTopics().names().thenApply(topics -> {
            Set<Map.Entry<String, KafkaFuture<TopicDescription>>> topicEntries = client.describeTopics(topics).values().entrySet();
            String topicName = "";
            System.out.println();
            for (Map.Entry<String, KafkaFuture<TopicDescription>> topicEntry : topicEntries) {
                topicName = topicEntry.getKey();
                LOG.info("topic name - {}", topicName);
            }
            return topics;
        });
    }

    /**
     *
     * 특정 Topic 의 내부 정보를 topic 이름으로 검색한다
     *
     * @param client
     * @param topicName
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected static void showTopicInfoByName(AdminClient client, String topicName) throws ExecutionException, InterruptedException {
        client.describeTopics(Arrays.asList(topicName)).all().thenApply(AdminUtils::retrieveTopic);
    }

    /**
     *
     * showTopicInfoByName 에서 토픽 내부의 정보를 탐색하기 위해 사용하는 메소드
     * Topic 내부의 파티션 정보를 보여준다.
     *
     * @param value
     * @return
     */
    private static TopicDescription retrieveTopic(Map<String, TopicDescription> value) {
        TopicDescription topicDesc = null;
        System.out.println();
        for (Map.Entry<String, TopicDescription> topicDescriptionEntry : value.entrySet()) {
            LOG.info("topic name - {}", topicDescriptionEntry.getKey());
            topicDesc = topicDescriptionEntry.getValue();
            LOG.info("internal topic - {}", topicDesc.isInternal());
            topicDesc.partitions().forEach(partition -> {
                StringBuilder isrStringBuilder = new StringBuilder();
                for (Node node : partition.isr()) {
                    isrStringBuilder.append(node.id()).append(" ");
                }
                StringBuilder replicaStringBuilder = new StringBuilder();
                for (Node node : partition.replicas()){
                    replicaStringBuilder.append(node.id()).append(" ");
                }
                LOG.info("partition {} / leader {} / replicas {} / ISR {}", partition.partition(),partition.leader().id(), replicaStringBuilder.toString(), isrStringBuilder.toString());
            });
        }
        return topicDesc;
    }

    /**
     *
     * Topic을 생성한다
     *
     * @param client
     * @param topicName topic이름
     * @param partitions topic의 partition, 이 숫자는 전송 속도에 영향을 미친다.
     * @param replicas topic의 복제본이 될 replica의 갯수, 이 숫자는 안정성에 영향을 미친다.
     * @return
     */
    protected static CreateTopicsResult createTopic(AdminClient client, String topicName, int partitions, short replicas){
        return client.createTopics(Arrays.asList(new NewTopic(topicName, partitions, replicas)));
    }

    /**
     * 특정 Topic의 전체 Config 정보를 보여준다
     *
     * @param client
     * @param topicName
     */
    protected static void showTopicConfigInfoByName(AdminClient client, String topicName) {
        ConfigResource resource = new ConfigResource((ConfigResource.Type.TOPIC), topicName);
        showConfigInfo(client, resource);
    }

    /**
     * 특정 Broker의 전체 Config 정보를 보여준다.
     *
     * @param client
     * @param brokerId broker.id에 설정한 값
     */
    protected static void showBrokerConfigInfoById(AdminClient client, String brokerId) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
        showConfigInfo(client, resource);
    }

    /**
     * showBrokerInfoById 와 showTopicInfoByName 이 공통으로 사용하는 Config 조회 메소드
     *
     * @param client
     * @param resource
     */
    protected static void showConfigInfo(AdminClient client, ConfigResource resource) {
        Set<Map.Entry<ConfigResource, KafkaFuture<Config>>> entries = client.describeConfigs(Arrays.asList(resource)).values().entrySet();
        for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : entries) {
            entry.getValue().thenApply(value -> {
                value.entries().forEach(configEntry -> {
                    LOG.info("[{}] {} = {}", configEntry.source().name(), configEntry.name(), configEntry.value());
                });
                return value;
            });
        }
    }

    /**
     * 특정 브로커의 Metric을 name으로 조회
     *
     * @param client
     * @param brokerId         broker.id 에 설정한 값, nodes 명령어로 조회 가능하다
     * @param brokerMetricName broker의 metric명 {@link CommonClientConfigs} 의 값 혹은 <a href="https://kafka.apache.org/documentation/#brokerconfigs">Apache Kafka Documents#BrokerConfigs</a>를 참고한다
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected static void showBrokerMetricByMetricName(AdminClient client, String brokerId, String brokerMetricName) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
        Set<Map.Entry<ConfigResource, KafkaFuture<Config>>> entries = client.describeConfigs(Arrays.asList(resource)).values().entrySet();
        for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : entries) {
            entry.getValue().thenApply(value -> {
                ConfigEntry configEntry = value.get(brokerMetricName);
                LOG.info("[{}] {} = {}", configEntry.source().name(), configEntry.name(), configEntry.value());
                return value;
            });
        }

    }

    /**
     * 전달 받은 AdminClient를 통해 Node들의 ConfigResource List를 순회하며 모든 Node의 Config 정보를 출력한다.
     * 사용자가 직접 설정한 STATIC_CONFIG 혹은 묵시적인 DEFAULT_CONFIG 등의 분류와 Broker에 설정된 해당 Config 값을 출력한다.
     *
     * @param client          {@link AdminClient}
     * @param configResources Node들의 ConfigResoure 정보가 담긴 리스트, {@link ConfigResource}
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected static void showAllBrokersConfigInfo(AdminClient client, List<ConfigResource> configResources) throws InterruptedException, ExecutionException {
        for (Map.Entry<ConfigResource, KafkaFuture<Config>> brokerEntry : client.describeConfigs(configResources).values().entrySet()) {
            ConfigResource configResource = brokerEntry.getKey();
            LOG.info("{} {}", configResource.type().name(), configResource.name());
            Config config = brokerEntry.getValue().get();
            config.entries().forEach(entry -> LOG.info("[{}] {} = {}", entry.source().name(), entry.name(), entry.value()));
        }
    }

    /**
     * @param nodes Cluster가 가지고 있는 Nodes Collection
     * @return Nodes(= Brokers)의 ConfigResource 정보 List
     */
    protected static List<ConfigResource> showAllNodesList(Collection<Node> nodes) {
        List<ConfigResource> resources = new ArrayList<>();
        nodes.forEach(node -> {
            String rackInfo = Optional.ofNullable(node.rack()).orElse("not configured");
            LOG.info("node {} : {}:{}, rack is {}", node.id(), node.host(), node.port(), rackInfo);
            resources.add(new ConfigResource(ConfigResource.Type.BROKER, node.idString()));
        });
        return resources;
    }

    /**
     * Cluster의 Controller 정보를 출력한다.
     *
     * @param controller Cluster의 controller Node
     * @return Controller Node
     */
    protected static Node showControllerInfo(Node controller) {
        String rackInfo = Optional.ofNullable(controller.rack()).orElse("not configured");
        LOG.info("controller : {}:{} - node {}, rack is {}", controller.host(), controller.port(), controller.idString(), rackInfo);
        return controller;
    }

    /**
     * @return AdminClient Broker, TOPIC, ConsumerGroup)의 정보를 얻는데 사용하는 Client
     */
    protected static AdminClient getClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        return AdminClient.create(props);
    }
}
