package spark.kafkaJavaAPP;

/**
 * Created by Dzhantao on 2018/6/1.
 * Kafka常用配置文件
 */
public class KafkaProperties {
    public static final String ZK = "192.168.44.10:2181,192.168.44.11:2181,192.168.44.12:2181";
    public static final String TOPIC = "test";
    public static final String BROKER_LIST = "192.168.44.10:9092,192.168.44.11:9092,192.168.44.12:9092";
    public static final String GROUP_ID = "test_group1";
}
