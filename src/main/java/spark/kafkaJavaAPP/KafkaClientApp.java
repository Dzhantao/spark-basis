package spark.kafkaJavaAPP;

/**
 * Created by Dzhantao on 2018/6/1.
 *
 *
 */
public class KafkaClientApp extends Thread{

    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }


}
