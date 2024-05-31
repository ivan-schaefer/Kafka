import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import persons.avro.PersonDataAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SchemaConsumerDemo {
    public static void main(String[] args) {

        Duration timeout = Duration.ofMillis(100);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "c1");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        String topic = "avro_persons";

        KafkaConsumer<String, PersonDataAvro> consumer = new KafkaConsumer<String, PersonDataAvro>(props);

        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            ConsumerRecords<String, PersonDataAvro> records = consumer.poll(timeout);
            for (ConsumerRecord<String, PersonDataAvro> records1: records) {
                System.out.println("Received entity full name is: " +
                        records1.value().getFullName());
            }
            consumer.commitSync();
        }

    }
}
