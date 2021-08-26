import com.dcss.anz.Metrics;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class Example {


    public static void main(String[] args) {
        var keepRunning = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> keepRunning.set(false)));

        //This parameter might speed things up if bulk inserting lots of things.
        var connectUrl = "jdbc:sqlserver://....;useBulkCopyForBatchInsert=true";

        //Naturally you want actual properties, and relevant deserializers
        Map<String, Object> conf = Map.of("enable.auto.commit", false);

        try (var connection = DriverManager.getConnection(connectUrl);
             var consumer = new KafkaConsumer<>(conf, new StringDeserializer(), new KafkaAvroDeserializer())) {

            consumer.subscribe(List.of("example_topic"));
            while (keepRunning.get()) {
                PreparedStatement ps = connection.prepareStatement("insert into weather (pressure, temp) values (?, ?)");
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Object> record: records) {
                    Metrics receivedData = (Metrics) record.value();
                    ps.setFloat(1, receivedData.getPayload().getPressure());
                    ps.setFloat(2, receivedData.getPayload().getTemperature());
                    ps.addBatch();
                }

                ps.executeBatch();
                consumer.commitSync();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
