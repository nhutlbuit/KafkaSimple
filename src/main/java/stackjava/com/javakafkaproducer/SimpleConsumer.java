package stackjava.com.javakafkaproducer;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer {
	public static void main(String[] args) {
		org.apache.log4j.BasicConfigurator.configure();
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		
		// đọc các message của topic từ thời điểm hiển tại (default)
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		// đọc tất cả các message  của topic từ offset ban đầu
		//		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("test"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}

	}
}

/*

after brew install kafka

run 2 configs: zookeeper and kafka server
$ zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
$ kafka-server-start /usr/local/etc/kafka/server.properties

Run SimpleConsumer to listen
run Producer to send message

Dữ liệu trong Kafka được lưu theo topic.
Các topic được phân vùng với nhau vào các partition.
Mỗi partition được chia nhỏ thành các segment.
Mỗi segment có một file log chứa nội dung message và một file index chứa vị trí của message trong file log.
Các partition khác nhau của một topic có thể nằm trên các broker khác nhau, nhưng mỗi một partition chỉ thuộc về 1 broker duy nhất.
Các partition được tự động nhân bản, bạn chỉ có thể đọc message từ phần đó khi nó trở thành Leader mới.

 */
