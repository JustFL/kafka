package main.java.disaster;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

public class KafkaUtil {
	
	public static void main(String[] args) {
		createTopic();
	}
	
	
	public static void createTopic() {

		Properties props = new Properties();
		AdminClient admin = AdminClient.create(props);
		NewTopic topic = new NewTopic("T1", 4, (short) 2);
		admin.createTopics(Arrays.asList(topic));
		admin.close();
	}
}
