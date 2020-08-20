package disaster;

import kafka.admin.TopicCommand;

public class KafkaUtil {
	
	public static void main(String[] args) {
		createTopic();
	}
	
	/**
	 * 创建一个主题  topic
	 * kafka-topics.sh \
	 * --create \
	 * --zookeeper hadoop02:2181,hadoop01:2181/kafka \
	 * --replication-factor 2 \
	 * --partitions 5 \
	 * --topic kafka_test
	 */
	public static void createTopic() {
		String[] ops = new String[] {
				"--create", 
				"--zookeeper", 
				"hadoop02:2181,hadoop01:2181/kafka",
				"--replication-factor", 
				"2", 
				"--partitions", 
				"5", 
				"--topic", 
				"kafka_test" };
		TopicCommand.main(ops);
	}
}
