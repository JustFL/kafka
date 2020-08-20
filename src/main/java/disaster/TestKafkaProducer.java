package disaster;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestKafkaProducer {
	
	public static void main(String[] args) throws IOException {
		StartTransactionsProducer();
	}
	
	
	public static Properties getProperties() {
		
		Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092");
        props.put("transactional.id", "my_transactional_id");
        
		return props;
	}
	
	
	public static void StartTransactionsProducer() {
		
		Properties props = getProperties();
		KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
		producer.initTransactions();
		
		//开启事务
		producer.beginTransaction();
		for (int i = 0; i < 100; i++) {
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("kafka_test", Integer.toString(i), "dd:"+i);
			producer.send(producerRecord);
		}
		//结束事务
		producer.commitTransaction();
		
		producer.close();
	}
	
	/**
	 * 专门加载配置文件
	 * 配置文件的格式：
	 * key=value
	 * 在代码中要尽量减少硬编码
	 * 不要将代码写死，要可配置化
	 * @throws IOException 
	 */
	public static void standardProducer() throws IOException {
		
		Properties properties = new Properties();
		InputStream in = TestKafkaProducer.class.getClassLoader().getResourceAsStream("producer.properties");
		properties.load(in);
//		properties.setProperty("", "")
		
		/**
		 * 两个泛型参数
		 * 第一个泛型参数：指的就是kafka中一条记录key的类型
		 * 第二个泛型参数：指的就是kafka中一条记录value的类型
		 */
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		String topic = properties.getProperty("producer.topic");
		
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, "key", "good");
		//发送消息
		producer.send(producerRecord);
		producer.close();
	}
}
