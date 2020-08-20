package disaster;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;



public class TestKafkaConsumer {
	
	
	public static void main(String[] args) {
		munualPollByPartition();
		
	}
	
	
	public static void autoCommit() {
		
		Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "true");
        //自动提交时间间隔
        props.put("auto.commit.interval.ms", "1000");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题，可以订阅多个主题
        consumer.subscribe(Arrays.asList("kafka_test"));
        //死循环不停的从broker中拿数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
	}
	
	
	public static void munualCommit() {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "false");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题，可以订阅多个主题
        consumer.subscribe(Arrays.asList("kafka_test"));
        final int minBatchSize = 50;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                //insertIntoDb(buffer);
                for (ConsumerRecord<String,String> bf : buffer) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", bf.offset(), bf.key(), bf.value());
                }
                
                //业务完成后 手动提交offset
                consumer.commitSync();
                buffer.clear();
            }
        }
    }
	
	
	//偏移量-手动按分区提交
	public static void munualCommitByPartition() {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "false");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题，可以订阅多个主题
        consumer.subscribe(Arrays.asList("kafka_test"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                //循环每个partition
                for (TopicPartition partition : records.partitions()) {
                	//获取当前partition的数据
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("partition: " + partition.partition() + " , " + record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    /*
                        提交的偏移量应该始终是您的应用程序将要读取的下一条消息的偏移量。因此，在调用commitSync（）时，
                        offset应该是处理的最后一条消息的偏移量加1
                        为什么这里要加上面不加喃？因为上面Kafka能够自动帮我们维护所有分区的偏移量设置，有兴趣的同学可以看看SubscriptionState.allConsumed()就知道
                     */
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
	
	//消费者从指定分区拉取数据
	public static void munualPollByPartition() {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "false");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题，并设置要拉取的分区
        TopicPartition partition0 = new TopicPartition("kafka_test", 0);
        consumer.assign(Arrays.asList(partition0));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("partition: " + partition.partition() + " , " + record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static void standardProducer() throws IOException {
		//加载配置文件
		Properties properties = new Properties();
		InputStream in = TestKafkaConsumer.class.getClassLoader().getResourceAsStream("consumer.properties");
		properties.load(in);
		
		//创建消费者
		Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		//topic列表
		Collection<String> topics = Arrays.asList("kafka_test");
		//消费者订阅topic
		consumer.subscribe(topics);
		
		ConsumerRecords<String, String> consumerRecords = null;
		while (true) {
			// 接下来就要从topic中拉取数据
			// 其实是一个阻塞的方法： 阻塞1秒钟，   每隔一秒钟从检测一下对应的topic是否有消息可拿
			consumerRecords = consumer.poll(1000);
			
			// 遍历每一条记录
			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				String topic = consumerRecord.topic();
				long offset = consumerRecord.offset();
				int partition = consumerRecord.partition();
				String key = consumerRecord.key();
				String value = consumerRecord.value();
				System.out.format("%s\t%d\t%d\t%s\t%s\n", topic, offset, partition, key, value);
			}
		}
	}
}


