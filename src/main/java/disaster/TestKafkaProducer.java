package main.java.disaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
//这个类定义了所有的生产者参数 再也不用记字符串的参数名字了 卧槽！
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit.Procedure;


public class TestKafkaProducer {
	
	public static void main(String[] args) throws IOException {
		startProducerWithInterceptor();
	}
	
	
	public static Properties getProperties() {
		
		Properties props = new Properties();
		
		//集群地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092");
        //ack应答级别 [all, -1, 0, 1] all等于-1
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //发送失败重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        //procedure会将发往同一个partition的record打包成一个batch发送 提高效率 这里设置为16k
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        //发送延迟时间 我们希望将多个record打包成一个batch发送 这需要生成record的速度快于发送record的速度 因为如果发送很快 生成较慢 每生成一条数据立马就被发走了 都来不及打包成batch
        //这个参数人为的让发送等一等 让batch中能多积累一些record
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        //RecordAccumulator大小 32M
        props.put("buffer.memory", "33554432");
        //指定key value的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //事务id 用来保证精准一次送往kafka集群
        //props.put("transactional.id", "my_transactional_id");
        
		return props;
	}
	
	
	public static void startTransactionsProducer() {
		
		Properties props = getProperties();
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
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
	public static void startStandardProducer() throws IOException {
		
		Properties props = getProperties();
		
		/**
		 * 两个泛型参数
		 * 第一个泛型参数：指的就是kafka中一条记录key的类型
		 * 第二个泛型参数：指的就是kafka中一条记录value的类型
		 */
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		String topic = "T1";
		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, "atguigu--"+i);
			//发送消息
			producer.send(producerRecord);
		}
		//关闭生产者会将缓存中的没有达到batch.size的数据或者batch没有等到linger.ms的数据 发送出去
		producer.close();
	}
	
	
	public static void startStandardProducerWithCallback() throws IOException {
		
		Properties props = getProperties();
		
		/**
		 * 两个泛型参数
		 * 第一个泛型参数：指的就是kafka中一条记录key的类型
		 * 第二个泛型参数：指的就是kafka中一条记录value的类型
		 */
		
		/**
		 * kafka-2.4.0之前的版本提供了一个默认策略：org.apache.kafka.clients.producer.internals.DefaultPartitioner
		 * 这个分区策略的流程是：如果消息key为空，先随机选择一个分区，后续按照轮询策略分配分区
		 * kafka-2.4.0及以后的版本做了如下的变更，提供了如下三个分区策略：
		 * 1、org.apache.kafka.clients.producer.internals.DefaultPartitioner
		 * 默认策略，做的变动是：如果消息键为空消息发送的分区先保持粘性（也就是先向同一个分区发送）；如果当前batch已满或者linger.ms超时已经发送，那么新的消息会发给另外的分区（选择策略还是Round-Robin）
		 * 这样变动的原因个人理解是为了减少客户端和服务端的交互次数，消息按照batchSize发送
		 * 2、org.apache.kafka.clients.producer.RoundRobinPartitioner
		 * 轮询策略，没啥可说的
		 * 3、org.apache.kafka.clients.producer.UniformStickyPartitioner
		 * 默认策略，消息key为空场景
		 */
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.RoundRobinPartitioner");
		
		
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		String topic = "T1";
		for (int i = 0; i < 10; i++) {
			String value = "atguigu--" + i;
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, value);
			//调用send方法后 依次经过拦截器 序列化器 分区器 在分区器中会将数据打包 按照RoundRobin轮循的将每条record分配进不同的分区
			//这里atguigu--0进入2号分区的batch atguigu--1进入0号分区的batch 依次类推 然后一起发送
			//在消费者端会将一个分区的数据整体拉取
			producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
				if (exception == null) {
					System.out.println(metadata.partition() + " -- " + metadata.offset() + "   " + value);
				} else {
					exception.printStackTrace();
				}
			});
		}
		producer.close();
	}
	
	
	public static void startPartitionProducer() throws IOException {
		
		Properties props = getProperties();
		//更换为自定义的分区器
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "main.java.disaster.MyPartitioner");
		
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		String topic = "T1";
		for (int i = 400; i < 450; i++) {
			String value = "atguigu--" + i;
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, value, value);
			producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
				if (exception == null) {
					System.out.println(metadata.partition() + " -- " + metadata.offset() + "   " + value);
				} else {
					exception.printStackTrace();
				}
			});
		}
		System.out.println("~~~~~~~~~~~");
		producer.close();
	}
	
	
public static void startProducerWithInterceptor() throws IOException {
		
		Properties props = getProperties();
		//更换为自定义的分区器
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.RoundRobinPartitioner");
		
		//添加拦截器链 注意顺序
		ArrayList<String> interceptorList = new ArrayList<String>();
		interceptorList.add("main.java.disaster.TimeInterceptor");
		interceptorList.add("main.java.disaster.CountInterceptor");
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorList);
		
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		String topic = "T1";
		for (int i = 600; i < 650; i++) {
			String value = "atguigu--" + i;
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, value);
			producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
				if (exception == null) {
					//添加的时间在这里看不到 回想生产者的执行流程 先send后 一次经过拦截器 序列化器 分区器 所以添加的时间在消费者端可以看到
					System.out.println(metadata.partition() + " -- " + metadata.offset() + "   " + value);
				} else {
					exception.printStackTrace();
				}
			});
		}
		producer.close();
	}
}
