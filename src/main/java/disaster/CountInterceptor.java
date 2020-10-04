package main.java.disaster;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CountInterceptor implements ProducerInterceptor<String, String> {

	int succeed;
	int failure;
	
	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		// TODO Auto-generated method stub
		return record;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		if (metadata != null) {
			succeed++;
		} else {
			failure++;
		}
	}

	@Override
	public void close() {
		System.out.println("succeed:" + succeed);
		System.out.println("failure:" + failure);
	}

}
