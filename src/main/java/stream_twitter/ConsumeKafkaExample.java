package stream_twitter;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import twitter4j.JSONException;
import twitter4j.JSONObject;

public class ConsumeKafkaExample {
	public static void main(String[] args) throws JSONException {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");

		props.put("group.id", "group." + UUID.randomUUID().toString());
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "latest");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		// Consume kafka multiple topic
		consumer.subscribe(Arrays.asList("akakom"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				// System.out.println(record.value());
				JSONObject json = new JSONObject(record.value().toString());
				
				
				MongoClient mg= new MongoClient("localhost",27017);
				MongoCredential credential= MongoCredential.createCredential("siUnyil", "twitter", "unyil".toCharArray());
//				 System.out.println("Connected to the database successfully");  
			     
			     // Accessing the database 
			     MongoDatabase database = mg.getDatabase("twitter"); 
			          
			     // Retieving a collection
			     MongoCollection<Document> collection = database.getCollection("tweet"); 
//			     System.out.println("Collection myCollection selected successfully");
			     
			     Document document = new Document("title", "MongoDB") 
			    	      .append("id", json.getString("id") )
			    	      .append("text", json.getString("text") );  
			    	      collection.insertOne(document); 
			    	      System.out.println("Document inserted successfully"); 
				}
			}
		}
}
