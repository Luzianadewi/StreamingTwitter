package stream_twitter;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class cobaMongo {
	public static void main(String[] args) {
	MongoClient mg= new MongoClient("localhost",27017);
	MongoCredential credential= MongoCredential.createCredential("siUnyil", "twitter", "unyil".toCharArray());
	 System.out.println("Connected to the database successfully");  
     
     // Accessing the database 
     MongoDatabase database = mg.getDatabase("twitter"); 
          
     // Retieving a collection
     MongoCollection<Document> collection = database.getCollection("tweet"); 
     System.out.println("Collection myCollection selected successfully");
     
     Document document = new Document("title", "MongoDB") 
    	      .append("id", "343546464")
    	      .append("text", "isi via java");  
    	      collection.insertOne(document); 
    	      System.out.println("Document inserted successfully"); 
	}
}
