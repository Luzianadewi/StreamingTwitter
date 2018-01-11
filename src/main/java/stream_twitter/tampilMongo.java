package stream_twitter;

import java.util.Iterator;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class tampilMongo {
	public static void main(String[] args) {
		Mongo mongo = new Mongo("localhost",27017);
		DB db = mongo.getDB("twitter");
		
		DBCollection collection = db.getCollection("tweet");
		
		BasicDBObject whereQuery = new BasicDBObject();
	    whereQuery.put("title", "MongoDB");
		BasicDBObject row = new BasicDBObject();
		row.put("text", 1);
		  DBCursor cursor = collection.find(whereQuery,row);
		  while (cursor.hasNext()) {
			System.out.println(cursor.next());
		  }
	}
}
