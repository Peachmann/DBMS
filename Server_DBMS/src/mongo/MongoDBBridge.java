package mongo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import message.Attribute;
import structure.DBStructure;

public class MongoDBBridge {

	private static MongoDBBridge singleton = null;
	private static String connectionString = "mongodb+srv://m001-student:m001-mongodb-basics@cluster0-dlhll.mongodb.net/test?retryWrites=true&w=majority";
	private static MongoClient mongoClient;
	 
	private MongoDBBridge() {

		mongoClient = MongoClients.create(connectionString);
	}
	
	public static MongoDBBridge getInstance() {
		
		if(singleton == null) {
			
			singleton = new MongoDBBridge();
		}
		return singleton;
	}
	
	public void mdbCreateDB(String dbname) {
		
		/* To be honest nothing really comes here */
	}
	
	public void mdbDropDB(String dbname) {
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		MongoIterable<String> collections = database.listCollectionNames();
		MongoCursor<String> cursor = collections.iterator();
		while(cursor.hasNext()){
			
			String table = cursor.next();
			database.getCollection(table).drop();
		}
	}
	
	public void mdbCreateTable(String dbname, String tbname) {
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		database.createCollection(tbname);
		
		//TO-DO : Create indexes on keys
	}
	
	public void mdbDropTable(String dbname, String tbname) {
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		MongoCollection<org.bson.Document> collection = database.getCollection(tbname);
		collection.drop();
	}
	
	public void mdbCreateIndex(String dbname, String tbname, String column) {
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		MongoCollection<Document> table = database.getCollection(tbname);
		table.createIndex(new BasicDBObject(column,1));
	}
	
	public void mdbInsertData(String dbname, String tbname, int tableLength, int totalInserts, ArrayList<Attribute> values) {
		
		String pk = DBStructure.getTablePK(dbname,tbname);
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		MongoCollection<Document> table = database.getCollection(tbname);
		
		for(int i = 0; i < totalInserts; i++) {
			
			Document row = new Document();
			for(int j = 0; j < tableLength; j++) {
				
				Attribute curr = values.get(i * tableLength + j);
				if(curr.getName().equals(pk)) {
					
					row.append(pk, curr.getValue());
				} else {
					
					row.append(curr.getName() + "#" + curr.getType(), curr.getValue());
				}
			}
			table.insertOne(row);
		}
		
	}
	
	public boolean mdbKeyExists(String dbname, String tbname, String key, String value) {

		MongoDatabase database = mongoClient.getDatabase(dbname);
		MongoCollection<Document> table = database.getCollection(tbname);
		
		FindIterable<Document> docs = table.find(new Document(key, value));
		for(Document row : docs) {
			if(row.containsKey(key) && row.getString(key).equals(value)) {
				return true;
			}
		}
		
		return false;
	}
	
	public void mdbDeleteData(String dbname, String tbname, ArrayList<Attribute> values) {
		
		String pk = DBStructure.getTablePK(dbname,tbname);
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		MongoCollection<Document> table = database.getCollection(tbname);
		
		for(int i = 0; i < values.size(); i++) {
			if(values.get(i).getName().equals(pk)) {
			
				table.deleteOne(new Document(pk, values.get(i).getValue()));
			}
		}
		
	}
	
	public ArrayList<String> mdbGetTableContent(String dbname, String tbname) {

		String pk = DBStructure.getTablePK(dbname,tbname);
		ArrayList<String> list = new ArrayList<String>();
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		MongoCollection<Document> table = database.getCollection(tbname);
		
		FindIterable<Document> docs = table.find();
		
		for(Document row : docs) {

			Set<String> keys = row.keySet();
			Iterator<String> iterator = keys.iterator();
			
			String send = row.getString(pk) + "#";
			while(iterator.hasNext()) {
				
				String key = iterator.next();
				if(!key.equals("_id") && !key.equals(pk)) {
					send += key + "#" + row.getString(key) + "#";
				}
			}
			list.add(send);
		}
		
		return list;
	}
	
}
