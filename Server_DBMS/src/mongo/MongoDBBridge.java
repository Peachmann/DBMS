package mongo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import message.Attribute;
import structure.DBStructure;

public class MongoDBBridge {

	private static MongoDBBridge singleton = null;
	private static String connectionString = "mongodb+srv://m001-student:m001-mongodb-basics@cluster0-dlhll.mongodb.net/test?retryWrites=true&w=majority";
	private static MongoClient mongoClient;
	private static final String AppDB = "PhysiaDB";
	 
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
		
		MongoDatabase client = mongoClient.getDatabase(AppDB);
		client.createCollection(dbname);
	}
	
	public void mdbDropDB(String dbname) {
		
		MongoDatabase client = mongoClient.getDatabase(AppDB);
		MongoCollection<org.bson.Document> collection = client.getCollection(dbname);
		collection.drop();
	}
	
	public void mdbCreateTable(String dbname, String tbname) {
		
		MongoDatabase client = mongoClient.getDatabase(AppDB);
		MongoCollection<org.bson.Document> database = client.getCollection(dbname);
		
		Document table = new Document("table#name", tbname);
		
		database.insertOne(table);
	}
	
	public void mdbDropTable(String dbname, String tbname) {
		
		MongoDatabase client = mongoClient.getDatabase(AppDB);
		MongoCollection<org.bson.Document> database = client.getCollection(dbname);
		
		database.deleteOne(Filters.eq("table#name",tbname));
	}
	
	public void mdbCreateIndex() {
		
		//TO-DO

	}
	
	public void mdbInsertData(String dbname, String tbname, int tableLength, int totalInserts, ArrayList<Attribute> values) {
		
		String pk = DBStructure.getTablePK(dbname,tbname);
		
		MongoDatabase client = mongoClient.getDatabase(AppDB);
		MongoCollection<Document> database = client.getCollection(dbname);
		
		FindIterable<Document> docs = database.find(new Document("table#name",tbname));
		
		Document updatedValue = new Document("table#name",tbname);
		
		for(int i = 0; i < totalInserts; i++) {
			
			String key = "";
			String value = "";
			for(int j = 0; j < tableLength; j++) {
				
				Attribute curr = values.get(i * tableLength + j);
				if(curr.getName().equals(pk)) {
					
					key = curr.getValue();
				} else {
					
					value += curr.getName() + "/" + curr.getType() + "/" + curr.getValue();
				}
			}
			updatedValue.append(key, value);
		}
		
		Bson updateOperation = new Document("$set",(Bson)updatedValue);
		for(Document doc : docs) {
			
			if(doc.containsKey("table#name") && doc.get("table#name").equals(tbname)) {

				database.updateOne(doc, updateOperation);
				break;
			}
		}
		
	}
	
	public boolean mdbKeyExists(String dbname, String tbname, Object key) {
		
		MongoDatabase client = mongoClient.getDatabase(AppDB);
		MongoCollection<Document> database = client.getCollection(dbname);
		
		FindIterable<Document> docs = database.find(new Document("table#name",tbname));
        for (Document doc : docs) {
        	
        	if(doc.containsKey("table#name") && doc.get("table#name").equals(tbname)) {
        	
        		if(doc.containsKey(key)) {
        		
        			return true;
        		}
        		break;
        	}
        }
		
		return false;
	}
	
	public void mdbDeleteData(String dbname, String tbname, ArrayList<Attribute> values) {
		
		String pk = DBStructure.getTablePK(dbname,tbname);
		
		MongoDatabase client = mongoClient.getDatabase(AppDB);
		MongoCollection<Document> database = client.getCollection(dbname);
		
		FindIterable<Document> docs = database.find(new Document("table#name",tbname));
		
		Document deletedValue = new Document("blank#value","");
		
		for(Document table : docs) {
			
			if(table.containsKey("table#name") && table.get("table#name").equals(tbname)) {
				
				for(int i = 0; i < values.size(); i++) {
					
					if(values.get(i).getName().equals(pk)) {
						
						deletedValue.append(values.get(i).getValue(), table.get(values.get(i).getValue()));
						/*System.out.println(pk + " " + values.get(i).getName());
						System.out.println(table.get(values.get(i).getValue()));
						table.remove(values.get(i).getValue());
						System.out.println(table.get(values.get(i).getValue()));*/
					}
				}
				break;
			}
		}
		
		docs = database.find(new Document("table#name",tbname));
		
		Bson updateOperation = new Document("$unset",(Bson)deletedValue);
		for(Document doc : docs) {
			
			if(doc.containsKey("table#name") && doc.get("table#name").equals(tbname)) {

				database.updateOne(doc, updateOperation);
				break;
			}
		}
		
	}
	
	public ArrayList<String> mdbGetTableContent(String dbname, String tbname) {
		
		ArrayList<String> list = new ArrayList<String>();
		
		MongoDatabase client = mongoClient.getDatabase(AppDB);
		MongoCollection<Document> database = client.getCollection(dbname);
		
		FindIterable<Document> docs = database.find(new Document("table#name",tbname));
		
		for(Document table : docs) {
			if(table.containsKey("table#name") && table.get("table#name").equals(tbname)) {
				
				Set<String> keys = table.keySet();
				Iterator<String> iterator = keys.iterator();
				
				while(iterator.hasNext()) {
					
					String key = iterator.next();
					if(!key.equals("table#name") && !key.equals("_id")) {
						
						list.add(key + "#" + table.get(key));
					}
				}
				break;
			}
		}
		
		return list;
	}
}
