package mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

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
	
	public void mdbCreateIndex() {
		
		//TO-DO
	}
}
