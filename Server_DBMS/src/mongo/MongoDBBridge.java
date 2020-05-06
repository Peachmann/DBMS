package mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
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
	
	public void mdbCreateIndexPK(String dbname, String tbname, String column) {
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		MongoCollection<Document> table = database.getCollection(tbname);
		table.createIndex(new BasicDBObject(column,1));
	}
	
	public void mdbCreateIndex(String dbname, String tbname, String column, String name) {
		
		String pk = DBStructure.getTablePK(dbname, tbname);
		String type = DBStructure.getAttributeType(dbname, tbname, column);
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		String indName = dbname + "#" + tbname + "#" + column + "#" + name;
		database.createCollection(indName);
		MongoCollection<Document> table = database.getCollection(tbname);
		MongoCollection<Document> index = database.getCollection(indName);
		
		FindIterable<Document> docs = table.find();
		Hashtable<String,String> values = new Hashtable<String,String>();
		for(Document d : docs) {
			String id = d.getString(pk);
			String[] data = d.getString("#data#").split("#");
			String attr = "";
			for(int i = 0; i < data.length; i += 3) {
				if(data[i].equals(column) && data[i + 1].equals(type)) {
					attr = data[i + 2];
					break;
				}
			}
			if(values.containsKey(attr)) {
				String getv = values.get(attr);
				getv += "#" + id;
				values.remove(attr);
				values.put(attr, getv);
			} else {
				values.put(attr, id);
			}
		}
		index.createIndex(new BasicDBObject(column,1));
		
		Set<String> keys = values.keySet();
		for(String k : keys) {
			String value = values.get(k);
			Document indData = new Document();
			indData.append(column, k);
			indData.append("ID", value);
			index.insertOne(indData);
		}
	}
	
	public void mdbInsertData(String dbname, String tbname, int tableLength, int totalInserts, ArrayList<Attribute> values) {
		
		String pk = DBStructure.getTablePK(dbname,tbname);
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		MongoCollection<Document> table = database.getCollection(tbname);
		
		for(int i = 0; i < totalInserts; i++) {
			
			Document row = new Document();
			String data = "";
			for(int j = 0; j < tableLength; j++) {
				
				Attribute curr = values.get(i * tableLength + j);
				if(curr.getName().equals(pk)) {
					
					row.append(pk, curr.getValue());
				} else {
					
					data += curr.getName() + "#" + curr.getType() + "#" + curr.getValue() + "#";
				}
			}
			row.append("#data#", data);
			table.insertOne(row);
		}
		
		Hashtable<String, String> indexes = DBStructure.getIndexes(dbname, tbname);
		Set<String> keys = indexes.keySet();
		for(String k : keys) {
			String col = indexes.get(k);
			if(col.equals(pk)) {
				continue;
			}
			Hashtable<String, String> indForCol = new Hashtable<String, String>();
			for(int i = 0; i < totalInserts; i++) {
				String pkVal = "";
				String colVal = "";
				int vals = 0;
				for(int j = 0; j < tableLength; j++) {
					
					Attribute curr = values.get(i * tableLength + j);
					if(curr.getName().equals(pk)) {
						
						pkVal = curr.getValue();
						vals++;
						if(vals == 2) {
							break;
						}
					}
					if(curr.getName().equals(col)) {
						
						colVal = curr.getValue();
						vals++;
						if(vals == 2) {
							break;
						}
					}
				}
				if(indForCol.containsKey(colVal)) {
					String old = indForCol.get(colVal);
					old += "#" + pkVal;
					indForCol.remove(colVal);
					indForCol.put(colVal, old);
				} else {
					indForCol.put(colVal, pkVal);
				}
			}
			
			MongoCollection<Document> index = database.getCollection(k);
			Set<String> insertInd = indForCol.keySet();
			for(String colValue : insertInd) {
				
				String old = "";
				FindIterable<Document> docs = index.find(new Document(col, colValue));
				for(Document val : docs) {
					old = val.getString("ID");
				}
				Document changedIndex = new Document(col, colValue);
				if(!old.isEmpty()) {

					index.deleteOne(new Document(col, colValue));
					changedIndex.append("ID", old + "#" + indForCol.get(colValue));
				} else {
					
					changedIndex.append("ID", indForCol.get(colValue));
				}
				index.insertOne(changedIndex);
			}
		}
		
	}
	
	public boolean mdbKeyExists(String dbname, String tbname, String key, String value) {

		String pk = DBStructure.getTablePK(dbname, tbname);
		String index = DBStructure.getIndexName(dbname, tbname, key);
		if(key.equals(pk) || index.equals("#NO_INDEX#")) {
			
			MongoDatabase database = mongoClient.getDatabase(dbname);
			MongoCollection<Document> table = database.getCollection(tbname);
			
			FindIterable<Document> docs = table.find();
			for(Document row : docs) {
				String[] data = row.getString("#data#").split("#");
				for(int i = 0; i < data.length; i += 3) {
					if(data[i].equals(key) && data[i+2].equals(value)) {
						return true;
					}
				}
			}
			
		} else {

			MongoDatabase database = mongoClient.getDatabase(dbname);
			MongoCollection<Document> ind = database.getCollection(index);
			
			FindIterable<Document> docs = ind.find(new Document(key, value));
			for(Document row : docs) {
				if(row.containsKey(key) && row.getString(key).equals(value) && !row.getString("ID").isEmpty()) {
					return true;
				}
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
		
		Hashtable<String, String> indexes = DBStructure.getIndexes(dbname, tbname);
		Set<String> keys = indexes.keySet();
		for(String k : keys) {
			String col = indexes.get(k);
			Hashtable<String, String> indForCol = new Hashtable<String, String>();
			int vals = 0;
			String pkVal = "";
			String colVal = "";
			for(int i = 0; i < values.size(); i++) {
				if(values.get(i).getName().equals(pk)) {
					
					pkVal = values.get(i).getValue();
					vals++;
				}
				if(values.get(i).getName().equals(col)) {
					
					colVal = values.get(i).getValue();
					vals++;
				}
				if(vals == 2) {
					
					if(indForCol.containsKey(colVal)) {
						String old = indForCol.get(colVal);
						old += "#" + pkVal;
						indForCol.remove(colVal);
						indForCol.put(colVal, old);
					} else {
						indForCol.put(colVal, pkVal);
					}
					vals = 0;
				}
			}
			
			MongoCollection<Document> index = database.getCollection(k);
			Set<String> insertInd = indForCol.keySet();
			for(String colValue : insertInd) {
				
				String old = "";
				FindIterable<Document> docs = index.find(new Document(col, colValue));
				for(Document val : docs) {
					old = val.getString("ID");
				}
				ArrayList<String> elements = new ArrayList<String>(Arrays.asList(old.split("#")));
				ArrayList<String> removal = new ArrayList<String>(Arrays.asList(indForCol.get(colValue).split("#")));
				for(String r : removal) {
					elements.remove(r);
				}
				if(elements.size() == 0) {
					
					index.deleteOne(new Document(col, colValue));
				} else {
					
					String newIDs = elements.get(0);
					int s = elements.size();
					for(int i = 1; i < s; i++) {
						newIDs += "#" + elements.get(i);
					}
					index.deleteOne(new Document(col, colValue));
					Document reborn = new Document(col, colValue);
					reborn.append("ID", newIDs);
					index.insertOne(reborn);
				}
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

			String send = row.getString(pk) + "#" + row.getString("#data#");
			list.add(send);
		}
		
		return list;
	}
	
}
