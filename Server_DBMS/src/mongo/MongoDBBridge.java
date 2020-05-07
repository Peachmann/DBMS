package mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
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
import message.Operator;
import message.Where;
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
			String id = d.get(pk).toString();
			String[] data = d.get("#data#").toString().split("#");
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
					old = val.get("ID").toString();
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
				String[] data = row.get("#data#").toString().split("#");
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
				if(row.containsKey(key) && row.get(key).toString().equals(value) && !row.get("ID").toString().isEmpty()) {
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
					old = val.get("ID").toString();
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

			String send = row.get(pk) + "#" + row.get("#data#").toString();
			list.add(send);
		}
		
		return list;
	}
	
	public ArrayList<String> mdbSelect(String dbname, ArrayList<String> selectList, ArrayList<Where> whereList) {
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		HashSet<String> select = new HashSet<String>();
		
		//without join
		if(whereList.size() == 0) {
			//there are not where statements
			ArrayList<String> indexNames = new ArrayList<String>();
			for(String selected : selectList) {
				String table = selected.substring(0, selected.indexOf('#'));
				String attr = selected.substring(selected.indexOf('#') + 1);
				String name = DBStructure.getIndexName(dbname, table, attr);
				String pk = DBStructure.getTablePK(dbname, table);
				if(name.equals("#NO_INDEX#") || attr.equals(pk)) {
					break;
				}
				indexNames.add(name);
			}
			if(indexNames.size() == selectList.size()) {
				
				Hashtable<String, String> row = new Hashtable<String, String>();
				for(int i = 0; i < indexNames.size(); i++) {
					MongoCollection<Document> ind = database.getCollection(indexNames.get(i));
					String table = selectList.get(i).substring(0, selectList.get(i).indexOf('#'));
					String attr = selectList.get(i).substring(selectList.get(i).indexOf('#') + 1);
					String type = DBStructure.getAttributeType(dbname, table, attr);
					FindIterable<Document> docs = ind.find();
					for(Document doc : docs) {
						String[] data = doc.get("ID").toString().split("#");
						for(String key : data) {
							if(row.containsKey(key)) {
								String rowData = row.get(key);
								row.remove(key);
								rowData += table + "#" + attr + "#" + type + "#" + doc.get(attr).toString() + "#";
								row.put(key, rowData);
							} else {
								String pk = DBStructure.getTablePK(dbname, indexNames.get(i));
								String rowData = "";
								if(selectList.contains(indexNames.get(i) + "#" + pk)) {
									rowData += table + "#" + pk + "#" + DBStructure.getAttributeType(dbname, indexNames.get(i), pk) + "#" + key + "#";
								}
								rowData += table + "#" + attr + "#" + type + "#" + doc.get(attr).toString() + "#";
								row.put(key, rowData);
							}
						}
					}
				}
				Set<String> keys = row.keySet();
				for(String k : keys) {
					select.add(row.get(k));
				}
				
			} else {
				String t = selectList.get(0).substring(0, selectList.get(0).indexOf("#"));
				MongoCollection<Document> table = database.getCollection(t);
				
				FindIterable<Document> docs = table.find();
				for(Document doc : docs) {
					String[] data = doc.get("#data#").toString().split("#");
					String sel = "";
					for(int i = 0; i < data.length; i += 3) {
						
						if(selectList.contains(t + "#" + data[i])) {
							sel += table + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2]; 
						}
					}
					String pk = DBStructure.getTablePK(dbname, t);
					if(selectList.contains(t + "#" + pk)) {
						sel = table + "#" + pk + "#" + DBStructure.getAttributeType(dbname, t, pk) + "#" + doc.get(pk) + "#" + sel;
					}
					select.add(sel);
				}
			}
		} else {
			//there are where statements
			Hashtable<String, String> all = new Hashtable<String, String>();
			String t = selectList.get(0).substring(0, selectList.get(0).indexOf('#'));
			String pk = DBStructure.getTablePK(dbname, t);
			MongoCollection<Document> table = database.getCollection(t);
			
			FindIterable<Document> docs = table.find();
			for(Document doc : docs) {
				String id = doc.get(pk).toString();
				String d = doc.get("#data#").toString();
				all.put(id, d);
			}
			
			//my favourite part, Where, where, where????
			for(Where where : whereList) {
				String name = where.getField1().substring(where.getField1().indexOf('#') + 1);
				String indexName = DBStructure.getIndexName(dbname, t, name);
				if(!indexName.equals("#NO_INDEX#")) {
					MongoCollection<Document> index = database.getCollection(indexName);
					FindIterable<Document> ind = index.find();
					for(Document doc : docs) {
						String val = doc.get(name).toString();
						String type = DBStructure.getAttributeType(dbname, t, name);
						if(!mdbCompare(type, val, where.getField2(), where.getOp())) {
							String[] data = doc.get("ID").toString().split("#"); //HIBA
							for(String lost : data) {
								all.remove(lost);
							}
						}
					}
				} else {
					
					Set<String> keys = all.keySet();
					for(String k : keys) {
						String[] data = all.get(k).split("#");
						for(int i = 0; i < data.length; i+=3) {
							if(data[i].equals(name)) {
								if(!mdbCompare(data[i + 1], data[i + 2], where.getField2(), where.getOp())) {
									all.remove(k);
								}
								break;
							}
						}
					}
				}
			}
			
			Set<String> keys = all.keySet();
			for(String k : keys) {
				String rowData = "";
				if(selectList.contains(t + "#" + pk)) {
					rowData += t + "#" + pk + "#" + DBStructure.getAttributeType(dbname, t, pk) + "#" + k + "#";
				}
				String[] data = all.get(k).split("#");
				for(int i = 0; i < data.length; i += 3) {
					if(selectList.contains(t + "#" + data[i])) {
						rowData += t + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2] + "#";
					}
				}
				select.add(rowData);
			}
		}
		
		ArrayList<String> elements = new ArrayList<String>();
		Iterator<String> i = select.iterator(); 
        while (i.hasNext()) {
        	elements.add(i.next());
        } 
		return elements;
	}
	
	private boolean mdbCompare(String type, String value1, String value2, Operator operator) {
		
		switch(type) {
		
		case "int":
			int v1i = (int)Double.parseDouble(value1);
			int v2i = Integer.parseInt(value2);
			switch(operator) {
			
			case EQ:
				return v1i == v2i;
			case GT:
				return v1i > v2i;
			case GTE:
				return v1i >= v2i;
			case LT:
				return v1i < v2i;
			case LTE:
				return v1i <= v2i;
			case NEQ:
				return v1i != v2i;
			}
			break;
			
		case "float":
			float v1f = Float.parseFloat(value1);
			float v2f = Float.parseFloat(value2);
			switch(operator) {
			
			case EQ:
				return v1f == v2f;
			case GT:
				return v1f > v2f;
			case GTE:
				return v1f >= v2f;
			case LT:
				return v1f < v2f;
			case LTE:
				return v1f <= v2f;
			case NEQ:
				return v1f != v2f;
			}
			break;
			
		case "varchar":
			switch(operator) {
			
			case EQ:
				return value1.equals(value2);
			case NEQ:
				return !value1.equals(value2);
			}
			break;
		
		case "bit":
			switch(operator) {
			
			case EQ:
				if(value1.equals(value2)) {
					return true;
				}
				if(value1.equals("1") && value2.equals("true")) {
					return true;
				}
				if(value2.equals("1") && value1.equals("true")) {
					return true;
				}
				if(value1.equals("0") && value2.equals("false")) {
					return true;
				}
				if(value2.equals("0") && value1.equals("false")) {
					return true;
				}
				return false;
			case NEQ:
				if(value1.equals("1") && value2.equals("false")) {
					return true;
				}
				if(value1.equals("1") && value2.equals("0")) {
					return true;
				}
				if(value2.equals("1") && value1.equals("false")) {
					return true;
				}
				if(value2.equals("1") && value1.equals("0")) {
					return true;
				}
				if(value1.equals("0") && value2.equals("true")) {
					return true;
				}
				if(value2.equals("0") && value1.equals("true")) {
					return true;
				}
				return false;
			}
			break;
			
		case "date":
			value1 = value1.toLowerCase();
			value2 = value2.toLowerCase();
			char delimiter = ' ';
			if(value1.contains("/")) {
				delimiter = '/';
			} else {
				if(value1.contains(".")) {
					delimiter = '.';
				} else {
					delimiter = '-';
				}
			}
			int y1 = 0, m1 = 0, d1 = 0;
			y1 = Integer.parseInt(value1.substring(0,value1.indexOf(delimiter)));
			try {
				m1 = Integer.parseInt(value1.substring(value1.indexOf(delimiter) + 1, value1.lastIndexOf(delimiter)));
			} catch(NumberFormatException e) {
				String m = value1.substring(value1.indexOf(delimiter) + 1, value1.lastIndexOf(delimiter));
				switch(m) {
				
				case "january":
					m1 = 1;
					break;
				case "february":
					m1 = 2;
					break;
				case "march":
					m1 = 3;
					break;
				case "april":
					m1 = 4;
					break;
				case "may":
					m1 = 5;
					break;
				case "june":
					m1 = 6;
					break;
				case "july":
					m1 = 7;
					break;
				case "august":
					m1 = 8;
					break;
				case "september":
					m1 = 9;
					break;
				case "october":
					m1 = 10;
					break;
				case "november":
					m1 = 11;
					break;
				case "december":
					m1 = 12;
					break;
				}
						
			}
			d1 = Integer.parseInt(value1.substring(value1.lastIndexOf(delimiter) + 1));
			
			if(value2.contains("/")) {
				delimiter = '/';
			} else {
				if(value2.contains(".")) {
					delimiter = '.';
				} else {
					delimiter = '-';
				}
			}
			int y2 = 0, m2 = 0, d2 = 0;
			y2 = Integer.parseInt(value2.substring(0,value2.indexOf(delimiter)));
			try {
				m2 = Integer.parseInt(value2.substring(value2.indexOf(delimiter) + 1, value2.lastIndexOf(delimiter)));
			} catch(NumberFormatException e) {
				String m = value1.substring(value2.indexOf(delimiter) + 1, value2.lastIndexOf(delimiter));
				switch(m) {
				
				case "january":
					m2 = 1;
					break;
				case "february":
					m2 = 2;
					break;
				case "march":
					m2 = 3;
					break;
				case "april":
					m2 = 4;
					break;
				case "may":
					m2 = 5;
					break;
				case "june":
					m2 = 6;
					break;
				case "july":
					m2 = 7;
					break;
				case "august":
					m2 = 8;
					break;
				case "september":
					m2 = 9;
					break;
				case "october":
					m2 = 10;
					break;
				case "november":
					m2 = 11;
					break;
				case "december":
					m2 = 12;
					break;
				}
						
			}
			d2 = Integer.parseInt(value2.substring(value2.lastIndexOf(delimiter) + 1));
			switch(operator) {
			
			case EQ:
				return y1 == y2 && m1 == m2 && d1 == d2;
			case GT:
				if(y1 > y2) {
					return true;
				}
				if(y1 < y2) {
					return false;
				}
				if(m1 > m2) {
					return true;
				}
				if(m1 < m2) {
					return false;
				}
				if(d1 > d2) {
					return true;
				}
				return false;
			case GTE:
				if(y1 == y2 && m1 == m2 && d1 == d2) {
					return true;
				}
				if(y1 > y2) {
					return true;
				}
				if(y1 < y2) {
					return false;
				}
				if(m1 > m2) {
					return true;
				}
				if(m1 < m2) {
					return false;
				}
				if(d1 > d2) {
					return true;
				}
				return false;
			case LT:
				if(y1 < y2) {
					return true;
				}
				if(y1 > y2) {
					return false;
				}
				if(m1 < m2) {
					return true;
				}
				if(m1 > m2) {
					return false;
				}
				if(d1 < d2) {
					return true;
				}
				return false;
			case LTE:
				if(y1 == y2 && m1 == m2 && d1 == d2) {
					return true;
				}
				if(y1 < y2) {
					return true;
				}
				if(y1 > y2) {
					return false;
				}
				if(m1 < m2) {
					return true;
				}
				if(m1 > m2) {
					return false;
				}
				if(d1 < d2) {
					return true;
				}
				return false;
			case NEQ:
				return y1 != y2 || m1 != m2 || d1 != d2;
			}
			break;
		}
		return false;
	}
	
}
