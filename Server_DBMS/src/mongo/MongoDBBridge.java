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

import message.Aggregate;
import message.Attribute;
import message.JoinOn;
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
			if(!key.equals(pk)) {
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
				MongoCollection<Document> table = database.getCollection(tbname);
				
				FindIterable<Document> docs = table.find(new Document(pk, value));
				for(Document row : docs) {
					if(row.get(pk).toString().equals(value)) {
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
	
	private HashSet<String> mdbgetJoinedSet(String dbname, HashSet<String> joined,JoinOn join, boolean first) {
		
		HashSet<String> newJoined = new HashSet<String>();
		MongoDatabase database = mongoClient.getDatabase(dbname);
		String pk = DBStructure.getTablePK(dbname, join.getTable1());
		String pk2 = DBStructure.getTablePK(dbname, join.getTable2());
		String pkt = DBStructure.getAttributeType(dbname, join.getTable1(), pk);
		String pk2t = DBStructure.getAttributeType(dbname, join.getTable2(), pk2);
		
		join.setAttribute1(join.getAttribute1().substring(join.getAttribute1().indexOf("#") + 1));
		join.setAttribute2(join.getAttribute2().substring(join.getAttribute2().indexOf("#") + 1));
		
		if(first) {
			if(!DBStructure.getIndexName(dbname, join.getTable1(), join.getAttribute1()).equals("#NO_INDEX#") 
					&& !join.getAttribute1().equals(pk)) {
				String help = join.getTable1();
				join.setTable1(join.getTable2());
				join.setTable2(help);
				help = join.getAttribute1();
				join.setAttribute1(join.getAttribute2());
				join.setAttribute2(help);
				help = pk;
				pk = pk2;
				pk2 = help;
			}
			
			MongoCollection<Document> table = database.getCollection(join.getTable1());
			FindIterable<Document> docs = table.find();
			
			if(!DBStructure.getIndexName(dbname, join.getTable2(), join.getAttribute2()).equals("#NO_INDEX#") && !join.getAttribute2().equals(pk2)) {
				//INL
				for(Document doc : docs) {
					String t1p = join.getTable1() + "#" + pk + "#" + pkt + "#" + doc.get(pk).toString() + "#";
					String indexName = DBStructure.getIndexName(dbname, join.getTable2(), join.getAttribute2());
					MongoCollection<Document> index = database.getCollection(indexName);
					String pv = "";
					if(pk.equals(join.getAttribute1())) {
						pv = doc.get(pk).toString();
						String[] data = doc.get("#data#").toString().split("#");
						for(int i = 0; i < data.length; i += 3) {
							t1p += join.getTable1() + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2] + "#";
						}
					} else {
						String[] data = doc.get("#data#").toString().split("#");
						for(int i = 0; i < data.length; i += 3) {
							t1p += join.getTable1() + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2] + "#";
							if(data[i].equals(join.getAttribute1())) {
								pv = data[i + 2];
							}
						}
					}
					MongoCollection<Document> table2 = database.getCollection(join.getTable2());
					FindIterable<Document> connect = index.find(new Document(join.getAttribute2(), pv));
					for(Document con : connect) { // 1 match
						String[] ids = con.get("ID").toString().split("#");
						for(String id : ids) {
							FindIterable<Document> match = table2.find(new Document(pk2, id));
							for(Document row : match) { // 1 match
								String tp = new String(t1p);
								tp += join.getTable2() + "#" + pk2 + "#" + pk2t + "#" + row.get(pk2) + "#";
								String[] rowd = row.get("#data#").toString().split("#");
								for(int j = 0; j < rowd.length; j += 3) {
									tp += join.getTable2() + "#" + rowd[j] + "#" + rowd[j + 1] + "#" + rowd[j + 2] + "#";
								}
								newJoined.add(tp);
							}
						}
					}
				}
			} else {
				//NL
				MongoCollection<Document> table2 = database.getCollection(join.getTable2());
				FindIterable<Document> docs2 = table2.find();
				
				for(Document doc1 : docs) {
					String pv = "";
					String t1p = join.getTable1() + "#" + pk + "#" + pkt + "#" + doc1.get(pk).toString() + "#";
					
					if(pk.equals(join.getAttribute1())) {
						pv = doc1.get(pk).toString();
						String[] data = doc1.get("#data#").toString().split("#");
						for(int i = 0; i < data.length; i += 3) {
							t1p += join.getTable1() + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2] + "#";
						}
					} else {
						String[] data = doc1.get("#data#").toString().split("#");
						for(int i = 0; i < data.length; i += 3) {
							t1p += join.getTable1() + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2] + "#";
							if(data[i].equals(join.getAttribute1())) {
								pv = data[i + 2];
							}
						}
					}
					for(Document doc2 : docs2) {
						String pv2 = "";
						String t2p = join.getTable2() + "#" + pk2 + "#" + pk2t + "#" + doc2.get(pk2).toString() + "#";
						
						if(pk2.equals(join.getAttribute2())) {
							pv2 = doc2.get(pk2).toString();
							if(!pv.equals(pv2)) {
								continue;
							}
							String[] data = doc2.get("#data#").toString().split("#");
							for(int i = 0; i < data.length; i += 3) {
								t2p += join.getTable2() + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2] + "#";
							}
						} else {
							String[] data = doc2.get("#data#").toString().split("#");
							for(int i = 0; i < data.length; i += 3) {
								t1p += join.getTable2() + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2] + "#";
								if(data[i].equals(join.getAttribute2())) {
									pv2 = data[i + 2];
									if(!pv.equals(pv2)) {
										break;
									}
								}
							}
						}
						if(pv.equals(pv2)) {
							//System.out.println(pv + " " + pv2);
							//System.out.println(t2p + " " + t2p);
							newJoined.add(t1p + t2p);
						}
					}
				}
			}
		} else {
			
			if(!DBStructure.getIndexName(dbname, join.getTable2(), join.getAttribute2()).equals("#NO_INDEX#") && !pk2.equals(join.getAttribute2())) {
				//INL
				String indexName = DBStructure.getIndexName(dbname, join.getTable2(), join.getAttribute2());
				MongoCollection<Document> index = database.getCollection(indexName);
				MongoCollection<Document> table2 = database.getCollection(join.getTable2());
				
				Iterator<String> rec = joined.iterator();
				
				while(rec.hasNext()) {
					String row = rec.next();
					String[] data = row.split("#");
					for(int i = 0; i < data.length; i += 4) {
						if(data[i].equals(join.getTable1()) && data[i + 1].equals(join.getAttribute1())) {
							
							FindIterable<Document> cons = index.find(new Document(join.getAttribute2(), data[i + 3]));
							for(Document con : cons) { // 1 match
								String[] ids = con.get("ID").toString().split("#");
								for(String id : ids) {
									FindIterable<Document> rowt2 = table2.find(new Document(pk2, id));
									for(Document rowFin : rowt2) { // 1 match
										String concat = join.getTable2() + "#" + pk2 + "#" + pk2t + "#" + id + "#";
										String[] rowData = rowFin.get("#data#").toString().split("#");
										for(int j = 0; j < rowData.length; j += 3) {
											concat += join.getTable2() + "#" + rowData[j] + "#" + rowData[j + 1] + "#" + rowData[j + 2] + "#";
										}
										newJoined.add(row + concat);
									}
								}
							}
							break;
						}
					}
				}
			} else {
				//NL
				MongoCollection<Document> table2 = database.getCollection(join.getTable2());
				FindIterable<Document> table2Data = table2.find();
				
				Iterator<String> rec = joined.iterator();
				
				while(rec.hasNext()) {
					String row = rec.next();
					String[] data = row.split("#");
					String pv = "";
					for(int i = 0; i < data.length; i += 4) {
						if(data[i].equals(join.getTable1()) && data[i+1].equals(join.getAttribute1())) {
							pv = data[i+3];
							break;
						}
					}
					for(Document rowT2 : table2Data) {
						String pr = "";
						if(pk2.equals(join.getAttribute2())) {
							pr = rowT2.get(pk2).toString();
							if(!rowT2.get(pk2).toString().equals(pv)) {
								continue;
							}
						}
						String concat = join.getTable2() + "#" + pk2 + "#" + pk2t + "#" + rowT2.get(pk2) + "#";
						String[] rowData = rowT2.get("#data#").toString().split("#");
						for(int i = 0; i < rowData.length; i += 3) {
							concat += join.getTable2() + "#" + rowData[i] + "#" + rowData[i+1] + "#" + rowData[i+2] + "#";
							if(rowData[i].equals(join.getAttribute2())) {
								pr = rowData[i];
								if(!pr.equals(pv)) {
									break;
								}
							}
						}
						if(pv.equals(pr)) {
							newJoined.add(row + concat);
						}
					}
				}
			}
		}
		
		
		return newJoined;
	}
	
	public ArrayList<String> mdbSelect(String dbname, ArrayList<String> selectList, ArrayList<Where> whereList, ArrayList<JoinOn> joins, String groupBy, ArrayList<Aggregate> selag, ArrayList<Aggregate> having) {
		
		MongoDatabase database = mongoClient.getDatabase(dbname);
		HashSet<String> select = new HashSet<String>();
		
		if(groupBy.isEmpty()) {
			if(selag.size() == 0) {
				if(joins.size() == 0) {
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
										sel += t + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2] + "#"; 
									}
								}
								String pk = DBStructure.getTablePK(dbname, t);
								if(selectList.contains(t + "#" + pk)) {
									sel = t + "#" + pk + "#" + DBStructure.getAttributeType(dbname, t, pk) + "#" + doc.get(pk).toString() + "#" + sel + "#";
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
							d = pk + "#" + DBStructure.getAttributeType(dbname, t, pk) + "#" + id + "#" + d;
							all.put(id, d);
						}
						
						//my favourite part, Where, where, where????
						for(Where where : whereList) {
							String name = where.getField1().substring(where.getField1().indexOf('#') + 1);
							String indexName = DBStructure.getIndexName(dbname, t, name);
							String pk2 = DBStructure.getTablePK(dbname, t);
							if(!indexName.equals("#NO_INDEX#") && !name.equals(pk2)) {
								MongoCollection<Document> index = database.getCollection(indexName);
								FindIterable<Document> ind = index.find();
								for(Document doc : ind) {
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
								Hashtable<String, String> help = new Hashtable<String, String>();
								for(String k : keys) {
									//System.out.println(k + " " +all.get(k));
									String[] data = all.get(k).split("#");
									for(int i = 0; i < data.length; i+=3) {
										if(data[i].equals(name)) {
											if(mdbCompare(data[i + 1], data[i + 2], where.getField2(), where.getOp())) {
												help.put(k, all.get(k));
											}
											break;
										}
									}
								}
								all.clear();
								all = help;
							}
						} 
						
						Set<String> keys = all.keySet();
						for(String k : keys) {
							String rowData = "";
							/*if(selectList.contains(t + "#" + pk)) {
								rowData += t + "#" + pk + "#" + DBStructure.getAttributeType(dbname, t, pk) + "#" + k + "#";
							}*/
							String[] data = all.get(k).split("#");
							for(int i = 0; i < data.length; i += 3) {
								if(selectList.contains(t + "#" + data[i])) {
									rowData += t + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2] + "#";
								}
							}
							select.add(rowData);
						}
					}
				} else {
					HashSet<String> rows = this.mdbgetJoinedSet(dbname, null, joins.get(0), true);
					for(int i = 1; i < joins.size(); i++) {
						rows = this.mdbgetJoinedSet(dbname, rows, joins.get(i), false);
					}
					
					Iterator<String> rec = rows.iterator();
					
					while(rec.hasNext()) {
						String row = rec.next();
						String[] data = row.split("#");
						boolean match = true;
						for(Where where : whereList) { // Where field1 - Table#Attr
							String[] wd = where.getField1().split("#");
							for(int i = 0; i < data.length; i += 4) {
								if(data[i].equals(wd[0]) && data[i+1].equals(wd[1])) {
									if(!mdbCompare(data[i + 2], data[i + 3], where.getField2(), where.getOp())) {
										match = false;
										break;
									}
								}
							}
							if(!match) {
								break;
							}
						}
						if(match) {
							String projRow = "";
							for(int i = 0; i < data.length; i += 4) {
								if(selectList.contains(data[i] + "#" + data[i+1])) {
									projRow += data[i] + "#" + data[i+1] + "#" + data[i+2] + "#" + data[i+3] + "#";
								}
							}
							select.add(projRow);
						}
					}
				}
			} else {
				// no group by but has aggregate functions

				ArrayList<String> isAg = new ArrayList<String>();
				for(Aggregate ag : selag) {
					isAg.add(ag.getTablename() + "#" + ag.getColumnname());
				}

				Hashtable<String, GBcounts> gba = new Hashtable<String, GBcounts>();
				
				if(joins.size() == 0) {
					//without join
					if(whereList.size() == 0) {
						//there are not where statements
						String t = selag.get(0).getTablename();
						MongoCollection<Document> table = database.getCollection(t);
							
						FindIterable<Document> docs = table.find();
						for(Document doc : docs) {
							String[] data = doc.get("#data#").toString().split("#");
							String sel = "";
							for(int i = 0; i < data.length; i += 3) {
									
								if(isAg.contains(t + "#" + data[i])) {
									if(gba.containsKey(t + "#" + data[i])) {
										GBcounts gb = gba.get(t + "#" + data[i]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 2]));
										gb.sumAdd(Double.parseDouble(data[i + 2]));
										gba.remove(t + "#" + data[i]);
										gba.put(t + "#" + data[i], gb);
									} else {
										GBcounts gb = new GBcounts(t + "#" + data[i]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 2]));
										gb.sumAdd(Double.parseDouble(data[i + 2]));
										gba.put(t + "#" + data[i], gb);
									}
								}
							}
							String pk = DBStructure.getTablePK(dbname, t);
							if(isAg.contains(t + "#" + pk)) {
								if(gba.containsKey(t + "#" + pk)) {
									GBcounts gb = gba.get(t + "#" + pk);
									gb.add();
									gb.mmChange(Double.parseDouble(doc.get(pk).toString()));
									gb.sumAdd(Double.parseDouble(doc.get(pk).toString()));
									gba.remove(t + "#" + pk);
									gba.put(t + "#" + pk, gb);
								} else {
									GBcounts gb = new GBcounts(t + "#" + pk);
									gb.add();
									gb.mmChange(Double.parseDouble(doc.get(pk).toString()));
									gb.sumAdd(Double.parseDouble(doc.get(pk).toString()));
									gba.put(t + "#" + pk, gb);
								}
							}
						}
						
						for(Aggregate ag : selag) {
							String sel = "";
							GBcounts gb = gba.get(ag.getTablename() + "#" + ag.getColumnname());
							sel += ag.getTablename() + "#";
							double val = 0;
							switch(ag.getType()) {
							case COUNT:
								val = gb.getCount();
								sel += "Count(" + ag.getColumnname() + ")#float#" + gb.getCount() + "#"; 
								break;
							case AVG:
								val = (gb.getSum() / gb.getCount());
								sel += "Avg(" + ag.getColumnname() + ")#float#" +  val + "#";
								break;
							case MIN:
								val = gb.getMin();
								sel += "Min(" + ag.getColumnname() + ")#float#" + gb.getMin() + "#";
								break;
							case MAX:
								val = gb.getMax();
								sel += "Max(" + ag.getColumnname() + ")#float#" + gb.getMax() + "#";
								break;
							case SUM:
								val = gb.getSum();
								sel += "Sum(" + ag.getColumnname() + ")#float#" + gb.getSum() + "#";
								break;
							default:
								break;
							}
							boolean isGood = true;
							for(Aggregate agh : having) {
								if(agh.getTablename().equals(ag.getTablename()) && agh.getColumnname().equals(ag.getColumnname())) {
									if(!mdbCompare("float",val + "",agh.getComparevalue(),agh.getOp())) {
										isGood = false;
										break;
									}
									break;
								}
							}
							if(isGood) {
								select.add(sel);
							}
						}
						
						
					} else {
						
						//there are where statements
						Hashtable<String, String> all = new Hashtable<String, String>();
						String t = selag.get(0).getTablename();
						String pk = DBStructure.getTablePK(dbname, t);
						MongoCollection<Document> table = database.getCollection(t);
						
						FindIterable<Document> docs = table.find();
						for(Document doc : docs) {
							String id = doc.get(pk).toString();
							String d = doc.get("#data#").toString();
							d = pk + "#" + DBStructure.getAttributeType(dbname, t, pk) + "#" + id + "#" + d;
							all.put(id, d);
						}
						
						//my favourite part, Where, where, where????
						for(Where where : whereList) {
							String name = where.getField1().substring(where.getField1().indexOf('#') + 1);
							String indexName = DBStructure.getIndexName(dbname, t, name);
							String pk2 = DBStructure.getTablePK(dbname, t);
							if(!indexName.equals("#NO_INDEX#") && !name.equals(pk2)) {
								MongoCollection<Document> index = database.getCollection(indexName);
								FindIterable<Document> ind = index.find();
								for(Document doc : ind) {
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
								Hashtable<String, String> help = new Hashtable<String, String>();
								for(String k : keys) {
									//System.out.println(k + " " +all.get(k));
									String[] data = all.get(k).split("#");
									for(int i = 0; i < data.length; i+=3) {
										if(data[i].equals(name)) {
											if(mdbCompare(data[i + 1], data[i + 2], where.getField2(), where.getOp())) {
												help.put(k, all.get(k));
											}
											break;
										}
									}
								}
								all.clear();
								all = help;
							}
						} 
						//NANANNANANAN
						Set<String> keys = all.keySet();
						for(String k : keys) {
							String[] data = all.get(k).split("#");
							for(int i = 0; i < data.length; i += 3) {
								if(isAg.contains(t + "#" + data[i])) {
									if(gba.containsKey(t + "#" + data[i])) {
										GBcounts gb = gba.get(t + "#" + data[i]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 2]));
										gb.sumAdd(Double.parseDouble(data[i + 2]));
										gba.remove(t + "#" + data[i]);
										gba.put(t + "#" + data[i], gb);
									} else {
										GBcounts gb = new GBcounts(t + "#" + data[i]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 2]));
										gb.sumAdd(Double.parseDouble(data[i + 2]));
										gba.put(t + "#" + data[i], gb);
									}
								}
							}
						}
						
						for(Aggregate ag : selag) {
							String sel = "";
							GBcounts gb = gba.get(ag.getTablename() + "#" + ag.getColumnname());
							sel += ag.getTablename() + "#";
							double val = 0;
							switch(ag.getType()) {
							case COUNT:
								val = gb.getCount();
								sel += "Count(" + ag.getColumnname() + ")#float#" + gb.getCount() + "#"; 
								break;
							case AVG:
								val = (gb.getSum() / gb.getCount());
								sel += "Avg(" + ag.getColumnname() + ")#float#" +  val + "#";
								break;
							case MIN:
								val = gb.getMin();
								sel += "Min(" + ag.getColumnname() + ")#float#" + gb.getMin() + "#";
								break;
							case MAX:
								val = gb.getMax();
								sel += "Max(" + ag.getColumnname() + ")#float#" + gb.getMax() + "#";
								break;
							case SUM:
								val = gb.getSum();
								sel += "Sum(" + ag.getColumnname() + ")#float#" + gb.getSum() + "#";
								break;
							default:
								break;
							}
							boolean isGood = true;
							for(Aggregate agh : having) {
								if(agh.getTablename().equals(ag.getTablename()) && agh.getColumnname().equals(ag.getColumnname())) {
									if(!mdbCompare("float",val + "",agh.getComparevalue(),agh.getOp())) {
										isGood = false;
										break;
									}
									break;
								}
							}
							if(isGood) {
								select.add(sel);
							}
						}
					}
				} else {
					HashSet<String> rows = this.mdbgetJoinedSet(dbname, null, joins.get(0), true);
					for(int i = 1; i < joins.size(); i++) {
						rows = this.mdbgetJoinedSet(dbname, rows, joins.get(i), false);
					}
					
					Iterator<String> rec = rows.iterator();
					
					while(rec.hasNext()) {
						String row = rec.next();
						String[] data = row.split("#");
						boolean match = true;
						for(Where where : whereList) { // Where field1 - Table#Attr
							String[] wd = where.getField1().split("#");
							for(int i = 0; i < data.length; i += 4) {
								if(data[i].equals(wd[0]) && data[i+1].equals(wd[1])) {
									if(!mdbCompare(data[i + 2], data[i + 3], where.getField2(), where.getOp())) {
										match = false;
										break;
									}
								}
							}
							if(!match) {
								break;
							}
						}
						//NANANANANNANNANANNA
						if(match) {
							String projRow = "";
							for(int i = 0; i < data.length; i += 4) {
								if(isAg.contains(data[i] + "#" + data[i+1])) {
									if(gba.containsKey(data[i] + "#" + data[i+1])) {
										GBcounts gb = gba.get(data[i] + "#" + data[i+1]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 3]));
										gb.sumAdd(Double.parseDouble(data[i + 3]));
										gba.remove(data[i] + "#" + data[i+1]);
										gba.put(data[i] + "#" + data[i+1], gb);
									} else {
										GBcounts gb = new GBcounts(data[i] + "#" + data[i+1]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 3]));
										gb.sumAdd(Double.parseDouble(data[i + 3]));
										gba.put(data[i] + "#" + data[i+1], gb);
									}
								}
							}
						}
					}
					
					for(Aggregate ag : selag) {
						String sel = "";
						GBcounts gb = gba.get(ag.getTablename() + "#" + ag.getColumnname());
						sel += ag.getTablename() + "#";
						double val = 0;
						switch(ag.getType()) {
						case COUNT:
							val = gb.getCount();
							sel += "Count(" + ag.getColumnname() + ")#float#" + gb.getCount() + "#"; 
							break;
						case AVG:
							val = (gb.getSum() / gb.getCount());
							sel += "Avg(" + ag.getColumnname() + ")#float#" +  val + "#";
							break;
						case MIN:
							val = gb.getMin();
							sel += "Min(" + ag.getColumnname() + ")#float#" + gb.getMin() + "#";
							break;
						case MAX:
							val = gb.getMax();
							sel += "Max(" + ag.getColumnname() + ")#float#" + gb.getMax() + "#";
							break;
						case SUM:
							val = gb.getSum();
							sel += "Sum(" + ag.getColumnname() + ")#float#" + gb.getSum() + "#";
							break;
						default:
							break;
						}
						boolean isGood = true;
						for(Aggregate agh : having) {
							if(agh.getTablename().equals(ag.getTablename()) && agh.getColumnname().equals(ag.getColumnname())) {
								if(!mdbCompare("float",val + "",agh.getComparevalue(),agh.getOp())) {
									isGood = false;
									break;
								}
								break;
							}
						}
						if(isGood) {
							select.add(sel);
						}
					}
				}
			}
		} else {
			//with group by
			
			if(selag.size() == 0) {
				if(joins.size() == 0) {
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
										sel += t + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2]; 
									}
								}
								String pk = DBStructure.getTablePK(dbname, t);
								if(selectList.contains(t + "#" + pk)) {
									sel = t + "#" + pk + "#" + DBStructure.getAttributeType(dbname, t, pk) + "#" + doc.get(pk).toString() + "#" + sel;
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
							d = pk + "#" + DBStructure.getAttributeType(dbname, t, pk) + "#" + id + "#" + d;
							all.put(id, d);
						}
						
						//my favourite part, Where, where, where????
						for(Where where : whereList) {
							String name = where.getField1().substring(where.getField1().indexOf('#') + 1);
							String indexName = DBStructure.getIndexName(dbname, t, name);
							String pk2 = DBStructure.getTablePK(dbname, t);
							if(!indexName.equals("#NO_INDEX#") && !name.equals(pk2)) {
								MongoCollection<Document> index = database.getCollection(indexName);
								FindIterable<Document> ind = index.find();
								for(Document doc : ind) {
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
								Hashtable<String, String> help = new Hashtable<String, String>();
								for(String k : keys) {
									//System.out.println(k + " " +all.get(k));
									String[] data = all.get(k).split("#");
									for(int i = 0; i < data.length; i+=3) {
										if(data[i].equals(name)) {
											if(mdbCompare(data[i + 1], data[i + 2], where.getField2(), where.getOp())) {
												help.put(k, all.get(k));
											}
											break;
										}
									}
								}
								all.clear();
								all = help;
							}
						} 
						
						Set<String> keys = all.keySet();
						for(String k : keys) {
							String rowData = "";
							/*if(selectList.contains(t + "#" + pk)) {
								rowData += t + "#" + pk + "#" + DBStructure.getAttributeType(dbname, t, pk) + "#" + k + "#";
							}*/
							String[] data = all.get(k).split("#");
							for(int i = 0; i < data.length; i += 3) {
								if(selectList.contains(t + "#" + data[i])) {
									rowData += t + "#" + data[i] + "#" + data[i + 1] + "#" + data[i + 2] + "#";
								}
							}
							select.add(rowData);
						}
					}
				} else {
					HashSet<String> rows = this.mdbgetJoinedSet(dbname, null, joins.get(0), true);
					for(int i = 1; i < joins.size(); i++) {
						rows = this.mdbgetJoinedSet(dbname, rows, joins.get(i), false);
					}
					
					Iterator<String> rec = rows.iterator();
					
					while(rec.hasNext()) {
						String row = rec.next();
						String[] data = row.split("#");
						boolean match = true;
						for(Where where : whereList) { // Where field1 - Table#Attr
							String[] wd = where.getField1().split("#");
							for(int i = 0; i < data.length; i += 4) {
								if(data[i].equals(wd[0]) && data[i+1].equals(wd[1])) {
									if(!mdbCompare(data[i + 2], data[i + 3], where.getField2(), where.getOp())) {
										match = false;
										break;
									}
								}
							}
							if(!match) {
								break;
							}
						}
						if(match) {
							String projRow = "";
							for(int i = 0; i < data.length; i += 4) {
								if(selectList.contains(data[i] + "#" + data[i+1])) {
									projRow += data[i] + "#" + data[i+1] + "#" + data[i+2] + "#" + data[i+3] + "#";
								}
							}
							select.add(projRow);
						}
					}
				}
			} else {
				// no group by + aggregate functions
				
				ArrayList<String> isAg = new ArrayList<String>();
				for(Aggregate ag : selag) {
					isAg.add(ag.getTablename() + "#" + ag.getColumnname());
				}

				Hashtable<String, GBcounts> gba = new Hashtable<String, GBcounts>();
				ArrayList<String> groups = new ArrayList<String>();
				String group = groupBy.substring(groupBy.indexOf("#") + 1);
				if(joins.size() == 0) {
					//without join
					if(whereList.size() == 0) {
						//there are not where statements
						String t = selag.get(0).getTablename();
						MongoCollection<Document> table = database.getCollection(t);
							
						FindIterable<Document> docs = table.find();
						for(Document doc : docs) {
							String[] data = doc.get("#data#").toString().split("#");
							String gbval = "";
							for(int i = 0; i < data.length; i += 3) {
								if(data[i].equals(group)) {
									gbval = data[i+2];
									break;
								}
							}
							if(!groups.contains(gbval)) {
								groups.add(gbval);
							}
							String sel = "";
							for(int i = 0; i < data.length; i += 3) {
									
								if(isAg.contains(t + "#" + data[i])) {
									if(gba.containsKey(t + "#" + gbval + "#" + data[i])) {
										GBcounts gb = gba.get(t + "#" + gbval + "#" + data[i]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 2]));
										gb.sumAdd(Double.parseDouble(data[i + 2]));
										gba.remove(t + "#" + gbval + "#" + data[i]);
										gba.put(t + "#" + gbval + "#" + data[i], gb);
									} else {
										GBcounts gb = new GBcounts(t + "#" + gbval + "#" + data[i]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 2]));
										gb.sumAdd(Double.parseDouble(data[i + 2]));
										gba.put(t + "#" + gbval + "#" + data[i], gb);
									}
								}
							}
							String pk = DBStructure.getTablePK(dbname, t);
							if(isAg.contains(t + "#" + pk)) {
								if(gba.containsKey(t + "#" + gbval + "#" + pk)) {
									GBcounts gb = gba.get(t + "#" + gbval + "#" + pk);
									gb.add();
									gb.mmChange(Double.parseDouble(doc.get(pk).toString()));
									gb.sumAdd(Double.parseDouble(doc.get(pk).toString()));
									gba.remove(t + "#" + gbval + "#" + pk);
									gba.put(t + "#" + gbval + "#" + pk, gb);
								} else {
									GBcounts gb = new GBcounts(t + "#" + gbval + "#" + pk);
									gb.add();
									gb.mmChange(Double.parseDouble(doc.get(pk).toString()));
									gb.sumAdd(Double.parseDouble(doc.get(pk).toString()));
									gba.remove(t + "#" + gbval + "#" + pk);
									gba.put(t + "#" + gbval + "#" + pk, gb);
								}
							}
						}
						
						for(String gbv : groups) {
							String sel = "";
							boolean isGood = true;
							if(selectList.contains(groupBy)) {
								sel = groupBy + "#" + DBStructure.getAttributeType(dbname, groupBy.substring(0, groupBy.indexOf("#")), group)+ "#" + gbv + "#";
							}
							for(Aggregate ag : selag) {
								GBcounts gb = gba.get(ag.getTablename() + "#" + gbv + "#" + ag.getColumnname());
								sel += ag.getTablename() + "#";
								double val = 0;
								switch(ag.getType()) {
								case COUNT:
									val = gb.getCount();
									sel += "Count(" + ag.getColumnname() + ")#float#" + gb.getCount() + "#"; 
									break;
								case AVG:
									val = (gb.getSum() / gb.getCount());
									sel += "Avg(" + ag.getColumnname() + ")#float#" +  val + "#";
									break;
								case MIN:
									val = gb.getMin();
									sel += "Min(" + ag.getColumnname() + ")#float#" + gb.getMin() + "#";
									break;
								case MAX:
									val = gb.getMax();
									sel += "Max(" + ag.getColumnname() + ")#float#" + gb.getMax() + "#";
									break;
								case SUM:
									val = gb.getSum();
									sel += "Sum(" + ag.getColumnname() + ")#float#" + gb.getSum() + "#";
									break;
								default:
									break;
								}
								for(Aggregate agh : having) {
									if(agh.getTablename().equals(ag.getTablename()) && agh.getColumnname().equals(ag.getColumnname())) {
										if(!mdbCompare("float",val + "",agh.getComparevalue(),agh.getOp())) {
											isGood = false;
											break;
										}
										break;
									}
								}
							}
							if(isGood) {
								select.add(sel);
							}
						}
						
						
					} else {
						
						//there are where statements
						Hashtable<String, String> all = new Hashtable<String, String>();
						String t = selag.get(0).getTablename();
						String pk = DBStructure.getTablePK(dbname, t);
						MongoCollection<Document> table = database.getCollection(t);
						
						FindIterable<Document> docs = table.find();
						for(Document doc : docs) {
							String id = doc.get(pk).toString();
							String d = doc.get("#data#").toString();
							d = pk + "#" + DBStructure.getAttributeType(dbname, t, pk) + "#" + id + "#" + d;
							all.put(id, d);
						}
						
						//my favourite part, Where, where, where????
						for(Where where : whereList) {
							String name = where.getField1().substring(where.getField1().indexOf('#') + 1);
							String indexName = DBStructure.getIndexName(dbname, t, name);
							String pk2 = DBStructure.getTablePK(dbname, t);
							if(!indexName.equals("#NO_INDEX#") && !name.equals(pk2)) {
								MongoCollection<Document> index = database.getCollection(indexName);
								FindIterable<Document> ind = index.find();
								for(Document doc : ind) {
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
								Hashtable<String, String> help = new Hashtable<String, String>();
								for(String k : keys) {
									//System.out.println(k + " " +all.get(k));
									String[] data = all.get(k).split("#");
									for(int i = 0; i < data.length; i+=3) {
										if(data[i].equals(name)) {
											if(mdbCompare(data[i + 1], data[i + 2], where.getField2(), where.getOp())) {
												help.put(k, all.get(k));
											}
											break;
										}
									}
								}
								all.clear();
								all = help;
							}
						} 
						//NANANNANANAN
						Set<String> keys = all.keySet();
						for(String k : keys) {
							String[] data = all.get(k).split("#");
							String gbval = "";
							for(int i = 0; i < data.length; i += 3) {
								if(data[i].equals(group)) {
									gbval = data[i+2];
									break;
								}
							}
							if(!groups.contains(gbval)) {
								groups.add(gbval);
							} // HEREEEEEEEEEEEEEEEEEEEEE
							for(int i = 0; i < data.length; i += 3) {
								
								if(isAg.contains(t + "#" + data[i])) {
									if(gba.containsKey(t + "#" + gbval + "#" + data[i])) {
										GBcounts gb = gba.get(t + "#" + data[i]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 2]));
										gb.sumAdd(Double.parseDouble(data[i + 2]));
										gba.remove(t + "#" + gbval + "#" + data[i]);
										gba.put(t + "#" + gbval + "#" + data[i], gb);
									} else {
										GBcounts gb = new GBcounts(t + "#" + gbval + "#" + data[i]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 2]));
										gb.sumAdd(Double.parseDouble(data[i + 2]));
										gba.put(t + "#" + gbval + "#" + data[i], gb);
									}
								}
							}
						}
						
						for(String gbv : groups) {
							String sel = "";
							boolean isGood = true;
							if(selectList.contains(groupBy)) {
								sel = groupBy + "#" + DBStructure.getAttributeType(dbname, groupBy.substring(0, groupBy.indexOf("#")), group)+ "#" + gbv + "#";
							}
							for(Aggregate ag : selag) {
								GBcounts gb = gba.get(ag.getTablename() + "#" + gbv + "#" + ag.getColumnname());
								sel += ag.getTablename() + "#";
								double val = 0;
								switch(ag.getType()) {
								case COUNT:
									val = gb.getCount();
									sel += "Count(" + ag.getColumnname() + ")#float#" + gb.getCount() + "#"; 
									break;
								case AVG:
									val = (gb.getSum() / gb.getCount());
									sel += "Avg(" + ag.getColumnname() + ")#float#" +  val + "#";
									break;
								case MIN:
									val = gb.getMin();
									sel += "Min(" + ag.getColumnname() + ")#float#" + gb.getMin() + "#";
									break;
								case MAX:
									val = gb.getMax();
									sel += "Max(" + ag.getColumnname() + ")#float#" + gb.getMax() + "#";
									break;
								case SUM:
									val = gb.getSum();
									sel += "Sum(" + ag.getColumnname() + ")#float#" + gb.getSum() + "#";
									break;
								default:
									break;
								}
								for(Aggregate agh : having) {
									if(agh.getTablename().equals(ag.getTablename()) && agh.getColumnname().equals(ag.getColumnname())) {
										if(!mdbCompare("float",val + "",agh.getComparevalue(),agh.getOp())) {
											isGood = false;
											break;
										}
										break;
									}
								}
							}
							if(isGood) {
								select.add(sel);
							}
						}
					}
				} else {
					HashSet<String> rows = this.mdbgetJoinedSet(dbname, null, joins.get(0), true);
					for(int i = 1; i < joins.size(); i++) {
						rows = this.mdbgetJoinedSet(dbname, rows, joins.get(i), false);
					}
					
					Iterator<String> rec = rows.iterator();
					
					String groupt = groupBy.substring(0,groupBy.indexOf("#"));
					
					while(rec.hasNext()) {
						String row = rec.next();
						String[] data = row.split("#");
						boolean match = true;
						for(Where where : whereList) { // Where field1 - Table#Attr
							String[] wd = where.getField1().split("#");
							for(int i = 0; i < data.length; i += 4) {
								if(data[i].equals(wd[0]) && data[i+1].equals(wd[1])) {
									if(!mdbCompare(data[i + 2], data[i + 3], where.getField2(), where.getOp())) {
										match = false;
										break;
									}
								}
							}
							if(!match) {
								break;
							}
						}
						//NANANANANNANNANANNA
						if(match) {
							String gbval = "";
							for(int i = 0; i < data.length; i += 4) {
								if(data[i].equals(groupt) && data[i+1].equals(group)) {
									
									gbval = data[i+3];
									if(!groups.contains(gbval)) {
										groups.add(gbval);
									}
									break;
								}
							}
							
							for(int i = 0; i < data.length; i += 4) {
								if(isAg.contains(data[i] + "#" + data[i+1])) {
									if(gba.containsKey(data[i] + "#" + gbval + "#" + data[i+1])) {
										GBcounts gb = gba.get(data[i] + "#" + gbval + "#" + data[i+1]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 3]));
										gb.sumAdd(Double.parseDouble(data[i + 3]));
										gba.remove(data[i] + "#" + gbval + "#" + data[i+1]);
										gba.put(data[i] + "#" + gbval + "#" + data[i+1], gb);
									} else {
										GBcounts gb = new GBcounts(data[i] + "#" + gbval + "#" + data[i+1]);
										gb.add();
										gb.mmChange(Double.parseDouble(data[i + 3]));
										gb.sumAdd(Double.parseDouble(data[i + 3]));
										gba.remove(data[i] + "#" + gbval + "#" + data[i+1]);
										gba.put(data[i] + "#" + gbval + "#" + data[i+1], gb);
									}
								}
							}
						}
					}
					
					for(String gbv : groups) {
						String sel = "";
						boolean isGood = true;
						if(selectList.contains(groupBy)) {
							sel = groupBy + "#" + DBStructure.getAttributeType(dbname, groupt, group)+ "#" + gbv + "#";
						}
						for(Aggregate ag : selag) {
							GBcounts gb = gba.get(ag.getTablename() + "#" + gbv + "#" + ag.getColumnname());
							sel += ag.getTablename() + "#";
							double val = 0;
							switch(ag.getType()) {
							case COUNT:
								val = gb.getCount();
								sel += "Count(" + ag.getColumnname() + ")#float#" + gb.getCount() + "#"; 
								break;
							case AVG:
								val = (gb.getSum() / gb.getCount());
								sel += "Avg(" + ag.getColumnname() + ")#float#" +  val + "#";
								break;
							case MIN:
								val = gb.getMin();
								sel += "Min(" + ag.getColumnname() + ")#float#" + gb.getMin() + "#";
								break;
							case MAX:
								val = gb.getMax();
								sel += "Max(" + ag.getColumnname() + ")#float#" + gb.getMax() + "#";
								break;
							case SUM:
								val = gb.getSum();
								sel += "Sum(" + ag.getColumnname() + ")#float#" + gb.getSum() + "#";
								break;
							default:
								break;
							}
							for(Aggregate agh : selag) {
								System.out.println(agh.getColumnname() + " " + agh.getOp() + " " + agh.getType() + " " + agh.getComparevalue());
							}
							System.out.println("NOOOOOOOOOOOOOOOOOOOO");
							for(Aggregate agh : having) {
								System.out.println(agh.getColumnname() + " " + agh.getOp() + " " + agh.getType() + " " + agh.getComparevalue());
							}
							for(Aggregate agh : having) {
								System.out.println(agh.getColumnname() + " " + val + " " + agh.getComparevalue() + " " + agh.getIsSelect() + " " + agh.getOp() + " " + agh.getType());
								if(agh.getTablename().equals(ag.getTablename()) && agh.getColumnname().equals(ag.getColumnname())) {
									
									if(!mdbCompare("float",val + "",agh.getComparevalue(),agh.getOp())) {
										isGood = false;
										break;
									}
									break;
								}
							}
						}
						if(isGood) {
							select.add(sel);
						}
					}
				}
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
