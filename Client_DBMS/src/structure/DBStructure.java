package structure;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.client.db.Listener;

// static class, methods give information about the databases and their tables
public final class DBStructure {
	
	private static String dbPath;
	private static String indexPath;
	
	// returns the Primary Key of a given table
	public static List<String> getPrimaryKey(String dbname, String tbname) {
		
		List<String> pk = new ArrayList<String>();
		
		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File(dbPath + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            
            NodeList nodeList = document.getElementsByTagName("Table");
            
            int l = nodeList.getLength();
            
            for(int i = 0; i < l; i++) {
            	
            	if(nodeList.item(i).getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element)nodeList.item(i);
            		if(element.getAttribute("tableName").equals(tbname)) {
            			
            			NodeList primk = element.getElementsByTagName("pkAttribute");
            			NodeList attr = element.getElementsByTagName("Attribute");
            			
            			String pkname = "";
            			for(int j = 0; j < primk.getLength(); j++) {
            				
            				if(primk.item(j).getNodeType() == Node.ELEMENT_NODE) {
            					
            					pkname += ((Element)primk.item(j)).getTextContent();
            					break;
            				}
            			}
            			for(int j = 0; j < attr.getLength(); j++) {
            				
            				if(attr.item(j).getNodeType() == Node.ELEMENT_NODE) {
            					
            					Element attre = (Element)attr.item(j);
            					if(attre.getAttribute("attributeName").equals(pkname)) {
            						
            						pk.add(attre.getAttribute("type") + "#" + pkname);
            						break;
            					}
            				}
            			}
            			break;
            		}
            	}
            }
            
		} catch(ParserConfigurationException | IOException | SAXException e) {
			
			e.printStackTrace();
		}
		
		return pk;
	}
	
	// return attributes from the given table
	public static List<String> getAttributes(String dbname, String tbname) {
		
		List<String> attributes = new ArrayList<String>();
		
		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File(dbPath + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            
            NodeList nodeList = document.getElementsByTagName("Table");
            
            int l = nodeList.getLength();
            
            for(int i = 0; i < l; i++) {
            	
            	if(nodeList.item(i).getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element)nodeList.item(i);
            		if(element.getAttribute("tableName").equals(tbname)) {
            			
            			NodeList attr = element.getElementsByTagName("Attribute");
            			for(int j = 0; j < attr.getLength(); j++) {
            				
            				if(attr.item(j).getNodeType() == Node.ELEMENT_NODE) {
            					
            					Element attre = (Element)attr.item(j);
            					attributes.add(attre.getAttribute("type") + "#" + attre.getAttribute("attributeName"));
            				}
            			}
            			break;
            		}
            	}
            }
            
		} catch(ParserConfigurationException | IOException | SAXException e) {
			
			e.printStackTrace();
		}
		
		return attributes;
	}

	// gets all current database names
	public static List<String> getDatabases() {
		
		List<String> databases = new ArrayList<String>();
		
		try(Stream<Path> walk = Files.walk(Paths.get(dbPath))) {
			
			databases = walk.map(x -> x.toString())
					.filter(f -> f.endsWith(".xml"))
					.map(x -> x.substring(0,x.indexOf('.')).substring(x.lastIndexOf('\\') + 1))
					.collect(Collectors.toList());

		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		return databases;
	}
	
	// gets all names of the tables currently present in the specified database
	public static List<String> getTables(String dbname) {
		
		
		List<String> tables = new ArrayList<String>();
		
		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File(dbPath + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            
            NodeList nodeList = document.getElementsByTagName("Table");
            
            int l = nodeList.getLength();
            
            for(int i = 0; i < l; i++) {
            	
            	Element element = (Element)nodeList.item(i);
            	tables.add(element.getAttribute("tableName"));
            }
            
		} catch(ParserConfigurationException | IOException | SAXException e) {
			
			e.printStackTrace();
		}
		
		return tables;
	}
	
	// gets all the databases and their tables names, returns a list containing String lists
	// the 0. element of a list is the database name, the other elements are the table names
	public static List<ArrayList<String>> getAllDBTables() {
		
		List<ArrayList<String>> structure = new ArrayList<ArrayList<String>>();
		
		List<String> databases = getDatabases();
		
		for(String db : databases) {
			
			ArrayList<String> dbt = new ArrayList<String>();
			dbt.add(db);
			dbt.addAll(getTables(db));
			structure.add(dbt);
		}
		
		return structure;
	}

	public static void setDbPath(String dbPath) {
		DBStructure.dbPath = dbPath;
	}

	public static void setIndexPath(String indexPath) {
		DBStructure.indexPath = indexPath;
	}
	
}

