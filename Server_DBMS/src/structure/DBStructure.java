package structure;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Hashtable;
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

import com.server.DDL;

import message.Attribute;
import message.Constraints;

// static class, methods give information about the databases and their tables
public final class DBStructure {
	
	public static String getTablePK(String dbname, String tbname) {
		
		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            DDL.removeEmptyText(document);
            
            NodeList pk = document.getElementsByTagName("pkAttribute");
            
            for(int i = 0; i < pk.getLength(); i++) {
            	
            	if(pk.item(i).getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element) pk.item(i);
            		if(((Element)element.getParentNode().getParentNode()).getAttribute("tableName").equals(tbname)) {
            			
            			return element.getTextContent();
            		}
            	}
            }
            
		} catch(ParserConfigurationException | IOException | SAXException e) {
			
			e.printStackTrace();
		}
		
		return "NO_PK_FOUND";
	}

	// gets all current database names
	public static List<String> getDatabases() {
		
		List<String> databases = new ArrayList<String>();
		
		try(Stream<Path> walk = Files.walk(Paths.get("databases//"))) {
			
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
            Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
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
	
	// get all columns of a specific table
	public static List<String> getColumns(String dbname, String tableName) {
		
		List<String> columns = new ArrayList<String>();
		
		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            
            NodeList nodeList = document.getElementsByTagName("Attribute");
            
            int l = nodeList.getLength();
            
            for(int i = 0; i < l; i++) {
            	
            	Element element = (Element)nodeList.item(i);
            	columns.add(element.getAttribute("attributeName"));
            }
            
		} catch(ParserConfigurationException | IOException | SAXException e) {
			
			e.printStackTrace();
		}
		
		return columns;
	}
	
	public static ArrayList<String> getUniques(String dbname, String tbname) {

		ArrayList<String> list = new ArrayList<String>();
		
		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            DDL.removeEmptyText(document);
            
            NodeList pk = document.getElementsByTagName("Attribute");
            
            for(int i = 0; i < pk.getLength(); i++) {
            	
            	if(pk.item(i).getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element) pk.item(i);
            		if(((Element)element.getParentNode().getParentNode()).getAttribute("tableName").equals(tbname) &&
            				element.getAttribute("isUnique").equals("true")) {
            			
            			list.add(element.getAttribute("attributeName"));
            		}
            	}
            }
            
		} catch(ParserConfigurationException | IOException | SAXException e) {
			
			e.printStackTrace();
		}
		
		list.add("NO_UNIQUE_FOUND");
		
		return list;
	}
	
	public static ArrayList<Attribute> getForeignKeys(String dbname, String tbname) {
		
		ArrayList<Attribute> list = new ArrayList<Attribute>();
		
		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            DDL.removeEmptyText(document);
            
            NodeList fk = document.getElementsByTagName("foreignKey");
            
            for(int i = 0; i < fk.getLength(); i++) {
            	
            	if(fk.item(i).getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element) fk.item(i);
            		if(((Element)element.getParentNode().getParentNode()).getAttribute("tableName").equals(tbname)) {
            			
            			String name = "";
            			String type = "";
            			String refTable = "";
            			String refAttribute = "";
            			NodeList e = element.getElementsByTagName("fkAttribute");
            			for(int j = 0; j < e.getLength(); j++) {
            				
            				if(e.item(j).getNodeType() == Node.ELEMENT_NODE) {
            					name = ((Element)e.item(j)).getTextContent();
            					break;
            				}
            			}
            			e = ((Element)(element.getParentNode().getParentNode())).getElementsByTagName("Attribute");
            			for(int j = 0; j < e.getLength(); j++) {
            				
            				if(e.item(j).getNodeType() == Node.ELEMENT_NODE && ((Element)e.item(j)).getAttribute("attributeName").equals(name)) {
            					type = ((Element)e.item(j)).getAttribute("type");
            					break;
            				}
            			}
            			Attribute fka = new Attribute(name,type,Constraints.FOREIGN_KEY);
            			e = element.getElementsByTagName("refTable");
            			for(int j = 0; j < e.getLength(); j++) {
            				
            				if(e.item(j).getNodeType() == Node.ELEMENT_NODE) {
            					refTable = ((Element)e.item(j)).getTextContent();
            					break;
            				}
            			}
            			e = element.getElementsByTagName("refAttribute");
            			for(int j = 0; j < e.getLength(); j++) {
            				
            				if(e.item(j).getNodeType() == Node.ELEMENT_NODE) {
            					refAttribute = ((Element)e.item(j)).getTextContent();
            					break;
            				}
            			}
            			fka.setReference(refTable, refAttribute);
            			list.add(fka);
            		}
            	}
            }
            
		} catch(ParserConfigurationException | IOException | SAXException e) {
			
			e.printStackTrace();
		}
		
		return list;
	}
	
	public static String getAttributeType(String dbname, String tbname, String attr) {

        try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));

            document.getDocumentElement().normalize();
            DDL.removeEmptyText(document);

            NodeList tb = document.getElementsByTagName("Table");

            for(int i = 0; i < tb.getLength(); i++) {
            	
            	if(tb.item(i).getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element)tb.item(i);
            		if(element.getAttribute("tableName").equals(tbname)) {
            			NodeList ns = element.getElementsByTagName("Attribute");
            			for(int j = 0; j < ns.getLength(); j++) {
            				if(ns.item(j).getNodeType() == Node.ELEMENT_NODE && ((Element)ns.item(j)).getAttribute("attributeName").equals(attr)) {
            					return ((Element)ns.item(j)).getAttribute("type");
            				}
            			}
            		}
            	}
            }

        } catch(ParserConfigurationException | IOException | SAXException e) {

            e.printStackTrace();
        }

        return "NO_COLUMN";
    }
	
	public static Hashtable<String, String> getIndexes(String dbname, String tbname) {
		
		Hashtable<String, String> indexes = new Hashtable<String, String>();
		
		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            DDL.removeEmptyText(document);
            
            NodeList tb = document.getElementsByTagName("Table");
            
            for(int i = 0; i < tb.getLength(); i++) {
            	
            	if(tb.item(i).getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element)tb.item(i);
            		if(element.getAttribute("tableName").equals(tbname)) {
            			NodeList ns = element.getElementsByTagName("IndexFile");
            			for(int j = 0; j < ns.getLength(); j++) {
            				if(ns.item(j).getNodeType() == Node.ELEMENT_NODE) {
            					Element ind = (Element)ns.item(j);
            					String name = ind.getAttribute("indexName");
            					name = name.substring(0,name.indexOf('.'));
            					NodeList at = ind.getElementsByTagName("IAttribute");
            					for(int l = 0; l < at.getLength(); l++) {
            						if(at.item(l).getNodeType() == Node.ELEMENT_NODE) {
            							
            							String attr = ((Element)at.item(l)).getTextContent();
            							indexes.put(name, attr);
            							break;
            						}
            					}
            				}
            			}
            		}
            	}
            }
            
		} catch(ParserConfigurationException | IOException | SAXException e) {
			
			e.printStackTrace();
		}
		
		return indexes;
	}
	
	public static String getIndexName(String dbname, String tbname, String column) {

		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            DDL.removeEmptyText(document);
            
            NodeList tb = document.getElementsByTagName("Table");
            
            for(int i = 0; i < tb.getLength(); i++) {
            	
            	if(tb.item(i).getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element)tb.item(i);
            		if(element.getAttribute("tableName").equals(tbname)) {
            			NodeList ns = element.getElementsByTagName("IndexFile");
            			for(int j = 0; j < ns.getLength(); j++) {
            				if(ns.item(j).getNodeType() == Node.ELEMENT_NODE) {
            					Element ind = (Element)ns.item(j);
            					String name = ind.getAttribute("indexName");
            					name = name.substring(0,name.indexOf('.'));
            					NodeList at = ind.getElementsByTagName("IAttribute");
            					for(int l = 0; l < at.getLength(); l++) {
            						if(at.item(l).getNodeType() == Node.ELEMENT_NODE) {
            							
            							String attr = ((Element)at.item(l)).getTextContent();
            							if(attr.equals(column)) {
            								
            								return name;
            							}
            							break;
            						}
            					}
            				}
            			}
            		}
            	}
            }
            
		} catch(ParserConfigurationException | IOException | SAXException e) {
			
			e.printStackTrace();
		}
		
		return "#NO_INDEX#";
	}
	
}
