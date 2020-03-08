package com.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import message.Attribute;

// static class, the methods represent the Data Definition Language (DDL) of the system
public final class DDL {

	private DDL() {}
	
	// CREATE DATABASE DDL command, returns 0 if successful, -1 if error
	public static int createDatabase(String dbname) {
		
		File file = new File("databases//" + dbname + ".xml");
		if(file.exists()) {
			
			return -5;
		}
		try {
			File file = new File("databases//" + dbname + ".xml");
			if(file.exists()) {	
				return -5;
			}

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.newDocument();
			
			Element tables = document.createElement("Tables");
			document.appendChild(tables);
			
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            DOMSource domSource = new DOMSource(document);
            StreamResult streamResult = new StreamResult(new File("databases//" + dbname + ".xml"));
            
            transformer.transform(domSource, streamResult);
            
		} catch (ParserConfigurationException e) {

			e.printStackTrace();
			return -1;
		} catch (TransformerException a) {
			
			a.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	// DROP DATABASE DDL command, returns 0 if successful, -1 if database does not exists or -2 if
	// delete wasn't successful
	public static int dropDatabase(String dbname) {
		
		File file = new File("databases//" + dbname + ".xml");
		if(!file.exists()) {
			
			return -1;
		}
		if(file.delete()) {
			
			return 0;
		}
		return -2;
		
	}
	
	// DROP TABLE DDL command, returns 0 if successful, -1 if error or, -2 if table was not in the database,
	// -5 if there are tables pointing to this table with FK
	public static int dropTable(String dbname, String tbname) {
		
		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            
            NodeList references = document.getElementsByTagName("refTable");
            int rl = references.getLength();
            for(int i = 0; i < rl; i++) {
            	
            	if(references.item(i).getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element)references.item(i);
            		System.out.println(element.getTextContent());
            		if(element.getTextContent().equals(tbname)) {
            			
            			return -5;
            		}
            	}
            }
            
            NodeList nodeList = document.getElementsByTagName("Table");
            
            boolean found = false;
            int l = nodeList.getLength();
            
            for (int i = 0; i < l; i++) {
             
            	Node node = nodeList.item(i);
             
            	if(node == null) {
            		
            		break;
            	}
            	if (node.getNodeType() == Node.ELEMENT_NODE) {
                
            		Element element = (Element)node;
            		if(element.getAttribute("tableName").equals(tbname)) {
            			
            			NodeList indexes = element.getElementsByTagName("IndexFile");
            			for(int j = 0; j < indexes.getLength(); j++) {
            				
            				String filename = ((Element)indexes.item(j)).getAttribute("indexName");
            				File file = new File("indexes//" + filename);
            				file.delete();
            			}
            			element.getParentNode().removeChild(element);
            			found = true;
            		}
             
            	}
            }
            
            if(!found) {
            	
            	return -2;
            }
            
            removeEmptyText(document.getFirstChild());
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            DOMSource domSource = new DOMSource(document);
            StreamResult streamResult = new StreamResult(new File("databases//" + dbname + ".xml"));
            
            transformer.transform(domSource, streamResult);
            
		} catch (ParserConfigurationException | IOException | SAXException | TransformerException e) {

			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	// CREATE TABLE DDL command, returns 0 if successful, -1 if error, -5 if the number of primary keys is not 1
	// -10 if there are attributes with illegal types, -2 if a table with this name already
	// exists or -7 if a Foreign Key reference is not possible
	public static int createTable(String dbname, String tbname, ArrayList<Attribute> columns) {
		
		if(tableExists(dbname,tbname)) {
			
			return -2;
		}
		
		try {

			DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
	        DocumentBuilder documentBuilder;
			documentBuilder = documentFactory.newDocumentBuilder();
			
			Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
	        document.getDocumentElement().normalize();
            
            Node tables = document.getFirstChild();
            
            // creating the new table node
            
            Element table = document.createElement("Table");
            tables.appendChild(table);
            table.setAttribute("tableName", tbname);
            table.setAttribute("fileName", "");
            table.setAttribute("rowLength", "");
            Element structure = document.createElement("Structure");
            Element pk = document.createElement("primaryKey");
            Element fk = document.createElement("foreignKeys");
            Element indexf = document.createElement("Indexfiles");
            table.appendChild(structure);
            table.appendChild(pk);
            table.appendChild(fk);
            table.appendChild(indexf);
            
            int numberOfPrimaryKeys = 0;
            
            for(Attribute attr : columns) {
            	
            	//add attribute to Table Structure
            	Element at = document.createElement("Attribute");
            	at.setAttribute("attributeName", attr.getName());
            	if(!typeCheck(attr.getType())) {
            		
            		return -10;
            	}
            	if(!attr.getType().contains("varchar")) {
            		
                	at.setAttribute("type", attr.getType());
            	} else {

            		at.setAttribute("type", "varchar");
            		at.setAttribute("length", attr.getType().substring(7));
            	}
            	structure.appendChild(at);
            	
            	//check and add constraints
            	switch(attr.getConstraint()) {
            	
            		case PRIMARY_KEY:
            			if(++numberOfPrimaryKeys > 1) {
            				
            				return -5;
            			}
            			Element pkat = document.createElement("pkAttribute");
            			pkat.setTextContent(attr.getName());
            			pk.appendChild(pkat);
            			break;
            			
            		case FOREIGN_KEY:
            			if(!referenceAttributeExists(dbname,attr,document)) {
            				
            				return -7;
            			}
            			Element fkmain = document.createElement("foreignKey");
            			Element fkattr = document.createElement("fkAttribute");
            			fkattr.setTextContent(attr.getName());
            			fkmain.appendChild(fkattr);
            			Element ref = document.createElement("references");
            			Element refTable = document.createElement("refTable");
            			refTable.setTextContent(attr.getRefTable());
            			Element refAttr = document.createElement("refAttribute");
            			refAttr.setTextContent(attr.getRefAttr());
            			ref.appendChild(refTable);
            			ref.appendChild(refAttr);
            			fkmain.appendChild(ref);
            			fk.appendChild(fkmain);
            			break;
            			
            		case NONE:
            			break;
            	}
            	
            }
            
            if(numberOfPrimaryKeys != 1) {
            	
            	return -5;
            }
            
            removeEmptyText(document.getFirstChild());
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            DOMSource domSource = new DOMSource(document);
            StreamResult streamResult = new StreamResult(new File("databases//" + dbname + ".xml"));
            
            transformer.transform(domSource, streamResult);
            
		} catch (ParserConfigurationException | IOException | SAXException | TransformerException e) {

			e.printStackTrace();
			return -1;
		}
        
		return 0;
	}
	
	// Creates an empty index file of the given column in the indexes directory, if it does not exist
	// returns 0 if successful, -5 if index file on the given attribute already exists, -1 if error
	// or -4 if name contains '_. /\' characters 
	public static int createIndex(String dbname, String tbname, String column, String name) {
		
		if(name.contains("_") || name.contains(".") || name.contains("/") || name.contains(" ") || name.contains("\\")) {
				
			return -4;
		}
		try {

			DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
	        DocumentBuilder documentBuilder;
			documentBuilder = documentFactory.newDocumentBuilder();
			
			Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
	        document.getDocumentElement().normalize();
            
            NodeList tables = document.getFirstChild().getChildNodes();
            
            String ind = dbname + "_" + tbname + "_" + column;
            // check if indexfile already exists
			NodeList currentIndexes = document.getElementsByTagName("IndexFile");
			int pl = currentIndexes.getLength();
			for(int i = 0; i < pl; i++) {
				
				if(currentIndexes.item(i).getNodeType() == Node.ELEMENT_NODE) {
					
					Element index = (Element)currentIndexes.item(i);
					String indname = index.getAttribute("indexName");
					if(indname.substring(0,indname.lastIndexOf('_')).equals(ind)) {
						
						return -5;
					}
				}
			}
			
			ind += "_" + name + ".ind";
            
            int l = tables.getLength();
            
            for(int i = 0; i < l; i++) {
            	
            	if(tables.item(i).getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element)tables.item(i);
            		if(element.getAttribute("tableName").equals(tbname)) {
            			
            			NodeList tstruc = element.getChildNodes();
            			int tl = tstruc.getLength();
            			for(int j = 0; j < tl; j++) {
            				
            				if(tstruc.item(j).getNodeType() == Node.ELEMENT_NODE) {
            					
            					Element struc = (Element)tstruc.item(j);
            					if(struc.getNodeName().equals("Indexfiles")) {
            						
            						Element indexFile = document.createElement("IndexFile");
            						Element indexAttributes = document.createElement("IndexAttributes");
            						Element iAttr = document.createElement("IAttribute");
            						indexFile.appendChild(indexAttributes);
            						indexAttributes.appendChild(iAttr);
            						indexFile.setAttribute("indexName", ind);
            						indexFile.setAttribute("keyLength", "");
            						indexFile.setAttribute("indexType", "");
            						iAttr.setTextContent(column);
            						struc.appendChild(indexFile);
            						File indFile = new File("indexes//" + ind);
            						indFile.createNewFile();
            					}
            				}
            			}
            		}
            	}
            }
            
            removeEmptyText(document.getFirstChild());
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            DOMSource domSource = new DOMSource(document);
            StreamResult streamResult = new StreamResult(new File("databases//" + dbname + ".xml"));
            
            transformer.transform(domSource, streamResult);
            
		} catch (ParserConfigurationException | IOException | SAXException | TransformerException e) {

			e.printStackTrace();
			return -1;
		}
        
		return 0;
	}
	
	// Check whether a given type is correct (int, float, bit, date or varchar)
	private static boolean typeCheck(String type) {
		
		if(type.equals("int") || type.equals("bit") || type.equals("float") || type.equals("date")) {
			
			return true;
		}
		if(type.substring(0,7).equals("varchar")) {
			
			try{
				
				Integer.parseInt(type.substring(7));
				return true;
			} catch(NumberFormatException e) {
				
				return false;
			}
		}
		return false;
	}
	
	// Checks if a Foreign Key attribute referenced table and referenced attribute exists
	private static boolean referenceAttributeExists(String dbname, Attribute attr, Document document) {
		
		NodeList nodeList = document.getElementsByTagName("Table");
		
		int l = nodeList.getLength();
		
		for (int i = 0; i < l; i++) {
		 
			Node node = nodeList.item(i);
		 
			if(node == null) {
				
				break;
			}
			if (node.getNodeType() == Node.ELEMENT_NODE) {
		    
				Element element = (Element)node;
				if(element.getAttribute("tableName").equals(attr.getRefTable())) {
					
					NodeList pks = element.getElementsByTagName("pkAttribute");
					int pl = pks.getLength();
					boolean okAttr = false, okType = false;
					for(int j = 0; j < pl; j++) {
						
						if(pks.item(j).getNodeType() == Node.ELEMENT_NODE) {
							
							if(((Element)pks.item(j)).getTextContent().equals(attr.getRefAttr())) {
								
								okAttr = true;
								break;
							}
						}
					}
					if(!okAttr) {
						
						return false;
					}
					NodeList attributes = element.getElementsByTagName("Attribute");
					pl = attributes.getLength();
					for(int j = 0; j < pl; j++) {
						
						if(attributes.item(j).getNodeType() == Node.ELEMENT_NODE) {
							
							Element attrel = (Element)attributes.item(j);
							if(attrel.getAttribute("attributeName").equals(attr.getRefAttr())) {
								
								String type = attrel.getAttribute("type");
								if(type.equals("varchar")) {
									
									type += attrel.getAttribute("length");
								}
								if(type.equals(attr.getType())) {
									
									okType = true;
								}
								break;
							}
						}
					}
					if(okType) {
						
						return true;
					}
					return false;
				}
			}
		}
        
        return false;
	}
	
	
	// Checks if a table with a given name already exists in the database
	private static boolean tableExists(String dbname, String tbname) {
		
		try {

            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File("databases//" + dbname + ".xml"));
			
            document.getDocumentElement().normalize();
            
            NodeList nodeList = document.getElementsByTagName("Table");
            
            int l = nodeList.getLength();
            
            for (int i = 0; i < l; i++) {
             
            	Node node = nodeList.item(i);
             
            	if(node == null) {
            		
            		break;
            	}
            	if (node.getNodeType() == Node.ELEMENT_NODE) {
                
            		Element element = (Element)node;
            		if(element.getAttribute("tableName").equals(tbname)) {
            			
            			return true;
            		}
             
            	}
            }
            
            
		} catch (ParserConfigurationException | IOException | SAXException e) {

			e.printStackTrace();
			return false;
		}
		return false;
	}
	
	// removing the indentation(empty) nodes from the structure
	private static void removeEmptyText(Node node) {
	    
		Node child = node.getFirstChild();
	    while(child!=null) {
	    	
	        Node sibling = child.getNextSibling();
	        if(child.getNodeType()==Node.TEXT_NODE) {
	        	
	            if(child.getTextContent().trim().isEmpty()) {
	            	
	                node.removeChild(child);
	            }
	        } else {
	        	
	            removeEmptyText(child);
	        }
	        child = sibling;
	    }
	}
	
}

