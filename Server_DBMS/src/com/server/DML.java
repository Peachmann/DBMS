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
import mongo.MongoDBBridge;
import structure.DBStructure;

public final class DML {

	private DML() {}
	
	private static boolean typeCheck(String type) {
		if(type.equals("int") || type.equals("bit") || type.equals("float") || type.equals("date") || type.equals("varchar")) {
			return true;
		}
		return false;
	}
	
	private static boolean valueCheck(String type, String value) {
		
		switch(type) {
		
		case "int":
			try {
				Integer.parseInt(value);
			} catch(NumberFormatException e) {
				return false;
			}
			break;
		case "float":
			try {
				Float.parseFloat(value);
			} catch(NumberFormatException | NullPointerException e) {
				return false;
			}
			break;
		case "bit":
			if(!value.equals("1") && !value.equals("0") && !value.equals("true") && !value.equals("false")) {
				
				return false;
			}
			break;
		case "varchar":
			if(value.contains("#") || value.contains("\\")) {
				
				return false;
			}
			break;
		case "date":
			/*format : YearMonthDay*/
			char delimiter = 'd';
			if(value.contains(".")) {
				delimiter = '.';
			}
			if(value.contains("/")) {
				
				if(delimiter != 'd') {
					
					return false;
				}
				delimiter = '/';
			}
			if(value.contains("-")) {
				
				if(delimiter != 'd') {
					
					return false;
				}
				delimiter = '-';
			}
			int firstD = value.indexOf(delimiter), lastD = value.lastIndexOf(delimiter);
			if(firstD == lastD) {
				
				return false;
			}
			int year;
			try {
				
				year = Integer.parseInt(value.substring(0, firstD));
				if(year < 0) {
					
					return false;
				}
			} catch(NumberFormatException e) {
				
				return false;
			}
			String m = value.substring(firstD + 1,  lastD).toLowerCase();
			int month = 0;
			switch(m) {
			
			case "january":
				month = 1;
				break;
			case "february":
				month = 2;
				break;
			case "march":
				month = 3;
				break;
			case "april":
				month = 4;
				break;
			case "may":
				month = 5;
				break;
			case "june":
				month = 6;
				break;
			case "july":
				month = 7;
				break;
			case "august":
				month = 8;
				break;
			case "september":
				month = 9;
				break;
			case "october":
				month = 10;
				break;
			case "november":
				month = 11;
				break;
			case "december":
				month = 12;
				break;
			default:
				try{
					
					month = Integer.parseInt(m);
					if(month < 1 || month > 12 || m.length() > 2) {
						
						return false;
					}
				}catch(NumberFormatException e) {
					
					return false;
				}
				break;
			}
			int[] days = {31,28,31,30,31,30,31,31,30,31,30,31};
			if(leapYear(year)) {
				
				days[1]++;
			}
			
			try{
				
				int day = Integer.parseInt(value.substring(lastD + 1));
				if(0 < day || days[month - 1] < day) {
					
					return false;
				}
			}catch(NumberFormatException e) {
				
				return false;
			}
			
			break;
		}
		
		return true;
	}
	
	private static boolean leapYear(int year) {

	    if(year%400==0 || (year%4==0 && year%100!=0)) {
	        
	    	return true;
	    }
	    return false;
	}
	
	// Insert command, -1 type problems, -2 if at least one primary key value already exists in the database
	public static int insertValues(MongoDBBridge mongo, String dbname, String tbname, String len, String inserts, ArrayList<Attribute> values) {
		
		String pk = DBStructure.getTablePK(dbname,tbname);
		Integer tableLength = Integer.valueOf(len);
		Integer totalInserts = Integer.valueOf(inserts);
		
		values.remove(0);
		
		for(int i = 0; i < totalInserts; i++) {
			for(int j = 0; j < tableLength; j++) {
				
				Attribute aux = values.get(i * tableLength + j);
				
				if(typeCheck(aux.getType())) {
					if(!valueCheck(aux.getType(),aux.getValue())) {
						
						return -1;
					}	
				}
				else {
					return -1;
				}
				
				if(aux.getName().equals(pk)) {
					
					if(mongo.mdbKeyExists(dbname, tbname, aux.getValue())) {
						
						return -2;
					}
				}
				
			}
		}
		return 0;
	}
	
	//-5 if there are tables with FK that points to this table, 0 if it is okay
	public static int deleteValues(String dbname, String tbname) {
		
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
            
		} catch (ParserConfigurationException | IOException | SAXException e) {

			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
}
