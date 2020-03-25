package com.server;

import java.util.ArrayList;

import message.Attribute;

public final class DML {

	private DML() {}
	
	private static boolean typeCheck(String type) {
		if(type.equals("int") || type.equals("bit") || type.equals("float") || type.equals("date") || type.equals("varchar")) {
			return true;
		}
		return false;
	}
	
	// Insert command
	public static int insertValues(String dbname, String tbname, String len, String inserts, ArrayList<Attribute> values) {
		Integer tableLength = Integer.valueOf(len);
		Integer totalInserts = Integer.valueOf(inserts);
		
		values.remove(0);
		
		for(int i = 0; i < totalInserts; i++) {
			for(int j = 0; j < tableLength; j++) {
				
				Attribute aux = values.get(i * tableLength + j);
				
				if(typeCheck(aux.getType())) {
					switch(aux.getType()) {
					case "int":
						Integer.parseInt(aux.getValue());
						break;
					case "bit":
						break;
					case "float":
						Float.parseFloat(aux.getValue());
						break;
					case "date":
						break;
					case "varchar":
						break;
					}		
				}
				else {
					return -1;
				}
				
			}
		}
		return 0;
	}
	
	public static int deleteValues() {
		return 0;
	}
	
}
