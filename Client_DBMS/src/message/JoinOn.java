package message;

import java.io.Serializable;

public class JoinOn implements Serializable {

	private String table1;
	private String table2;
	private String attribute1;
	private String attribute2;
	
	public JoinOn() {
		
		table1 = "";
		table2 = "";
		attribute1 = "";
		attribute2 = "";
	}
	
	public JoinOn(String table1, String table2, String attribute1, String attribute2) {
		
		this.table1 = table1;
		this.table2 = table2;
		this.attribute1 = attribute1;
		this.attribute2 = attribute2;
	}

	public String getTable1() {
		return table1;
	}

	public void setTable1(String table1) {
		this.table1 = table1;
	}

	public String getTable2() {
		return table2;
	}

	public void setTable2(String table2) {
		this.table2 = table2;
	}

	public String getAttribute1() {
		return attribute1;
	}

	public void setAttribute1(String attribute1) {
		this.attribute1 = attribute1;
	}

	public String getAttribute2() {
		return attribute2;
	}

	public void setAttribute2(String attribute2) {
		this.attribute2 = attribute2;
	}
	
}
