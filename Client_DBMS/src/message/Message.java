package message;

import java.util.ArrayList;

public class Message {

	private MessageType msType;
	private String dbname;
	private String tbname;
	private ArrayList<Attribute> columns;
	private String userGivenName;
	
	public MessageType getMsType() {
		
		return msType;
	}
	public void setMsType(MessageType msType) {
		
		this.msType = msType;
	}
	public String getDBname() {
		
		return dbname;
	}
	public void setDBname(String dbname) {
		
		this.dbname = dbname;
	}
	public String getTbname() {
		
		return tbname;
	}
	public void setTbname(String tbname) {
		
		this.tbname = tbname;
	}
	public ArrayList<Attribute> getColumns() {
		
		return columns;
	}
	public void setColumns(ArrayList<Attribute> columns) {
		
		this.columns = columns;
	}
	public String getUserGivenName() {
		
		return userGivenName;
	}
	public void setUserGivenName(String userGivenName) {
		
		this.userGivenName = userGivenName;
	}
	public void addAttribute(Attribute attr) {
		
		this.columns.add(attr);
	}
}
