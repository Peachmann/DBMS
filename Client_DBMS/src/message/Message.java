package message;

import java.io.Serializable;
import java.util.ArrayList;

public class Message implements Serializable {

	private MessageType msType;
	private String dbname;
	private String tbname;
	private ArrayList<Attribute> columns;
	private String userGivenName;
	private String response;
	private ArrayList<String> resp;
	
	public ArrayList<String> getResp() {
		return resp;
	}
	public void setResp(ArrayList<String> resp) {
		this.resp = resp;
	}
	
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
	public void setResponse(String response) {
		
		this.response = response;
	}
	public String getResponse() {
		
		return this.response;
	}
}
