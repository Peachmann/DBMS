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
	private String groupBy = "";
	private ArrayList<String> resp, selectList;
	private ArrayList<Where> whereList;
	private ArrayList<JoinOn> joins;
	private ArrayList<Aggregate> selectAgg, havingAgg;
	
	public ArrayList<Aggregate> getSelectAgg() {
		return selectAgg;
	}
	public void setSelectAgg(ArrayList<Aggregate> selectAgg) {
		this.selectAgg = selectAgg;
	}
	public ArrayList<Aggregate> getHavingAgg() {
		return havingAgg;
	}
	public void setHavingAgg(ArrayList<Aggregate> havingAgg) {
		this.havingAgg = havingAgg;
	}
	public String getGroupBy() {
		return groupBy;
	}
	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}
	public ArrayList<String> getSelectList() {
		return selectList;
	}
	public void setSelectList(ArrayList<String> selectList) {
		this.selectList = selectList;
	}
	public ArrayList<Where> getWhereList() {
		return whereList;
	}
	public void setWhereList(ArrayList<Where> whereList) {
		this.whereList = whereList;
	}
	
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
	public ArrayList<JoinOn> getJoins() {
		
		return this.joins;
	}
	public void setJoins(ArrayList<JoinOn> joins) {
		this.joins = joins;
	}
}
