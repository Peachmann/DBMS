package message;

import java.io.Serializable;

public class Aggregate implements Serializable {

	private AggregateType type;
	private String tablename, columnname;
	Boolean isSelect = false;
	
	public AggregateType getType() {
		return type;
	}
	public void setType(AggregateType type) {
		this.type = type;
	}
	public String getTablename() {
		return tablename;
	}
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
	public String getColumnname() {
		return columnname;
	}
	public void setColumnname(String columnname) {
		this.columnname = columnname;
	}
	public Boolean getIsSelect() {
		return isSelect;
	}
	public void setIsSelect(Boolean isSelect) {
		this.isSelect = isSelect;
	}
	
	
}
