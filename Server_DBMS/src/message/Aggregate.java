package message;

import java.io.Serializable;

public class Aggregate implements Serializable {

	private AggregateType type;
	private Operator op;
	private String tablename, columnname, comparevalue;
	private Boolean isSelect = false;
	
	public Operator getOp() {
		return op;
	}
	public void setOp(Operator op) {
		this.op = op;
	}
	public String getComparevalue() {
		return comparevalue;
	}
	public void setComparevalue(String comparevalue) {
		this.comparevalue = comparevalue;
	}
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
