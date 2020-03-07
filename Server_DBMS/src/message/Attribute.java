package message;

public class Attribute {

	private String name;
	private String type;
	private Constraints constraint;
	private String refTable;
	private String refAttr;
	
	public Attribute(String name, String type, Constraints constraint) {
		
		this.name = name;
		this.type = type;
		this.constraint = constraint;
	}
	
	public Attribute(String name, String type) {
		
		this.name = name;
		this.type = type;
		this.constraint = Constraints.NONE;
	}
	
	public String getName() {
		
		return this.name;
	}
	
	public String getType() {
		
		return this.type;
	}
	
	public Constraints getConstraint() {
		
		return this.constraint;
	}
	
	public String getRefTable() {
		
		return this.refTable;
	}
	
	public String getRefAttr() {
		
		return this.refAttr;
	}
	
	//foreign key reference set, returns false if the attribute is not a foreign key, true if it is successful
	public boolean setReference(String refTable, String refAttr) {
		
		if(this.constraint != Constraints.FOREIGN_KEY) {
			
			return false;
		}
		this.refTable = refTable;
		this.refAttr = refAttr;
		
		return true;
	}
}
